package imessage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"

	"github.com/urmzd/orchagent/flow/connectors/internal/httpjson"
)

// API is the slice of the BlueBubbles server REST API this connector uses:
// send a text, send a file, download a received file. A BlueBubbles server
// fronts one Mac's Messages account, so an API is one server.
type API interface {
	// SendText sends a text to a chat and returns the sent message's guid,
	// or "" when the server did not report one.
	SendText(ctx context.Context, m Text) (guid string, err error)
	// SendAttachment sends a file to a chat and returns the sent message's
	// guid, or "".
	SendAttachment(ctx context.Context, a Attachment) (guid string, err error)
	// DownloadAttachment opens a received attachment's content. The caller
	// closes it.
	DownloadAttachment(ctx context.Context, guid string) (io.ReadCloser, error)
}

// Send methods BlueBubbles offers. AppleScript works on any server; the
// Private API needs the server's helper bundle installed, and is what audio
// messages (Attachment.Audio) and some group features need.
const (
	MethodAppleScript = "apple-script"
	MethodPrivateAPI  = "private-api"
)

// Text is a text message to send.
type Text struct {
	// Chat is the chat guid: "iMessage;-;+15551234567".
	Chat    string
	Message string
	// Method is MethodAppleScript or MethodPrivateAPI. Empty means
	// MethodAppleScript.
	Method string
}

// Attachment is a file to send.
type Attachment struct {
	Chat     string
	Name     string
	MimeType string
	Data     []byte
	// Audio sends the file as an audio message (a voice note) rather than
	// as a file. BlueBubbles honours it with the Private API.
	Audio  bool
	Method string
}

// HTTPAPI is the real API, over net/http.
//
// BlueBubbles authenticates every call with the server password as a query
// parameter. The connector's errors redact query strings (see
// internal/httpjson), so the password does not reach a log through a failed
// call; a proxy in front of the server still sees it in its access log.
type HTTPAPI struct {
	client   *httpjson.Client
	password string
}

// NewHTTPAPI returns an API for the BlueBubbles server at base (its URL as
// the server shows it: "https://example.trycloudflare.com" or
// "http://mac.local:1234") with its server password.
func NewHTTPAPI(base, password string, httpClient *http.Client) *HTTPAPI {
	header := http.Header{}
	header.Set("Accept", "application/json")
	return &HTTPAPI{
		client:   &httpjson.Client{HTTP: httpClient, Base: strings.TrimRight(base, "/"), Header: header},
		password: password,
	}
}

var (
	_ API = (*HTTPAPI)(nil)
	_ API = (*Fake)(nil)
)

// path adds the password to an API path.
func (a *HTTPAPI) path(p string, q url.Values) string {
	if q == nil {
		q = url.Values{}
	}
	q.Set("password", a.password)
	return p + "?" + q.Encode()
}

// answer is the envelope every BlueBubbles call answers with.
type answer struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
	Error   any    `json:"error"`
	Data    struct {
		GUID string `json:"guid"`
	} `json:"data"`
}

func (r answer) check(what string) error {
	if r.Status != 0 && r.Status != http.StatusOK {
		return fmt.Errorf("imessage: %s: status %d: %s", what, r.Status, r.Message)
	}
	return nil
}

func method(m string) string {
	if m == "" {
		return MethodAppleScript
	}
	return m
}

// tempGUID is the client-side id BlueBubbles matches a send against.
func tempGUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "temp-" + hex.EncodeToString(b)
}

// SendText implements API: POST /api/v1/message/text.
func (a *HTTPAPI) SendText(ctx context.Context, m Text) (string, error) {
	req := map[string]any{
		"chatGuid": m.Chat,
		"tempGuid": tempGUID(),
		"message":  m.Message,
		"method":   method(m.Method),
	}
	var out answer
	if err := a.client.Do(ctx, http.MethodPost, a.path("/api/v1/message/text", nil), req, &out); err != nil {
		return "", fmt.Errorf("imessage: send text: %w", err)
	}
	if err := out.check("send text"); err != nil {
		return "", err
	}
	return out.Data.GUID, nil
}

// SendAttachment implements API: POST /api/v1/message/attachment, multipart.
func (a *HTTPAPI) SendAttachment(ctx context.Context, att Attachment) (string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	_ = w.WriteField("chatGuid", att.Chat)
	_ = w.WriteField("tempGuid", tempGUID())
	_ = w.WriteField("name", fileName(att.Name))
	_ = w.WriteField("method", method(att.Method))
	if att.Audio {
		_ = w.WriteField("isAudioMessage", "true")
	}
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="attachment"; filename=%q`, fileName(att.Name)))
	if att.MimeType != "" {
		h.Set("Content-Type", att.MimeType)
	}
	part, err := w.CreatePart(h)
	if err == nil {
		_, err = part.Write(att.Data)
	}
	if err == nil {
		err = w.Close()
	}
	if err != nil {
		return "", fmt.Errorf("imessage: send attachment: %w", err)
	}
	var out answer
	if err := a.client.Send(ctx, http.MethodPost, a.path("/api/v1/message/attachment", nil), w.FormDataContentType(), &buf, &out); err != nil {
		return "", fmt.Errorf("imessage: send attachment: %w", err)
	}
	if err := out.check("send attachment"); err != nil {
		return "", err
	}
	return out.Data.GUID, nil
}

// DownloadAttachment implements API: GET /api/v1/attachment/{guid}/download.
// The server converts an audio message from Core Audio Format to a common
// one when it is configured to, which is what a transcriber wants, so the
// original is not asked for.
func (a *HTTPAPI) DownloadAttachment(ctx context.Context, guid string) (io.ReadCloser, error) {
	body, _, err := a.client.Open(ctx, a.path("/api/v1/attachment/"+url.PathEscape(guid)+"/download", nil))
	if err != nil {
		return nil, fmt.Errorf("imessage: download attachment %s: %w", guid, err)
	}
	return body, nil
}

// fileName keeps a filename to characters that cannot break a multipart
// header.
func fileName(name string) string {
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == '"' || r == '\\' || r == '/' || r == 0x7f {
			return -1
		}
		return r
	}, name)
	if name == "" {
		return "attachment"
	}
	return name
}
