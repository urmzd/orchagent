package whatsapp

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"

	"github.com/urmzd/orchagent/flow/connectors/internal/httpjson"
)

// API is the slice of the WhatsApp Cloud API this connector uses: send a
// text, look up a received media object, upload a file, send an audio
// message. A handful of Graph API calls behind an interface, so everything
// above the wire is tested without a network or a Meta app.
//
// PhoneNumberID is on every send so that an implementation serving several
// business numbers can choose a token per number. HTTPAPI holds one token,
// which a system user token scoped to the WhatsApp Business Account covers.
type API interface {
	// SendText sends a text message and returns its id (wamid...).
	SendText(ctx context.Context, m Text) (id string, err error)
	// Media returns a received media object: its short-lived download URL,
	// its type and its size. GET /{media-id}.
	Media(ctx context.Context, id string) (Media, error)
	// UploadMedia uploads a file for a later send and returns its media id.
	// POST /{phone-number-id}/media.
	UploadMedia(ctx context.Context, u Upload) (id string, err error)
	// SendAudio sends an uploaded audio file as an audio message and returns
	// its id.
	SendAudio(ctx context.Context, m AudioMessage) (id string, err error)
}

// Text is a text message to send.
type Text struct {
	PhoneNumberID string
	// To is the recipient's WhatsApp id.
	To   string
	Body string
	// ReplyTo quotes the message with this id. Empty sends a plain message.
	ReplyTo string
}

// Media is a media object as GET /{media-id} describes it.
type Media struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	MimeType string `json:"mime_type"`
	SHA256   string `json:"sha256"`
	Size     int64  `json:"file_size"`
}

// Upload is a file to upload for sending.
type Upload struct {
	PhoneNumberID string
	// MimeType is the file's type. WhatsApp accepts audio/aac, audio/amr,
	// audio/mpeg, audio/mp4 and audio/ogg (Opus) for audio; only Ogg Opus is
	// shown to the recipient as a voice note.
	MimeType string
	Filename string
	Data     []byte
}

// AudioMessage sends an uploaded audio file.
type AudioMessage struct {
	PhoneNumberID string
	To            string
	MediaID       string
}

// GraphBaseURL is the Graph API root, pinned to a version so a change on
// Meta's side is a deliberate upgrade here.
const GraphBaseURL = "https://graph.facebook.com/v23.0"

// HTTPAPI is the real API, over net/http.
type HTTPAPI struct {
	client *httpjson.Client
}

// NewHTTPAPI returns an API bound to an access token: a system user token
// with whatsapp_business_messaging, or a temporary token from the app
// dashboard while developing.
func NewHTTPAPI(token string, httpClient *http.Client) *HTTPAPI {
	return NewHTTPAPIAt(GraphBaseURL, token, httpClient)
}

// NewHTTPAPIAt is NewHTTPAPI against another Graph API root: a recorded
// server in a test, or another API version.
func NewHTTPAPIAt(base, token string, httpClient *http.Client) *HTTPAPI {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)
	header.Set("Accept", "application/json")
	return &HTTPAPI{client: &httpjson.Client{HTTP: httpClient, Base: strings.TrimRight(base, "/"), Header: header}}
}

var (
	_ API = (*HTTPAPI)(nil)
	_ API = (*Fake)(nil)
)

// sent is the answer to POST /{phone-number-id}/messages.
type sent struct {
	Messages []struct {
		ID string `json:"id"`
	} `json:"messages"`
}

func (s sent) id(what string) (string, error) {
	if len(s.Messages) == 0 || s.Messages[0].ID == "" {
		return "", fmt.Errorf("whatsapp: %s: the answer carries no message id", what)
	}
	return s.Messages[0].ID, nil
}

// SendText implements API.
func (a *HTTPAPI) SendText(ctx context.Context, m Text) (string, error) {
	req := map[string]any{
		"messaging_product": "whatsapp",
		"recipient_type":    "individual",
		"to":                m.To,
		"type":              "text",
		"text":              map[string]any{"body": m.Body, "preview_url": false},
	}
	if m.ReplyTo != "" {
		req["context"] = map[string]string{"message_id": m.ReplyTo}
	}
	var out sent
	if err := a.client.Do(ctx, http.MethodPost, "/"+url.PathEscape(m.PhoneNumberID)+"/messages", req, &out); err != nil {
		return "", fmt.Errorf("whatsapp: send text: %w", err)
	}
	return out.id("send text")
}

// SendAudio implements API.
func (a *HTTPAPI) SendAudio(ctx context.Context, m AudioMessage) (string, error) {
	req := map[string]any{
		"messaging_product": "whatsapp",
		"recipient_type":    "individual",
		"to":                m.To,
		"type":              "audio",
		"audio":             map[string]string{"id": m.MediaID},
	}
	var out sent
	if err := a.client.Do(ctx, http.MethodPost, "/"+url.PathEscape(m.PhoneNumberID)+"/messages", req, &out); err != nil {
		return "", fmt.Errorf("whatsapp: send audio: %w", err)
	}
	return out.id("send audio")
}

// Media implements API.
func (a *HTTPAPI) Media(ctx context.Context, id string) (Media, error) {
	var out Media
	if err := a.client.Do(ctx, http.MethodGet, "/"+url.PathEscape(id), nil, &out); err != nil {
		return Media{}, fmt.Errorf("whatsapp: media %s: %w", id, err)
	}
	if out.URL == "" {
		return Media{}, fmt.Errorf("whatsapp: media %s has no download url", id)
	}
	return out, nil
}

// UploadMedia implements API.
func (a *HTTPAPI) UploadMedia(ctx context.Context, u Upload) (string, error) {
	body, contentType, err := multipartUpload(u)
	if err != nil {
		return "", err
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := a.client.Send(ctx, http.MethodPost, "/"+url.PathEscape(u.PhoneNumberID)+"/media", contentType, body, &out); err != nil {
		return "", fmt.Errorf("whatsapp: upload media: %w", err)
	}
	if out.ID == "" {
		return "", fmt.Errorf("whatsapp: upload media: the answer carries no media id")
	}
	return out.ID, nil
}

func multipartUpload(u Upload) (*bytes.Buffer, string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	_ = w.WriteField("messaging_product", "whatsapp")
	_ = w.WriteField("type", u.MimeType)
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, uploadName(u.Filename)))
	h.Set("Content-Type", u.MimeType)
	part, err := w.CreatePart(h)
	if err != nil {
		return nil, "", fmt.Errorf("whatsapp: upload media: %w", err)
	}
	if _, err := part.Write(u.Data); err != nil {
		return nil, "", fmt.Errorf("whatsapp: upload media: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, "", fmt.Errorf("whatsapp: upload media: %w", err)
	}
	return &buf, w.FormDataContentType(), nil
}

// uploadName keeps a filename to characters that cannot break the
// Content-Disposition header.
func uploadName(name string) string {
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == '"' || r == '\\' || r == 0x7f {
			return -1
		}
		return r
	}, name)
	if name == "" {
		return "audio"
	}
	return name
}
