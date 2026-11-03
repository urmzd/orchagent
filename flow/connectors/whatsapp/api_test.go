package whatsapp_test

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/urmzd/orchagent/flow/connectors/whatsapp"
)

// recorded is one request the Graph API stand-in received.
type recorded struct {
	method, path, auth, contentType string
	body                            []byte
}

func graph(t *testing.T, answer string) (*whatsapp.HTTPAPI, *[]recorded) {
	t.Helper()
	var got []recorded
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = append(got, recorded{r.Method, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Content-Type"), b})
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, answer)
	}))
	t.Cleanup(srv.Close)
	return whatsapp.NewHTTPAPIAt(srv.URL+"/v23.0", "EAAG-token", srv.Client()), &got
}

func TestSendTextPostsACloudAPITextMessage(t *testing.T) {
	api, got := graph(t, `{"messaging_product":"whatsapp","contacts":[{"input":"15551234567","wa_id":"15551234567"}],"messages":[{"id":"wamid.OUT"}]}`)
	id, err := api.SendText(context.Background(), whatsapp.Text{PhoneNumberID: number, To: person, Body: "hi", ReplyTo: "wamid.IN"})
	if err != nil || id != "wamid.OUT" {
		t.Fatalf("SendText = %q, %v", id, err)
	}
	r := (*got)[0]
	if r.method != http.MethodPost || r.path != "/v23.0/"+number+"/messages" || r.auth != "Bearer EAAG-token" {
		t.Fatalf("request %+v", r)
	}
	var body map[string]any
	_ = json.Unmarshal(r.body, &body)
	if body["messaging_product"] != "whatsapp" || body["to"] != person || body["type"] != "text" ||
		body["text"].(map[string]any)["body"] != "hi" || body["context"].(map[string]any)["message_id"] != "wamid.IN" {
		t.Fatalf("body %s", r.body)
	}
}

func TestAnAnswerWithNoMessageIDIsAnError(t *testing.T) {
	api, _ := graph(t, `{"messages":[]}`)
	if _, err := api.SendText(context.Background(), whatsapp.Text{PhoneNumberID: number, To: person, Body: "hi"}); err == nil {
		t.Fatal("SendText succeeded without a message id")
	}
}

func TestMediaLooksTheObjectUpByID(t *testing.T) {
	api, got := graph(t, `{"url":"https://lookaside.fbsbx.com/whatsapp_business/attachments/?mid=1","mime_type":"audio/ogg","sha256":"abc","file_size":42,"id":"MEDIA1","messaging_product":"whatsapp"}`)
	m, err := api.Media(context.Background(), "MEDIA1")
	if err != nil || m.Size != 42 || m.MimeType != "audio/ogg" || !strings.HasPrefix(m.URL, "https://lookaside.fbsbx.com/") {
		t.Fatalf("Media = %+v, %v", m, err)
	}
	if r := (*got)[0]; r.method != http.MethodGet || r.path != "/v23.0/MEDIA1" {
		t.Fatalf("request %+v", r)
	}
}

func TestUploadMediaSendsAMultipartFileThenSendAudioRefersToIt(t *testing.T) {
	api, got := graph(t, `{"id":"MEDIA9","messages":[{"id":"wamid.AUDIO"}]}`)
	id, err := api.UploadMedia(context.Background(), whatsapp.Upload{PhoneNumberID: number, MimeType: "audio/ogg", Filename: "re\"ply.ogg", Data: []byte(oggOpus)})
	if err != nil || id != "MEDIA9" {
		t.Fatalf("UploadMedia = %q, %v", id, err)
	}
	r := (*got)[0]
	if r.path != "/v23.0/"+number+"/media" {
		t.Fatalf("path %s", r.path)
	}
	_, params, err := mime.ParseMediaType(r.contentType)
	if err != nil {
		t.Fatal(err)
	}
	form, err := multipart.NewReader(strings.NewReader(string(r.body)), params["boundary"]).ReadForm(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if form.Value["messaging_product"][0] != "whatsapp" || form.Value["type"][0] != "audio/ogg" || len(form.File["file"]) != 1 {
		t.Fatalf("form %+v", form)
	}
	if f := form.File["file"][0]; f.Filename != "reply.ogg" || f.Size != int64(len(oggOpus)) {
		t.Fatalf("file %+v", f)
	}

	msg, err := api.SendAudio(context.Background(), whatsapp.AudioMessage{PhoneNumberID: number, To: person, MediaID: id})
	if err != nil || msg != "wamid.AUDIO" {
		t.Fatalf("SendAudio = %q, %v", msg, err)
	}
	var body map[string]any
	_ = json.Unmarshal((*got)[1].body, &body)
	if body["type"] != "audio" || body["audio"].(map[string]any)["id"] != "MEDIA9" {
		t.Fatalf("body %s", (*got)[1].body)
	}
}

func TestAGraphErrorCarriesMetasReason(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"message":"Re-engagement message","code":131047}}`)
	}))
	t.Cleanup(srv.Close)
	api := whatsapp.NewHTTPAPIAt(srv.URL, "t", srv.Client())
	_, err := api.SendText(context.Background(), whatsapp.Text{PhoneNumberID: number, To: person, Body: "hi"})
	if err == nil || !strings.Contains(err.Error(), "131047") {
		t.Fatalf("err = %v", err)
	}
}
