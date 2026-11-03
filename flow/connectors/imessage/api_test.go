package imessage_test

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

	"github.com/urmzd/orchagent/flow/connectors/imessage"
)

type call struct {
	method, path, password, contentType string
	body                                []byte
}

func bluebubbles(t *testing.T, status int, answer string) (*imessage.HTTPAPI, *[]call) {
	t.Helper()
	var got []call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = append(got, call{r.Method, r.URL.Path, r.URL.Query().Get("password"), r.Header.Get("Content-Type"), b})
		w.WriteHeader(status)
		_, _ = io.WriteString(w, answer)
	}))
	t.Cleanup(srv.Close)
	return imessage.NewHTTPAPI(srv.URL+"/", "server-pass", srv.Client()), &got
}

func TestSendTextPostsTheChatAndMessageWithThePassword(t *testing.T) {
	api, got := bluebubbles(t, 200, `{"status":200,"message":"Message sent!","data":{"guid":"SENT-1","text":"hi"}}`)
	guid, err := api.SendText(context.Background(), imessage.Text{Chat: dm, Message: "hi"})
	if err != nil || guid != "SENT-1" {
		t.Fatalf("SendText = %q, %v", guid, err)
	}
	c := (*got)[0]
	if c.method != http.MethodPost || c.path != "/api/v1/message/text" || c.password != "server-pass" {
		t.Fatalf("call %+v", c)
	}
	var body map[string]string
	_ = json.Unmarshal(c.body, &body)
	if body["chatGuid"] != dm || body["message"] != "hi" || body["method"] != imessage.MethodAppleScript || !strings.HasPrefix(body["tempGuid"], "temp-") {
		t.Fatalf("body %s", c.body)
	}
}

func TestAFailedSendDoesNotLeakThePassword(t *testing.T) {
	api, _ := bluebubbles(t, 500, `{"status":500,"message":"Message failed to send!","error":{"type":"iMessage Error"}}`)
	_, err := api.SendText(context.Background(), imessage.Text{Chat: dm, Message: "hi"})
	if err == nil || strings.Contains(err.Error(), "server-pass") || !strings.Contains(err.Error(), "failed to send") {
		t.Fatalf("err = %v", err)
	}
}

func TestSendAttachmentIsAMultipartUploadMarkedAsAudio(t *testing.T) {
	api, got := bluebubbles(t, 200, `{"status":200,"message":"Attachment sent!","data":{"guid":"SENT-2"}}`)
	guid, err := api.SendAttachment(context.Background(), imessage.Attachment{Chat: dm, Name: "Audio Message.caf", MimeType: "audio/x-caf", Data: []byte(cafHdr), Audio: true, Method: imessage.MethodPrivateAPI})
	if err != nil || guid != "SENT-2" {
		t.Fatalf("SendAttachment = %q, %v", guid, err)
	}
	c := (*got)[0]
	_, params, _ := mime.ParseMediaType(c.contentType)
	form, err := multipart.NewReader(strings.NewReader(string(c.body)), params["boundary"]).ReadForm(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if c.path != "/api/v1/message/attachment" || form.Value["chatGuid"][0] != dm || form.Value["isAudioMessage"][0] != "true" ||
		form.Value["method"][0] != imessage.MethodPrivateAPI || form.Value["name"][0] != "Audio Message.caf" || len(form.File["attachment"]) != 1 {
		t.Fatalf("call %+v form %+v", c, form)
	}
}

func TestDownloadAttachmentStreamsTheFile(t *testing.T) {
	api, got := bluebubbles(t, 200, cafHdr)
	rc, err := api.DownloadAttachment(context.Background(), "ATT/1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	b, _ := io.ReadAll(rc)
	if string(b) != cafHdr || (*got)[0].path != "/api/v1/attachment/ATT/1/download" || (*got)[0].password != "server-pass" {
		t.Fatalf("got %q from %+v", b, (*got)[0])
	}
}
