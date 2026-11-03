package imessage_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/orchagent/flow/address"
	"github.com/urmzd/orchagent/flow/attachment"
	"github.com/urmzd/orchagent/flow/blob"
	"github.com/urmzd/orchagent/flow/connector"
	"github.com/urmzd/orchagent/flow/connectors"
	"github.com/urmzd/orchagent/flow/connectors/imessage"
	"github.com/urmzd/orchagent/flow/connectors/internal/rendertest"
	"github.com/urmzd/orchagent/flow/connectors/mention"
	"github.com/urmzd/orchagent/flow/event"
)

const (
	secret = "s3cret-webhook"
	server = "home"
	dm     = "iMessage;-;+15551234567"
	group  = "iMessage;+;chat123456789"
	sender = "+15551234567"
	cafHdr = "caff\x00\x01\x00\x00desc\x00\x00\x00\x00\x00\x00\x00\x20"
)

func start(t *testing.T, cfg imessage.Config) (*imessage.Connector, <-chan event.Envelope) {
	t.Helper()
	if cfg.API == nil {
		cfg.API = imessage.NewFake()
	}
	if cfg.Server == "" {
		cfg.Server = server
	}
	if cfg.Secret == "" && cfg.Verifier == nil {
		cfg.Secret = secret
	}
	c, err := imessage.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	events := make(chan event.Envelope, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Ingest(ctx, func(_ context.Context, e event.Envelope) error {
			events <- e
			return nil
		})
	}()
	t.Cleanup(func() { cancel(); <-done })
	for range 1000 {
		if c.Ingesting() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	return c, events
}

func post(h http.Handler, query, body string) int {
	req := httptest.NewRequest(http.MethodPost, "/connectors/imessage?"+query, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Code
}

const authed = "secret=" + secret

// newMessage is a new-message webhook as BlueBubbles sends one.
func newMessage(guid, chat, text string, extra string) string {
	return fmt.Sprintf(`{"type":"new-message","data":{
  "originalROWID": 42, "guid": %q, "text": %q, "isFromMe": false,
  "handle": {"address": %q, "service": "iMessage"},
  "chats": [{"guid": %q, "chatIdentifier": "x", "displayName": ""}],
  "attachments": [], "dateCreated": 1700000000123, "itemType": 0,
  "associatedMessageGuid": null, "associatedMessageType": null%s
}}`, guid, text, sender, chat, extra)
}

func drain(events <-chan event.Envelope) []event.Envelope {
	var out []event.Envelope
	for {
		select {
		case e := <-events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestTheWebhookSecretIsCheckedBeforeAnythingIsRead(t *testing.T) {
	c, events := start(t, imessage.Config{})
	body := newMessage("G1", dm, "hi", "")
	for name, q := range map[string]string{
		"no secret":         "",
		"wrong secret":      "secret=s3cret-webhooK",
		"prefix of secret":  "secret=s3cret",
		"secret twice":      authed + "&" + authed,
		"another parameter": "password=" + secret,
	} {
		if code := post(c, q, body); code != http.StatusUnauthorized {
			t.Errorf("%s: status %d, want 401", name, code)
		}
	}
	if code := post(c, authed, "{not json"); code != http.StatusBadRequest {
		t.Errorf("malformed: status %d, want 400", code)
	}
	if got := drain(events); len(got) != 0 {
		t.Fatalf("published %+v", got)
	}
}

func TestAConnectorWithNoSecretAndNoVerifierRefusesEverything(t *testing.T) {
	c, err := imessage.New(imessage.Config{API: imessage.NewFake(), Server: server})
	if err != nil {
		t.Fatal(err)
	}
	if code := post(c, "", newMessage("G1", dm, "hi", "")); code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", code)
	}
}

type headerVerifier struct{}

func (headerVerifier) Verify(h http.Header, _ []byte) error {
	if h.Get("X-Gateway") != "ok" {
		return errors.New("no")
	}
	return nil
}

func TestAVerifierCanReplaceTheURLSecret(t *testing.T) {
	c, events := start(t, imessage.Config{Verifier: headerVerifier{}})
	if code := post(c, "", newMessage("G1", dm, "hi", "")); code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", code)
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(newMessage("G1", dm, "hi", "")))
	req.Header.Set("X-Gateway", "ok")
	w := httptest.NewRecorder()
	c.ServeHTTP(w, req)
	if w.Code != http.StatusOK || len(drain(events)) != 1 {
		t.Fatalf("status %d", w.Code)
	}
}

func TestANewMessageIsPublishedFromItsSenderWithItsGUID(t *testing.T) {
	c, events := start(t, imessage.Config{Tenant: "acme", Agent: "concierge"})
	if code := post(c, authed, newMessage("p:0/ABC-123", dm, "what is on today?", "")); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	got := drain(events)
	if len(got) != 1 {
		t.Fatalf("published %d", len(got))
	}
	e := got[0]
	b := e.Data.(event.MessagePosted)
	want := imessage.Address(server, dm).String()
	if e.ID != "p:0/ABC-123" || e.Channel != want || e.Tenant != "acme" || !e.Time.Equal(time.UnixMilli(1700000000123)) {
		t.Fatalf("envelope %+v", e)
	}
	if b.Text != "what is on today?" || b.Actor.ID != sender || strings.Join(b.Mentions, ",") != "concierge" {
		t.Fatalf("request %+v", b.Request)
	}
	if r := b.IMessage; r == nil || r.Server != server || r.Chat != dm || r.Sender != sender || r.GUID != "p:0/ABC-123" || r.Group {
		t.Fatalf("ref %+v", b.IMessage)
	}
	if !event.DirectMessage(b) {
		t.Fatal("a one-to-one chat is not a direct message")
	}
	back, err := address.Parse(e.Channel)
	if err != nil || back.Path[0] != dm || back.Workspace != server {
		t.Fatalf("channel %q parses as %+v, %v", e.Channel, back, err)
	}
	rendertest.Check(t, e)
}

func TestAGroupMessageNamesItsAgentsByMention(t *testing.T) {
	c, events := start(t, imessage.Config{Agents: mention.NewNames("docs-bot")})
	post(c, authed, newMessage("G2", group, "@docs-bot summarise", ""))
	got := drain(events)
	b := got[0].Data.(event.MessagePosted)
	if !b.IMessage.Group || event.DirectMessage(b) || b.Text != "summarise" || strings.Join(b.Mentions, ",") != "docs-bot" {
		t.Fatalf("body %+v ref %+v", b.Request, b.IMessage)
	}
}

func TestOwnMessagesTapbacksAndOtherWebhooksPublishNothing(t *testing.T) {
	c, events := start(t, imessage.Config{})
	for name, body := range map[string]string{
		"from me":          strings.Replace(newMessage("G1", dm, "hi", ""), `"isFromMe": false`, `"isFromMe": true`, 1),
		"tapback":          newMessage("G2", dm, "Loved “hi”", `, "associatedMessageType": 2000`),
		"tapback by name":  newMessage("G3", dm, "x", `, "associatedMessageType": "love"`),
		"group event":      strings.Replace(newMessage("G4", group, "", ""), `"itemType": 0`, `"itemType": 2`, 1),
		"updated message":  strings.Replace(newMessage("G5", dm, "hi", ""), "new-message", "updated-message", 1),
		"typing":           `{"type":"typing-indicator","data":{"display":true,"guid":"` + dm + `"}}`,
		"no text no files": newMessage("G6", dm, "￼", ""),
		"no handle":        strings.Replace(newMessage("G7", dm, "hi", ""), `"handle": {"address": "+15551234567", "service": "iMessage"}`, `"handle": null`, 1),
		"no chat":          strings.Replace(newMessage("G8", dm, "hi", ""), `"chats": [{"guid": "iMessage;-;+15551234567", "chatIdentifier": "x", "displayName": ""}]`, `"chats": []`, 1),
	} {
		if code := post(c, authed, body); code != http.StatusOK {
			t.Errorf("%s: status %d", name, code)
		}
		if got := drain(events); len(got) != 0 {
			t.Errorf("%s: published %+v", name, got)
		}
	}
}

func TestAnAudioMessageArrivesAsAnAudioAttachment(t *testing.T) {
	api := imessage.NewFake()
	api.AddAttachment("ATT-1", []byte(cafHdr))
	store := blob.NewMemory()
	c, events := start(t, imessage.Config{API: api, Tenant: "acme", Files: connectors.Files{Store: store}})
	att := `, "isAudioMessage": true, "attachments": [{"guid": "ATT-1", "uti": "com.apple.coreaudio-format", "mimeType": null, "transferName": "Audio Message.caf", "totalBytes": 20}]`
	body := strings.Replace(newMessage("G9", dm, "￼", att), `"attachments": [], `, "", 1)
	if code := post(c, authed, body); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	got := drain(events)
	if len(got) != 1 {
		t.Fatalf("published %d", len(got))
	}
	b := got[0].Data.(event.MessagePosted)
	audio := attachment.OfKind(b.Attachments, attachment.KindAudio)
	if b.Text != "" || !b.IMessage.Audio || len(audio) != 1 || audio[0].MediaType != "audio/x-caf" || audio[0].Name != "Audio Message.caf" {
		t.Fatalf("body %+v ref %+v", b.Request, b.IMessage)
	}
	rc, _, err := store.Get(context.Background(), "acme", blob.Digest(audio[0].Digest))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	if data, _ := io.ReadAll(rc); string(data) != cafHdr {
		t.Fatalf("stored %q", data)
	}
}

func TestAnAttachmentTheServerCannotServeIsCarriedUnread(t *testing.T) {
	c, events := start(t, imessage.Config{Tenant: "acme", Files: connectors.Files{Store: blob.NewMemory()}})
	att := `, "attachments": [{"guid": "GONE", "mimeType": "image/jpeg", "transferName": "x.jpg", "totalBytes": 9}]`
	post(c, authed, strings.Replace(newMessage("G10", dm, "look", att), `"attachments": [], `, "", 1))
	got := drain(events)
	b := got[0].Data.(event.MessagePosted)
	if len(b.Attachments) != 1 || b.Attachments[0].Unread == "" {
		t.Fatalf("attachments %+v", b.Attachments)
	}
}

func runEvent(seq uint64, typ, data string) event.Envelope {
	body, err := event.Decode(typ, json.RawMessage(data))
	if err != nil {
		panic(err)
	}
	return event.Envelope{ID: fmt.Sprintf("r1:%d", seq), Source: "fiber://run/r1", Type: typ, Channel: imessage.Address(server, dm).String(), Run: "r1", Seq: seq, Agent: "concierge", Time: time.Unix(1700000000, 0), Data: body}
}

func TestARunIsSentOnceAsPlainTextWhenItEnds(t *testing.T) {
	api := imessage.NewFake()
	c, _ := start(t, imessage.Config{API: api, Method: imessage.MethodPrivateAPI})
	target := imessage.Address(server, dm)
	evs := []event.Envelope{
		runEvent(1, event.TypeStarted, `{}`),
		runEvent(2, event.TypeText, `{"text":"## Today\n**Standup** at 9, see [notes](https://x.example/n)."}`),
		runEvent(3, event.TypeCompleted, `{"text":"## Today\n**Standup** at 9, see [notes](https://x.example/n)."}`),
	}
	for _, e := range append(evs, evs...) {
		if err := c.Deliver(context.Background(), target, e); err != nil {
			t.Fatal(err)
		}
	}
	msgs := api.Messages()
	if len(msgs) != 1 || msgs[0].Chat != dm || msgs[0].Method != imessage.MethodPrivateAPI ||
		msgs[0].Text != "Today\nStandup at 9, see notes (https://x.example/n)." {
		t.Fatalf("sent %+v", msgs)
	}
}

func TestDeliveryToAnotherServerIsUndeliverable(t *testing.T) {
	c, _ := start(t, imessage.Config{})
	for _, raw := range []string{"imessage://office/x", "whatsapp://1/2", "imessage://home"} {
		if err := c.Deliver(context.Background(), address.MustParse(raw), runEvent(3, event.TypeCompleted, `{"text":"x"}`)); !errors.Is(err, connector.ErrUndeliverable) {
			t.Errorf("%s: %v", raw, err)
		}
	}
}

func TestTheSendActionAndSendAudioReachTheRunsChat(t *testing.T) {
	api := imessage.NewFake()
	c, _ := start(t, imessage.Config{API: api})
	acts := c.Actions()
	if len(acts) != 1 || acts[0].Name != "imessage_send_message" {
		t.Fatalf("actions %+v", acts)
	}
	target := imessage.Address(server, group)
	res, err := acts[0].Invoke(context.Background(), target, json.RawMessage(`{"text":"On it."}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.SendAudio(context.Background(), target, imessage.Audio{Data: []byte(cafHdr), MimeType: "audio/x-caf", Voice: true}); err != nil {
		t.Fatal(err)
	}
	msgs := api.Messages()
	if len(msgs) != 2 || msgs[0].Chat != group || msgs[0].Text != "On it." || msgs[1].File == nil || !msgs[1].File.Audio || msgs[1].File.Name != "Audio Message.caf" {
		t.Fatalf("sent %+v", msgs)
	}
	if id, _ := res.Address.Param("message"); id != msgs[0].GUID {
		t.Fatalf("result %+v", res)
	}
}

func TestNewRefusesAServerNameNoAddressCanHold(t *testing.T) {
	for _, s := range []string{"", "my mac", "home.local"} {
		if _, err := imessage.New(imessage.Config{API: imessage.NewFake(), Server: s}); err == nil {
			t.Errorf("server %q accepted", s)
		}
	}
	if _, err := imessage.New(imessage.Config{API: imessage.NewFake(), Server: server, Method: "carrier-pigeon"}); err == nil {
		t.Error("unknown method accepted")
	}
}
