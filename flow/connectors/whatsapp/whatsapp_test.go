package whatsapp_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/orchagent/flow/address"
	"github.com/urmzd/orchagent/flow/attachment"
	"github.com/urmzd/orchagent/flow/attachment/intake"
	"github.com/urmzd/orchagent/flow/blob"
	"github.com/urmzd/orchagent/flow/connector"
	"github.com/urmzd/orchagent/flow/connectors"
	"github.com/urmzd/orchagent/flow/connectors/internal/rendertest"
	"github.com/urmzd/orchagent/flow/connectors/mention"
	"github.com/urmzd/orchagent/flow/connectors/whatsapp"
	"github.com/urmzd/orchagent/flow/event"
	"github.com/urmzd/orchagent/flow/signature"
)

const (
	appSecret   = "meta-app-secret"
	verifyToken = "verify-me"
	number      = "106540352242922"
	person      = "15551234567"
)

func start(t *testing.T, cfg whatsapp.Config) (*whatsapp.Connector, <-chan event.Envelope) {
	t.Helper()
	if cfg.API == nil {
		cfg.API = whatsapp.NewFake()
	}
	if cfg.Verifier == nil {
		cfg.Verifier = signature.NewMeta(appSecret)
	}
	if cfg.VerifyToken == "" {
		cfg.VerifyToken = verifyToken
	}
	c, err := whatsapp.New(cfg)
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

func sign(body string) string {
	m := hmac.New(sha256.New, []byte(appSecret))
	m.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

func post(t *testing.T, h http.Handler, body string, signed bool) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/connectors/whatsapp", strings.NewReader(body))
	if signed {
		req.Header.Set(signature.HeaderMetaSignature, sign(body))
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Code
}

// notify wraps message JSON objects in a webhook notification, as Meta
// delivers them.
func notify(phoneNumberID string, messages ...string) string {
	return fmt.Sprintf(`{
  "object": "whatsapp_business_account",
  "entry": [{"id": "WABA1", "changes": [{"field": "messages", "value": {
    "messaging_product": "whatsapp",
    "metadata": {"display_phone_number": "+1 555-010-0000", "phone_number_id": %q},
    "contacts": [{"profile": {"name": "Ada"}, "wa_id": %q}],
    "messages": [%s]
  }}]}]
}`, phoneNumberID, person, strings.Join(messages, ","))
}

func textMessage(id, body string) string {
	return fmt.Sprintf(`{"from": %q, "id": %q, "timestamp": "1700000000", "type": "text", "text": {"body": %q}}`, person, id, body)
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

func TestTheSubscriptionHandshakeEchoesTheChallengeOnlyForTheVerifyToken(t *testing.T) {
	c, _ := start(t, whatsapp.Config{})
	for _, tc := range []struct {
		name  string
		query string
		code  int
	}{
		{"right token", "hub.mode=subscribe&hub.verify_token=verify-me&hub.challenge=1158201444", http.StatusOK},
		{"wrong token", "hub.mode=subscribe&hub.verify_token=verify-mf&hub.challenge=1", http.StatusForbidden},
		{"no token", "hub.mode=subscribe&hub.challenge=1", http.StatusForbidden},
		{"wrong mode", "hub.mode=unsubscribe&hub.verify_token=verify-me&hub.challenge=1", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/connectors/whatsapp?"+tc.query, nil))
			if w.Code != tc.code {
				t.Fatalf("status %d, want %d", w.Code, tc.code)
			}
			if tc.code == http.StatusOK && w.Body.String() != "1158201444" {
				t.Fatalf("body %q, want the challenge", w.Body.String())
			}
		})
	}
}

func TestAConnectorWithNoVerifyTokenRefusesTheHandshake(t *testing.T) {
	c, err := whatsapp.New(whatsapp.Config{API: whatsapp.NewFake()})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	c.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/?hub.mode=subscribe&hub.verify_token=&hub.challenge=1", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403", w.Code)
	}
}

func TestAnUnsignedOrForgedNotificationPublishesNothing(t *testing.T) {
	c, events := start(t, whatsapp.Config{})
	body := notify(number, textMessage("wamid.A", "hi"))
	if code := post(t, c, body, false); code != http.StatusUnauthorized {
		t.Fatalf("unsigned: status %d, want 401", code)
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set(signature.HeaderMetaSignature, sign(body+" "))
	w := httptest.NewRecorder()
	c.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("forged: status %d, want 401", w.Code)
	}
	if got := drain(events); len(got) != 0 {
		t.Fatalf("published %d events", len(got))
	}
}

func TestATextMessageIsPublishedFromItsSenderWithItsMessageID(t *testing.T) {
	c, events := start(t, whatsapp.Config{Tenant: "acme", Agent: "concierge", Agents: mention.NewNames("concierge", "docs-bot")})
	if code := post(t, c, notify(number, textMessage("wamid.HBgL1", "@docs-bot what is on today?")), true); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	got := drain(events)
	if len(got) != 1 {
		t.Fatalf("published %d events, want 1", len(got))
	}
	e := got[0]
	b := e.Data.(event.MessagePosted)
	want := whatsapp.Address(number, person).String()
	if e.ID != "wamid.HBgL1" || e.Channel != want || e.Source != want || e.Tenant != "acme" || e.Type != event.TypeMessagePosted {
		t.Fatalf("envelope %+v", e)
	}
	if b.Text != "what is on today?" || b.Actor != (event.Actor{ID: person, Display: "Ada"}) {
		t.Fatalf("request %+v", b.Request)
	}
	if strings.Join(b.Mentions, ",") != "concierge,docs-bot" {
		t.Fatalf("mentions %v", b.Mentions)
	}
	if b.WhatsApp == nil || b.WhatsApp.MessageID != "wamid.HBgL1" || b.WhatsApp.From != person || b.WhatsApp.PhoneNumberID != number || b.WhatsApp.Type != "text" {
		t.Fatalf("ref %+v", b.WhatsApp)
	}
	if !event.DirectMessage(b) {
		t.Fatal("a WhatsApp message is not a direct message")
	}
	if !e.Time.Equal(time.Unix(1700000000, 0)) {
		t.Fatalf("time %v", e.Time)
	}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	rendertest.Check(t, e)
}

func TestEveryMessageInANotificationIsItsOwnEvent(t *testing.T) {
	c, events := start(t, whatsapp.Config{})
	post(t, c, notify(number, textMessage("wamid.1", "one"), textMessage("wamid.2", "two")), true)
	got := drain(events)
	if len(got) != 2 || got[0].ID != "wamid.1" || got[1].ID != "wamid.2" {
		t.Fatalf("published %+v", got)
	}
}

func TestStatusesReactionsOwnMessagesAndOtherNumbersPublishNothing(t *testing.T) {
	c, events := start(t, whatsapp.Config{PhoneNumberID: number})
	for name, body := range map[string]string{
		"status callback": `{"object":"whatsapp_business_account","entry":[{"id":"W","changes":[{"field":"messages","value":{
			"metadata":{"phone_number_id":"` + number + `"},"statuses":[{"id":"wamid.X","status":"delivered","recipient_id":"` + person + `"}]}}]}]}`,
		"reaction":           notify(number, `{"from":"`+person+`","id":"wamid.R","timestamp":"1","type":"reaction","reaction":{"message_id":"wamid.X","emoji":"👍"}}`),
		"from the business":  notify(number, `{"from":"15550100000","id":"wamid.O","timestamp":"1","type":"text","text":{"body":"echo"}}`),
		"another number":     notify("999", textMessage("wamid.N", "hi")),
		"another field":      `{"object":"whatsapp_business_account","entry":[{"id":"W","changes":[{"field":"account_update","value":{}}]}]}`,
		"another object":     `{"object":"page","entry":[]}`,
		"empty text":         notify(number, textMessage("wamid.E", "")),
		"media with no id":   notify(number, `{"from":"`+person+`","id":"wamid.M","timestamp":"1","type":"audio","audio":{"mime_type":"audio/ogg"}}`),
		"message with no id": notify(number, `{"from":"`+person+`","timestamp":"1","type":"text","text":{"body":"x"}}`),
	} {
		if code := post(t, c, body, true); code != http.StatusOK {
			t.Errorf("%s: status %d, want 200 so Meta does not retry", name, code)
		}
		if got := drain(events); len(got) != 0 {
			t.Errorf("%s: published %+v", name, got)
		}
	}
}

func TestANotificationWhileNotIngestingAsksMetaToRetry(t *testing.T) {
	c, err := whatsapp.New(whatsapp.Config{API: whatsapp.NewFake(), Verifier: signature.NewMeta(appSecret)})
	if err != nil {
		t.Fatal(err)
	}
	if code := post(t, c, notify(number, textMessage("wamid.1", "hi")), true); code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", code)
	}
}

func TestPrepareCanRefuseAnEvent(t *testing.T) {
	c, events := start(t, whatsapp.Config{Prepare: func(context.Context, *event.Envelope) error { return errors.New("no") }})
	if code := post(t, c, notify(number, textMessage("wamid.1", "hi")), true); code != http.StatusForbidden {
		t.Fatalf("status %d, want 403", code)
	}
	if got := drain(events); len(got) != 0 {
		t.Fatalf("published %+v", got)
	}
}

const oggOpus = "OggS\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x13OpusHead\x01\x01"

func loopbackOnly(a netip.Addr) error {
	if a.Unmap() == netip.MustParseAddr("127.0.0.1") {
		return nil
	}
	return intake.PublicOnly(a)
}

// A voice note becomes an audio attachment on the event, downloaded through
// the media lookup, with empty text and the ref saying it is a voice note,
// so a host can find it and transcribe it, in Prepare or before the run.
func TestAVoiceNoteArrivesAsAnAudioAttachmentAPreparerCanSee(t *testing.T) {
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, oggOpus)
	}))
	t.Cleanup(srv.Close)
	api := whatsapp.NewFake()
	api.AddMedia(whatsapp.Media{ID: "MEDIA1", URL: srv.URL + "/voice", MimeType: "audio/ogg; codecs=opus", Size: int64(len(oggOpus))})
	store := blob.NewMemory()
	var prepared []attachment.Attachment
	c, events := start(t, whatsapp.Config{
		API: api, Tenant: "acme", MediaToken: "EAAG-token",
		Files: connectors.Files{Store: store, Caps: intake.Caps{MaxFileBytes: 1 << 20}},
		Prepare: func(_ context.Context, e *event.Envelope) error {
			r, _ := event.RequestOf(e.Data)
			prepared = attachment.OfKind(r.Attachments, attachment.KindAudio)
			return nil
		},
	})
	whatsapp.UseFetcher(c, intake.NewFetcher(store, intake.Hosts("127.0.0.1"),
		intake.WithPlainHTTP(), intake.WithNetworkPolicy(loopbackOnly),
		intake.WithHeaders(func(*url.URL) http.Header { return http.Header{"Authorization": {"Bearer EAAG-token"}} })))

	voice := `{"from":"` + person + `","id":"wamid.V","timestamp":"1700000000","type":"audio","audio":{"id":"MEDIA1","mime_type":"audio/ogg; codecs=opus","voice":true}}`
	if code := post(t, c, notify(number, voice), true); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	got := drain(events)
	if len(got) != 1 {
		t.Fatalf("published %d events", len(got))
	}
	b := got[0].Data.(event.MessagePosted)
	if b.Text != "" || b.WhatsApp.Type != "audio" || !b.WhatsApp.Voice {
		t.Fatalf("body %+v ref %+v", b.Request, b.WhatsApp)
	}
	if len(b.Attachments) != 1 || b.Attachments[0].Kind() != attachment.KindAudio || b.Attachments[0].MediaType != "audio/ogg" || b.Attachments[0].Unread != "" {
		t.Fatalf("attachments %+v", b.Attachments)
	}
	if len(prepared) != 1 || prepared[0].Digest != b.Attachments[0].Digest {
		t.Fatalf("Prepare saw %+v", prepared)
	}
	if auth != "Bearer EAAG-token" {
		t.Fatalf("download sent Authorization %q", auth)
	}
	rc, _, err := store.Get(context.Background(), "acme", blob.Digest(b.Attachments[0].Digest))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	if data, _ := io.ReadAll(rc); string(data) != oggOpus {
		t.Fatalf("stored %q", data)
	}
}

func TestTheAccessTokenGoesOnlyToMetasMediaHostsOverHTTPS(t *testing.T) {
	headers := whatsapp.MediaHeaders("EAAG-token")
	for raw, want := range map[string]bool{
		"https://lookaside.fbsbx.com/whatsapp_business/attachments/?mid=1": true,
		"https://scontent.fbsbx.com/x":                                     true,
		"http://lookaside.fbsbx.com/x":                                     false,
		"https://lookaside.fbsbx.com.evil.example/x":                       false,
		"https://example.com/x":                                            false,
	} {
		u, _ := url.Parse(raw)
		if got := headers(u).Get("Authorization") != ""; got != want {
			t.Errorf("%s: token sent = %v, want %v", raw, got, want)
		}
		if got := whatsapp.MediaHostPolicy()(u) == nil; got != want && u.Scheme == "https" {
			t.Errorf("%s: host allowed = %v, want %v", raw, got, want)
		}
	}
}

func runEvent(seq uint64, typ, data string) event.Envelope {
	body, err := event.Decode(typ, json.RawMessage(data))
	if err != nil {
		panic(err)
	}
	ch := whatsapp.Address(number, person).String()
	return event.Envelope{ID: fmt.Sprintf("r1:%d", seq), Source: "fiber://run/r1", Type: typ, Channel: ch, Run: "r1", Seq: seq, Agent: "concierge", Time: time.Unix(1700000000, 0), Data: body}
}

func TestARunIsSentOnceWhenItEndsInWhatsAppFormatting(t *testing.T) {
	api := whatsapp.NewFake()
	c, _ := start(t, whatsapp.Config{API: api, PhoneNumberID: number})
	target := whatsapp.Address(number, person)
	ctx := context.Background()
	events := []event.Envelope{
		runEvent(1, event.TypeStarted, `{}`),
		runEvent(2, event.TypeText, `{"text":"**Today**: standup at 9."}`),
		runEvent(3, event.TypeCompleted, `{"text":"**Today**: standup at 9."}`),
	}
	for _, e := range append(events, events...) {
		if err := c.Deliver(ctx, target, e); err != nil {
			t.Fatal(err)
		}
	}
	msgs := api.Messages()
	if len(msgs) != 1 || msgs[0].Body != "*Today*: standup at 9." || msgs[0].To != person || msgs[0].PhoneNumberID != number {
		t.Fatalf("sent %+v", msgs)
	}
}

func TestAQuestionIsSentAsPlainTextWhileTheRunWaits(t *testing.T) {
	api := whatsapp.NewFake()
	c, _ := start(t, whatsapp.Config{API: api})
	target := whatsapp.Address(number, person)
	if err := c.Deliver(context.Background(), target, runEvent(4, event.TypeParked, `{"question":{"id":"q1","kind":"reply","prompt":"Which day?","options":["Mon","Tue"]}}`)); err != nil {
		t.Fatal(err)
	}
	msgs := api.Messages()
	if len(msgs) != 1 || !strings.Contains(msgs[0].Body, "Which day?") || !strings.Contains(msgs[0].Body, "- Mon") {
		t.Fatalf("sent %+v", msgs)
	}
}

func TestALongAnswerIsClampedToWhatWhatsAppAccepts(t *testing.T) {
	api := whatsapp.NewFake()
	c, _ := start(t, whatsapp.Config{API: api})
	long := strings.Repeat("é", 5000)
	if err := c.Deliver(context.Background(), whatsapp.Address(number, person), runEvent(3, event.TypeCompleted, `{"text":"`+long+`"}`)); err != nil {
		t.Fatal(err)
	}
	msgs := api.Messages()
	if len(msgs) != 1 || !strings.HasSuffix(msgs[0].Body, "[truncated]") {
		t.Fatalf("sent %d messages", len(msgs))
	}
}

func TestDeliveryElsewhereIsUndeliverable(t *testing.T) {
	c, _ := start(t, whatsapp.Config{PhoneNumberID: number})
	for _, raw := range []string{"slack://T01/C02", "whatsapp://999/" + person, "whatsapp://" + number} {
		err := c.Deliver(context.Background(), address.MustParse(raw), runEvent(3, event.TypeCompleted, `{"text":"x"}`))
		if !errors.Is(err, connector.ErrUndeliverable) {
			t.Errorf("%s: %v, want ErrUndeliverable", raw, err)
		}
	}
}

func TestTheSendActionReachesOnlyThePersonTheRunAnswers(t *testing.T) {
	api := whatsapp.NewFake()
	c, _ := start(t, whatsapp.Config{API: api, PhoneNumberID: number})
	actions := c.Actions()
	if len(actions) != 1 || actions[0].Name != "whatsapp_send_message" {
		t.Fatalf("actions %+v", actions)
	}
	var schema map[string]any
	if err := json.Unmarshal(actions[0].Schema, &schema); err != nil {
		t.Fatal(err)
	}
	res, err := actions[0].Invoke(context.Background(), whatsapp.Address(number, person), json.RawMessage(`{"text":"On it."}`))
	if err != nil {
		t.Fatal(err)
	}
	msgs := api.Messages()
	if len(msgs) != 1 || msgs[0].To != person || msgs[0].Body != "On it." {
		t.Fatalf("sent %+v", msgs)
	}
	if id, _ := res.Address.Param("message"); id != msgs[0].ID {
		t.Fatalf("result %+v", res)
	}
	if _, err := actions[0].Invoke(context.Background(), whatsapp.Address(number, person), json.RawMessage(`{"text":"x","to":"1"}`)); err != nil {
		// Unknown fields are ignored by encoding/json; the recipient is
		// still the conversation's.
		t.Fatal(err)
	}
	if got := api.Messages(); got[len(got)-1].To != person {
		t.Fatalf("sent to %s", got[len(got)-1].To)
	}
	if _, err := actions[0].Invoke(context.Background(), whatsapp.Address("999", person), json.RawMessage(`{"text":"x"}`)); !errors.Is(err, connector.ErrUndeliverable) {
		t.Fatalf("another number: %v", err)
	}
}

func TestSendAudioUploadsThenSendsAnAudioMessage(t *testing.T) {
	api := whatsapp.NewFake()
	c, _ := start(t, whatsapp.Config{API: api})
	id, err := c.SendAudio(context.Background(), whatsapp.Address(number, person), whatsapp.Audio{Data: []byte(oggOpus), MimeType: "audio/ogg", Name: "reply.ogg"})
	if err != nil {
		t.Fatal(err)
	}
	msgs := api.Messages()
	if len(msgs) != 1 || msgs[0].ID != id || msgs[0].Type != "audio" {
		t.Fatalf("sent %+v", msgs)
	}
	up, ok := api.Uploaded(msgs[0].MediaID)
	if !ok || !bytes.Equal(up.Data, []byte(oggOpus)) || up.MimeType != "audio/ogg" {
		t.Fatalf("upload %+v", up)
	}
	if _, err := c.SendAudio(context.Background(), whatsapp.Address(number, person), whatsapp.Audio{}); err == nil {
		t.Fatal("empty audio was sent")
	}
}

func TestRolesAreDerivedFromTheFaces(t *testing.T) {
	c, _ := start(t, whatsapp.Config{})
	r := connector.RolesOf(c)
	if !r.Trigger || !r.Sink || !r.Actor {
		t.Fatalf("roles %+v", r)
	}
}
