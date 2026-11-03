// Package whatsapp is the WhatsApp peer in the mesh, over Meta's WhatsApp
// Cloud API: trigger, sink and actor.
//
// # Addresses
//
// A conversation is one business number and one person:
//
//	whatsapp://<phone-number-id>/<wa-id>
//	whatsapp://106540352242922/15551234567
//
// The workspace is the business number's Graph API phone number id (not the
// phone number itself), and the path is the person's WhatsApp id, which is
// their phone number in international form without the "+". Address builds
// one. Every message from a person lands on their conversation's channel, and
// a run's answer goes back out on it.
//
// # Trigger
//
// Meta delivers webhooks as a POST signed with the app secret
// (X-Hub-Signature-256, see signature.Meta), so the connector is both an
// http.Handler and a pkg/connector.Trigger. Nothing is parsed before the
// signature is checked. The same handler answers Meta's GET subscription
// handshake (hub.mode=subscribe, hub.verify_token, hub.challenge) by
// comparing the verify token with Config.VerifyToken in constant time.
//
// A text, a button or list reply, and a media message (audio, voice note,
// image, video, document) each publish message.posted.v1 with the sender as
// Actor and the WhatsApp message id (wamid...) as the event id, which Meta
// reuses on every retry of the same message. Status callbacks (sent,
// delivered, read), reactions and everything else are acknowledged and
// dropped, as is a message from the business number itself.
//
// # Voice notes and other media
//
// The bytes of a media message are not in the webhook. With Config.Files set
// the connector looks the media id up (GET /{media-id}), downloads it from
// MediaHosts with the access token, stores it under the tenant and carries
// it as an attachment on the event, whose media type is sniffed from the
// bytes. A voice note is an attachment of kind audio (attachment.KindAudio)
// and the event's WhatsAppRef has Type "audio" and Voice set, with empty
// text: transcription is the host's business, and Config.Prepare, which runs
// after the files are in, is where a host can do it before the event is
// published.
//
// # Sink
//
// WhatsApp cannot edit a sent message, so this sink never streams. It sends
// one message when a run is waiting on a person (a question or an approval,
// as plain text; answering happens on the host) and one when the run is
// over, with the answer converted to WhatsApp's formatting and clamped to
// MaxBodyChars. See internal/sink's Once for the idempotency argument.
// Meta accepts a free-form reply only within 24 hours of the person's last
// message; a run that outlives that window is refused by the API.
//
// # Actor
//
// whatsapp_send_message sends a message to the person the run is answering,
// now: a progress note on a long task, say. It cannot address anyone else.
// SendAudio sends an audio file (a spoken reply) to a conversation; it is a
// method for the host, not an action, because a model has no audio to give
// it.
package whatsapp

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"unicode/utf8"

	"github.com/urmzd/orchagent/flow/address"
	"github.com/urmzd/orchagent/flow/attachment/intake"
	"github.com/urmzd/orchagent/flow/connector"
	"github.com/urmzd/orchagent/flow/connectors"
	"github.com/urmzd/orchagent/flow/connectors/internal/chattext"
	"github.com/urmzd/orchagent/flow/connectors/internal/files"
	"github.com/urmzd/orchagent/flow/connectors/internal/inbound"
	"github.com/urmzd/orchagent/flow/connectors/internal/render"
	"github.com/urmzd/orchagent/flow/connectors/internal/sink"
	"github.com/urmzd/orchagent/flow/connectors/mention"
	"github.com/urmzd/orchagent/flow/event"
)

// Name is the address scheme this connector owns.
const Name = "whatsapp"

// MaxBodyChars is WhatsApp's limit on a text message body, in characters.
// The renderer clamps to it in bytes, which is never longer.
const MaxBodyChars = 4096

// Verifier authenticates a raw request body against its headers.
// *signature.Meta satisfies it, and is what a deployment should pass.
type Verifier interface {
	Verify(header http.Header, body []byte) error
}

// Config is everything the WhatsApp connector needs. New validates it.
type Config struct {
	// API talks to the Cloud API. Required. NewHTTPAPI in production,
	// NewFake in tests, examples and local runs.
	API API

	// Verifier authenticates webhook POSTs: signature.NewMeta(appSecret).
	// Without one every POST is refused.
	Verifier Verifier

	// VerifyToken is the token configured with the webhook in the Meta app
	// dashboard, echoed back on the GET subscription handshake. Without one
	// the handshake is refused.
	VerifyToken string

	// PhoneNumberID is the business number this connector serves. When
	// set, a notification for any other number is dropped and a delivery to
	// one is refused with ErrUndeliverable. Empty serves every number the
	// app receives, which is only correct when API can send from each.
	PhoneNumberID string

	// Agent is the agent this number fronts, if one. It is listed as a
	// mention on every message, and an "@agent" in the text is stripped.
	Agent string

	// Agents names the agents a message may address by "@name". Nil names
	// none beyond Agent.
	Agents mention.Set

	// Prepare applies application policy to a verified event before it is
	// published, after its files are in. An error refuses the request.
	Prepare func(context.Context, *event.Envelope) error

	// MaxBody bounds an inbound request body. Zero means
	// inbound.DefaultMaxBody.
	MaxBody int64

	// Tenant is stamped on every event, and files are stored under it.
	Tenant string

	// Files brings in the media on a message (voice notes, audio, images,
	// video, documents) as the event's attachments. Needs Tenant and
	// MediaToken.
	Files connectors.Files

	// MediaToken is the access token media downloads are made with: the
	// same token the API holds. It is sent to MediaHosts only, over HTTPS.
	MediaToken string
}

// Connector is the WhatsApp peer. It implements pkg/connector's Trigger,
// Sink and Actor faces, and net/http's Handler.
type Connector struct {
	cfg     Config
	gate    inbound.Gate
	pipe    inbound.Pipe
	once    *sink.Once
	fetcher *intake.Fetcher
}

var (
	_ connector.Trigger = (*Connector)(nil)
	_ connector.Sink    = (*Connector)(nil)
	_ connector.Actor   = (*Connector)(nil)
	_ http.Handler      = (*Connector)(nil)
)

// New returns a WhatsApp connector.
func New(cfg Config) (*Connector, error) {
	if cfg.API == nil {
		return nil, fmt.Errorf("whatsapp: config needs an API")
	}
	if cfg.PhoneNumberID != "" {
		if err := address.ValidWorkspace(cfg.PhoneNumberID); err != nil {
			return nil, fmt.Errorf("whatsapp: phone number id %q cannot be an address workspace: %w", cfg.PhoneNumberID, err)
		}
	}
	if cfg.Agent != "" && !mention.Valid(cfg.Agent) {
		return nil, fmt.Errorf("whatsapp: agent %q is not a usable agent name", cfg.Agent)
	}
	c := &Connector{cfg: cfg}
	if cfg.Files.Enabled() {
		if cfg.Tenant == "" || cfg.MediaToken == "" {
			return nil, fmt.Errorf("whatsapp: media intake needs a tenant to store files under and a token to download them with")
		}
		c.fetcher = intake.NewFetcher(cfg.Files.Store, mediaHosts(),
			intake.WithHeaders(mediaHeaders(cfg.MediaToken)),
			intake.WithMaxBytes(cfg.Files.Caps.MaxFileBytes))
	}
	c.gate = inbound.Gate{Verifier: cfg.Verifier, MaxBody: cfg.MaxBody}
	c.once = sink.NewOnce(poster{c: c}, body)
	return c, nil
}

// Address is the channel of the conversation between a business number and
// a person: whatsapp://<phone-number-id>/<wa-id>.
func Address(phoneNumberID, waID string) address.Address {
	return address.Address{Connector: Name, Workspace: phoneNumberID, Path: []string{waID}}
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return Name }

// Ingest implements connector.Trigger. It parks until ctx is cancelled while
// ServeHTTP publishes.
func (c *Connector) Ingest(ctx context.Context, publish connector.Publish) error {
	return c.pipe.Serve(ctx, publish)
}

// Ingesting reports whether an Ingest is running, for a readiness probe.
func (c *Connector) Ingesting() bool { return c.pipe.Ingesting() }

// ServeHTTP handles the webhook: the GET subscription handshake, or a
// signed POST of notifications.
//
// A POST may carry several messages. Each is published as its own event;
// if any publish fails the request answers 503 and Meta retries the whole
// notification, whose messages keep their ids, so the ones already
// published join their runs.
func (c *Connector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		c.serveHandshake(w, r)
		return
	}
	raw, ok := c.gate.Body(w, r)
	if !ok {
		return
	}
	var n notification
	if err := json.Unmarshal(raw, &n); err != nil {
		http.Error(w, "malformed payload", http.StatusBadRequest)
		return
	}
	for _, rcv := range c.eventsFrom(n) {
		if code := c.publish(r.Context(), rcv); code != 0 {
			http.Error(w, http.StatusText(code), code)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// publish brings in one event's media, applies Prepare and publishes it. It
// returns the status to fail the request with, or 0.
func (c *Connector) publish(ctx context.Context, rcv received) int {
	e := rcv.env
	origin, _ := address.Parse(e.Channel)
	ctx, cancel := files.Attach(ctx, c.cfg.Files, Name, e.Tenant, origin, &e, c.candidate(rcv.media))
	defer cancel()
	if c.cfg.Prepare != nil {
		if err := c.cfg.Prepare(ctx, &e); err != nil {
			return http.StatusForbidden
		}
	}
	if err := c.pipe.Send(ctx, e); err != nil {
		return inbound.SendStatus(err)
	}
	return 0
}

// serveHandshake answers Meta's subscription check. The verify token is a
// shared secret, so it is compared in constant time, and an unconfigured
// one refuses rather than matching an empty parameter.
func (c *Connector) serveHandshake(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	token := q.Get("hub.verify_token")
	if c.cfg.VerifyToken == "" || q.Get("hub.mode") != "subscribe" ||
		subtle.ConstantTimeCompare([]byte(token), []byte(c.cfg.VerifyToken)) != 1 {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(q.Get("hub.challenge")))
}

// Deliver implements connector.Sink.
func (c *Connector) Deliver(ctx context.Context, target address.Address, e event.Envelope) error {
	if err := c.reachable(target); err != nil {
		return err
	}
	return c.once.Deliver(ctx, target, e)
}

// Flush retries every message a failed send left owed. A server calls it on
// its own ticker and at shutdown.
func (c *Connector) Flush(ctx context.Context) error { return c.once.Flush(ctx) }

// reachable reports whether this connector can deliver to target.
func (c *Connector) reachable(target address.Address) error {
	if target.Connector != Name {
		return fmt.Errorf("%w: %s is not a whatsapp address", connector.ErrUndeliverable, target)
	}
	if c.cfg.PhoneNumberID != "" && target.Workspace != c.cfg.PhoneNumberID {
		return fmt.Errorf("%w: %s is for number %q, this connector serves %q",
			connector.ErrUndeliverable, target, target.Workspace, c.cfg.PhoneNumberID)
	}
	if len(target.Path) != 1 || target.Path[0] == "" {
		return fmt.Errorf("%w: %s does not name a person (want whatsapp://<phone-number-id>/<wa-id>)",
			connector.ErrUndeliverable, target)
	}
	return nil
}

// poster adapts the API to the send-once engine.
type poster struct{ c *Connector }

func (p poster) Post(ctx context.Context, target address.Address, body string) error {
	_, err := p.c.cfg.API.SendText(ctx, Text{PhoneNumberID: target.Workspace, To: target.Path[0], Body: body})
	return err
}

// body renders a moment of a run for WhatsApp.
func body(v render.View) string {
	return clamp(sink.Layout(markup{}, sink.MapText(v, chattext.WhatsApp)))
}

// clamp bounds a body to MaxBodyChars, by bytes and therefore by
// characters too.
func clamp(s string) string {
	if utf8.RuneCountInString(s) <= MaxBodyChars {
		return s
	}
	return sink.Clamp(s, MaxBodyChars)
}

// markup is WhatsApp's formatting: *bold*, _italic_, ```monospace```.
type markup struct{}

func (markup) Bold(s string) string   { return "*" + s + "*" }
func (markup) Italic(s string) string { return "_" + s + "_" }
func (markup) Code(s string) string   { return "```" + s + "```" }
func (markup) Item(s string) string   { return "- " + s }

// Link renders "text (uri)": WhatsApp links a bare URL and has no syntax
// for a labelled one.
func (markup) Link(text, uri string) string {
	if text == "" || text == uri {
		return uri
	}
	return text + " (" + uri + ")"
}

// Actions implements connector.Actor.
func (c *Connector) Actions() []connector.Action {
	return []connector.Action{{
		Name: "whatsapp_send_message",
		Description: "Send a WhatsApp message now to the person this run is answering, " +
			"such as a progress note on a long task. The final answer is delivered " +
			"automatically when the run ends and does not need to be sent.",
		Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "text": {"type": "string", "description": "Message body. WhatsApp formatting: *bold*, _italic_."}
  },
  "required": ["text"],
  "additionalProperties": false
}`),
		Invoke: c.sendMessage,
	}}
}

func (c *Connector) sendMessage(ctx context.Context, target address.Address, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return connector.Result{}, fmt.Errorf("whatsapp: whatsapp_send_message args: %w", err)
	}
	if args.Text == "" {
		return connector.Result{}, fmt.Errorf("whatsapp: whatsapp_send_message needs text")
	}
	if err := c.reachable(target); err != nil {
		return connector.Result{}, err
	}
	id, err := c.cfg.API.SendText(ctx, Text{PhoneNumberID: target.Workspace, To: target.Path[0], Body: clamp(args.Text)})
	if err != nil {
		return connector.Result{}, err
	}
	return connector.Result{
		Summary: "sent a WhatsApp message",
		Address: Address(target.Workspace, target.Path[0]).WithParam("message", id),
	}, nil
}

// Audio is an audio file to send.
type Audio struct {
	// Data is the file's content.
	Data []byte
	// MimeType is its type. Send audio/ogg (Opus) for WhatsApp to show it as
	// a voice note; audio/mpeg, audio/mp4, audio/aac and audio/amr are sent
	// as audio files.
	MimeType string
	// Name is a filename for the upload. Optional.
	Name string
}

// SendAudio uploads a and sends it to the conversation at target, returning
// the message id. It is how a host replies with a voice note.
func (c *Connector) SendAudio(ctx context.Context, target address.Address, a Audio) (string, error) {
	if err := c.reachable(target); err != nil {
		return "", err
	}
	if len(a.Data) == 0 || a.MimeType == "" {
		return "", fmt.Errorf("whatsapp: audio needs content and a media type")
	}
	media, err := c.cfg.API.UploadMedia(ctx, Upload{PhoneNumberID: target.Workspace, MimeType: a.MimeType, Filename: a.Name, Data: a.Data})
	if err != nil {
		return "", err
	}
	return c.cfg.API.SendAudio(ctx, AudioMessage{PhoneNumberID: target.Workspace, To: target.Path[0], MediaID: media})
}
