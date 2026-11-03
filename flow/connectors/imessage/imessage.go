// Package imessage is the iMessage peer in the mesh, through a BlueBubbles
// server: trigger, sink and actor.
//
// Apple offers no iMessage API. BlueBubbles (bluebubbles.app) is an
// open-source server the operator runs on a Mac signed in to Messages; it
// relays new messages to a webhook and sends messages through a REST API.
// This connector speaks to that server, and nothing here talks to Apple.
//
// # Addresses
//
// A conversation is one chat on one server:
//
//	imessage://<server>/<chat-guid>
//	imessage://home/iMessage%3B-%3B+15551234567
//
// The workspace is a name the operator gives the server (Config.Server,
// subject-token safe: letters, digits, "-" and "_"), and the path is Apple's
// chat guid ("iMessage;-;+15551234567" one to one, "iMessage;+;chat123..."
// for a group), path-escaped by address.Address.String and unescaped by
// address.Parse. Address builds one.
//
// # Trigger
//
// BlueBubbles signs nothing, and its webhook settings take only a URL. So
// the shared secret goes in the URL the operator registers
// (https://host/connectors/imessage?secret=...) and is compared in constant
// time before the body is read, let alone parsed. Config.Verifier can add
// (or, behind a gateway that injects a header, replace) a header check.
// With neither, every request is refused. A secret in a URL is weaker than a
// MAC: it shows in access logs, and anyone holding it can post any body. Keep
// the server and this endpoint on TLS, and rotate the secret with the
// webhook.
//
// A new-message webhook publishes message.posted.v1 with the sending handle
// as Actor and the message guid as the event id. Messages this account sent
// (isFromMe, which includes every answer this connector sends), tapbacks,
// group events and every other webhook type are acknowledged and dropped.
// A one-to-one chat is a direct message (event.DirectMessage); in a group,
// an agent is addressed by "@name" (Config.Agents).
//
// # Voice notes and other files
//
// With Config.Files set, the files on a message are downloaded from the
// server (GET /api/v1/attachment/{guid}/download), stored under the tenant
// and carried as attachments, typed by their bytes. An audio message is an
// attachment of kind audio: Core Audio Format (audio/x-caf) as Messages
// records it, or a common format when the server converts it on download.
// The event's IMessageRef has Audio set. Transcription is the host's;
// Config.Prepare runs after the files are in.
//
// # Sink
//
// A BlueBubbles edit needs the Private API, and an edited iMessage shows its
// history to the reader, so this sink never streams. It sends one message
// when a run is waiting on a person and one when the run is over, as plain
// text (iMessage has no formatting), clamped to MaxBodyChars.
//
// # Actor
//
// imessage_send_message sends a message to the chat the run is answering,
// now. It cannot address another chat. SendAudio sends an audio file (a
// spoken reply) to a conversation, for the host.
package imessage

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/urmzd/orchagent/flow/address"
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
const Name = "imessage"

// MaxBodyChars bounds a sent message, in bytes. Apple documents no limit;
// this one keeps an answer readable on a phone and well inside what the
// relay handles.
const MaxBodyChars = 10000

// SecretParam is the query parameter the webhook secret travels in.
const SecretParam = "secret"

// Verifier authenticates a raw request body against its headers.
type Verifier interface {
	Verify(header http.Header, body []byte) error
}

// Config is everything the iMessage connector needs. New validates it.
type Config struct {
	// API talks to the BlueBubbles server. Required. NewHTTPAPI in
	// production, NewFake in tests, examples and local runs.
	API API

	// Server names the BlueBubbles server: the address workspace of every
	// chat it relays. Required, subject-token safe ("home", "office-mac").
	Server string

	// Secret is the shared secret the webhook URL carries as ?secret=. It
	// is compared in constant time before the body is read.
	Secret string

	// Verifier is an optional header check, applied after Secret. A gateway
	// that authenticates the server itself can pass one and leave Secret
	// empty. With neither, every request is refused.
	Verifier Verifier

	// Method is the send method: MethodAppleScript (the default) or
	// MethodPrivateAPI.
	Method string

	// Agent is the agent this account fronts, if one. It is listed as a
	// mention on every message.
	Agent string

	// Agents names the agents a message may address by "@name".
	Agents mention.Set

	// Prepare applies application policy to a verified event before it is
	// published, after its files are in. An error refuses the request.
	Prepare func(context.Context, *event.Envelope) error

	// MaxBody bounds an inbound request body. Zero means
	// inbound.DefaultMaxBody.
	MaxBody int64

	// Tenant is stamped on every event, and files are stored under it.
	Tenant string

	// Files brings in the files on a message (audio messages, images,
	// documents) as the event's attachments. Needs Tenant.
	Files connectors.Files
}

// Connector is the iMessage peer. It implements pkg/connector's Trigger,
// Sink and Actor faces, and net/http's Handler.
type Connector struct {
	cfg  Config
	gate inbound.Gate
	pipe inbound.Pipe
	once *sink.Once
}

var (
	_ connector.Trigger = (*Connector)(nil)
	_ connector.Sink    = (*Connector)(nil)
	_ connector.Actor   = (*Connector)(nil)
	_ http.Handler      = (*Connector)(nil)
)

// New returns an iMessage connector.
func New(cfg Config) (*Connector, error) {
	if cfg.API == nil {
		return nil, fmt.Errorf("imessage: config needs an API")
	}
	if err := address.ValidWorkspace(cfg.Server); err != nil {
		return nil, fmt.Errorf("imessage: server %q cannot be an address workspace: %w", cfg.Server, err)
	}
	if cfg.Agent != "" && !mention.Valid(cfg.Agent) {
		return nil, fmt.Errorf("imessage: agent %q is not a usable agent name", cfg.Agent)
	}
	switch cfg.Method {
	case "", MethodAppleScript, MethodPrivateAPI:
	default:
		return nil, fmt.Errorf("imessage: unknown send method %q", cfg.Method)
	}
	if cfg.Files.Enabled() && cfg.Tenant == "" {
		return nil, fmt.Errorf("imessage: file intake needs a tenant to store files under")
	}
	c := &Connector{cfg: cfg}
	var v inbound.Verifier
	switch {
	case cfg.Verifier != nil:
		v = cfg.Verifier
	case cfg.Secret != "":
		// The secret was checked from the URL before the gate ran.
		v = passed{}
	}
	c.gate = inbound.Gate{Verifier: v, MaxBody: cfg.MaxBody}
	c.once = sink.NewOnce(poster{c: c}, body)
	return c, nil
}

// passed is the gate's verifier when the URL secret is the only check: by
// the time the gate runs, ServeHTTP has already verified it.
type passed struct{}

func (passed) Verify(http.Header, []byte) error { return nil }

// Address is the channel of a chat on a server:
// imessage://<server>/<chat-guid>.
func Address(server, chat string) address.Address {
	return address.Address{Connector: Name, Workspace: server, Path: []string{chat}}
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return Name }

// Ingest implements connector.Trigger.
func (c *Connector) Ingest(ctx context.Context, publish connector.Publish) error {
	return c.pipe.Serve(ctx, publish)
}

// Ingesting reports whether an Ingest is running, for a readiness probe.
func (c *Connector) Ingesting() bool { return c.pipe.Ingesting() }

// ServeHTTP handles one BlueBubbles webhook.
func (c *Connector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if c.cfg.Secret != "" {
		got := r.URL.Query()[SecretParam]
		if len(got) != 1 || subtle.ConstantTimeCompare([]byte(got[0]), []byte(c.cfg.Secret)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	raw, ok := c.gate.Body(w, r)
	if !ok {
		return
	}
	var h hook
	if err := json.Unmarshal(raw, &h); err != nil {
		http.Error(w, "malformed payload", http.StatusBadRequest)
		return
	}
	rcv, ok := c.eventFrom(h)
	if !ok {
		w.WriteHeader(http.StatusOK)
		return
	}
	e := rcv.env
	origin, _ := address.Parse(e.Channel)
	ctx, cancel := files.Attach(r.Context(), c.cfg.Files, Name, e.Tenant, origin, &e, c.candidates(rcv.files))
	defer cancel()
	if c.cfg.Prepare != nil {
		if err := c.cfg.Prepare(ctx, &e); err != nil {
			http.Error(w, "request refused", http.StatusForbidden)
			return
		}
	}
	if err := c.pipe.Send(ctx, e); err != nil {
		http.Error(w, "not ready", inbound.SendStatus(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Deliver implements connector.Sink.
func (c *Connector) Deliver(ctx context.Context, target address.Address, e event.Envelope) error {
	if err := c.reachable(target); err != nil {
		return err
	}
	return c.once.Deliver(ctx, target, e)
}

// Flush retries every message a failed send left owed.
func (c *Connector) Flush(ctx context.Context) error { return c.once.Flush(ctx) }

func (c *Connector) reachable(target address.Address) error {
	if target.Connector != Name {
		return fmt.Errorf("%w: %s is not an imessage address", connector.ErrUndeliverable, target)
	}
	if target.Workspace != c.cfg.Server {
		return fmt.Errorf("%w: %s is on server %q, this connector serves %q",
			connector.ErrUndeliverable, target, target.Workspace, c.cfg.Server)
	}
	if len(target.Path) != 1 || target.Path[0] == "" {
		return fmt.Errorf("%w: %s does not name a chat (want imessage://<server>/<chat-guid>)",
			connector.ErrUndeliverable, target)
	}
	return nil
}

type poster struct{ c *Connector }

func (p poster) Post(ctx context.Context, target address.Address, body string) error {
	_, err := p.c.cfg.API.SendText(ctx, Text{Chat: target.Path[0], Message: body, Method: p.c.cfg.Method})
	return err
}

// body renders a moment of a run as plain text.
func body(v render.View) string {
	return sink.Clamp(sink.Layout(markup{}, sink.MapText(v, chattext.Plain)), MaxBodyChars)
}

// markup is plain text: iMessage renders no formatting.
type markup struct{}

func (markup) Bold(s string) string   { return s }
func (markup) Italic(s string) string { return s }
func (markup) Code(s string) string   { return s }
func (markup) Item(s string) string   { return "- " + s }

func (markup) Link(text, uri string) string {
	if text == "" || text == uri {
		return uri
	}
	return text + " (" + uri + ")"
}

// Actions implements connector.Actor.
func (c *Connector) Actions() []connector.Action {
	return []connector.Action{{
		Name: "imessage_send_message",
		Description: "Send an iMessage now to the chat this run is answering, such as " +
			"a progress note on a long task. The final answer is delivered " +
			"automatically when the run ends and does not need to be sent.",
		Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "text": {"type": "string", "description": "Message body, plain text."}
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
		return connector.Result{}, fmt.Errorf("imessage: imessage_send_message args: %w", err)
	}
	if args.Text == "" {
		return connector.Result{}, fmt.Errorf("imessage: imessage_send_message needs text")
	}
	if err := c.reachable(target); err != nil {
		return connector.Result{}, err
	}
	guid, err := c.cfg.API.SendText(ctx, Text{Chat: target.Path[0], Message: sink.Clamp(args.Text, MaxBodyChars), Method: c.cfg.Method})
	if err != nil {
		return connector.Result{}, err
	}
	sent := Address(target.Workspace, target.Path[0])
	if guid != "" {
		sent = sent.WithParam("message", guid)
	}
	return connector.Result{Summary: "sent an iMessage", Address: sent}, nil
}

// Audio is an audio file to send.
type Audio struct {
	Data []byte
	// MimeType is its type: audio/x-caf, audio/mp4 (m4a) or audio/mpeg.
	MimeType string
	// Name is a filename. An audio message is conventionally
	// "Audio Message.caf".
	Name string
	// Voice sends it as an audio message (a voice note) rather than a file.
	// BlueBubbles honours it with the Private API (Config.Method).
	Voice bool
}

// SendAudio sends a to the chat at target and returns the message guid, or
// "" when the server reported none. It is how a host replies with a voice
// note.
func (c *Connector) SendAudio(ctx context.Context, target address.Address, a Audio) (string, error) {
	if err := c.reachable(target); err != nil {
		return "", err
	}
	if len(a.Data) == 0 {
		return "", fmt.Errorf("imessage: audio needs content")
	}
	name := a.Name
	if name == "" {
		name = "Audio Message.caf"
	}
	return c.cfg.API.SendAttachment(ctx, Attachment{
		Chat: target.Path[0], Name: name, MimeType: a.MimeType, Data: a.Data, Audio: a.Voice, Method: c.cfg.Method,
	})
}
