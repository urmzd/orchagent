package flow

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/urmzd/orchagent/flow/bus"
	"github.com/urmzd/orchagent/flow/event"
	"github.com/urmzd/orchagent/flow/runtime"
	"github.com/urmzd/orchagent/flow/session"
	"github.com/urmzd/orchagent/flow/thread"
)

// Session is one agent's continuing conversation. A handle holds no worker or
// subscription; each Stream belongs to its caller and must be closed.
type Session struct {
	app    *App
	record session.Record
}

// Session opens or continues (parent, agent, label) in the App's tenant.
// Parent is a place URI or another session ID. Hosts enforce principal access
// before opening a handle; agent calls use the pinned runtime call policy.
func (a *App) Session(ctx context.Context, o session.Open) (*Session, error) {
	if a.opt.Tenant == "" {
		return nil, configf("session: the App has no tenant (WithTenant)")
	}
	if o.Parent == "" {
		id, err := askID()
		if err != nil {
			return nil, err
		}
		o.Parent = "console://" + a.opt.Tenant + "/" + id
	}
	if _, err := a.defs.Resolve(ctx, a.opt.Tenant, o.Agent); err != nil {
		return nil, err
	}
	r, err := a.sessions.Open(ctx, a.opt.Tenant, o)
	if err != nil {
		return nil, err
	}
	return &Session{app: a, record: r}, nil
}

// OpenSession gets a previously created session, scoped to the App's tenant.
func (a *App) OpenSession(ctx context.Context, id string) (*Session, error) {
	r, err := a.sessions.Get(ctx, a.opt.Tenant, id)
	if err != nil {
		return nil, err
	}
	return &Session{app: a, record: r}, nil
}
func (s *Session) Record() session.Record { return s.record }
func (a *App) Sessions() session.Store    { return a.sessions }

// Post accepts a turn and returns its durable run identity. Continuation uses
// the same journal even when this handle was reopened in a different process.
func (s *Session) Post(ctx context.Context, text string) (runtime.Acceptance, error) {
	if strings.TrimSpace(text) == "" {
		return runtime.Acceptance{}, fmt.Errorf("%w: empty message", session.ErrInvalid)
	}
	id, err := askID()
	if err != nil {
		return runtime.Acceptance{}, err
	}
	r := s.record
	actor, _ := event.ActorFrom(ctx)
	e := event.Envelope{ID: "post-" + id, Source: r.Key.Parent, Type: event.TypeMessagePosted, Channel: r.Channel, Time: time.Now().UTC(), Tenant: r.Key.Tenant,
		Data: event.MessagePosted{Request: event.Request{Text: text, Mentions: []string{r.Key.Agent}, Actor: actor}}}
	return s.app.rt.AcceptSession(ctx, e, r.Key.Agent, r)
}

// Stream tails this session and its children. Cursors are per run because run
// sequence numbers restart for every turn; a single maximum would lose events.
// The retained bus window is replayed and filtered by the caller's cursor.
func (s *Session) Stream(ctx context.Context, from map[string]uint64) (bus.Stream, error) {
	channels := []string{s.record.Channel, "session://" + url.PathEscape(s.record.Key.Tenant)}
	stream, err := s.app.broker.Subscribe(ctx, bus.Subscription{Tenant: s.record.Key.Tenant, Channels: channels, Types: []string{"run"}})
	if err != nil {
		return nil, err
	}
	cursors := map[string]uint64{}
	for id, seq := range from {
		cursors[id] = seq
	}
	return &sessionStream{Stream: stream, app: s.app, record: s.record, from: cursors}, nil
}

type sessionStream struct {
	bus.Stream
	app    *App
	record session.Record
	from   map[string]uint64
}

func (s *sessionStream) Recv(ctx context.Context) (event.Envelope, error) {
	for {
		e, err := s.Stream.Recv(ctx)
		if err != nil {
			return e, err
		}
		if e.Seq <= s.from[e.Run] {
			continue
		}
		// Several agents can answer one surface; scope the stream to this tree.
		if e.Session == s.record.ID || e.ParentSession == s.record.ID {
			s.from[e.Run] = e.Seq
			return e, nil
		}
	}
}

// Read returns the next bounded page after since. The cursor is the last
// returned journal sequence; no caller can request an unbounded transcript.
func (s *Session) Read(ctx context.Context, since uint64) ([]thread.Entry, error) {
	return thread.ReadWindow(ctx, s.app.threads, s.record.ThreadKey(), since, session.MaxPage)
}

// Summary is an extractive, deterministic catch-up. It makes no hidden model
// call and cites each entry's sequence and run rather than inventing findings.
func (s *Session) Summary(ctx context.Context, since uint64) (string, error) {
	entries, err := s.Read(ctx, since)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	for _, e := range entries {
		body := e.Text
		if len([]rune(body)) > 400 {
			body = string([]rune(body)[:400]) + "…"
		}
		fmt.Fprintf(&out, "[%d] run=%s %s: %s\n", e.Seq, e.RunID, e.Role, body)
	}
	return out.String(), nil
}
func (s *Session) Children(ctx context.Context, n int) ([]session.Record, error) {
	return s.app.sessions.List(ctx, s.record.Key.Tenant, session.Filter{Parent: s.record.ID, Limit: n})
}
func (s *Session) Link(ctx context.Context, target string) error {
	return s.app.sessions.Link(ctx, s.record.Key.Tenant, s.record.ID, session.Link{Target: target, Kind: "context"})
}

// Usage is the live session-tree total, including children that finish after
// their parent run. Immutable run outputs remain snapshots of their completion.
func (s *Session) Usage(ctx context.Context) (event.RunUsage, error) {
	store, ok := s.app.sessions.(session.UsageStore)
	if !ok {
		return event.RunUsage{}, fmt.Errorf("%w: usage store unavailable", session.ErrInvalid)
	}
	return store.Usage(ctx, s.record.Key.Tenant, s.record.ID)
}
