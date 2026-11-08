package flow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/urmzd/orchagent/durable"
	drun "github.com/urmzd/orchagent/durable/run"

	"github.com/urmzd/orchagent/flow/bus"
	"github.com/urmzd/orchagent/flow/event"
	"github.com/urmzd/orchagent/flow/runtime"
	"github.com/urmzd/orchagent/flow/session"
	"github.com/urmzd/orchagent/flow/thread"
)

// ErrRunFailed is returned by Session.Wait when the run failed terminally.
var ErrRunFailed = errors.New("flow: run failed")

// ErrRunStopped is returned by Session.Wait when the run was asked to stop.
// The text the agent had answered so far is returned with it.
var ErrRunStopped = errors.New("flow: run stopped")

// Session is one agent's continuing conversation. A handle holds no worker or
// subscription; each Stream belongs to its caller and must be closed.
//
//	s, err := app.Session(ctx, session.Open{Agent: "oncall"})
//	if err != nil { ... }
//	accepted, err := s.Post(ctx, "api is down")
//	if err != nil { ... }
//	answer, err := s.Wait(ctx, accepted.RunID)
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
		id, err := newID()
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
// Wait returns the run's answer.
func (s *Session) Post(ctx context.Context, text string) (runtime.Acceptance, error) {
	if strings.TrimSpace(text) == "" {
		return runtime.Acceptance{}, fmt.Errorf("%w: empty message", session.ErrInvalid)
	}
	id, err := newID()
	if err != nil {
		return runtime.Acceptance{}, err
	}
	r := s.record
	actor, _ := event.ActorFrom(ctx)
	e := event.Envelope{ID: "post-" + id, Source: r.Key.Parent, Type: event.TypeMessagePosted, Channel: r.Channel, Time: time.Now().UTC(), Tenant: r.Key.Tenant,
		Data: event.MessagePosted{Request: event.Request{Text: text, Mentions: []string{r.Key.Agent}, Actor: actor}}}
	return s.app.rt.AcceptSession(ctx, e, r.Key.Agent, r)
}

// Wait blocks until the run runID of this session ends and returns its final
// answer. A terminal failure returns ErrRunFailed; a stop returns the partial
// answer and ErrRunStopped. A failed attempt that will be retried does not end
// the wait, and neither does a run parked on a question; ctx bounds it. A run
// that is not this session's is ErrNotFound.
//
// The run's ledger record decides how it ended, so Wait is race free after
// Post and still answers once the run has left the bus's retained window or
// exhausted its retries with no terminal event. The run's terminal event on
// the bus, when it arrives first, only answers sooner.
func (s *Session) Wait(ctx context.Context, runID string) (string, error) {
	if runID == "" {
		return "", fmt.Errorf("%w: no run to wait for", session.ErrInvalid)
	}
	if err := s.owns(ctx, runID); err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := s.app.broker.Subscribe(ctx, bus.Subscription{Tenant: s.record.Key.Tenant, Channels: s.channels(), Types: []string{"run"}, Run: runID})
	if err != nil {
		return "", fmt.Errorf("flow: wait for run %s: %w", runID, err)
	}
	defer func() { _ = stream.Close() }()
	type ending struct {
		text string
		err  error
	}
	ends := make(chan ending, 2)
	go func() { // The fast path. A stream that ends early leaves the ledger to answer.
		for {
			e, err := stream.Recv(ctx)
			if err != nil {
				return
			}
			if e.Run != runID || e.Session != s.record.ID || !terminal(e) {
				continue
			}
			switch b := e.Data.(type) {
			case event.Completed:
				ends <- ending{b.Text, nil}
			case event.Stopped:
				ends <- ending{b.Text, fmt.Errorf("flow: run %s: %w: %s", runID, ErrRunStopped, b.Reason)}
			case event.Failed:
				ends <- ending{"", fmt.Errorf("flow: run %s: %w: %s", runID, ErrRunFailed, b.Error)}
			}
			return
		}
	}()
	go func() { // The authority.
		out, err := s.app.rt.Result(ctx, runID)
		var failed *durable.RunFailedError
		switch {
		case errors.As(err, &failed):
			ends <- ending{"", fmt.Errorf("flow: run %s: %w: %s", runID, ErrRunFailed, failed.Message)}
		case err != nil:
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			ends <- ending{"", fmt.Errorf("flow: wait for run %s: %w", runID, err)}
		case out.Stopped != nil:
			ends <- ending{out.Text, fmt.Errorf("flow: run %s: %w: %s", runID, ErrRunStopped, out.Stopped.Reason)}
		default:
			ends <- ending{out.Text, nil}
		}
	}()
	end := <-ends
	return end.text, end.err
}

// owns reports ErrNotFound unless runID is a run of this session, read from
// its ledger record: another tenant's or another session's run is
// indistinguishable from one that does not exist.
func (s *Session) owns(ctx context.Context, runID string) error {
	rec, err := s.app.ledger.GetRun(ctx, runID)
	if errors.Is(err, drun.ErrNotFound) {
		return fmt.Errorf("%w: run %q", ErrNotFound, runID)
	}
	if err != nil {
		return fmt.Errorf("flow: read run %q: %w", runID, err)
	}
	var in runtime.Input
	if err := json.Unmarshal(rec.Input, &in); err != nil {
		return fmt.Errorf("flow: decode run %q input: %w", runID, err)
	}
	if in.Trigger.Tenant != s.record.Key.Tenant || in.Session == nil || in.Session.ID != s.record.ID {
		return fmt.Errorf("%w: run %q", ErrNotFound, runID)
	}
	return nil
}

// Stream tails this session and its children. Cursors are per run because run
// sequence numbers restart for every turn; a single maximum would lose events.
// The retained bus window is replayed and filtered by the caller's cursor.
func (s *Session) Stream(ctx context.Context, from map[string]uint64) (bus.Stream, error) {
	stream, err := s.app.broker.Subscribe(ctx, bus.Subscription{Tenant: s.record.Key.Tenant, Channels: s.channels(), Types: []string{"run"}})
	if err != nil {
		return nil, err
	}
	cursors := map[string]uint64{}
	for id, seq := range from {
		cursors[id] = seq
	}
	return &sessionStream{Stream: stream, app: s.app, record: s.record, from: cursors}, nil
}

// channels are where this session's runs and its children's runs publish: the
// session's own channel, and the session scheme children get by default.
func (s *Session) channels() []string {
	return []string{s.record.Channel, "session://" + url.PathEscape(s.record.Key.Tenant)}
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

// Link records that this session refers to the session target, as kind
// session.LinkContext or session.LinkEvidence. Any other kind, an unknown
// target or the session itself is refused by the store. Linking twice is a
// no-op, and a link grants no access.
func (s *Session) Link(ctx context.Context, target, kind string) error {
	return s.app.sessions.Link(ctx, s.record.Key.Tenant, s.record.ID, session.Link{Target: target, Kind: kind})
}

// Links returns at most n of this session's links, oldest first; n <= 0 or
// above session.MaxPage reads session.MaxPage.
func (s *Session) Links(ctx context.Context, n int) ([]session.Link, error) {
	return s.app.sessions.Links(ctx, s.record.Key.Tenant, s.record.ID, n)
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

// terminal reports whether e ends its run: completed, stopped, or a failure
// that will not be retried.
func terminal(e event.Envelope) bool {
	switch b := e.Data.(type) {
	case event.Completed, event.Stopped:
		return true
	case event.Failed:
		return b.Terminal
	}
	return false
}

func newID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
