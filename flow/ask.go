package flow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/urmzd/orchagent/flow/bus"
	"github.com/urmzd/orchagent/flow/definition"
	"github.com/urmzd/orchagent/flow/event"
)

// ErrRunFailed is returned by Reply.Text when the run failed terminally.
var ErrRunFailed = errors.New("flow: run failed")

// ErrRunStopped is returned by Reply.Text when the run was asked to stop.
// The text the agent had answered so far is returned with it.
var ErrRunStopped = errors.New("flow: run stopped")

// Define stores an agent from its .agent.md content under the App's tenant
// and returns the stored record. Definitions are insert only: storing
// identical content again is a no-op, and storing different content under an
// existing name and version is definition.ErrExists.
func (a *App) Define(ctx context.Context, content string) (definition.Record, error) {
	if a.opt.Tenant == "" {
		return definition.Record{}, configf("define: the App has no tenant (WithTenant)")
	}
	return a.store.Put(ctx, definition.Record{Tenant: a.opt.Tenant, Content: content, CreatedBy: "orchagent"})
}

// Ask starts one run of agent on text and returns its Reply. The message is
// posted on a channel private to this call, so no membership is needed and
// no other subscriber sees it unless it subscribes to that channel. It uses
// the App's tenant (WithTenant). The App must be running (Run or
// RunExecution) for the run to make progress.
//
//	reply, err := app.Ask(ctx, "oncall", "api is down")
//	if err != nil { ... }
//	defer reply.Close()
//	answer, err := reply.Text(ctx)
func (a *App) Ask(ctx context.Context, agent, text string) (*Reply, error) {
	tenant := a.opt.Tenant
	if tenant == "" {
		return nil, configf("ask %s: the App has no tenant (WithTenant)", agent)
	}
	if agent == "" {
		return nil, configf("ask: no agent")
	}
	id, err := askID()
	if err != nil {
		return nil, fmt.Errorf("flow: ask %s: %w", agent, err)
	}
	trigger := event.Envelope{
		ID:      "ask-" + id,
		Source:  "orchagent:" + tenant,
		Type:    event.TypeMessagePosted,
		Channel: "orchagent://" + tenant + "/ask/" + id,
		Time:    time.Now().UTC(),
		Tenant:  tenant,
		Data:    event.MessagePosted{Request: event.Request{Text: text, Mentions: []string{agent}}},
	}
	// Subscribe before accepting so the first event cannot be missed.
	stream, err := a.broker.Subscribe(ctx, bus.Subscription{
		Tenant:   tenant,
		Channels: []string{trigger.Channel},
		Types:    []string{"run"},
	})
	if err != nil {
		return nil, fmt.Errorf("flow: ask %s: subscribe: %w", agent, err)
	}
	acc, err := a.rt.Accept(ctx, trigger, agent)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("flow: ask %s: %w", agent, err)
	}
	return &Reply{RunID: acc.RunID, Channel: trigger.Channel, stream: stream}, nil
}

// Reply is one run started by Ask. Read it once, with Events or Text, and
// Close it when done.
type Reply struct {
	// RunID identifies the run, for App.Status, App.Stop and App.Answer.
	RunID string
	// Channel is where the run's events are published.
	Channel string

	stream bus.Stream
	last   *event.Envelope
}

// Events yields the run's events in order, ending after its terminal event
// (run.completed, run.stopped, or a terminal run.failed). A failed attempt
// that will be retried is yielded and the iteration continues. An error ends
// the iteration.
func (r *Reply) Events(ctx context.Context) iter.Seq2[event.Envelope, error] {
	return func(yield func(event.Envelope, error) bool) {
		for r.last == nil {
			e, err := r.stream.Recv(ctx)
			if err != nil {
				yield(event.Envelope{}, err)
				return
			}
			if e.Run != r.RunID {
				continue
			}
			if terminal(e) {
				r.last = &e
				_ = r.stream.Close()
			}
			if !yield(e, nil) {
				return
			}
		}
	}
}

// Text waits for the run to end and returns its final answer. A terminal
// failure returns ErrRunFailed; a stop returns the partial answer and
// ErrRunStopped.
func (r *Reply) Text(ctx context.Context) (string, error) {
	for _, err := range r.Events(ctx) {
		if err != nil {
			return "", err
		}
	}
	if r.last == nil {
		return "", fmt.Errorf("flow: run %s: %w", r.RunID, bus.ErrClosed)
	}
	switch b := r.last.Data.(type) {
	case event.Completed:
		return b.Text, nil
	case event.Stopped:
		return b.Text, fmt.Errorf("run %s: %w: %s", r.RunID, ErrRunStopped, b.Reason)
	case event.Failed:
		return "", fmt.Errorf("run %s: %w: %s", r.RunID, ErrRunFailed, b.Error)
	}
	return "", fmt.Errorf("flow: run %s: unexpected terminal event %s", r.RunID, r.last.Type)
}

// Close releases the subscription. It is idempotent.
func (r *Reply) Close() error { return r.stream.Close() }

func terminal(e event.Envelope) bool {
	switch b := e.Data.(type) {
	case event.Completed, event.Stopped:
		return true
	case event.Failed:
		return b.Terminal
	}
	return false
}

func askID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
