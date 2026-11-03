package sink

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/urmzd/orchagent/flow/address"
	"github.com/urmzd/orchagent/flow/connectors/internal/render"
	"github.com/urmzd/orchagent/flow/event"
)

// Poster is a surface that can send a message and never change it again:
// WhatsApp, which has no edit at all, and iMessage, whose edit is unreliable
// through a relay and shows the reader an "Edited" history anyway.
type Poster interface {
	Post(ctx context.Context, target address.Address, body string) error
}

// Once is the send-once engine: the counterpart of Engine for a surface that
// cannot edit.
//
// A streaming run cannot be shown on such a surface without sending a
// message per edit, which is the flood Engine exists to prevent. So Once
// sends nothing while a run works, and sends one message at each moment a
// person is owed one:
//
//   - the run is waiting on a person (a question or an approval, its own or
//     one a delegated run is waiting on), sent once per question;
//   - the run is over (completed, failed or stopped), sent once with the
//     final answer.
//
// A run waiting on another run (a delegation) is waiting on nobody a person
// can help, and sends nothing.
//
// Idempotency is the same arithmetic as Engine's, applied to moments rather
// than edits: the document is rebuilt from every event in any order, each
// moment has a key, and a moment whose key was already sent is never sent
// again, however many times its events are redelivered. A failed send leaves
// the moment owed, and the next Deliver or Flush retries it.
//
// The end is sent as soon as the run is terminal. Its answer is the text the
// terminal event carries (run.completed.v1 and run.stopped.v1 carry the whole
// answer), so the message does not depend on every text delta having arrived
// first; a terminal event with no text is sent from the deltas assembled so
// far, and an empty one waits for text.
//
// What is remembered is in memory and bounded (WithMaxRuns), as with Engine:
// a process restart, or an eviction, forgets which moments were sent, and a
// redelivery after that sends the moment again.
type Once struct {
	poster  Poster
	render  Renderer
	maxRuns int

	mu    sync.Mutex
	byKey map[string]*list.Element
	order *list.List
}

// NewOnce returns a send-once engine that renders each moment with r and
// sends it with p. Of the Engine options only WithMaxRuns applies.
func NewOnce(p Poster, r Renderer, opts ...Option) *Once {
	e := &Engine{maxRuns: DefaultMaxRuns}
	for _, opt := range opts {
		opt(e)
	}
	return &Once{poster: p, render: r, maxRuns: e.maxRuns, byKey: make(map[string]*list.Element), order: list.New()}
}

type onceState struct {
	key    string
	target address.Address

	mu   sync.Mutex
	doc  *render.Doc
	sent map[string]bool
}

// Moment is one message a person is owed: a key that identifies it across
// redeliveries, and the part of the run's document it shows.
type Moment struct {
	Key  string
	View render.View
}

// Moments lists what a person is owed for the run as v shows it, in the
// order they are sent. The views carry only what the moment is about: a
// question carries the question and not the partial answer, and the end
// carries the answer, its sources and what changed, and never the agent's
// header, tool calls or reasoning, which are noise in a chat.
func Moments(v render.View) []Moment {
	var out []Moment
	if v.Terminal() {
		return append(out, Moment{Key: "end", View: render.View{
			RunID: v.RunID, Channel: v.Channel, Tenant: v.Tenant, Status: v.Status,
			Error: v.Error, Reason: v.Reason, Text: v.Text, Citations: v.Citations, Actions: v.Actions,
		}})
	}
	if q := v.Question; q != nil && v.Status == render.StatusParked && q.Kind != "delegation" {
		out = append(out, Moment{Key: "ask\x00" + questionKey(q), View: render.View{
			RunID: v.RunID, Channel: v.Channel, Tenant: v.Tenant, Status: v.Status, Question: q,
		}})
	}
	if q := v.Waiting; q != nil {
		out = append(out, Moment{Key: "wait\x00" + questionKey(q), View: render.View{
			RunID: v.RunID, Channel: v.Channel, Tenant: v.Tenant, Status: v.Status, Waiting: q,
		}})
	}
	return out
}

// questionKey identifies a question by what it asks and of whom.
func questionKey(q *render.Question) string {
	return strings.Join([]string{q.RunID, q.Kind, q.Prompt, q.Tool, q.Args, strings.Join(q.Options, "\x01")}, "\x00")
}

// Deliver applies e and sends whatever moment the run now owes.
func (o *Once) Deliver(ctx context.Context, target address.Address, e event.Envelope) error {
	if o.render == nil || o.poster == nil {
		return fmt.Errorf("sink: send-once engine is not configured with a poster and a renderer")
	}
	st := o.state(key(e.Run, e.Channel), target)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.doc.Apply(e)
	return o.send(ctx, st)
}

// Flush retries every moment a failed send left owed.
func (o *Once) Flush(ctx context.Context) error {
	o.mu.Lock()
	states := make([]*onceState, 0, len(o.byKey))
	for _, el := range o.byKey {
		states = append(states, el.Value.(*onceState))
	}
	o.mu.Unlock()

	var errs []error
	for _, st := range states {
		st.mu.Lock()
		if err := o.send(ctx, st); err != nil {
			errs = append(errs, err)
		}
		st.mu.Unlock()
	}
	return errors.Join(errs...)
}

// send posts each owed moment that was not sent yet. The caller holds st.mu.
func (o *Once) send(ctx context.Context, st *onceState) error {
	for _, m := range Moments(st.doc.View()) {
		if st.sent[m.Key] {
			continue
		}
		body := o.render(m.View)
		if body == "" {
			// Nothing to say yet: a terminal event that carried no text and
			// arrived ahead of the deltas. The moment stays owed until the
			// document has something to send.
			continue
		}
		if err := o.poster.Post(ctx, st.target, body); err != nil {
			return fmt.Errorf("sink: send message for run %s on %s: %w", m.View.RunID, st.target, err)
		}
		st.sent[m.Key] = true
	}
	return nil
}

func (o *Once) state(k string, target address.Address) *onceState {
	o.mu.Lock()
	defer o.mu.Unlock()
	if el, ok := o.byKey[k]; ok {
		o.order.MoveToFront(el)
		return el.Value.(*onceState)
	}
	st := &onceState{key: k, target: target, doc: render.New(), sent: map[string]bool{}}
	o.byKey[k] = o.order.PushFront(st)
	for o.order.Len() > o.maxRuns {
		oldest := o.order.Back()
		o.order.Remove(oldest)
		delete(o.byKey, oldest.Value.(*onceState).key)
	}
	return st
}
