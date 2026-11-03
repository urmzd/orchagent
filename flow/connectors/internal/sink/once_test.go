package sink_test

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/urmzd/orchagent/flow/address"
	"github.com/urmzd/orchagent/flow/connectors/internal/sink"
	"github.com/urmzd/orchagent/flow/event"
)

// poster is an in-memory surface with no edit: every Post is a new message.
type poster struct {
	mu    sync.Mutex
	sent  []string
	failN int
}

func (p *poster) Post(_ context.Context, _ address.Address, body string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failN > 0 {
		p.failN--
		return errSurface
	}
	p.sent = append(p.sent, body)
	return nil
}

func (p *poster) messages() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.sent...)
}

func deliverAll(t *testing.T, o *sink.Once, events []event.Envelope) {
	t.Helper()
	for _, e := range events {
		if err := o.Deliver(context.Background(), target, e); err != nil {
			t.Fatalf("Deliver(%d): %v", e.Seq, err)
		}
	}
}

func TestSendOnceSendsNothingWhileARunWorksAndTheAnswerOnceItEnds(t *testing.T) {
	p := &poster{}
	o := sink.NewOnce(p, layout)
	deliverAll(t, o, []event.Envelope{
		ev(1, event.TypeAccepted, `{}`),
		ev(2, event.TypeStarted, `{}`),
		ev(3, event.TypeToolCall, `{"id":"c1","name":"search","phase":"executing"}`),
		text(4, "Hello "),
		text(5, "there."),
	})
	if got := p.messages(); len(got) != 0 {
		t.Fatalf("a working run sent %q", got)
	}
	deliverAll(t, o, []event.Envelope{ev(6, event.TypeCompleted, `{}`)})
	got := p.messages()
	if len(got) != 1 || got[0] != "Hello there." {
		t.Fatalf("sent %q, want the answer alone, once", got)
	}
}

func TestSendOnceIsIdempotentUnderRedeliveryInAnyOrder(t *testing.T) {
	events := []event.Envelope{
		ev(1, event.TypeStarted, `{}`),
		text(2, "one "),
		text(3, "two"),
		ev(4, event.TypeCompleted, `{"text":"one two"}`),
	}
	for seed := int64(0); seed < 20; seed++ {
		p := &poster{}
		o := sink.NewOnce(p, layout)
		shuffled := append(append([]event.Envelope(nil), events...), events...)
		rand.New(rand.NewSource(seed)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		deliverAll(t, o, shuffled)
		got := p.messages()
		if len(got) != 1 || got[0] != "one two" {
			t.Fatalf("seed %d: sent %d messages %q, want 1", seed, len(got), got)
		}
	}
}

func TestSendOnceAsksAPersonOncePerQuestionAndThenAnswers(t *testing.T) {
	p := &poster{}
	o := sink.NewOnce(p, layout)
	park := ev(5, event.TypeParked, `{"question":{"id":"q1","kind":"approval","prompt":"Deploy to prod?","tool":{"name":"deploy","arguments":{"env":"prod"}}}}`)
	deliverAll(t, o, []event.Envelope{ev(1, event.TypeStarted, `{}`), text(2, "Checking."), park, park})
	got := p.messages()
	if len(got) != 1 || !strings.Contains(got[0], "Deploy to prod?") || strings.Contains(got[0], "Checking.") {
		t.Fatalf("after the park sent %q, want the question alone, once", got)
	}
	deliverAll(t, o, []event.Envelope{
		ev(6, event.TypeResumed, `{"answer":{"question_id":"q1","decision":"approve","by":"ana"}}`),
		ev(7, event.TypeParked, `{"question":{"id":"q2","kind":"reply","prompt":"Which region?"}}`),
		ev(8, event.TypeResumed, `{"answer":{"question_id":"q2","decision":"reply","note":"eu"}}`),
		text(9, " Deployed."),
		ev(10, event.TypeCompleted, `{}`),
		park,
	})
	got = p.messages()
	if len(got) != 3 || !strings.Contains(got[1], "Which region?") || got[2] != "Checking. Deployed." {
		t.Fatalf("sent %q", got)
	}
}

func TestSendOnceDoesNotAskAPersonAboutADelegation(t *testing.T) {
	p := &poster{}
	o := sink.NewOnce(p, layout)
	deliverAll(t, o, []event.Envelope{ev(9, event.TypeParked, `{"question":{"id":"q1","kind":"delegation","prompt":"deploy","tool":{"name":"tag_ops"},"child_run_id":"r-child"}}`)})
	if got := p.messages(); len(got) != 0 {
		t.Fatalf("a delegation sent %q", got)
	}
}

func TestSendOnceSaysWhyARunEndedWithoutAnAnswer(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  event.Envelope
		want string
	}{
		{"failed", ev(3, event.TypeFailed, `{"error":"provider timeout"}`), "failed: provider timeout"},
		{"stopped", ev(3, event.TypeStopped, `{"reason":"wrong repo","text":"I was"}`), "stopped: wrong repo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &poster{}
			o := sink.NewOnce(p, layout)
			deliverAll(t, o, []event.Envelope{ev(1, event.TypeStarted, `{}`), tc.end})
			got := p.messages()
			if len(got) != 1 || !strings.Contains(got[0], tc.want) {
				t.Fatalf("sent %q, want one message containing %q", got, tc.want)
			}
		})
	}
}

func TestSendOnceRetriesAFailedSendAndThenNeverAgain(t *testing.T) {
	p := &poster{failN: 1}
	o := sink.NewOnce(p, layout)
	ctx := context.Background()
	if err := o.Deliver(ctx, target, text(1, "hi")); err != nil {
		t.Fatal(err)
	}
	if err := o.Deliver(ctx, target, ev(2, event.TypeCompleted, `{}`)); !errors.Is(err, errSurface) {
		t.Fatalf("Deliver = %v, want the surface error", err)
	}
	if err := o.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := o.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := o.Deliver(ctx, target, ev(2, event.TypeCompleted, `{}`)); err != nil {
		t.Fatal(err)
	}
	if got := p.messages(); len(got) != 1 || got[0] != "hi" {
		t.Fatalf("sent %q, want one retry and nothing more", got)
	}
}
