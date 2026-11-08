package flow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/orchagent/durable"
	"github.com/urmzd/orchagent/durable/worker"
	"github.com/urmzd/orchagent/flow/bus"
	"github.com/urmzd/orchagent/flow/event"
	"github.com/urmzd/orchagent/flow/runtime"
	"github.com/urmzd/orchagent/flow/session"
	"github.com/urmzd/orchagent/flow/thread"
	"github.com/urmzd/orchagent/flow/turn"
	"github.com/urmzd/orchagent/flow/turn/native"
	"github.com/urmzd/orchagent/flow/turn/native/agenttest"
	saigetypes "github.com/urmzd/orchagent/flow/turn/native/saigetypes"
)

func TestSessionContinuesHistoryAndFreshKeepsItSeparate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	app, err := New(ctx, WithTenant("tenant"), WithTurnRuntime(turn.Func(func(ctx context.Context, _ turn.Pinned, in turn.Input, emit func(context.Context, event.Body) error) (turn.Result, error) {
		return turn.Result{Text: fmt.Sprintf("history=%d", len(in.History))}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close() }()
	_, err = app.Define(ctx, "---\napiVersion: saige/v1\nname: helper\nversion: 1.0.0\nmodel: offline/echo\nlimits:\n  budget:\n    max_requests: 20\n---\nHelp.")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- app.RunExecution(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v", err)
		}
	}()
	s, err := app.Session(ctx, session.Open{Parent: "console://tenant/chat", Agent: "helper"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		acc, err := s.Post(ctx, "help")
		if err != nil {
			t.Fatal(err)
		}
		out, err := app.Runtime().Result(ctx, acc.RunID)
		if err != nil {
			t.Fatal(err)
		}
		if out.Text != fmt.Sprintf("history=%d", 2*i) {
			t.Fatalf("turn %d: %s", i, out.Text)
		}
	}
	reopened, err := app.Session(ctx, session.Open{Parent: "console://tenant/chat", Agent: "helper"})
	if err != nil || reopened.Record().ID != s.Record().ID {
		t.Fatalf("continue: %v", err)
	}
	entries, err := reopened.Read(ctx, 2)
	if err != nil || len(entries) != 2 {
		t.Fatalf("cursor: %v %v", entries, err)
	}
	summary, err := reopened.Summary(ctx, 2)
	if err != nil || !strings.Contains(summary, "[3]") {
		t.Fatalf("summary: %q %v", summary, err)
	}
	fresh, err := app.Session(ctx, session.Open{Parent: "console://tenant/chat", Agent: "helper", Fresh: true})
	if err != nil {
		t.Fatal(err)
	}
	acc, err := fresh.Post(ctx, "again")
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Runtime().Result(ctx, acc.RunID)
	if err != nil || out.Text != "history=0" {
		t.Fatalf("fresh: %+v %v", out, err)
	}
}

func TestSessionFanOutAndListenFreeTheOnlyWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	parentKey := session.Key{Tenant: "fanout", Parent: "console://fanout/test", Agent: "lead"}
	child := func(label string) string {
		return (session.Key{Tenant: "fanout", Parent: parentKey.ID(), Agent: "helper", Label: label}).ID()
	}
	model := &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{
		sessionResponse(agenttest.ToolCallResponse("c1", "session_call", map[string]any{"agent": "helper", "text": "one", "label": "a", "wait": false})),
		sessionResponse(agenttest.ToolCallResponse("c2", "session_call", map[string]any{"agent": "helper", "text": "two", "label": "b", "wait": false})),
		sessionResponse(agenttest.ToolCallResponse("c3", "session_listen", map[string]any{"sessions": []any{child("a"), child("b")}})),
		sessionResponse(agenttest.TextResponse("both checked")),
	}}
	helper := &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{sessionResponse(agenttest.TextResponse("one checked")), sessionResponse(agenttest.TextResponse("two checked"))}}
	app, err := New(ctx, WithTenant("fanout"), WithConcurrency(1), WithModelProvider("test", func(s native.Spec) (saigetypes.Provider, error) {
		if s.Name == "lead" {
			return model, nil
		}
		return helper, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close() }()
	for _, doc := range []string{
		"---\napiVersion: saige/v1\nname: helper\nversion: 1.0.0\nmodel: test/model\nlimits:\n  budget:\n    max_requests: 20\n---\nHelp.",
		"---\napiVersion: saige/v1\nname: lead\nversion: 1.0.0\nmodel: test/model\nlimits:\n  budget:\n    max_requests: 6\nsubagents:\n  - ref: helper\n    budget:\n      max_requests: 1\n---\nLead.",
	} {
		if _, err := app.Define(ctx, doc); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- app.RunExecution(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v", err)
		}
	}()
	s, err := app.Session(ctx, session.Open{Parent: parentKey.Parent, Agent: "lead"})
	if err != nil {
		t.Fatal(err)
	}
	acc, err := s.Post(ctx, "check both")
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Runtime().Result(ctx, acc.RunID)
	if err != nil || out.Text != "both checked" {
		t.Fatalf("fanout with one worker: %+v %v", out, err)
	}
	children, err := s.Children(ctx, 10)
	if err != nil || len(children) != 2 {
		t.Fatalf("children: %v %v", children, err)
	}
	for _, c := range children {
		h, err := app.OpenSession(ctx, c.ID)
		if err != nil {
			t.Fatal(err)
		}
		entries, err := h.Read(ctx, 0)
		if err != nil || len(entries) != 2 {
			t.Fatalf("child transcript: %v %v", entries, err)
		}
	}
}

func TestSessionPostDeniesMissingBudgetBeforeStartingRun(t *testing.T) {
	ctx := context.Background()
	app, err := New(ctx, WithTenant("t"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close() }()
	if _, err := app.Define(ctx, "---\napiVersion: saige/v1\nname: helper\nversion: 1.0.0\nmodel: offline/echo\n---\nHelp."); err != nil {
		t.Fatal(err)
	}
	s, err := app.Session(ctx, session.Open{Agent: "helper"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Post(ctx, "help"); !errors.Is(err, session.ErrBudget) {
		t.Fatalf("zero budget admitted: %v", err)
	}
	runs, err := app.sessions.Runs(ctx, "t", s.Record().ID, 10)
	if err != nil || len(runs) != 0 {
		t.Fatal(runs, err)
	}
}

func TestSessionMessageListenPinsCursorAndReleasesWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rootKey := session.Key{Tenant: "message-wait", Parent: "console://message-wait/test", Agent: "lead"}
	childKey := session.Key{Tenant: rootKey.Tenant, Parent: rootKey.ID(), Agent: "helper"}
	model := &agenttest.ScriptedProvider{Responses: [][]saigetypes.Delta{
		sessionResponse(agenttest.ToolCallResponse("listen", "session_listen", map[string]any{"sessions": []any{childKey.ID()}, "until": "message"})),
		sessionResponse(agenttest.TextResponse("new message read")),
	}}
	app, err := New(ctx, WithTenant(rootKey.Tenant), WithConcurrency(1), WithWorkerOptions(worker.WithJanitorEvery(20*time.Millisecond), worker.WithLeaseTTL(100*time.Millisecond)), WithModelProvider("test", func(native.Spec) (saigetypes.Provider, error) { return model, nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close() }()
	for _, name := range []string{"lead", "helper"} {
		if _, err := app.Define(ctx, "---\napiVersion: saige/v1\nname: "+name+"\nversion: 1.0.0\nmodel: test/model\nlimits:\n  budget:\n    max_requests: 5\n---\nHelp."); err != nil {
			t.Fatal(err)
		}
	}
	parent, err := app.Session(ctx, session.Open{Parent: rootKey.Parent, Agent: "lead"})
	if err != nil {
		t.Fatal(err)
	}
	child, err := app.Session(ctx, session.Open{Parent: parent.Record().ID, Agent: "helper"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := app.threads.Append(ctx, child.Record().ThreadKey(), thread.Entry{Role: thread.RoleUser, Text: "old", RunID: "old"}); err != nil {
		t.Fatal(err)
	}
	stream, err := parent.Stream(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx, RoleExecution) }()
	defer func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v", err)
		}
	}()
	acc, err := parent.Post(ctx, "wait for news")
	if err != nil {
		t.Fatal(err)
	}
	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if p, ok := e.Data.(event.Parked); ok && p.Question != nil && p.Question.Tool != nil && p.Question.Tool.Name == "session_listen" {
			break
		}
	}
	// No child run exists: this listener is waiting for a future message. Its
	// pinned cursor must survive janitor replays instead of moving to the new head.
	if _, err := app.threads.Append(ctx, child.Record().ThreadKey(), thread.Entry{Role: thread.RoleUser, Text: "new evidence", RunID: "new"}); err != nil {
		t.Fatal(err)
	}
	out, err := app.Runtime().Result(ctx, acc.RunID)
	if err != nil || out.Text != "new message read" {
		t.Fatal(out, err)
	}
	if len(model.Calls) != 2 {
		t.Fatalf("replayed model while parked: %d calls", len(model.Calls))
	}
}

func sessionResponse(ds []saigetypes.Delta) []saigetypes.Delta {
	return append(ds, saigetypes.UsageDelta{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2})
}

func TestSessionRedeliveryRetainsAcceptedBudgetAfterDefinitionChanges(t *testing.T) {
	ctx := context.Background()
	app, err := New(ctx, WithTenant("t"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close() }()
	define := func(version, cap int) {
		budget := ""
		if cap > 0 {
			budget = fmt.Sprintf("limits:\n  budget:\n    max_requests: %d\n", cap)
		}
		_, err := app.Define(ctx, fmt.Sprintf("---\napiVersion: saige/v1\nname: helper\nversion: %d.0.0\nmodel: offline/echo\n%s---\nHelp.", version, budget))
		if err != nil {
			t.Fatal(err)
		}
	}
	define(1, 5)
	s, err := app.Session(ctx, session.Open{Agent: "helper"})
	if err != nil {
		t.Fatal(err)
	}
	e := event.Envelope{Tenant: "t", ID: "stable-event", Source: s.Record().Key.Parent, Channel: s.Record().Channel, Type: event.TypeMessagePosted, Time: time.Now(), Data: event.MessagePosted{Request: event.Request{Text: "help"}}}
	first, err := app.rt.AcceptSession(ctx, e, "helper", s.Record())
	if err != nil {
		t.Fatal(err)
	}
	define(2, 10)
	again, err := app.rt.AcceptSession(ctx, e, "helper", s.Record())
	if err != nil || again.RunID != first.RunID {
		t.Fatal("redelivery rejected instead of joining", again, err)
	}
	define(3, 0)
	if _, err := app.rt.AcceptSession(ctx, e, "helper", s.Record()); err != nil {
		t.Fatal("removed grant changed accepted run", err)
	}
	account, err := app.sessions.(session.BudgetStore).BudgetAccount(ctx, "t", first.RunID)
	if err != nil || account.Limit.Requests != 5 {
		t.Fatal("redelivery changed budget", account, err)
	}
}

const budgetedHelper = "---\napiVersion: saige/v1\nname: helper\nversion: 1.0.0\nmodel: offline/echo\nlimits:\n  budget:\n    max_requests: 20\n---\nHelp."

// Wait ends with how the run ended, as its record says: the answer, ErrRunFailed
// for a terminal failure and for retries exhausted with no terminal event on
// the bus, ErrRunStopped for a stop, and ctx's error for a run that never runs.
func TestSessionWaitEndsWithHowTheRunEnded(t *testing.T) {
	// A host that owns admission lets durable retry a failed turn.
	hostAdmitted := []Option{
		WithWorkerOptions(worker.WithBackoff(func(int) time.Duration { return time.Millisecond })),
		WithRuntimeOptions(runtime.WithChildStarter(func(context.Context, runtime.Input, runtime.SessionCall) (string, error) {
			return "", errors.New("unused child admission")
		})),
	}
	for _, tc := range []struct {
		name    string
		result  turn.Result
		err     error
		opts    []Option
		stop    bool // ask the run to stop before a worker picks it up
		idle    bool // run no worker, so only ctx ends the wait
		want    string
		wantErr error
	}{
		{name: "a completed run returns its answer", result: turn.Result{Text: "the answer"}, want: "the answer"},
		{name: "a terminal failure is ErrRunFailed", err: durable.NonRetryable(errors.New("boom")), wantErr: ErrRunFailed},
		{name: "exhausted retries are ErrRunFailed", err: errors.New("flaky"), opts: hostAdmitted, wantErr: ErrRunFailed},
		{name: "a stopped run is ErrRunStopped", result: turn.Result{Text: "never said"}, stop: true, wantErr: ErrRunStopped},
		{name: "ctx ends the wait for a run that never runs", idle: true, wantErr: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model := turn.Func(func(context.Context, turn.Pinned, turn.Input, func(context.Context, event.Body) error) (turn.Result, error) {
				return tc.result, tc.err
			})
			app := newApp(t, append([]Option{WithTenant("t"), WithTurnRuntime(model)}, tc.opts...)...)
			ctx := context.Background()
			if _, err := app.Define(ctx, budgetedHelper); err != nil {
				t.Fatal(err)
			}
			s, err := app.Session(ctx, session.Open{Agent: "helper"})
			if err != nil {
				t.Fatal(err)
			}
			acc, err := s.Post(ctx, "help")
			if err != nil {
				t.Fatal(err)
			}
			if tc.stop {
				if err := app.Stop(ctx, runtime.StopRequest{Tenant: "t", RunID: acc.RunID, Reason: "enough"}); err != nil {
					t.Fatal(err)
				}
			}
			if !tc.idle {
				start(t, app.RunExecution)
			}
			bound := 15 * time.Second
			if tc.idle {
				bound = 100 * time.Millisecond
			}
			waitCtx, cancel := context.WithTimeout(ctx, bound)
			defer cancel()
			got, err := s.Wait(waitCtx, acc.RunID)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("Wait error = %v, want %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("Wait = %q, want %q", got, tc.want)
			}
			// Only the exhausted retries end with no terminal event on the bus,
			// which is what makes the ledger the authority.
			if want := !tc.idle && tc.opts == nil; retainedTerminal(t, app, s.Record(), acc.RunID) != want {
				t.Fatalf("the bus retains a terminal event: %v, want %v", !want, want)
			}
		})
	}
}

// retainedTerminal reports whether the bus retains a terminal event of runID.
func retainedTerminal(t *testing.T, app *App, r session.Record, runID string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	stream, err := app.Bus().Subscribe(ctx, bus.Subscription{Tenant: r.Key.Tenant, Channels: []string{r.Channel}, Types: []string{"run"}, Run: runID})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			return false
		}
		if terminal(e) {
			return true
		}
	}
}

// Wait reads only its own session's runs. Another session's run and a run
// that does not exist are one ErrNotFound, returned at once rather than when
// ctx ends.
func TestSessionWaitIsConfinedToItsOwnRuns(t *testing.T) {
	app := newApp(t, WithTenant("t"), WithTurnRuntime(turn.Func(func(context.Context, turn.Pinned, turn.Input, func(context.Context, event.Body) error) (turn.Result, error) {
		return turn.Result{Text: "private"}, nil
	})))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := app.Define(ctx, budgetedHelper); err != nil {
		t.Fatal(err)
	}
	start(t, app.RunExecution)
	mine, err := app.Session(ctx, session.Open{Parent: "console://t/chat", Agent: "helper"})
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := app.Session(ctx, session.Open{Parent: "console://t/chat", Agent: "helper", Label: "other"})
	if err != nil {
		t.Fatal(err)
	}
	acc, err := theirs.Post(ctx, "help")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := theirs.Wait(ctx, acc.RunID); err != nil || got != "private" {
		t.Fatalf("own run = %q, %v", got, err)
	}
	for _, tc := range []struct {
		name    string
		runID   string
		wantErr error
	}{
		{name: "another session's run", runID: acc.RunID, wantErr: ErrNotFound},
		{name: "a run that does not exist", runID: "r00000000000000000000000000000000", wantErr: ErrNotFound},
		{name: "no run", runID: "", wantErr: session.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mine.Wait(ctx, tc.runID)
			if !errors.Is(err, tc.wantErr) || got != "" {
				t.Fatalf("Wait = %q, %v, want %v", got, err, tc.wantErr)
			}
		})
	}
}

// Link records a context or evidence link once, Links reads them back in
// order, and any other kind, an unknown target and the session itself are
// refused.
func TestSessionLinkTakesOnlyKnownKindsAndLinksReadsThemBack(t *testing.T) {
	app := newApp(t, WithTenant("t"))
	ctx := context.Background()
	if _, err := app.Define(ctx, budgetedHelper); err != nil {
		t.Fatal(err)
	}
	s, err := app.Session(ctx, session.Open{Parent: "console://t/chat", Agent: "helper"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := app.Session(ctx, session.Open{Parent: "console://t/chat", Agent: "helper", Label: "other"})
	if err != nil {
		t.Fatal(err)
	}
	target := other.Record().ID
	for _, tc := range []struct {
		name    string
		target  string
		kind    string
		wantErr error
	}{
		{name: "context", target: target, kind: session.LinkContext},
		{name: "evidence", target: target, kind: session.LinkEvidence},
		{name: "the same link again is a no-op", target: target, kind: session.LinkContext},
		{name: "no kind", target: target, kind: "", wantErr: session.ErrInvalid},
		{name: "an unknown kind", target: target, kind: "citation", wantErr: session.ErrInvalid},
		{name: "itself", target: s.Record().ID, kind: session.LinkContext, wantErr: session.ErrInvalid},
		{name: "an unknown target", target: "s_missing", kind: session.LinkContext, wantErr: session.ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := s.Link(ctx, tc.target, tc.kind)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("Link(%q, %q) = %v, want %v", tc.target, tc.kind, err, tc.wantErr)
			}
		})
	}
	links, err := s.Links(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 || links[0].Target != target || links[0].Kind != session.LinkContext || links[1].Target != target || links[1].Kind != session.LinkEvidence {
		t.Fatalf("Links = %+v, want context then evidence to %s", links, target)
	}
	if links, err := s.Links(ctx, 1); err != nil || len(links) != 1 {
		t.Fatalf("Links(1) = %+v, %v", links, err)
	}
	if links, err := other.Links(ctx, 0); err != nil || len(links) != 0 {
		t.Fatalf("a link is one-way: target's Links = %+v, %v", links, err)
	}
}
