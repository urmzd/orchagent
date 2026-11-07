package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urmzd/orchagent/durable/worker"
	"github.com/urmzd/orchagent/flow/event"
	"github.com/urmzd/orchagent/flow/runtime"
	"github.com/urmzd/orchagent/flow/session"
	"github.com/urmzd/orchagent/flow/turn"
	"github.com/urmzd/orchagent/flow/turn/native"
)

type failOnceUsageStore struct {
	*session.Memory
	writes atomic.Int32
}

type delayedUsageStore struct {
	*session.Memory
	blocked atomic.Bool
	writes  atomic.Int32
}

func (s *delayedUsageStore) RecordUsage(ctx context.Context, tenant, id string, c event.UsageCall) error {
	s.writes.Add(1)
	if s.blocked.Load() {
		return errors.New("usage index unavailable")
	}
	return s.Memory.RecordUsage(ctx, tenant, id, c)
}
func waitForRecovery(t *testing.T, ctx context.Context, ready func() bool) {
	t.Helper()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}

func TestSessionUsageReconciliationRestartsWithoutRepeatingPaidWork(t *testing.T) {
	for _, stop := range []bool{false, true} {
		name := "recover"
		if stop {
			name = "stop during recovery"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			st := &delayedUsageStore{Memory: session.NewMemory()}
			st.blocked.Store(true)
			var calls atomic.Int32
			model := turn.Func(func(ctx context.Context, _ turn.Pinned, in turn.Input, emit func(context.Context, event.Body) error) (turn.Result, error) {
				calls.Add(1)
				err := emit(ctx, event.UsageCall{AccountingID: in.Accounting + "/paid", Requests: 1, PromptTokens: 10})
				return turn.Result{Text: "completed model work"}, err
			})
			opts := []Option{WithTenant("t"), WithSessions(st), WithConcurrency(1), WithTurnRuntime(model), WithWorkerOptions(worker.WithJanitorEvery(20*time.Millisecond), worker.WithLeaseTTL(100*time.Millisecond))}
			first, err := New(ctx, opts...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = first.Close() })
			recoveryDefine(t, first, "---\napiVersion: saige/v1\nname: lead\nversion: 1.0.0\nmodel: offline/echo\nlimits:\n  budget:\n    max_requests: 5\n---\nHelp.")
			s, err := first.Session(ctx, session.Open{Agent: "lead"})
			if err != nil {
				t.Fatal(err)
			}
			acc, err := s.Post(ctx, "test")
			if err != nil {
				t.Fatal(err)
			}
			firstCtx, stopFirst := context.WithCancel(ctx)
			doneFirst := make(chan error, 1)
			go func() { doneFirst <- first.RunExecution(firstCtx) }()
			waitForRecovery(t, ctx, func() bool { return st.writes.Load() >= 3 })
			stopFirst()
			if err := <-doneFirst; err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			_, records, err := first.ledger.Load(ctx, acc.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if receipt, ok := runtime.RecordedUsage(records); !ok || receipt.Total.Requests != 1 {
				t.Fatal("paid call receipt missing before restart", receipt)
			}
			baseline := st.writes.Load()
			opts = append(opts, WithDurable(first.ledger, first.queue), WithDefinitions(first.store), WithThreads(first.threads))
			second, err := New(ctx, opts...)
			if err != nil {
				t.Fatal(err)
			}
			secondCtx, stopSecond := context.WithCancel(ctx)
			recoveryWorker(t, second, secondCtx, stopSecond)
			waitForRecovery(t, ctx, func() bool { return st.writes.Load() >= baseline+2 })
			if stop {
				if err := second.Runtime().Stop(ctx, runtime.StopRequest{Tenant: "t", RunID: acc.RunID, By: "person", Reason: "cancel pending work"}); err != nil {
					t.Fatal(err)
				}
			}
			st.blocked.Store(false)
			out, err := second.Runtime().Result(ctx, acc.RunID)
			if err != nil || out.Text != "completed model work" {
				t.Fatal(out, err)
			}
			reopened, err := second.OpenSession(ctx, s.Record().ID)
			if err != nil {
				t.Fatal(err)
			}
			usage, err := reopened.Usage(ctx)
			if err != nil || usage.Total.Requests != 1 || calls.Load() != 1 {
				t.Fatalf("usage=%+v model calls=%d err=%v", usage, calls.Load(), err)
			}
			budget, err := st.BudgetAccount(ctx, "t", acc.RunID)
			if err != nil || !budget.Ready() || budget.Spent().Requests != 1 {
				t.Fatal("pending recovery did not settle exactly once", budget, err)
			}
		})
	}
}

type stopOnReserveStore struct {
	*session.Memory
	afterReserve func()
}

func (s *stopOnReserveStore) Reserve(ctx context.Context, tenant, parent, call, child string, limit int) error {
	if err := s.Memory.Reserve(ctx, tenant, parent, call, child, limit); err != nil {
		return err
	}
	if s.afterReserve != nil {
		s.afterReserve()
	}
	return nil
}
func TestSessionStopAcrossReservationAndAdmission(t *testing.T) {
	for _, phase := range []string{"reservation", "accepted admission"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			st := &stopOnReserveStore{Memory: session.NewMemory()}
			var a *App
			var parentID, childID string
			var admitted, helpers atomic.Int32
			stopParent := func() {
				if err := a.Runtime().Stop(ctx, runtime.StopRequest{Tenant: "t", RunID: parentID, By: "person", Reason: "stop racing admission"}); err != nil {
					t.Error(err)
				}
			}
			if phase == "reservation" {
				st.afterReserve = stopParent
			}
			starter := func(ctx context.Context, parent runtime.Input, call runtime.SessionCall) (string, error) {
				admitted.Add(1)
				if phase == "accepted admission" {
					stopParent()
				}
				e := event.Envelope{ID: call.ID, Source: "delegation:" + parentID, Type: event.TypeMessagePosted, Channel: call.Session.Channel, Tenant: "t", Time: time.Now(), Data: event.MessagePosted{Request: event.Request{Text: call.Text}}}
				acc, err := a.Runtime().AcceptOwnSessionChild(ctx, e, call.Agent, call.Session, parentID, call.QuestionID)
				childID = acc.RunID
				return acc.RunID, err
			}
			model := turn.Func(func(ctx context.Context, p turn.Pinned, _ turn.Input, _ func(context.Context, event.Body) error) (turn.Result, error) {
				if p.Name == "helper" {
					helpers.Add(1)
					return turn.Result{Text: "should not execute"}, nil
				}
				req, _ := runtime.RequestFrom(ctx)
				parentID = req.RunID
				args, _ := json.Marshal(map[string]any{"agent": "helper", "text": "work", "wait": false})
				parked, err := native.ParkedTo(native.Parked{Question: event.Question{ID: "q", Kind: event.QuestionDelegation, Tool: &event.QuestionTool{Name: "session_call", CallID: "call", Arguments: args}}, CallID: "call"})
				return turn.Result{Parked: &parked}, err
			})
			var err error
			a, err = New(ctx, WithTenant("t"), WithSessions(st), WithConcurrency(1), WithTurnRuntime(model), WithRuntimeOptions(runtime.WithChildStarter(starter), runtime.WithChildRunID(func(parent runtime.Input, call runtime.SessionCall) string {
				return runtime.RunID(parent.Trigger.Tenant, call.ID)
			})))
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			recoveryWorker(t, a, ctx, cancel)
			recoveryDefine(t, a, "---\napiVersion: saige/v1\nname: helper\nversion: 1.0.0\nmodel: offline/echo\n---\nHelp.")
			recoveryDefine(t, a, "---\napiVersion: saige/v1\nname: lead\nversion: 1.0.0\nmodel: offline/echo\nsubagents:\n  - ref: helper\n---\nLead.")
			s, err := a.Session(ctx, session.Open{Agent: "lead"})
			if err != nil {
				t.Fatal(err)
			}
			acc, err := s.Post(ctx, "test")
			if err != nil {
				t.Fatal(err)
			}
			out, err := a.Runtime().Result(ctx, acc.RunID)
			if err != nil || out.Stopped == nil {
				t.Fatal("parent did not stop", out, err)
			}
			if phase == "reservation" {
				if admitted.Load() != 0 {
					t.Fatal("admission escaped reservation fence")
				}
				claims, err := st.Reservations(ctx, "t", s.Record().ID)
				if err != nil || len(claims) != 0 {
					t.Fatal("unused reservation leaked", claims, err)
				}
			} else {
				child, err := a.Runtime().Result(ctx, childID)
				if err != nil || child.Stopped == nil || helpers.Load() != 0 {
					t.Fatal("accepted child was not stopped before execution", child, helpers.Load(), err)
				}
			}
		})
	}
}

type firstWritePerCallStore struct {
	*session.Memory
	mu   sync.Mutex
	seen map[string]bool
}

func (s *firstWritePerCallStore) RecordUsage(ctx context.Context, tenant, id string, c event.UsageCall) error {
	s.mu.Lock()
	first := !s.seen[c.AccountingID]
	s.seen[c.AccountingID] = true
	s.mu.Unlock()
	if first {
		return errors.New("first projection write unavailable")
	}
	return s.Memory.RecordUsage(ctx, tenant, id, c)
}

func TestSessionUsageReconcilesNewReceiptsAfterRetryableModelFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	st := &firstWritePerCallStore{Memory: session.NewMemory(), seen: map[string]bool{}}
	var calls atomic.Int32
	model := turn.Func(func(ctx context.Context, _ turn.Pinned, in turn.Input, emit func(context.Context, event.Body) error) (turn.Result, error) {
		n := calls.Add(1)
		req, _ := runtime.RequestFrom(ctx)
		if err := emit(ctx, event.UsageCall{AccountingID: fmt.Sprintf("%s/a%d/paid", req.RunID, req.Attempt), Requests: 1}); err != nil {
			return turn.Result{}, err
		}
		if n == 1 {
			return turn.Result{}, errors.New("provider temporarily failed after billing")
		}
		return turn.Result{Text: "recovered"}, nil
	})
	// A host-owned admission path owns budgets and permits its normal provider
	// retries. Each retry still has a distinct, retained actual-call receipt.
	a, err := New(ctx, WithTenant("t"), WithSessions(st), WithConcurrency(1), WithTurnRuntime(model), WithWorkerOptions(worker.WithBackoff(func(int) time.Duration { return time.Millisecond })), WithRuntimeOptions(runtime.WithChildStarter(func(context.Context, runtime.Input, runtime.SessionCall) (string, error) {
		return "", errors.New("unused child admission")
	})))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	recoveryWorker(t, a, ctx, cancel)
	recoveryDefine(t, a, "---\napiVersion: saige/v1\nname: lead\nversion: 1.0.0\nmodel: offline/echo\n---\nHelp.")
	s, err := a.Session(ctx, session.Open{Agent: "lead"})
	if err != nil {
		t.Fatal(err)
	}
	acc, err := s.Post(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	out, err := a.Runtime().Result(ctx, acc.RunID)
	if err != nil || out.Usage == nil || out.Usage.Total.Requests != 2 {
		t.Fatal(out, err)
	}
	u, err := s.Usage(ctx)
	if err != nil || u.Total.Requests != 2 || calls.Load() != 2 {
		t.Fatal("new retry receipt hidden by previous reconciliation", u, calls.Load(), err)
	}
}

func (s *failOnceUsageStore) RecordUsage(ctx context.Context, tenant, id string, c event.UsageCall) error {
	if s.writes.Add(1) == 1 {
		return errors.New("injected transient usage write failure")
	}
	return s.Memory.RecordUsage(ctx, tenant, id, c)
}
func recoveryDefine(t *testing.T, a *App, doc string) {
	t.Helper()
	if _, err := a.Define(t.Context(), doc); err != nil {
		t.Fatal(err)
	}
}
func recoveryWorker(t *testing.T, a *App, ctx context.Context, cancel context.CancelFunc) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- a.RunExecution(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("execution cleanup: %v", err)
		}
		_ = a.Close()
	})
}
func TestSessionUsageRepairsTransientWriteFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	st := &failOnceUsageStore{Memory: session.NewMemory()}
	a, err := New(ctx, WithTenant("t"), WithSessions(st), WithConcurrency(1), WithTurnRuntime(turn.Func(func(ctx context.Context, _ turn.Pinned, in turn.Input, emit func(context.Context, event.Body) error) (turn.Result, error) {
		err := emit(ctx, event.UsageCall{AccountingID: in.Accounting + "/billed", Requests: 1, PromptTokens: 10, CompletionTokens: 5})
		return turn.Result{}, err
	})))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	recoveryWorker(t, a, ctx, cancel)
	recoveryDefine(t, a, "---\napiVersion: saige/v1\nname: lead\nversion: 1.0.0\nmodel: offline/echo\nlimits:\n  budget:\n    max_requests: 5\n---\nHelp.")
	s, err := a.Session(ctx, session.Open{Agent: "lead"})
	if err != nil {
		t.Fatal(err)
	}
	acc, err := s.Post(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	_, resultErr := a.Runtime().Result(ctx, acc.RunID)
	_, records, err := a.ledger.Load(ctx, acc.RunID)
	if err != nil {
		t.Fatal(err)
	}
	recorded, ok := runtime.RecordedUsage(records)
	live, err := s.Usage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || recorded.Total.Requests != 1 {
		t.Fatalf("fixture did not persist billed call: %+v %v", recorded, ok)
	}
	if live.Total.Requests != recorded.Total.Requests {
		t.Fatalf("durable receipt requests=%d, live session requests=%d, usage writes=%d, result=%v", recorded.Total.Requests, live.Total.Requests, st.writes.Load(), resultErr)
	}
}
func TestSessionRecordedStopFencesNewChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	var a *App
	var err error
	var helpers atomic.Int32
	a, err = New(ctx, WithTenant("t"), WithConcurrency(1), WithTurnRuntime(turn.Func(func(ctx context.Context, p turn.Pinned, in turn.Input, emit func(context.Context, event.Body) error) (turn.Result, error) {
		if p.Name == "helper" {
			helpers.Add(1)
			return turn.Result{Text: "child ran after stop"}, nil
		}
		req, _ := runtime.RequestFrom(ctx)
		if err := a.Runtime().Stop(ctx, runtime.StopRequest{Tenant: "t", RunID: req.RunID, Reason: "stop before delegation", By: "person"}); err != nil {
			return turn.Result{}, err
		}
		args, _ := json.Marshal(map[string]any{"agent": "helper", "text": "do work", "wait": false})
		parked, err := native.ParkedTo(native.Parked{Question: event.Question{ID: "q", Kind: event.QuestionDelegation, Tool: &event.QuestionTool{Name: "session_call", CallID: "call", Arguments: args}}, CallID: "call"})
		return turn.Result{Parked: &parked}, err
	})))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	recoveryWorker(t, a, ctx, cancel)
	recoveryDefine(t, a, "---\napiVersion: saige/v1\nname: helper\nversion: 1.0.0\nmodel: offline/echo\nlimits:\n  budget:\n    max_requests: 5\n---\nHelp.")
	recoveryDefine(t, a, "---\napiVersion: saige/v1\nname: lead\nversion: 1.0.0\nmodel: offline/echo\nlimits:\n  budget:\n    max_requests: 5\nsubagents:\n  - ref: helper\n    budget:\n      max_requests: 1\n---\nLead.")
	s, err := a.Session(ctx, session.Open{Agent: "lead"})
	if err != nil {
		t.Fatal(err)
	}
	acc, err := s.Post(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	out, err := a.Runtime().Result(ctx, acc.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if out.Stopped == nil {
		t.Fatal("fixture parent did not stop")
	}
	children, err := s.Children(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(children) == 0 {
		return
	}
	runs, err := a.sessions.Runs(ctx, "t", children[0].ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) == 0 {
		return
	}
	child, err := a.Runtime().Result(ctx, runs[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("stopped parent admitted child: helper executions=%d child=%+v", helpers.Load(), child)
}
