package runschema_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/orchagent/durable/adapters/postgres/pgledger"
	"github.com/urmzd/orchagent/durable/run"
	"github.com/urmzd/orchagent/flow/bus"
	"github.com/urmzd/orchagent/flow/event"
	"github.com/urmzd/orchagent/flow/runschema"
)

// newOutbox opens the run schema and the bus on one schema of the test's own.
func newOutbox(t *testing.T) (*runschema.Outbox, *pgledger.Ledger, *pgxpool.Pool) {
	t.Helper()
	pool := newPool(t)
	ctx := t.Context()
	lgr, _, err := runschema.Open(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	b := bus.NewPostgres(pool)
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return runschema.NewOutbox(pool, lgr, b), lgr, pool
}

func accepted(runID string) event.Envelope {
	return event.Envelope{
		ID: runID + "#1", Source: "fiber://run/" + runID, Type: event.TypeAccepted,
		Channel: "slack://T01/C02", Tenant: "acme", Run: runID, Seq: 1, Agent: "docs-bot",
		Data: event.Accepted{},
	}
}

func pending(runID string) run.Run {
	return run.Run{ID: runID, Name: "turn", Input: []byte(`{}`), Status: run.StatusPending, MaxAttempts: 3, CreatedAt: time.Now()}
}

func countRows(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The run's ledger row and its accepted event commit together, and a
// retried Accept writes each once.
func TestOutboxCommitsTheRunWithItsEvent(t *testing.T) {
	o, lgr, pool := newOutbox(t)
	for range 2 {
		if err := o.Accept(t.Context(), accepted("run_1"), pending("run_1")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := lgr.GetRun(t.Context(), "run_1"); err != nil {
		t.Fatalf("the ledger does not hold the run: %v", err)
	}
	if n := countRows(t, pool, `SELECT count(*) FROM fiber_events WHERE run = 'run_1'`); n != 1 {
		t.Fatalf("accepted events = %d, want 1", n)
	}
}

// A ledger write that fails takes the event with it: nobody is told about a
// run the ledger does not hold.
func TestOutboxRollsTheEventBackWhenTheRunIsRefused(t *testing.T) {
	o, lgr, pool := newOutbox(t)
	if _, err := pool.Exec(t.Context(), `ALTER TABLE `+runschema.RunsTable+` ADD CONSTRAINT refuse_all CHECK (false) NOT VALID`); err != nil {
		t.Fatal(err)
	}
	if err := o.Accept(t.Context(), accepted("run_1"), pending("run_1")); err == nil {
		t.Fatal("Accept succeeded with a ledger that refuses every run")
	}
	if n := countRows(t, pool, `SELECT count(*) FROM fiber_events`); n != 0 {
		t.Fatalf("the event outlived the refused run: %d rows", n)
	}
	if _, err := lgr.GetRun(context.Background(), "run_1"); err == nil {
		t.Fatal("the ledger holds a refused run")
	}
}
