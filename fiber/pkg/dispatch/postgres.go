package dispatch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// runsSchema is the one table PostgresRuns owns: the runs started per
// channel, newest first by started_at.
const runsSchema = `
CREATE TABLE IF NOT EXISTS fiber_dispatch_runs (
	tenant     text        NOT NULL,
	channel    text        NOT NULL,
	run        text        NOT NULL,
	started_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (tenant, channel, run)
);
CREATE INDEX IF NOT EXISTS fiber_dispatch_runs_started ON fiber_dispatch_runs (tenant, channel, started_at DESC);
CREATE INDEX IF NOT EXISTS fiber_dispatch_runs_at ON fiber_dispatch_runs (started_at);
`

// migrateLock is the advisory lock every fiber store takes to migrate.
const migrateLock = 0x6d616e64 // "mand"

// DefaultRunsTTL is how long PostgresRuns remembers a run: a session stop
// reaches runs started within it.
const DefaultRunsTTL = 24 * time.Hour

// PostgresRuns is Runs shared by every replica on one database, so a session
// stop reaches a run whichever replica started it, and after a failover. It
// keeps the newest DefaultRunsPerChannel runs per channel and forgets a run
// DefaultRunsTTL after it started. It does not own the pool.
type PostgresRuns struct {
	pool *pgxpool.Pool

	mu    sync.Mutex
	swept time.Time
}

var _ Runs = (*PostgresRuns)(nil)

// NewPostgresRuns returns Runs on pool. Call Migrate once before first use.
func NewPostgresRuns(pool *pgxpool.Pool) *PostgresRuns {
	return &PostgresRuns{pool: pool, swept: time.Now()}
}

// Migrate creates the table if it is not there. It is idempotent.
func (p *PostgresRuns) Migrate(ctx context.Context) error {
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(migrateLock)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, runsSchema)
		return err
	})
	if err != nil {
		return fmt.Errorf("dispatch: migrate: %w", err)
	}
	return nil
}

// Add implements Runs.
func (p *PostgresRuns) Add(ctx context.Context, tenant, channel, runID string) error {
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO fiber_dispatch_runs (tenant, channel, run) VALUES ($1, $2, $3)
ON CONFLICT (tenant, channel, run) DO NOTHING`, tenant, channel, runID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM fiber_dispatch_runs WHERE tenant = $1 AND channel = $2
AND run NOT IN (SELECT run FROM fiber_dispatch_runs WHERE tenant = $1 AND channel = $2
	ORDER BY started_at DESC, run DESC LIMIT $3)`, tenant, channel, DefaultRunsPerChannel)
		return err
	})
	if err != nil {
		return fmt.Errorf("dispatch: add %s on %s: %w", runID, channel, err)
	}
	p.sweep(ctx)
	return nil
}

// On implements Runs.
func (p *PostgresRuns) On(ctx context.Context, tenant, channel string) ([]string, error) {
	rows, err := p.pool.Query(ctx, `SELECT run FROM fiber_dispatch_runs WHERE tenant = $1 AND channel = $2
AND started_at > now() - $3 * interval '1 second'
ORDER BY started_at, run LIMIT $4`, tenant, channel, DefaultRunsTTL.Seconds(), DefaultRunsPerChannel)
	if err != nil {
		return nil, fmt.Errorf("dispatch: runs on %s: %w", channel, err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("dispatch: runs on %s: %w", channel, err)
	}
	return out, nil
}

// sweep forgets runs older than DefaultRunsTTL, at most four times per TTL.
// A failure is ignored: the next sweep tries again.
func (p *PostgresRuns) sweep(ctx context.Context) {
	p.mu.Lock()
	due := time.Since(p.swept) >= DefaultRunsTTL/4
	if due {
		p.swept = time.Now()
	}
	p.mu.Unlock()
	if due {
		_, _ = p.pool.Exec(ctx, `DELETE FROM fiber_dispatch_runs WHERE started_at < now() - $1 * interval '1 second'`, DefaultRunsTTL.Seconds())
	}
}
