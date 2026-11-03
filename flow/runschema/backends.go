package runschema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urmzd/orchagent/durable/adapters/postgres/pgledger"
	"github.com/urmzd/orchagent/durable/adapters/postgres/pgqueue"
)

// Open migrates the schema and returns durable's Postgres ledger and queue
// mapped onto it, both validated against the live database.
func Open(ctx context.Context, pool *pgxpool.Pool, opts ...pgqueue.Option) (*pgledger.Ledger, *pgqueue.Queue, error) {
	if err := Migrate(ctx, pool); err != nil {
		return nil, nil, err
	}

	lm := pgledger.DefaultMapping()
	lm.Runs.Table = RunsTable
	lm.Records.Table = RecordsTable
	lgr, err := pgledger.New(pool, lm)
	if err != nil {
		return nil, nil, fmt.Errorf("runschema: ledger: %w", err)
	}
	if err := lgr.Validate(ctx); err != nil {
		return nil, nil, fmt.Errorf("runschema: ledger: %w", err)
	}

	qm := pgqueue.DefaultMapping()
	qm.Table = QueueTable
	q, err := pgqueue.New(pool, qm, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("runschema: queue: %w", err)
	}
	if err := q.Validate(ctx); err != nil {
		return nil, nil, fmt.Errorf("runschema: queue: %w", err)
	}
	return lgr, q, nil
}
