package runschema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/orchagent/durable/adapters/postgres/pgledger"
	"github.com/urmzd/orchagent/durable/run"
	"github.com/urmzd/orchagent/flow/bus"
	"github.com/urmzd/orchagent/flow/event"
)

// Outbox accepts a run and publishes the event that announces it in one
// Postgres transaction: the ledger row (fiber_runs) and the bus row
// (fiber_events) commit together or not at all. It satisfies
// runtime.Outbox; pass it with runtime.WithOutbox.
//
// The ledger and the bus must be on pool's database and schema.
type Outbox struct {
	pool   *pgxpool.Pool
	ledger *pgledger.Ledger
	bus    bus.TxPublisher
}

// NewOutbox returns an outbox over pool for ledger and b.
func NewOutbox(pool *pgxpool.Pool, ledger *pgledger.Ledger, b bus.TxPublisher) *Outbox {
	return &Outbox{pool: pool, ledger: ledger, bus: b}
}

// Accept publishes e and registers r in one transaction. Both halves are
// idempotent, so a retried Accept writes each once.
func (o *Outbox) Accept(ctx context.Context, e event.Envelope, r run.Run) error {
	err := pgx.BeginFunc(ctx, o.pool, func(tx pgx.Tx) error {
		// The event first: its publish takes the tenant's lock, so the
		// lock is held for the ledger insert too and released at commit.
		if err := o.bus.PublishTx(ctx, tx, e); err != nil {
			return err
		}
		return o.ledger.AcceptTx(ctx, tx, r)
	})
	if err != nil {
		return fmt.Errorf("runschema: outbox accept %s: %w", r.ID, err)
	}
	return nil
}
