package connectors

import (
	"context"
	"github.com/urmzd/orchagent/flow/address"
	"github.com/urmzd/orchagent/flow/event"
)

// FeedbackTarget identifies an answer the application actually delivered. The
// host resolves native message handles from its delivery records, never from
// text supplied by a person or model. Unknown handles produce Found=false.
type FeedbackTarget struct {
	Found   bool
	Run     string
	Session string
	Message string
	Finding string
	Channel string
}

// FeedbackHandler resolves a verified native message and persists feedback.
// Record must deduplicate its native event ID and own the event sequence. These
// seams allow the host's existing durable feedback store to be the sole writer.
type FeedbackHandler struct {
	Resolve func(context.Context, string, address.Address) (FeedbackTarget, error)
	Record  func(context.Context, string, string, event.Feedback) error
	Stop    func(context.Context, string, string, event.Actor) error
}

// Delivery identifies the native answer a sink actually created. Hosts persist
// this mapping before acknowledging delivery so reactions can find their run
// after a restart, without trusting text or guessing from recent history.
type Delivery struct {
	Tenant  string
	Run     string
	Channel string
	Target  address.Address
	Message address.Address
	Finding string
}

type DeliveryObserver func(context.Context, Delivery) error
