// Package orchagent is the orchestration stack for all your agent needs.
//
// New composes the whole stack (agent definitions, the event bus, the
// durable runtime, the sandbox and the connectors) into an App, and Ask
// runs one agent and returns its reply:
//
//	app, err := orchagent.New(ctx,
//		orchagent.WithTenant("acme"),
//		orchagent.WithModelProvider("house", houseProvider),
//		orchagent.WithTools(runbookTool{}),
//	)
//	if err != nil { ... }
//	defer app.Close()
//	go app.Run(ctx)
//	_, err = app.Define(ctx, oncallAgentMD) // an .agent.md file
//	reply, err := app.Ask(ctx, "oncall", "api is down")
//	if err != nil { ... }
//	defer reply.Close()
//	answer, err := reply.Text(ctx)
//
// This package names the common path. Everything else is in its component:
// github.com/urmzd/orchagent/flow for every option and store,
// github.com/urmzd/orchagent/durable for durable execution on its own, and
// github.com/urmzd/orchagent/sandbox for sandboxed tools on their own.
package orchagent

import (
	"context"

	"github.com/urmzd/orchagent/flow"
	"github.com/urmzd/orchagent/flow/definition"
	"github.com/urmzd/orchagent/flow/event"
	"github.com/urmzd/orchagent/flow/turn/native"
	saigetypes "github.com/urmzd/orchagent/flow/turn/native/saigetypes"
)

// App is the composed stack. See flow.App.
type App = flow.App

// Option is one choice New is given. Every flow.With* option is an Option.
type Option = flow.Option

// Reply is one run started by App.Ask.
type Reply = flow.Reply

// Event is one event on the bus.
type Event = event.Envelope

// Tool is a tool an agent can be granted by name (WithTools).
type Tool = saigetypes.Tool

// Tool definition types, for implementing Tool.
type (
	ToolDef         = saigetypes.ToolDef
	ParameterSchema = saigetypes.ParameterSchema
	PropertyDef     = saigetypes.PropertyDef
)

// Model provider types, for WithModelProvider and WithOpenAICompatible.
type (
	ProviderFunc     = native.ProviderFunc
	Spec             = native.Spec
	Provider         = saigetypes.Provider
	OpenAICompatible = native.OpenAICompatible
)

// Errors from App.Define and Reply.Text.
var (
	ErrExists     = definition.ErrExists
	ErrRunFailed  = flow.ErrRunFailed
	ErrRunStopped = flow.ErrRunStopped
)

// New composes the stack. Anything not passed as an Option is the complete
// in-memory implementation.
func New(ctx context.Context, opts ...Option) (*App, error) { return flow.New(ctx, opts...) }

// The options most programs need. The rest are in package flow.
var (
	WithTenant           = flow.WithTenant
	WithLogger           = flow.WithLogger
	WithPostgres         = flow.WithPostgres
	WithBus              = flow.WithBus
	WithModelProvider    = flow.WithModelProvider
	WithOpenAICompatible = flow.WithOpenAICompatible
	WithTools            = flow.WithTools
	WithConnector        = flow.WithConnector
)
