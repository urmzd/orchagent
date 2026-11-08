// Command downstream is what a team building on flow ships instead of a fork:
// its own program, composing flow in Go, plus what it registers.
//
// It adds three things in Go:
//
//   - a model provider named "house", standing in for an in-house model
//     client. Here it is saige's scripted provider, so the example runs with
//     no key and no network; a real one returns the team's own
//     saige Provider.
//   - an OpenAI-compatible endpoint named "fireworks", selected by the same
//     model.use field, with its key read from $FIREWORKS_API_KEY at run time.
//   - a tool, "lookup_runbook", that any agent can be granted by name.
//
// It stores the agent agents/oncall.agent.md defines, on the house provider, asks it one question in process and
// prints the answer:
//
//	go run ./examples/downstream
package main

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"github.com/urmzd/orchagent"
	"github.com/urmzd/orchagent/flow/event"
	"github.com/urmzd/orchagent/flow/turn/native/agenttest"
	"github.com/urmzd/orchagent/flow/turn/native/saigetypes"
)

// oncall is the agent, as a saige definition file.
//
//go:embed agents/oncall.agent.md
var oncall string

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "downstream:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Everything is in memory unless an option says otherwise: a program
	// on Postgres adds orchagent.WithPostgres(pool) and orchagent.WithBus(...).
	app, err := orchagent.New(ctx,
		orchagent.WithTenant("acme"),
		orchagent.WithModelProvider("house", houseProvider),
		orchagent.WithOpenAICompatible(orchagent.OpenAICompatible{
			Name:    "fireworks",
			BaseURL: "https://api.fireworks.ai/inference/v1",
			KeyEnv:  "FIREWORKS_API_KEY",
		}),
		orchagent.WithTools(runbookTool{}),
	)
	if err != nil {
		return err
	}
	defer func() { _ = app.Close() }()

	// Delivery and execution, in this process, until ctx ends.
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()

	// Define is insert-only: storing the same file again is a no-op.
	if _, err := app.Define(ctx, oncall); err != nil {
		return err
	}

	// The session can be reopened to continue this agent's conversation.
	conversation, err := app.Session(ctx, orchagent.SessionOptions{Agent: "oncall", Parent: "console://downstream"})
	if err != nil {
		return err
	}
	stream, err := conversation.Stream(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()
	accepted, err := conversation.Post(ctx, "api is down")
	if err != nil {
		return err
	}
	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			return err
		}
		if e.Run != accepted.RunID {
			continue
		}
		if e.Type == event.TypeFailed || e.Type == event.TypeStopped {
			detail, renderErr := event.Render(e)
			if renderErr != nil {
				return renderErr
			}
			return fmt.Errorf("run %s: %s", e.Run, detail)
		}
		if e.Type == event.TypeCompleted {
			break
		}
	}
	entries, err := conversation.Read(ctx, 0)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.RunID == accepted.RunID && string(entry.Role) == "assistant" {
			fmt.Println(entry.Text)
		}
	}
	fmt.Printf("run %s: completed\n", accepted.RunID)
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// houseProvider builds the model client for one agent version. It receives
// the whole spec, so it can key on the model name or the agent.
func houseProvider(spec orchagent.Spec) (orchagent.Provider, error) {
	p := &agenttest.ScriptedProvider{}
	p.Responses = append(p.Responses,
		append(agenttest.TextResponse(spec.Name+" answered on the house model ("+spec.Model+")."),
			saigetypes.UsageDelta{PromptTokens: 20, CompletionTokens: 10, TotalTokens: 30}))
	return &houseModel{ScriptedProvider: p, model: spec.Model}, nil
}

// The example declares an illustrative rate card so session admission can
// enforce its explicit budget. A real provider returns its actual capabilities.
type houseModel struct {
	*agenttest.ScriptedProvider
	model string
}

func (p *houseModel) Capabilities() saigetypes.ModelCapabilities {
	return saigetypes.ModelCapabilities{Model: p.model, Known: true, ContextWindow: 8192,
		Pricing: saigetypes.Pricing{InputPerMTok: 1, OutputPerMTok: 1, Source: "offline example"}}
}

// runbookTool is a tool the team owns. Any orchagent.Tool works.
type runbookTool struct{}

func (runbookTool) Definition() orchagent.ToolDef {
	return orchagent.ToolDef{
		Name:        "lookup_runbook",
		Description: "Look up the on-call runbook for a service.",
		Parameters: orchagent.ParameterSchema{
			Type:       "object",
			Properties: map[string]orchagent.PropertyDef{"service": {Type: "string"}},
			Required:   []string{"service"},
		},
	}
}

func (runbookTool) Execute(_ context.Context, args map[string]any) (string, error) {
	service, _ := args["service"].(string)
	return "Restart " + service + " with `make restart`, then page the owner.", nil
}
