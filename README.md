<p align="center">
  <h1 align="center">orchagent</h1>
  <p align="center">
    The orchestration stack for all your agent needs.
    <br /><br />
    <a href="#installation">Install</a>
    &middot;
    <a href="https://github.com/urmzd/orchagent/issues">Report Bug</a>
    &middot;
    <a href="https://pkg.go.dev/github.com/urmzd/orchagent">Go Docs</a>
  </p>
</p>

<p align="center">
  <a href="https://github.com/urmzd/orchagent/actions/workflows/ci.yml"><img src="https://github.com/urmzd/orchagent/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  &nbsp;
  <a href="https://pkg.go.dev/github.com/urmzd/orchagent"><img src="https://pkg.go.dev/badge/github.com/urmzd/orchagent.svg" alt="Go Reference"></a>
  &nbsp;
  <a href="LICENSE"><img src="https://img.shields.io/github/license/urmzd/orchagent" alt="License"></a>
</p>

## Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Examples](#examples)
- [Usage](#usage)
- [Stored names](#stored-names)
- [Development](#development)
- [Documentation](#documentation)
- [Agent Skill](#agent-skill)
- [License](#license)

Define an agent in a `.agent.md` file, connect it to Slack, GitHub, Jira,
cron or a webhook, and orchagent runs it: each run finishes exactly once
despite crashes, touches only what its policy grants, and streams its events to
every subscriber. It is a Go library. It starts no server and reads no
configuration: you compose it with `orchagent.New`, and anything you do not
pass is a complete in-memory implementation.

## Features

- **One entry point**: `orchagent.New(ctx, opts...)` returns an `App` with the bus, definitions, durable runtime, sandbox and connectors wired. `app.Define` stores an agent and `app.Session` opens a conversation with it: `Post` sends a message, `Wait` returns the answer.
- **Agents are documents**: store a saige `.agent.md` and it runs, with no redeploy. A run pins its definition's version, so a retry never runs a newer prompt.
- **Durable runs**: a ledger, a queue with leases and fencing, and a worker that resumes after a crash without re-executing recorded steps.
- **Sandboxed tools**: tools see only the workspace their NGAC policy grants, in process or isolated with Docker, gVisor or Kubernetes.
- **Connectors in and out**: an event can arrive from GitHub and its answer stream to Slack, while a webhook receives only the completion.
- **Backends are values**: `orchagent.WithPostgres(pool)` and `orchagent.WithBus(...)` move every store to Postgres. No config files, no environment variables.
- **Each layer stands alone**: import `durable` or `sandbox` without `flow` when you need only durable execution or sandboxing.

## Installation

### Prerequisites

| Requirement | Version |
|-------------|---------|
| Go | 1.26+ |
| Docker | for the Postgres and Redis adapters (`compose.yaml`) |

```sh
go get github.com/urmzd/orchagent
```

The Kubernetes Job runner is its own module, so only programs that use it inherit client-go:

```sh
go get github.com/urmzd/orchagent/sandbox/runner/k8s
```

## Quick Start

An agent with its own model provider and tool, answering one message, with no
API key and no external services:

<!-- fsrc src="flow/examples/downstream/main.go" fence="auto" -->
```go
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
	// Post starts a turn; Wait returns its answer, or ErrRunFailed or
	// ErrRunStopped.
	accepted, err := conversation.Post(ctx, "api is down")
	if err != nil {
		return err
	}
	answer, err := conversation.Wait(ctx, accepted.RunID)
	if err != nil {
		return err
	}
	fmt.Printf("%s\nrun %s: completed\n", answer, accepted.RunID)
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
```
<!-- /fsrc -->

```sh
go run ./flow/examples/downstream
```

The agent it runs is a definition file:

<!-- fsrc src="flow/examples/downstream/agents/oncall.agent.md" fence="auto" -->
```markdown
---
apiVersion: saige/v1
name: oncall
version: 1.0.0
description: helps the on-call engineer
model:
  use: house/house-1
tools:
  registry:
    - lookup_runbook
limits:
  budget:
    max_cost: 1
    max_requests: 4
    max_tokens: 4096
---
Help the on-call engineer.
```
<!-- /fsrc -->

## Examples

| Example | Description |
|---------|-------------|
| [`flow/examples/downstream`](flow/examples/downstream/) | Your own program on orchagent: a model provider, an OpenAI-compatible endpoint and a tool |
| [`flow/examples/local`](flow/examples/local/) | The full stack offline: versioned agents, one run to two sinks, citations, replay |
| [`durable/examples/quickstart`](durable/examples/quickstart/) | An order workflow on Postgres and Redis that survives a killed worker |
| [`durable/examples/chaos`](durable/examples/chaos/) | Cross-process chaos test that SIGKILLs a worker mid-activity |
| [`sandbox/examples/basic`](sandbox/examples/basic/) | A sandboxed tool on a bounded pool, with metrics |
| [`sandbox/examples/ngac`](sandbox/examples/ngac/) | Access defined as an NGAC policy graph, including agent spawn |
| [`sandbox/examples/saige`](sandbox/examples/saige/) | Saige agents run as sandbox workloads |

The durable examples need Postgres and Redis:

```sh
docker compose up -d --wait
go run ./durable/examples/quickstart
```

## Usage

Pick the import that matches the job:

| You need | Import | Start with |
|----------|--------|------------|
| Run an agent and read its answer | `orchagent` | `orchagent.New`, `app.Define`, `app.Session` |
| Every option and store: connectors, memberships, knowledge | `orchagent/flow` | `flow.With*`, `app.Publish`, `app.Memberships` |
| Work that finishes once despite crashes | `orchagent/durable` | `durable.New`, `durable.Start`, `durable.Exec` |
| Tools confined by policy | `orchagent/sandbox/...` | `controlplane.NewMemory`, `sandbox.Policy` |

Each component ships a CLI that validates it on its own:

| Command | Validates |
|---------|-----------|
| `go run ./flow/cmd/flow run` | an `.agent.md` against a message, printing its events |
| `go run ./sandbox/cmd/sandbox run "go test ./..."` | a command under a sandbox policy |
| `go run ./durable/adapters/postgres/cmd/durable` | a Postgres ledger: list, show, replay |

## Stored names

Names that live in databases or on the wire keep their original spelling so
existing data stays readable: `fiber_*` tables, `fiber://`
channel URIs, `fiber.*` event kinds and the `X-Fiber-Secret` header. Renaming
them needs a data migration.

## Development

```sh
make init               # git hooks, download modules
make check              # fmt, vet, lint, race tests for both modules
make test-integration   # Postgres and Redis adapters (uses compose.yaml)
```

## Documentation

- [Documentation index](docs/README.md)
- [Contributing](CONTRIBUTING.md)

## Agent Skill

This repo's conventions are available as portable agent skills in [`skills/`](skills/).

## License

Apache-2.0. See [LICENSE](LICENSE).
