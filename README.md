<p align="center">
  <h1 align="center">orchagent</h1>
  <p align="center">
    The durable stack for agents in Go: durable runs, sandboxes, and the event flow that connects them.
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

orchagent is a library. It runs agent work so that it finishes once despite
crashes, touches only what its policy grants, and streams its events to
wherever they need to go. It starts no server and reads no configuration: you
compose it in Go.

## Features

- **Durable runs** ([`durable`](durable/README.md)): a ledger of runs, a queue with leases and fencing, a worker that resumes after a crash, and replay, with Postgres and Redis adapters.
- **Sandboxes** ([`sandbox`](sandbox/README.md)): sandboxed tools on a shared workspace, NGAC access control, isolated instances (Docker, gVisor), and local and Kubernetes runners.
- **Event flow** ([`flow`](flow/README.md)): the event union, the channel bus, connectors (Slack, GitHub, Jira, cron, webhooks), agent definitions with policy, the durable turn, threads and knowledge.
- **No service to run**: no listener, no config files, no environment variables. Backends are chosen in code.
- **Validation CLIs**: each component ships a command that exercises it on its own.

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

A sandboxed tool on a bounded pool, with no external services:

<!-- fsrc src="sandbox/examples/basic/main.go" fence="auto" -->
```go
// Basic example: compose a control plane from the orthogonal pieces
// (a local workspace, a sandboxed tool, an in-process node factory, and an
// in-memory metrics recorder), then deploy a service, size its pool, submit
// tasks, and print the resulting metrics.
//
// Prerequisites: none. Run with:
//
//	go run ./examples/basic/
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/urmzd/orchagent/sandbox/controlplane"
	"github.com/urmzd/orchagent/sandbox/metrics"
	"github.com/urmzd/orchagent/sandbox/node/inproc"
	"github.com/urmzd/orchagent/sandbox/sandbox"
	"github.com/urmzd/orchagent/sandbox/task"
	"github.com/urmzd/orchagent/sandbox/tool"
	"github.com/urmzd/orchagent/sandbox/workspace"
)

func main() {
	ctx := context.Background()

	// Shared workspace: every node reads and writes the same backend.
	dir, err := os.MkdirTemp("", "playscape-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ws, err := workspace.NewLocal(dir)
	if err != nil {
		log.Fatal(err)
	}

	// One tool. It sees only the workspace view its policy grants below.
	registry := tool.NewRegistry()
	err = registry.Register(tool.Func("greet", func(ctx context.Context, rt tool.Runtime, in []byte) ([]byte, error) {
		out := []byte("hello, " + string(in))
		// Allowed: "greetings/..." is inside this tool's area.
		if err := rt.Workspace().Write(ctx, "greetings/last", bytes.NewReader(out)); err != nil {
			return nil, err
		}
		// Denied by the sandbox: "secrets/..." is outside the area.
		if err := rt.Workspace().Write(ctx, "secrets/steal", bytes.NewReader(out)); err != nil {
			fmt.Printf("sandbox blocked the leak: %v\n", err)
		}
		return out, nil
	}))
	if err != nil {
		log.Fatal(err)
	}

	// Compose the control plane and deploy one service.
	rec := metrics.NewMemory()
	plane := controlplane.NewMemory(inproc.NewFactory(registry, ws), rec)
	err = plane.Deploy(ctx, controlplane.ServiceSpec{
		Name:     "greeter",
		Replicas: 1,
		Policies: []sandbox.Policy{{Tool: "greet", Areas: []sandbox.Area{{Prefix: "greetings"}}}},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Grow the pool to three nodes; each runs one task at a time.
	if err := plane.Scale(ctx, "greeter", 3); err != nil {
		log.Fatal(err)
	}
	for _, name := range []string{"ada", "grace", "alan"} {
		res, err := plane.Submit(ctx, "greeter", task.Task{Tool: "greet", Input: []byte(name)})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s ran on %s: %s\n", name, res.NodeID, res.Output)
	}

	// Metrics accumulated along the way.
	fmt.Println("\nmetrics:")
	snap := rec.Snapshot()
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %s %g\n", k, snap[k])
	}
}
```
<!-- /fsrc -->

```sh
go run ./sandbox/examples/basic
```

## Examples

| Example | Description |
|---------|-------------|
| [`sandbox/examples/basic`](sandbox/examples/basic/) | A sandboxed tool on a bounded pool, with metrics |
| [`sandbox/examples/ngac`](sandbox/examples/ngac/) | Access defined as an NGAC policy graph, including agent spawn |
| [`sandbox/examples/saige`](sandbox/examples/saige/) | Saige agents run as sandbox workloads |
| [`flow/examples/local`](flow/examples/local/) | An agent on the bus, offline, with fake connectors |
| [`flow/examples/downstream`](flow/examples/downstream/) | A program that composes flow and registers its own provider |
| [`durable/examples/quickstart`](durable/examples/quickstart/) | An order workflow on Postgres and Redis that survives a killed worker |
| [`durable/examples/chaos`](durable/examples/chaos/) | Cross-process chaos test that SIGKILLs a worker mid-activity |

The durable examples need Postgres and Redis:

```sh
docker compose up -d --wait
go run ./durable/examples/quickstart
```

## Usage

Each component ships a CLI that validates it on its own:

| Command | Validates |
|---------|-----------|
| `go run ./sandbox/cmd/sandbox run "go test ./..."` | a command under a sandbox policy |
| `go run ./flow/cmd/flow run` | an `.agent.md` against a message, printing its events |
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
