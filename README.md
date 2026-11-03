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

## Where it sits

| Layer | Repository | Owns |
|-------|------------|------|
| Harness | [saige](https://github.com/urmzd/saige) | model providers, the agent loop, tools, MCP, retrieval, memory, evals |
| **Durable stack** | **orchagent** | durable runs, sandboxes, the event flow |
| Platform | [humagen](https://github.com/urmzd/humagen) | API, console, Slack, identity, tenancy, configuration, deploy |

## Components

| Package | What it does | Was |
|---------|--------------|-----|
| [`durable`](durable/README.md) | A ledger of runs, a queue with leases and fencing, a worker that resumes after a crash, replay; Postgres and Redis adapters | `urmzd/anchor` |
| [`sandbox`](sandbox/README.md) | Sandboxed tools on a shared workspace, NGAC access control, isolated instances (Docker, gVisor), local and Kubernetes runners | `urmzd/playscape` |
| [`flow`](flow/README.md) | The event union, the channel bus, connectors (Slack, GitHub, Jira, cron, webhooks), agent definitions with policy, the durable turn, threads, knowledge | `urmzd/fiber`, then `humagen/fiber` |

`flow` builds on `durable` and `sandbox`; neither of those imports `flow`.

## Installation

```sh
go get github.com/urmzd/orchagent
```

The Kubernetes Job runner is its own module, so only programs that use it inherit client-go:

```sh
go get github.com/urmzd/orchagent/sandbox/runner/k8s
```

Each component ships a CLI that validates it on its own:

| Command | Validates |
|---------|-----------|
| `go run ./sandbox/cmd/sandbox run "go test ./..."` | a command under a sandbox policy |
| `go run ./flow/cmd/flow run` | an `.agent.md` against a message, printing its events |
| `go run ./durable/adapters/postgres/cmd/durable` | a Postgres ledger: list, show, replay |

## Quick Start

```sh
go run ./sandbox/examples/basic      # a sandboxed tool on a bounded pool
go run ./flow/examples/local         # an agent on the bus, offline
docker compose up -d --wait          # Postgres and Redis for durable
go run ./durable/examples/quickstart
```

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

## License

Apache-2.0. See [LICENSE](LICENSE).
