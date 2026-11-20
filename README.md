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

- **One entry point**: `orchagent.New(ctx, opts...)` returns an `App` with the bus, definitions, durable runtime, sandbox and connectors wired. `app.Define` stores an agent, `app.Session` opens a conversation and `Session.Post` starts a run.
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

From the repository root, run an on-call agent using a scripted provider and in-memory stores.

```sh
go run ./flow/examples/downstream
```

The program prints the agent’s answer and its run ID. Follow the [first-run walkthrough](docs/guides/first-run.md) to change the model and see how the session works.

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
| Run an agent and read its reply | `orchagent` | `app.Define`, `app.Session`, `Session.Post` |
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

- [Orchagent documentation](https://docs.urmzd.com/orchagent/)
- [Documentation source](docs/README.md)
- [Contributing](CONTRIBUTING.md)

To preview or build the documentation from the repository root with [uv](https://docs.astral.sh/uv/):

```sh
uvx --from zensical==0.1.2 zensical serve
uvx --from zensical==0.1.2 zensical build
```

To see the shared fonts, colors and logo locally, serve the generated guide and the account’s brand assets on one origin. [Preview a guide with the shared brand](https://github.com/urmzd/urmzd.github.io#preview-a-guide-with-the-shared-brand)

The build writes static files to `site/`. Pull requests run a strict build. After a merge to `main`, [docs.urmzd.com](https://github.com/urmzd/docs.urmzd.com) rebuilds the guide and publishes it at `/orchagent/` within 30 minutes.

## Agent Skill

This repo's conventions are available as portable agent skills in [`skills/`](skills/).

## License

Apache-2.0. See [LICENSE](LICENSE).
