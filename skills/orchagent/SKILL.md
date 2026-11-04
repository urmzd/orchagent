---
name: orchagent
description: Compose durable agent runs, sandboxed tools and the event flow in Go with github.com/urmzd/orchagent. Use when importing orchagent's durable, sandbox or flow packages, wiring Postgres or Redis adapters, writing sandbox policies, or defining agents on the flow bus.
---

# orchagent

orchagent is a Go library, not a service. Compose it in code: no listener, no config files, no environment variables decide what gets built.

## Components

| Import | Use it for |
|--------|------------|
| `github.com/urmzd/orchagent/durable` | Runs that finish once despite crashes: ledger, leased queue, worker, replay |
| `github.com/urmzd/orchagent/sandbox` | Tools that touch only what their policy grants: workspace, registry, control plane, NGAC |
| `github.com/urmzd/orchagent/flow` | Events in and out: bus, connectors, `.agent.md` definitions, the durable turn |
| `github.com/urmzd/orchagent/sandbox/runner/k8s` | Kubernetes Job runner (separate module, pulls client-go) |

## Rules

- `flow` may import `durable` and `sandbox`. `durable` and `sandbox` never import `flow` or each other.
- Pick backends by passing values in Go. Anything not passed is the in-memory implementation.
- Keep stored names as they are: `fiber_*` tables, `fiber://` URIs, `fiber.*` event kinds, `X-Fiber-Secret`. Renaming needs a data migration.

## Start from an example

| Goal | Example |
|------|---------|
| Sandboxed tool, no services | `go run ./sandbox/examples/basic` |
| Agent on the bus, offline | `go run ./flow/examples/local` |
| Durable workflow on Postgres and Redis | `docker compose up -d --wait && go run ./durable/examples/quickstart` |

## Verify

```sh
make check              # fmt, vet, lint, race tests
make test-integration   # Postgres and Redis
```
