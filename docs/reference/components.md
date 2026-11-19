# Component reference

Start with the root `orchagent` package to run a conversation. The packages below expose the storage, delivery and execution interfaces you can compose independently.

| When you need to… | Package | Read next |
| --- | --- | --- |
| Deliver messages and observe typed events | `flow` | [Architecture](flow/overview.md) · [Event catalog](flow/events.md) |
| Resume a workflow after worker failure | `durable` | [Ledger, queue and workers](durable/overview.md) · [Replay specification](durable/replay.md) |
| Enforce an agent’s workspace and tool permissions | `sandbox` | [Sandbox architecture](sandbox/overview.md) |
| Connect an external surface | `flow/connectors` | [Slack](flow/slack.md) · [GitHub](flow/github.md) |

## Runnable examples

Run these commands from the repository root. Each uses in-memory backends.

```sh
go run ./flow/examples/local
go run ./sandbox/examples/basic
```

The flow example delivers a run to two sinks and replays recorded work. The sandbox example runs a tool under a bounded pool. Browse the [Go package reference](https://pkg.go.dev/github.com/urmzd/orchagent) for signatures and available options.
