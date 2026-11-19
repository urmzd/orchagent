# Run your first agent

From a checkout of Orchagent, run the on-call example with Go 1.26.10 or later. Go downloads the module dependencies on the first build. The agent itself uses a scripted provider and in-memory stores.

```sh
git clone https://github.com/urmzd/orchagent.git
cd orchagent
go run ./flow/examples/downstream
```

The first line is the agent’s reply. The second includes a different run ID each time.

```text
oncall answered on the house model (house-1).
run <run-id>: completed
```

## Read the definition

Open [oncall.agent.md](https://github.com/urmzd/orchagent/blob/main/flow/examples/downstream/agents/oncall.agent.md). Its `model.use` value, `house/house-1`, selects the provider named `house` and its model named `house-1`. The Go program registers that provider. The `limits.budget` block bounds cost, model requests and tokens. A standalone session post requires a positive budget.

Change `house/house-1` to `house/house-2` and run the command again. The scripted reply now names `house-2`. The example embeds the definition at build time and creates fresh stores on each invocation.

## Follow one message

1. `orchagent.New` composes the app for tenant `acme`. `app.Run` drives its workers.
2. `app.Define` stores the agent document. Submitting identical content under the same name and version is idempotent.
3. `app.Session` opens the on-call conversation under `console://downstream`. `conversation.Post` accepts the message and returns a run ID.
4. The example subscribes to the session before posting, waits for the run’s completion and prints the assistant’s transcript entry.

Inspect the [complete Go program](https://github.com/urmzd/orchagent/blob/main/flow/examples/downstream/main.go), then [retrieve a final answer from a session](sessions.md).
