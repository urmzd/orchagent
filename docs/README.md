# Run agents in Go

Orchagent runs versioned agent definitions inside your Go program. Start a conversation with one agent, post a message and follow its run to a final answer.

**[Run your first agent](guides/first-run.md)** to try a complete conversation with an in-memory worker and a scripted model. The example prints the reply and the ID of the run that produced it.

## What you work with

| Item | What it identifies |
| --- | --- |
| Definition | A versioned `.agent.md` file containing an agent’s instructions, model, tools and limits. |
| Session | One continuing conversation for an agent in a tenant. Reopen the same session to continue its history. |
| Run | The work accepted for one message. Its ID lets the caller inspect progress and retrieve its result. |
| Event | One typed update, such as a text delta or completion, published on a channel. |

## Continue building

[Work with sessions](guides/sessions.md) to retrieve a final answer, resume a conversation or observe live events. For replay rules, sandbox policies and connector delivery, use the [component reference](reference/components.md).
