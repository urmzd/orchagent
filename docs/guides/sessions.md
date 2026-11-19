# Work with sessions

A session groups turns by tenant, parent, agent and optional label. The parent can be a place URI, such as a conversation in your application, or another session ID. Supply a stable parent when you want a later call to reopen the same conversation.

## Get a final answer

The function below fits in a trusted Go host that has already stored the definition and started an execution worker. It accepts one message and waits for that message’s durable result. The run ID comes directly from the session’s acceptance.

```go
func answer(ctx context.Context, app *orchagent.App, text string) (string, error) {
    chat, err := app.Session(ctx, orchagent.SessionOptions{
        Parent: "console://downstream", Agent: "oncall",
    })
    if err != nil {
        return "", err
    }
    run, err := chat.Post(ctx, text)
    if err != nil {
        return "", err
    }
    result, err := app.Runtime().Result(ctx, run.RunID)
    if err != nil {
        return "", err
    }
    if result.Stopped != nil {
        return result.Text, fmt.Errorf("run stopped: %s", result.Stopped.Reason)
    }
    return result.Text, nil
}
```

Import `context`, `fmt` and `github.com/urmzd/orchagent`. `Result` polls the durable ledger with backoff until the run finishes or the wait context ends. A stopped result may contain partial text. Cancelling the wait ends the caller’s observation. Use the app’s stop operation when you intend to stop the run itself.

## Follow live output

Subscribe with `chat.Stream(ctx, nil)` before posting. Close the stream when the consumer exits. Match events to the accepted run ID because a session stream also includes direct children. A failed attempt with `Terminal: false` can retry. A cursor tracks the last sequence **per run** when reconnecting.

## Continue later

Reopen the same parent, agent and label, or retain the record’s ID and pass it to `app.OpenSession`. Use `Read(ctx, since)` to read at most 200 transcript entries, then pass the final entry’s sequence as the next cursor. The default in-memory stores live for the lifetime of the app. Use persistent stores when a conversation must survive a restart.

For budget accounting, session trees and access grants, see the [session architecture](../reference/flow/sessions.md).
