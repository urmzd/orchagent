# Slack connector capabilities

Flow owns signature verification, mention and slash-command translation,
threaded message delivery, reaction APIs, and the optional `AgentAPI` methods
for session status and command registration. Ordinary messaging clients need
only implement `API`; agent-session features are a separate capability.

`Config.Prepare` lets an application check a verified event (`*event.Envelope`)
before it is published. The configured team boundary applies to mentions,
direct messages, stop events, and slash commands after signature verification.
A delivery from another team is acknowledged without invoking application
policy or publishing anything.

Session lifecycle policy, command discovery, refusal logging, app manifests,
and installation configuration live in
[Humagen](https://github.com/urmzd/humagen/blob/main/docs/reference/slack.md).
Use `HUMAGEN_SLACK_*` with `humagen serve` for that experience.
`flow.WithSlack` takes the token, signing secret, team, app id and bot
mapping; it does not enable product session or command policy, which is
Humagen's.

## What Slack publishes

| Type | Raised by |
|------|-----------|
| `message.posted.v1` | A mention of the app or a direct message, with the agents it names (through `Bots`) in `Mentions` |
| `slack.command.invoked.v1` | A slash command |
| `slack.session.stopped.v1` | A person pressing stop on an agent session |

Each is published on the conversation's channel,
`slack://<team>/<channel>[?thread=<ts>]`. The connector chooses no agent: the
channel's members do (`pkg/membership`), and the Slack sink answers on the
same channel, editing one message per run.

## Stopping a session

Slack sends `agent_session_stopped` when a person presses stop, and the
connector publishes it as `slack.session.stopped.v1` on the session's channel.
It starts nothing. The dispatcher (`pkg/dispatch`) maps it to a stop request
(`control.stop.v1`) for each run it started on that channel, with the person
who pressed stop as the asker, so no application bookkeeping is needed.

The runtime records a stop before it announces it when built with
`runtime.WithStopLedger`, as `flow.New` does. The run then ends with
`run.stopped.v1`, and the sink edits the session's message to say so under
what the agent had written. A host that keeps its own mapping can still stop
a run directly with `App.Stop`, using the run id `Acceptance.RunID` reported
and every event of the run carries, `runtime.RunKey(tenant, event id, agent)`.
It is not the Slack event id.

## Feedback ownership

Signed reaction additions and removals use Slack's `event_ts` to order changes
for one native message, actor and rating. `DeliveryState` retains the addition's
event ID and a removal watermark across replica changes. Older or ambiguous
equal-time deliveries cannot overwrite a newer reaction; a removal received
before its addition prevents that late addition from resurrecting feedback.
Missing or malformed event timestamps are ignored for ratings.

Removal calls optional `Feedback.Withdraw` with the original addition identity,
never generic `Record(Removed=true)`. The host must remove only the assessment
still owned by that addition, preserving newer console or native assessments
even when their rating matches. Without `Withdraw`, removal does nothing.
The ordering field follows Slack's [Events API contract](https://docs.slack.dev/apis/events-api/).
