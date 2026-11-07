# Agent sessions

A session is one agent's conversation, keyed by `(tenant, parent, agent, label)`.
The parent is a place URI or another session ID. Reopening a key continues its
journal. A label isolates an independent pass; `Fresh` generates a new label.

```go
s, err := app.Session(ctx, session.Open{
    Parent: "github://acme/service/pull/5", Agent: "reviewer",
    Channel: "github://acme/service/pull/5",
})
if err != nil { return err }
accepted, err := s.Post(ctx, "Review this change")
```

`Post` returns a durable acceptance. `Stream` tails the session and newly created
direct children, with a map of run IDs to sequence cursors. `Read` returns at most
200 journal entries after a cursor. `Summary` returns deterministic excerpts with
entry citations. `Children` and `Link` expose the tree and context references.

Hosts authenticate and authorize before opening a handle. `AcceptOwnSession`
preserves a host's already unique per-agent event ID. Its child variant records
the durable parent and question for terminal-result notification.

## Agent tools and governance

`runtime.WithSessionRuntime` installs `session_call`, `session_listen`,
`session_read`, `session_status` and `session_link` in the sandbox. Calls use the
parent's pinned `Access.Spawn`. The pinned policy's `session_depth` defaults to
3 and `open_children` to 8. Admission atomically reserves a slot per parent across
replicas. Retrying a call reuses its reservation; only failed admission or a
terminal ledger result releases it.

`session_call` continues a child by default. `label` and `fresh` isolate contexts.
`wait: false` returns its session and run IDs. `session_listen` snapshots up to
eight sessions' current runs and parks on their terminal results. With
`until: "message"`, it pins transcript heads and waits for later entries, including
sessions with no run yet. Optional `cursors: [{session, since}]` supplies last-seen
positions. Results include bounded messages and their next cursors. The worker's
janitor repairs missed notifications, including future messages. Both waits are
durable and release their worker. No terminal state is inferred from silence.

Agent reads stay within their tree unless an `AccessStore` explicitly grants
access. A context or evidence link grants nothing. Grants and revocations are
authenticated host operations. They never cross tenants and are not inherited
by another agent merely because it is a child of the granted session.

A host uses `runtime.WithChildStarter` for its own governed admission path. The
hook must use `SessionCall.ID` as its idempotency key, return immediately after
durable acceptance, and never poll for an answer. Humagen uses this seam for
`pkg/call`. `WithChildRunID` supplies its deterministic run ID before admission;
the starter must return that ID. Recovery preserves accepted work and fences
abandoned admission with a failed ledger record before releasing the slot.
`WithSessionRuntime` must also be set on a host-created sandbox.

## Storage and roles

`WithSessions` replaces the memory default. `WithPostgres` persists identity,
links, run indexes, grants and reservations on the caller's pool. Custom stores
must implement `AdmissionStore` for agent calls, or admission fails closed.
Without `AccessStore`, cross-tree reads are denied.

Unlabelled surface sessions retain their existing `fiber_thread_*` journal keys.
Children and labelled sessions have isolated keys. Old durable inputs replay
without session metadata and keep their original journal identity.

`Run(ctx, RoleExecution)` runs workers only. `RoleIngress`, `RoleDispatch` and
`RoleDelivery` select the other responsibilities. Omitting roles starts all of
them. `Handler()` is the combined webhook handler. The older one-shot `Ask` and
`Reply`, `RunDelivery`, `RunExecution` and `WebhookHandler` remain compatibility
entry points while callers migrate.

## Budgets and live usage

Standalone `Session.Post` requires a positive `limits.budget`. Each subagent call
also requires its own explicit `subagents[].budget` allocation. Zero grants no
work. `BudgetStore` atomically reserves parent capacity across concurrent calls,
persists integer cost, token and request caps, and settles immutable receipts.
Each native turn receives the remaining reservation through saige's budget.
Unpriced models require token or request caps without a money ceiling. Unknown
spend retains the reservation. An abandoned attempt is charged conservatively;
a failed budgeted turn ends rather than retrying beyond its reserved allowance.

A completed parent account can retain outstanding child reservations. Late child
or grandchild receipts settle through closed ancestors, once, after restart.
`Session.Usage` reads live tree totals from `UsageStore`: actual model call IDs
are deduplicated at every ancestor, so child totals are never added a second
time. Completed run outputs and events remain immutable snapshots.

Hosts with `WithChildStarter` own budget admission and native request caps; flow
does not reserve the same money again. Legacy `Accept` and `AcceptOwn` inputs
without budget declarations remain compatible. Explicit session posts and
standalone child calls fail closed without a grant or capable budget store.
Custom turn runtimes receive `turn.Input.Budget` and must enforce those caps.

## Verification

Race tests cover identity creation, labelled and fresh history, tenant isolation,
explicit grants and revocation, concurrent admission and budget reservations,
immutable settlement receipts, runtime restart with late descendant settlement,
usage redelivery, native request caps, and both terminal and message waits with
only one worker. Postgres tests require `FLOW_POSTGRES_TEST_URL`.
