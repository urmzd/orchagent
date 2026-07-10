---
name: extend-dispatch
description: Extend the dispatch control plane with new tools, workspace backends, queue backends, or execution substrates while preserving the sandbox boundary and the package dependency DAG. Use when adding capabilities to dispatch or reviewing changes to it.
---

# Extend dispatch

dispatch composes small interfaces: `workspace.Workspace` (shared storage), `sandbox.Policy` (default-deny confinement), `tool.Tool` (capability), `queue.Queue`/`queue.Results` (producer/consumer seam), `node.Factory` (execution substrate), `metrics.Recorder` (observability), `controlplane.ControlPlane` (composition root). Extend by implementing an interface, never by widening one.

## Add a tool

1. Implement `tool.Tool` or wrap a function with `tool.Func(name, fn)`.
2. Inside the tool, reach the workspace only through `rt.Workspace()` and spawn sub-tasks only through `rt.Spawn(ctx, task.Task{...})`. Both are pre-confined.
3. Register it: `registry.Register(myTool)`.
4. Grant capabilities in the `ServiceSpec`: `sandbox.Policy{Tool: "name", Areas: [...], Spawn: [...]}`. No policy means no workspace access and no spawning.

## Add a workspace backend (GCS, S3)

1. Create `pkg/workspace/<backend>.go` implementing `Workspace` (Read/Write/List/Delete over slash-separated keys).
2. Validate every key with `workspace.ValidKey`; return `workspace.ErrNotFound` and `workspace.ErrInvalidKey` sentinels.
3. Mirror `local_test.go` for the new backend: round trip, list ordering, invalid keys.

## Add a queue backend (Redis, Pub/Sub, SQS)

1. Implement `queue.Queue` (Enqueue, blocking Dequeue) and `queue.Results` (Report, Await, Get).
2. `Dequeue` must honor context cancellation; see `httpqueue` for a remote reference.

## Add an execution substrate (containers, VMs)

Implement `node.Factory` and `node.Node`. Honor `node.Spec`: build one sandboxed runtime per policy and wire `Spec.Spawn` into `tool.Runtime.Spawn` gated by `policy.MaySpawn`. `pkg/worker` and the control plane need no changes.

## Rules

- The sandbox is a security boundary: any change near `pkg/sandbox` needs tests proving confinement (traversal, sibling prefixes, list-leak, spawn gating).
- Preserve the DAG: nothing under `pkg/` imports `pkg/controlplane`; `task`, `workspace`, `metrics` stay stdlib-only.
- Verify with `make check`; run `go run ./examples/basic/` to see the composition end to end.
