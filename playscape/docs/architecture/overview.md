# Architecture Overview

dispatch is a control plane for agent execution nodes. It exists so that a user can deploy a single service, scale its agents out with proper metrics, and trust that every tool is confined to the workspace areas it was granted. This document describes the beta architecture; interfaces are stabilizing but pre-1.0.

## Design Goals

1. **One service, scaled execution.** The unit users deploy is a service spec. Execution capacity scales by adding consumers, never by redeploying the service.
2. **Default-deny sandbox.** Tools are locked into declared areas of the workspace and declared spawn targets. Anything not granted is denied, so a misbehaving or compromised tool cannot leak state outside its area.
3. **One shared workspace.** Every node mounts the same storage backend. State lives in exactly one place regardless of how many agents run or where.
4. **Orthogonality.** Each package owns one concern and composes through small interfaces. The dependency graph is a strict DAG; changing a backend, substrate, or transport touches one package.

## Package Graph

```text
task        (leaf: Task, Result)
workspace   (leaf: shared blob store; Local backend)
metrics     (leaf: Recorder; Nop, Memory)
   |
sandbox     -> workspace          (Policy: areas + spawn allowlist; Scope decorator)
tool        -> task, workspace    (Tool, Runtime, Registry)
queue       -> task               (Queue, Results; Memory, httpqueue)
node        -> task, sandbox      (Node, Spec, Factory, Spawner)
node/inproc -> node, tool, ...    (in-process execution substrate)
worker      -> queue, node, metrics (consumer loop)
   |
controlplane -> everything above  (composition root: Deploy/Scale/Submit/Status)
internal/server                   (HTTP translation layer over controlplane)
internal/cli                      (serve, work, version, update)
```

Arrows point at dependencies. Nothing depends downward on `controlplane`, and the leaves depend on nothing but the standard library.

## Producer/Consumer Execution

Every deployment owns a queue. Producers put tasks on it; consumers compete to take them off:

```text
producers                      queue                consumers
---------                      -----                ---------
HTTP API  --Submit/Async-->  [ task task task ]  <--Dequeue-- local goroutines (Scale)
tools     --Spawn---------->                     <--Lease---- dispatch work pods (k8s/HPA)
```

- **Local scaling**: `Scale(n)` runs n worker goroutines inside the control plane process. Right default for a single binary.
- **Remote scaling**: `dispatch work --server <url> --deployment <name>` runs the same consumer loop in its own process, leasing over HTTP (long poll) and reporting results back. Kubernetes Deployments, HPAs, or serverless containers replicate this process; the control plane never changes. Deploy with `"replicas": -1` for a fleet-only deployment.
- The two kinds of consumers share one queue, so they can coexist.

Results flow back through `queue.Results`: synchronous submitters block on `Await`, asynchronous submitters poll by task ID.

Beta caveats: the queue is in-memory and delivery is at-most-once (a consumer that dies mid-task drops it). Durable brokers (Redis, Pub/Sub, SQS) implement the same two interfaces when they land.

## Sandbox Model

A `sandbox.Policy` binds one tool name to its capabilities:

- **Areas**: workspace key prefixes the tool may touch, each optionally read-only. Matching is path-segment aware (`runs` matches `runs/1/out`, not `runs-archive`), and traversal (`..`, absolute keys) is rejected before matching.
- **Spawn**: tool names this tool may submit sub-tasks for.

`sandbox.Scope` wraps the raw workspace in a policy-checking decorator; nodes hand tools that scoped view, never the backend. `List` filters out keys the policy cannot read, so a tool cannot even observe what exists outside its areas. A tool with no policy runs with an empty one: no areas, no spawn targets. The sandbox is a security boundary; a bypass is a vulnerability (see SECURITY.md).

## Self-Referential Agents

`tool.Runtime.Spawn` submits a sub-task back into the tool's own deployment and returns its task ID. The child task competes on the queue like any other work, so an agent calling another agent (or itself) is just another producer. Recursion is bounded by policy: a tool can only spawn tools on its allowlist, and the default is none.

## Metrics

Components record through `metrics.Recorder` (counters, gauges, distributions) and stay ignorant of transport. The beta ships an in-memory recorder exposed as text at `GET /metrics`:

| Series | Meaning |
|--------|---------|
| `dispatch_nodes{deployment}` | current local node count |
| `dispatch_tasks_submitted_total{deployment,tool}` | tasks produced |
| `dispatch_tasks_total{deployment,tool,status}` | tasks executed locally |
| `dispatch_tasks_total{deployment,source="remote",status}` | tasks reported by remote consumers |
| `dispatch_task_seconds_{count,sum}{deployment,tool}` | execution latency |

A Prometheus or OpenTelemetry recorder replaces the in-memory one without any caller changing.

## Workspace Backends

`workspace.Workspace` is a minimal blob store (Read/Write/List/Delete over slash-separated keys). The beta ships `workspace.Local`, a directory on disk. Because the interface is the only thing the rest of the system sees, GCS/S3-style backends slot in without touching sandbox, nodes, or the control plane. Until a networked backend lands, remote workers on separate hosts each see their own local workspace; deploy them with a shared mount if tools must exchange artifacts.

## Validated Paths

- `go test ./...` covers sandbox confinement (including traversal and list-leak cases), queue-based scaling, policy-gated spawning, and a full remote-consumer round trip over HTTP.
- `examples/saige/` runs real [saige](https://github.com/urmzd/saige) agents as workloads, including an agent delegating to a sub-agent.
- `deploy/k8s/dispatch.yaml` was validated on minikube: one server pod, a worker fleet scaled 2 to 5 with `kubectl scale`, all tasks executed remotely.

## Roadmap

- Durable queue backends (Redis, Pub/Sub, SQS) behind `queue.Queue`
- GCS/S3 workspace backends behind `workspace.Workspace`
- Prometheus exposition for `metrics.Recorder`
- Container-per-node execution substrate behind `node.Factory`
- Lease acknowledgment for at-least-once delivery
