// Package inproc provides an in-process node.Factory: each node executes
// tools on the calling goroutine, resolving them from a shared registry and
// confining each to its sandbox policy. It is the execution substrate for
// both the single-binary control plane and the `dispatch work` consumer that
// Kubernetes or serverless containers scale out.
package inproc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/urmzd/dispatch/pkg/node"
	"github.com/urmzd/dispatch/pkg/sandbox"
	"github.com/urmzd/dispatch/pkg/task"
	"github.com/urmzd/dispatch/pkg/tool"
	"github.com/urmzd/dispatch/pkg/workspace"
)

// Factory creates in-process nodes over a shared tool registry and
// workspace backend.
type Factory struct {
	registry *tool.Registry
	ws       workspace.Workspace
	seq      atomic.Uint64
}

// NewFactory returns a factory whose nodes resolve tools from registry and
// reach ws only through each tool's sandbox policy.
func NewFactory(registry *tool.Registry, ws workspace.Workspace) *Factory {
	return &Factory{registry: registry, ws: ws}
}

// New implements node.Factory.
func (f *Factory) New(_ context.Context, spec node.Spec) (node.Node, error) {
	runtimes := make(map[string]*runtime, len(spec.Policies))
	for _, p := range spec.Policies {
		runtimes[p.Tool] = &runtime{
			ws:     sandbox.Scope(f.ws, p),
			policy: p,
			spawn:  spec.Spawn,
		}
	}
	return &inprocNode{
		id:       fmt.Sprintf("%s-%d", spec.Deployment, f.seq.Add(1)),
		registry: f.registry,
		runtimes: runtimes,
		// Tools with no policy run against an empty one: no workspace
		// areas, no spawn targets — default deny, not default allow.
		deny: &runtime{ws: sandbox.Scope(f.ws, sandbox.Policy{})},
	}, nil
}

// runtime implements tool.Runtime for one tool under one policy.
type runtime struct {
	ws     workspace.Workspace
	policy sandbox.Policy
	spawn  node.Spawner
}

func (r *runtime) Workspace() workspace.Workspace { return r.ws }

func (r *runtime) Spawn(ctx context.Context, t task.Task) (string, error) {
	if r.spawn == nil {
		return "", fmt.Errorf("%w: tool %q: spawning disabled on this node", sandbox.ErrDenied, r.policy.Tool)
	}
	if !r.policy.MaySpawn(t.Tool) {
		return "", fmt.Errorf("%w: tool %q may not spawn %q", sandbox.ErrDenied, r.policy.Tool, t.Tool)
	}
	return r.spawn(ctx, t)
}

type inprocNode struct {
	id       string
	registry *tool.Registry
	runtimes map[string]*runtime
	deny     *runtime

	mu     sync.RWMutex
	closed bool
}

func (n *inprocNode) ID() string { return n.id }

// Run implements node.Node.
func (n *inprocNode) Run(ctx context.Context, t task.Task) (task.Result, error) {
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return task.Result{}, fmt.Errorf("node %s: stopped", n.id)
	}

	tl, ok := n.registry.Get(t.Tool)
	if !ok {
		return task.Result{}, fmt.Errorf("node %s: unknown tool %q", n.id, t.Tool)
	}
	rt, ok := n.runtimes[t.Tool]
	if !ok {
		rt = n.deny
	}

	out, err := tl.Call(ctx, rt, t.Input)
	if err != nil {
		return task.Result{}, fmt.Errorf("node %s: tool %q: %w", n.id, t.Tool, err)
	}
	return task.Result{TaskID: t.ID, NodeID: n.id, Output: out}, nil
}

// Health implements node.Node.
func (n *inprocNode) Health(context.Context) node.Health {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.closed {
		return node.Stopped
	}
	return node.Ready
}

// Close implements node.Node.
func (n *inprocNode) Close(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closed = true
	return nil
}
