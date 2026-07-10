package server_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/urmzd/dispatch/internal/server"
	"github.com/urmzd/dispatch/pkg/controlplane"
	"github.com/urmzd/dispatch/pkg/metrics"
	"github.com/urmzd/dispatch/pkg/node"
	"github.com/urmzd/dispatch/pkg/node/inproc"
	"github.com/urmzd/dispatch/pkg/queue/httpqueue"
	"github.com/urmzd/dispatch/pkg/sandbox"
	"github.com/urmzd/dispatch/pkg/task"
	"github.com/urmzd/dispatch/pkg/tool"
	"github.com/urmzd/dispatch/pkg/worker"
	"github.com/urmzd/dispatch/pkg/workspace"
)

// TestRemoteConsumerRoundTrip exercises the full producer/consumer path a
// Kubernetes worker pod takes: the control plane has no local nodes, a
// remote worker leases over HTTP, executes, reports back, and the producer
// receives the result.
func TestRemoteConsumerRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Control plane with zero local nodes: consumers must come from outside.
	planeWS, err := workspace.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	registry := tool.NewRegistry()
	err = registry.Register(tool.Func("echo", func(_ context.Context, _ tool.Runtime, in []byte) ([]byte, error) {
		return in, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	rec := metrics.NewMemory()
	plane := controlplane.NewMemory(inproc.NewFactory(registry, planeWS), rec)
	srv := httptest.NewServer(server.New(plane, plane, rec))
	defer srv.Close()

	err = plane.Deploy(ctx, controlplane.ServiceSpec{
		Name:     "svc",
		Replicas: -1, // remote consumers only
		Policies: []sandbox.Policy{{Tool: "echo", Areas: []sandbox.Area{{Prefix: "echo"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Remote worker: its own workspace dir and registry, attached over HTTP —
	// exactly what `dispatch work` builds.
	client := httpqueue.New(srv.URL, "svc")
	client.LeaseWait = time.Second
	spec, err := client.Spec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pdp, err := spec.PDP()
	if err != nil {
		t.Fatal(err)
	}
	workerWS, err := workspace.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	workerRegistry := tool.NewRegistry()
	err = workerRegistry.Register(tool.Func("echo", func(_ context.Context, _ tool.Runtime, in []byte) ([]byte, error) {
		return append([]byte("remote:"), in...), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	n, err := inproc.NewFactory(workerRegistry, workerWS).New(ctx, node.Spec{
		Deployment: "svc",
		PDP:        pdp,
		Spawn:      client.SubmitAsync,
	})
	if err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithCancel(ctx)
	defer wcancel()
	go func() {
		w := &worker.Worker{Deployment: "svc", Queue: client, Results: client, Node: n}
		w.Run(wctx) //nolint:errcheck // exits on cancel
	}()

	// Produce a task through the plane; the remote worker must execute it.
	res, err := plane.Submit(ctx, "svc", task.Task{Tool: "echo", Input: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Output) != "remote:hello" {
		t.Fatalf("output = %q, want remote execution", res.Output)
	}

	key := `dispatch_tasks_total{deployment="svc",source="remote",status="ok"}`
	if got := rec.Snapshot()[key]; got != 1 {
		t.Fatalf("%s = %v, want 1", key, got)
	}
}
