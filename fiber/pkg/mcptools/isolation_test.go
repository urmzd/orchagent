package mcptools_test

import (
	"context"
	"os"
	"testing"

	"github.com/urmzd/humagen/fiber/pkg/isolation"
	"github.com/urmzd/humagen/fiber/pkg/isolation/isolationtest"
	"github.com/urmzd/humagen/fiber/pkg/mcptools"
	"github.com/urmzd/humagen/fiber/pkg/toolprovider"
)

func stdioProvider(t *testing.T) toolprovider.Provider {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return toolprovider.Provider{
		Tenant: tenant, Name: "fake", Transport: toolprovider.TransportStdio,
		Endpoint: exe, Args: []string{fakeArg}, CredentialRef: "vault:fake", CredentialEnv: "FAKE_TOKEN", Version: 1,
	}
}

// callEnv calls the fake server's env tool on s.
func callEnv(t *testing.T, s *mcptools.Session) string {
	t.Helper()
	ctx := context.Background()
	offered, err := s.Offered(ctx)
	if err != nil {
		t.Fatal(err)
	}
	snap := toolprovider.Snapshot{Tenant: tenant, Provider: "fake", Version: 1, Tools: pick(t, offered, map[string]toolprovider.Class{"env": toolprovider.ClassRead})}
	ts, err := s.Toolset(ctx, snap, toolprovider.GrantRead)
	if err != nil {
		t.Fatal(err)
	}
	env, err := ts.Lookup("fake__env")
	if err != nil {
		t.Fatal(err)
	}
	out, err := env.Execute(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// A stdio server dialled for an isolated run runs in that run's instance, in
// its connectors compartment, with its credential in the command's own
// environment and nothing of the host's.
func TestAStdioServerOfAnIsolatedRunRunsInItsConnectorsCompartment(t *testing.T) {
	t.Setenv(canary, "host-secret")
	leaser := &isolationtest.Leaser{Dir: t.TempDir()}
	run := isolation.NewRun(leaser, isolation.Request{Tenant: tenant, Run: "r1", Connectors: true}, nil)
	ctx := isolation.WithRun(context.Background(), run)

	s, err := mcptools.Connect(ctx, stdioProvider(t), mcptools.WithResolver(resolver(t)))
	if err != nil {
		t.Fatalf("Connect in a run: %v", err)
	}
	if s.Where() != "run" {
		t.Fatalf("Where = %q, want run", s.Where())
	}
	if out := callEnv(t, s); out != "token="+mcptools.Redacted+" canary=false" {
		t.Fatalf("the server reported %q", out)
	}
	insts := leaser.Instances()
	if len(insts) != 1 {
		t.Fatalf("%d instances leased, want 1", len(insts))
	}
	execs := insts[0].Execs()
	if len(execs) != 1 || execs[0].In != isolation.Connectors {
		t.Fatalf("execs = %+v, want one in the connectors compartment", execs)
	}
	if execs[0].Env["FAKE_TOKEN"] != token || len(execs[0].Env) != 1 {
		t.Fatalf("the server's environment held %d variables; want exactly its credential", len(execs[0].Env))
	}
	_ = s.Close()
	if err := run.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !insts[0].Closed() {
		t.Fatal("the run's instance outlived its release")
	}
}

// Outside a run, WithLeaser starts the server in an instance of its own,
// destroyed with the session.
func TestAStdioServerOutsideARunGetsAnInstanceOfItsOwn(t *testing.T) {
	leaser := &isolationtest.Leaser{Dir: t.TempDir()}
	s, err := mcptools.Connect(context.Background(), stdioProvider(t), mcptools.WithResolver(resolver(t)), mcptools.WithLeaser(leaser))
	if err != nil {
		t.Fatal(err)
	}
	if s.Where() != "instance" {
		t.Fatalf("Where = %q, want instance", s.Where())
	}
	callEnv(t, s)
	insts := leaser.Instances()
	if len(insts) != 1 || !insts[0].Request().Connectors {
		t.Fatalf("leased %d instances; want one with a connectors compartment", len(insts))
	}
	_ = s.Close()
	if len(leaser.Open()) != 0 {
		t.Fatal("the session's instance outlived the session")
	}
}

// A grant expanded for an isolated run gets a session of that run's: never
// another run's, and closed when the run releases its instance.
func TestAGrantInAnIsolatedRunHasASessionOfTheRunsOwn(t *testing.T) {
	store := toolprovider.NewMemory()
	rec := stdioProvider(t)
	rec.Version = 0
	p, err := store.PutProvider(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := mcptools.Connect(context.Background(), p, mcptools.WithResolver(resolver(t)))
	if err != nil {
		t.Fatal(err)
	}
	accept(t, store, probe, 0, map[string]toolprovider.Class{"env": toolprovider.ClassRead})
	_ = probe.Close()

	leaser := &isolationtest.Leaser{Dir: t.TempDir()}
	g := mcptools.NewGrants(store, mcptools.Dialer(mcptools.WithResolver(resolver(t))))
	t.Cleanup(func() { _ = g.Close() })

	expand := func(run *isolation.Run) {
		t.Helper()
		ctx := isolation.WithRun(context.Background(), run)
		for range 2 { // twice in one run: one server
			tools, ok, err := g.Expand(ctx, tenant, "mcp:fake")
			if !ok || err != nil || len(tools) != 1 {
				t.Fatalf("Expand = %d tools, ok %v, %v", len(tools), ok, err)
			}
			if _, err := tools[0].Execute(ctx, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	a := isolation.NewRun(leaser, isolation.Request{Tenant: tenant, Run: "a", Connectors: true}, nil)
	b := isolation.NewRun(leaser, isolation.Request{Tenant: tenant, Run: "b", Connectors: true}, nil)
	expand(a)
	expand(b)
	insts := leaser.Instances()
	if len(insts) != 2 {
		t.Fatalf("%d instances for two runs, want 2", len(insts))
	}
	for _, i := range insts {
		if n := len(i.Execs()); n != 1 {
			t.Fatalf("instance %s started %d servers, want 1", i.ID(), n)
		}
	}
	for _, r := range []*isolation.Run{a, b} {
		if err := r.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if open := leaser.Open(); len(open) != 0 {
		t.Fatalf("%d instances outlived their runs", len(open))
	}
}
