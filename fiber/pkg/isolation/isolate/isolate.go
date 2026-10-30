// Package isolate is the isolation.Leaser over playscape's pkg/isolate: a
// Pool of instances started by a playscape Driver (docker with runsc or
// runc, or local). It converts values and nothing more; what an instance
// is, how it is confined and how instances are kept warm are playscape's.
//
// Every lease is for one run and is destroyed when the run releases it. The
// spec a lease asks for is the same for every run with the same needs, so
// the pool's warm instances serve them all: a run's identity is never
// written into the spec (that would make every run a spec of its own, and
// the pool would warm one instance per run).
package isolate

import (
	"context"
	"errors"
	"fmt"

	"github.com/urmzd/playscape/pkg/isolate"
	"github.com/urmzd/playscape/pkg/workspace"

	"github.com/urmzd/humagen/fiber/pkg/isolation"
)

// Config is what every instance a Leaser leases is started with.
type Config struct {
	// Image is the workload's root filesystem. Required.
	Image string
	// ConnectorsImage is the connectors compartment's; empty is Image.
	ConnectorsImage string
	// Resources bound the workload, and ConnectorResources the connectors
	// compartment. Zero fields are playscape's small defaults.
	Resources          isolate.Resources
	ConnectorResources isolate.Resources
	// Labels mark every instance for the operator. Keys may not start
	// with "playscape.".
	Labels map[string]string
	// Pool configures the pool: how many instances are kept warm per
	// spec, and how many may exist at once.
	Pool isolate.PoolConfig
}

// Leaser leases instances from a playscape pool.
type Leaser struct {
	pool *isolate.Pool
	cfg  Config
}

var _ isolation.Leaser = (*Leaser)(nil)

// New returns a Leaser starting instances with d. It checks the spec it
// will lease with, so a bad image fails here and not on a run's first tool.
func New(d isolate.Driver, cfg Config) (*Leaser, error) {
	if d == nil {
		return nil, errors.New("isolation/isolate: no driver")
	}
	l := &Leaser{cfg: cfg}
	for _, connectors := range []bool{false, true} {
		if err := l.spec(isolation.Request{Connectors: connectors}).Validate(); err != nil {
			return nil, fmt.Errorf("isolation/isolate: %w", err)
		}
	}
	l.pool = isolate.NewPool(d, cfg.Pool)
	return l, nil
}

// spec is the playscape spec for a run's request.
func (l *Leaser) spec(req isolation.Request) isolate.Spec {
	s := isolate.Spec{
		Image:     l.cfg.Image,
		Mounts:    isolate.Mounts{Project: req.Mounts.Project, Me: req.Mounts.Me},
		Resources: l.cfg.Resources,
		Labels:    l.cfg.Labels,
	}
	if req.Connectors {
		s.Connectors = &isolate.ConnectorSpec{Image: l.cfg.ConnectorsImage, Resources: l.cfg.ConnectorResources}
	}
	return s
}

// Lease implements isolation.Leaser.
func (l *Leaser) Lease(ctx context.Context, req isolation.Request) (isolation.Instance, error) {
	lease, err := l.pool.Lease(ctx, l.spec(req))
	if err != nil {
		return nil, err
	}
	return instance{lease: lease}, nil
}

// Stats is a snapshot of the pool.
func (l *Leaser) Stats() isolate.PoolStats { return l.pool.Stats() }

// Close stops warming and destroys every warm instance. Leases still out
// are destroyed when their runs release them.
func (l *Leaser) Close(ctx context.Context) error { return l.pool.Close(ctx) }

// instance is a pool lease as an isolation.Instance.
type instance struct{ lease *isolate.Lease }

func (i instance) ID() string                 { return i.lease.ID() }
func (i instance) Files() workspace.Workspace { return i.lease.Files() }
func (i instance) Close(ctx context.Context) error {
	return i.lease.Release(ctx)
}

func (i instance) Exec(ctx context.Context, c isolation.Command, s isolation.Stdio) (isolation.ExitStatus, error) {
	in := isolate.Workload
	if c.In == isolation.Connectors {
		in = isolate.Connectors
	}
	st, err := i.lease.Exec(ctx, isolate.Command{Argv: c.Argv, Env: c.Env, Dir: c.Dir, Timeout: c.Timeout, In: in},
		isolate.Stdio{Stdin: s.Stdin, Stdout: s.Stdout, Stderr: s.Stderr})
	return isolation.ExitStatus{Code: st.Code}, err
}
