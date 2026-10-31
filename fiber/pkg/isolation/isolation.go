// Package isolation is the seam between a run's tools and the isolated
// instance they execute in (RFC 0004, "The sandbox").
//
// The agent loop stays in the trusted process. What a run's tools do, the
// shell, the file tools and stdio MCP servers, happens in an Instance leased
// for that run and destroyed when it ends. A model key, the platform's
// database credentials and other people's secrets never enter one.
//
// The types mirror playscape's pkg/isolate (Instance, Command, Stdio,
// ExitStatus, Compartment), so the adapter onto a playscape driver and pool
// only converts values. This package names no technology: a Leaser decides
// what an instance is.
//
// # One instance per run
//
// A Run is one run's claim on an instance. It leases lazily, on the first
// tool that needs one, so a turn that calls no tool starts nothing. It is
// never handed to another run: a run called by another (a durable child, or
// a delegated sub-task) gets a Run of its own. Release destroys the instance
// whatever ended the run, success, failure or cancellation, and a released
// Run cannot lease again.
//
// # Files
//
// The instance's work directory starts from a Files source and is written
// back to it when the run ends. Today that source is a conversation's
// scratch directory, so a follow-up turn sees what the last one wrote; the
// memory and files manifest (RFC 0004, layer 6) replaces it behind the same
// interface.
//
// # Unisolated
//
// A Run with no Leaser is unisolated: Isolated reports false and Instance
// fails with ErrUnisolated. Callers keep today's host behavior for what they
// can do safely on the host (the file tools, over the conversation's
// directory) and refuse the rest (the shell). It is for development and
// tests, and the platform says so when it starts.
package isolation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/urmzd/playscape/pkg/workspace"
)

// Paths inside an instance, as playscape's isolate lays one out.
const (
	// WorkDir holds the run's own files, read and write. Commands start
	// here.
	WorkDir = "/work"
	// ProjectDir holds the project's base snapshot, read only.
	ProjectDir = "/project"
	// MeDir holds the requester's volume, read and write.
	MeDir = "/me"
)

var (
	// ErrUnisolated reports a request for an instance in a run that has
	// none: the platform runs tools on its own host.
	ErrUnisolated = errors.New("isolation: this run is not isolated")
	// ErrReleased reports a lease asked of a Run that has ended.
	ErrReleased = errors.New("isolation: the run's instance was released")
	// ErrNoConnectors reports a command for the connectors compartment in
	// an instance leased without one.
	ErrNoConnectors = errors.New("isolation: the instance has no connectors compartment")
)

// Instance is one started sandbox. It belongs to one run. It mirrors
// playscape's isolate.Instance.
type Instance interface {
	// ID names the instance.
	ID() string
	// Exec runs cmd to completion inside the instance. A non-zero exit is
	// an ExitStatus, not an error.
	Exec(ctx context.Context, cmd Command, stdio Stdio) (ExitStatus, error)
	// Files is the instance's work directory, for the host to seed inputs
	// and collect outputs. It is valid until Close.
	Files() workspace.Workspace
	// Close stops every process in the instance and destroys it.
	Close(ctx context.Context) error
}

// Compartment selects where a Command runs.
type Compartment int

const (
	// Workload is the compartment of the shell and the file tools.
	Workload Compartment = iota
	// Connectors is the compartment of tool servers: it holds their
	// credentials, which code in the workload cannot read.
	Connectors
)

func (c Compartment) String() string {
	switch c {
	case Workload:
		return "workload"
	case Connectors:
		return "connectors"
	}
	return "compartment(" + strconv.Itoa(int(c)) + ")"
}

// Command is one command to run in an instance.
type Command struct {
	// Argv is the full argv, never passed through a shell unless Argv[0]
	// is one.
	Argv []string
	// Env is the whole environment: nothing from the host is inherited.
	Env map[string]string
	// Dir is relative to WorkDir; empty is WorkDir. Workload only.
	Dir string
	// Timeout bounds the command; zero is the driver's default.
	Timeout time.Duration
	// In is the compartment the command runs in.
	In Compartment
}

// Stdio connects a command's standard streams. Nil Stdin reads as empty;
// nil Stdout or Stderr discards.
type Stdio struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// ExitStatus is the outcome of a command that started and exited.
type ExitStatus struct {
	Code int
}

// Mounts are the host directories an instance sees besides its work
// directory. Empty mounts nothing.
type Mounts struct {
	// Project is mounted read only at ProjectDir.
	Project string
	// Me is mounted read and write at MeDir, only in that person's runs.
	Me string
}

// Request is what one run asks a Leaser for.
type Request struct {
	// Tenant and Run name the run the instance belongs to. They label the
	// instance; they never choose one.
	Tenant, Run string
	// Mounts are the run's mounts.
	Mounts Mounts
	// Connectors asks for the connectors compartment, for a run that may
	// start a tool server.
	Connectors bool
	// Egress are the hosts the run may reach, each an exact name or
	// "*.domain", through the sandbox's egress gate. Empty reaches none.
	// Like Tenant and Run it never chooses an instance: it is granted to
	// the lease, not written into what the pool warms by.
	Egress []string
}

// Leaser starts an instance for one run. The caller owns it and Closes it,
// which destroys it; it is never leased again. playscape's isolate.Pool is
// the production Leaser, through an adapter.
type Leaser interface {
	Lease(ctx context.Context, req Request) (Instance, error)
}

// LeaserFunc adapts a function to Leaser.
type LeaserFunc func(ctx context.Context, req Request) (Instance, error)

// Lease calls f.
func (f LeaserFunc) Lease(ctx context.Context, req Request) (Instance, error) { return f(ctx, req) }

// Files is where a run's work directory comes from and goes back to.
type Files interface {
	// Load copies the run's starting files into dst, the instance's work
	// directory.
	Load(ctx context.Context, dst workspace.Workspace) error
	// Save writes what the run left in src back.
	Save(ctx context.Context, src workspace.Workspace) error
}

// Sandbox is a deployment's choice of where tools run.
type Sandbox struct {
	// Leaser starts instances. Nil is unisolated.
	Leaser Leaser
	// Name says what the Leaser is, for the start message and the run's
	// log: "docker/runsc", "docker/runc", "unisolated".
	Name string
	// Egress reports that instances reach the hosts a Request names
	// through an egress gate. False is no network at all.
	Egress bool
}

// Isolated reports whether the sandbox starts instances.
func (s Sandbox) Isolated() bool { return s.Leaser != nil }

// String is the sandbox's name.
func (s Sandbox) String() string {
	if s.Name != "" {
		return s.Name
	}
	if s.Isolated() {
		return "isolated"
	}
	return "unisolated"
}

// Run is one run's claim on an instance. It is safe for concurrent use.
type Run struct {
	leaser Leaser
	req    Request
	files  Files

	mu       sync.Mutex
	inst     Instance
	leasing  chan struct{} // closed when the lease in flight settles
	err      error         // a failed lease, returned again
	released bool
	hooks    []func(context.Context) error
}

// NewRun returns a run's claim. leaser nil makes it unisolated. files nil
// starts the work directory empty and keeps nothing at the end.
func NewRun(leaser Leaser, req Request, files Files) *Run {
	return &Run{leaser: leaser, req: req, files: files}
}

// Unisolated returns a Run that never leases.
func Unisolated(req Request) *Run { return &Run{req: req} }

// Isolated reports whether r leases instances.
func (r *Run) Isolated() bool { return r != nil && r.leaser != nil }

// Request is what r leases with.
func (r *Run) Request() Request { return r.req }

// Instance returns the run's instance, leasing it and loading its files on
// the first call. Calls that race wait for one lease. A lease that failed
// fails every later call the same way; a released Run fails with
// ErrReleased.
func (r *Run) Instance(ctx context.Context) (Instance, error) {
	if !r.Isolated() {
		return nil, ErrUnisolated
	}
	for {
		r.mu.Lock()
		switch {
		case r.released:
			r.mu.Unlock()
			return nil, ErrReleased
		case r.inst != nil:
			inst := r.inst
			r.mu.Unlock()
			return inst, nil
		case r.err != nil:
			err := r.err
			r.mu.Unlock()
			return nil, err
		case r.leasing != nil:
			wait := r.leasing
			r.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		done := make(chan struct{})
		r.leasing = done
		r.mu.Unlock()

		inst, err := r.lease(ctx)

		r.mu.Lock()
		r.leasing = nil
		close(done)
		if err == nil && r.released {
			// Released while the lease was in flight: nobody will
			// release this one.
			r.mu.Unlock()
			_ = inst.Close(context.WithoutCancel(ctx))
			return nil, ErrReleased
		}
		if err != nil {
			// A lease the caller's context ended is not the run's
			// failure: the next caller tries again.
			if ctx.Err() == nil {
				r.err = err
			}
			r.mu.Unlock()
			return nil, err
		}
		r.inst = inst
		r.mu.Unlock()
		return inst, nil
	}
}

func (r *Run) lease(ctx context.Context) (Instance, error) {
	inst, err := r.leaser.Lease(ctx, r.req)
	if err != nil {
		return nil, fmt.Errorf("isolation: run %s: lease: %w", r.req.Run, err)
	}
	if r.files != nil {
		if err := r.files.Load(ctx, inst.Files()); err != nil {
			_ = inst.Close(context.WithoutCancel(ctx))
			return nil, fmt.Errorf("isolation: run %s: load files: %w", r.req.Run, err)
		}
	}
	return inst, nil
}

// Leased reports whether r holds an instance now.
func (r *Run) Leased() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.inst != nil
}

// OnRelease registers fn to run when r is released, before its files are
// saved and its instance destroyed: a tool server started in the instance
// is closed there. Registered on a released Run, fn runs at once.
func (r *Run) OnRelease(ctx context.Context, fn func(context.Context) error) error {
	r.mu.Lock()
	if !r.released {
		r.hooks = append(r.hooks, fn)
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()
	return fn(ctx)
}

// Release ends the run's claim: it runs the OnRelease hooks, saves the work
// directory to the Files source and destroys the instance. It is called
// once the run's turn is over, however it ended; ctx should outlive the
// run's own cancellation. Calling it again does nothing.
func (r *Run) Release(ctx context.Context) error {
	r.mu.Lock()
	if r.released {
		r.mu.Unlock()
		return nil
	}
	r.released = true
	hooks := r.hooks
	r.hooks = nil
	wait := r.leasing
	r.mu.Unlock()
	if wait != nil {
		// A lease in flight closes its own instance when it sees the
		// release; waiting keeps Release from returning before it has.
		<-wait
	}
	r.mu.Lock()
	inst := r.inst
	r.inst = nil
	r.mu.Unlock()

	var errs []error
	for i := len(hooks) - 1; i >= 0; i-- {
		errs = append(errs, hooks[i](ctx))
	}
	if inst == nil {
		return errors.Join(errs...)
	}
	if r.files != nil {
		if err := r.files.Save(ctx, inst.Files()); err != nil {
			errs = append(errs, fmt.Errorf("isolation: run %s: save files: %w", r.req.Run, err))
		}
	}
	if err := inst.Close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("isolation: run %s: close instance %s: %w", r.req.Run, inst.ID(), err))
	}
	return errors.Join(errs...)
}

type runKey struct{}

// WithRun puts a run's claim on ctx, where its tools find it.
func WithRun(ctx context.Context, r *Run) context.Context {
	return context.WithValue(ctx, runKey{}, r)
}

// RunFrom returns the run's claim ctx carries.
func RunFrom(ctx context.Context) (*Run, bool) {
	r, ok := ctx.Value(runKey{}).(*Run)
	return r, ok && r != nil
}
