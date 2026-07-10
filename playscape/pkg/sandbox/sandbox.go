// Package sandbox confines workspace access to declared areas.
//
// A sandbox is a decorator over workspace.Workspace: Scope wraps a workspace
// with a Policy and rejects any operation outside the policy's areas. Tools
// never receive the raw workspace — the control plane hands them a scoped
// view, so a tool can only leak what its policy explicitly grants.
//
// The default is deny: an empty policy permits nothing.
package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/urmzd/dispatch/pkg/workspace"
)

// ErrDenied is returned when an operation falls outside every area granted
// by the active policy.
var ErrDenied = errors.New("sandbox: access denied by policy")

// Area grants access to one workspace key prefix. A key matches an area when
// it equals the prefix or sits underneath it as a path segment
// ("runs" matches "runs/1/out" but not "runs-archive").
type Area struct {
	// Prefix is the workspace key prefix this area covers.
	Prefix string `json:"prefix"`
	// ReadOnly limits the area to Read and List when true.
	ReadOnly bool `json:"read_only,omitempty"`
}

// Policy binds a tool to the capabilities it may exercise: the workspace
// areas it may touch and the tools it may spawn sub-tasks for. Tools without
// a policy get neither.
type Policy struct {
	// Tool is the name of the tool this policy applies to.
	Tool string `json:"tool"`
	// Areas are the prefixes the tool may access. Empty means deny all.
	Areas []Area `json:"areas"`
	// Spawn lists the tool names this tool may submit sub-tasks for —
	// the self-referential path where an agent calls another agent (or
	// itself). Empty means the tool cannot spawn anything.
	Spawn []string `json:"spawn,omitempty"`
}

// MaySpawn reports whether the policy allows spawning a sub-task for tool.
func (p Policy) MaySpawn(tool string) bool {
	for _, s := range p.Spawn {
		if s == tool {
			return true
		}
	}
	return false
}

// Scope returns a view of ws restricted to the areas in p. Every operation
// on the returned workspace is checked against the policy before it reaches
// the backend; violations return ErrDenied.
func Scope(ws workspace.Workspace, p Policy) workspace.Workspace {
	return &scoped{ws: ws, policy: p}
}

type scoped struct {
	ws     workspace.Workspace
	policy Policy
}

func (s *scoped) allow(key string, write bool) error {
	if !workspace.ValidKey(key) {
		return fmt.Errorf("%w: %q", workspace.ErrInvalidKey, key)
	}
	for _, a := range s.policy.Areas {
		if write && a.ReadOnly {
			continue
		}
		if key == a.Prefix || strings.HasPrefix(key, a.Prefix+"/") {
			return nil
		}
	}
	return fmt.Errorf("%w: tool %q, key %q", ErrDenied, s.policy.Tool, key)
}

func (s *scoped) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := s.allow(key, false); err != nil {
		return nil, err
	}
	return s.ws.Read(ctx, key)
}

func (s *scoped) Write(ctx context.Context, key string, r io.Reader) error {
	if err := s.allow(key, true); err != nil {
		return err
	}
	return s.ws.Write(ctx, key, r)
}

// List returns only the keys the policy permits reading, so a scoped tool
// cannot observe the existence of blobs outside its areas.
func (s *scoped) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := s.ws.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	visible := keys[:0]
	for _, k := range keys {
		if s.allow(k, false) == nil {
			visible = append(visible, k)
		}
	}
	return visible, nil
}

func (s *scoped) Delete(ctx context.Context, key string) error {
	if err := s.allow(key, true); err != nil {
		return err
	}
	return s.ws.Delete(ctx, key)
}
