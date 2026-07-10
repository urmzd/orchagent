package workspace

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Local is a Workspace backed by a directory on the local filesystem. It is
// the default backend for single-machine deployments and for tests.
type Local struct {
	root string
}

// NewLocal creates (if needed) and opens a workspace rooted at dir.
func NewLocal(dir string) (*Local, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("workspace: resolve root: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("workspace: create root: %w", err)
	}
	return &Local{root: abs}, nil
}

func (l *Local) path(key string) (string, error) {
	if !ValidKey(key) {
		return "", fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}
	return filepath.Join(l.root, filepath.FromSlash(key)), nil
}

// Read implements Workspace.
func (l *Local) Read(_ context.Context, key string) (io.ReadCloser, error) {
	p, err := l.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, key)
	}
	return f, err
}

// Write implements Workspace.
func (l *Local) Write(_ context.Context, key string, r io.Reader) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("workspace: write %q: %w", key, err)
	}
	f, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return fmt.Errorf("workspace: write %q: %w", key, err)
	}
	defer os.Remove(f.Name())
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return fmt.Errorf("workspace: write %q: %w", key, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("workspace: write %q: %w", key, err)
	}
	return os.Rename(f.Name(), p)
}

// List implements Workspace.
func (l *Local) List(_ context.Context, prefix string) ([]string, error) {
	if prefix != "" && !ValidKey(prefix) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidKey, prefix)
	}
	var keys []string
	err := filepath.WalkDir(l.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || strings.HasPrefix(d.Name(), ".tmp-") {
			return err
		}
		rel, err := filepath.Rel(l.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if prefix == "" || key == prefix || strings.HasPrefix(key, prefix+"/") {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("workspace: list %q: %w", prefix, err)
	}
	sort.Strings(keys)
	return keys, nil
}

// Delete implements Workspace.
func (l *Local) Delete(_ context.Context, key string) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); os.IsNotExist(err) {
		return fmt.Errorf("%w: %q", ErrNotFound, key)
	} else if err != nil {
		return fmt.Errorf("workspace: delete %q: %w", key, err)
	}
	return nil
}
