package mcptools

import (
	"context"
	"fmt"
)

// TokenSource supplies one connection's current OAuth access token. The host
// owns discovery, consent, refresh and encrypted storage. Sources are bound to
// a provider and, for person scope, a person by the host, never by model input.
// A source serializes refreshes; it must return a cached token until refresh is
// needed. Keep the same source pointer while the connection remains authorized.
type TokenSource struct {
	gate  chan struct{}
	fetch func(context.Context) (Secret, error)
}

// NewTokenSource wraps a host's refresh-aware token lookup.
func NewTokenSource(fetch func(context.Context) (Secret, error)) *TokenSource {
	return &TokenSource{fetch: fetch, gate: make(chan struct{}, 1)}
}

func (s *TokenSource) token(ctx context.Context) (Secret, error) {
	if s == nil || s.fetch == nil {
		return Secret{}, fmt.Errorf("%w: no token source", ErrCredential)
	}
	if err := ctx.Err(); err != nil {
		return Secret{}, fmt.Errorf("%w: %w", ErrCredential, err)
	}
	select {
	case s.gate <- struct{}{}:
		defer func() { <-s.gate }()
	case <-ctx.Done():
		return Secret{}, fmt.Errorf("%w: %w", ErrCredential, ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return Secret{}, fmt.Errorf("%w: %w", ErrCredential, err)
	}
	token, err := s.fetch(ctx)
	if cause := ctx.Err(); cause != nil {
		return Secret{}, fmt.Errorf("%w: %w", ErrCredential, cause)
	}
	// OAuth errors may contain refresh tokens. The host reports details in its
	// own credential boundary; no raw error crosses into tools or model output.
	if err != nil || token.Empty() {
		return Secret{}, fmt.Errorf("%w: OAuth token unavailable; reconnect this connection", ErrCredential)
	}
	return token, nil
}

// WithTokenSource selects refresh-aware authentication for a shared HTTP
// provider without CredentialRef. Person-scoped providers use
// WithPersonCredential(PersonCredential{Source: source}) instead.
func WithTokenSource(source *TokenSource) Option { return func(c *config) { c.tokens = source } }
