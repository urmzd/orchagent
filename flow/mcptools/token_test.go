package mcptools

import (
	"context"
	"errors"
	"github.com/urmzd/orchagent/flow/toolprovider"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type tokenTransportFunc func(*http.Request) (*http.Response, error)

func (f tokenTransportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRotatingTokensAreOriginBoundAndEveryIssuedValueIsRedacted(t *testing.T) {
	values := []string{"old-token-value", "new-token-value"}
	calls := 0
	source := NewTokenSource(func(context.Context) (Secret, error) {
		v := values[min(calls, len(values)-1)]
		calls++
		return NewSecret(v), nil
	})
	red := redactor{rotating: &secretSet{}}
	var headers []string
	b := bearer{source: source, red: red, origin: "https://provider.test:443", ok: true, next: tokenTransportFunc(func(r *http.Request) (*http.Response, error) {
		headers = append(headers, r.Header.Get("Authorization"))
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	for _, url := range []string{"https://provider.test/mcp", "https://elsewhere.test/mcp", "https://provider.test/mcp"} {
		req, _ := http.NewRequest(http.MethodPost, url, nil)
		req.Header.Set("Authorization", "Bearer stale")
		if _, err := b.RoundTrip(req); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 || headers[0] != "Bearer "+values[0] || headers[1] != "" || headers[2] != "Bearer "+values[1] {
		t.Fatalf("calls=%d headers=%v", calls, headers)
	}
	cleaned := red.text(strings.Join(values, " "))
	for _, v := range values {
		if strings.Contains(cleaned, v) {
			t.Fatal("rotated token leaked")
		}
	}
}
func TestTokenFailureDoesNotSendAnExpiredTokenOrExposeRefreshSecrets(t *testing.T) {
	source := NewTokenSource(func(context.Context) (Secret, error) { return Secret{}, errors.New("refresh credential SECRET") })
	called := false
	b := bearer{source: source, secret: NewSecret("expired"), origin: "https://provider.test:443", ok: true, next: tokenTransportFunc(func(*http.Request) (*http.Response, error) { called = true; return nil, nil })}
	req, _ := http.NewRequest(http.MethodPost, "https://provider.test/mcp", nil)
	_, err := b.RoundTrip(req)
	if err == nil || called || strings.Contains(err.Error(), "SECRET") || !errors.Is(err, ErrCredential) {
		t.Fatalf("failure not closed: called=%v error=%v", called, err)
	}
}
func TestSharedOAuthSourceCannotBecomeAPersonFallback(t *testing.T) {
	called := false
	source := NewTokenSource(func(context.Context) (Secret, error) { called = true; return NewSecret("shared"), nil })
	c := config{tokens: source}
	_, _, err := c.credential(context.Background(), toolprovider.Provider{Name: "notion", Transport: toolprovider.TransportHTTP, CredentialScope: toolprovider.ScopePerson})
	if err == nil || called {
		t.Fatalf("shared fallback called=%v error=%v", called, err)
	}
	alice := PersonCredential{Source: source}
	bob := PersonCredential{Source: NewTokenSource(func(context.Context) (Secret, error) { return NewSecret("bob"), nil })}
	if alice.same(bob) || !alice.same(alice) {
		t.Fatal("source identity must isolate person session cache")
	}
}

func TestRotatingTokenRedactionHandlesOverlappingValues(t *testing.T) {
	for _, values := range [][]string{{"token-a", "token-a-secret-tail"}, {"token-a-secret-tail", "token-a"}} {
		red := redactor{secret: NewSecret(values[0]), rotating: &secretSet{}}
		red.rotating.add(NewSecret(values[1]))
		if got := red.text(strings.Join(values, " ")); got != Redacted+" "+Redacted {
			t.Fatalf("overlapping tokens left visible material: %q", got)
		}
	}
}

func TestCancelledTokenWaitDoesNotWaitForAnotherRefresh(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var calls atomic.Int32
	source := NewTokenSource(func(context.Context) (Secret, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return NewSecret("current"), nil
	})
	first := make(chan error, 1)
	go func() { _, err := source.token(context.Background()); first <- err }()
	<-started
	ctx, cancel := context.WithCancel(t.Context())
	second := make(chan error, 1)
	go func() { _, err := source.token(ctx); second <- err }()
	cancel()
	select {
	case err := <-second:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrCredential) {
			t.Fatalf("cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled request waited for unrelated refresh")
	}
	if calls.Load() != 1 {
		t.Fatal("cancelled request fetched a token")
	}
	once.Do(func() { close(release) })
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}
