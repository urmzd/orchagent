package orchagent_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/orchagent"
	"github.com/urmzd/orchagent/flow/event"
	"github.com/urmzd/orchagent/flow/runtime"
	"github.com/urmzd/orchagent/flow/turn/native/agenttest"
)

// helper declares the budget a standalone Session.Post requires.
const helper = `---
apiVersion: saige/v1
name: helper
version: 1.0.0
description: answers in tests
model:
  use: scripted/one
limits:
  budget:
    max_requests: 4
---
Answer briefly.
`

func scripted(text string) orchagent.ProviderFunc {
	return func(orchagent.Spec) (orchagent.Provider, error) {
		p := &agenttest.ScriptedProvider{}
		p.Responses = append(p.Responses, agenttest.TextResponse(text))
		return p, nil
	}
}

// failing builds no model client, so every turn fails.
func failing(orchagent.Spec) (orchagent.Provider, error) {
	return nil, errors.New("no model for this agent")
}

// newApp composes an App with the helper agent defined and no worker running.
func newApp(t *testing.T, opts ...orchagent.Option) (context.Context, *orchagent.App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	app, err := orchagent.New(ctx, append([]orchagent.Option{orchagent.WithTenant("acme")}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	if _, err := app.Define(ctx, helper); err != nil {
		t.Fatal(err)
	}
	return ctx, app
}

// run runs app until the test ends.
func run(t *testing.T, ctx context.Context, app *orchagent.App) {
	t.Helper()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- app.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v", err)
		}
	})
}

func start(t *testing.T, opts ...orchagent.Option) (context.Context, *orchagent.App) {
	t.Helper()
	ctx, app := newApp(t, opts...)
	run(t, ctx, app)
	return ctx, app
}

// Post then Wait is the one way to run an agent and read its answer: the
// answer of a completed run, ErrRunFailed for a run that failed terminally,
// and ErrRunStopped with the partial answer for a run asked to stop.
func TestSessionPostThenWaitReturnsHowTheRunEnded(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider orchagent.ProviderFunc
		stop     bool
		want     string
		wantErr  error
	}{
		{name: "a completed run returns its answer", provider: scripted("hello from helper"), want: "hello from helper"},
		{name: "a terminal failure is ErrRunFailed", provider: failing, wantErr: orchagent.ErrRunFailed},
		{name: "a stopped run is ErrRunStopped", provider: scripted("never said"), stop: true, wantErr: orchagent.ErrRunStopped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, app := newApp(t, orchagent.WithModelProvider("scripted", tc.provider))
			s, err := app.Session(ctx, orchagent.SessionOptions{Agent: "helper"})
			if err != nil {
				t.Fatal(err)
			}
			accepted, err := s.Post(ctx, "hi")
			if err != nil {
				t.Fatal(err)
			}
			if accepted.RunID == "" {
				t.Fatal("no run id")
			}
			if tc.stop {
				if err := app.Stop(ctx, runtime.StopRequest{Tenant: "acme", RunID: accepted.RunID, Reason: "enough"}); err != nil {
					t.Fatal(err)
				}
			}
			run(t, ctx, app)
			got, err := s.Wait(ctx, accepted.RunID)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("Wait error = %v, want %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("Wait = %q, want %q", got, tc.want)
			}
		})
	}
}

// Stream carries the posted run to its terminal event, and Wait called after
// the run already ended still returns its answer from the replayed window.
func TestSessionStreamEndsAtTheTerminalEventAndALateWaitStillAnswers(t *testing.T) {
	ctx, app := start(t, orchagent.WithModelProvider("scripted", scripted("streamed")))
	s, err := app.Session(ctx, orchagent.SessionOptions{Agent: "helper"})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := s.Stream(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	accepted, err := s.Post(ctx, "hi")
	if err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if e.Run != accepted.RunID {
			t.Fatalf("event for run %s, want %s", e.Run, accepted.RunID)
		}
		if b, ok := e.Data.(event.Text); ok {
			text.WriteString(b.Text)
		}
		if _, ok := e.Data.(event.Completed); ok {
			break
		}
	}
	if text.String() != "streamed" {
		t.Fatalf("streamed text = %q", text.String())
	}
	if got, err := s.Wait(ctx, accepted.RunID); err != nil || got != "streamed" {
		t.Fatalf("Wait after the run ended = %q, %v", got, err)
	}
}

func TestDefineIsInsertOnly(t *testing.T) {
	ctx, app := start(t, orchagent.WithModelProvider("scripted", scripted("x")))
	if _, err := app.Define(ctx, helper); err != nil {
		t.Fatalf("identical Define = %v, want no-op", err)
	}
	changed := strings.Replace(helper, "Answer briefly.", "Answer at length.", 1)
	if _, err := app.Define(ctx, changed); !errors.Is(err, orchagent.ErrExists) {
		t.Fatalf("rewritten version = %v, want ErrExists", err)
	}
}

func TestSessionOfAnUndefinedAgentIsRefused(t *testing.T) {
	ctx, app := start(t, orchagent.WithModelProvider("scripted", scripted("x")))
	if _, err := app.Session(ctx, orchagent.SessionOptions{Agent: "nobody"}); err == nil {
		t.Fatal("Session of an undefined agent succeeded")
	}
}

func TestSessionAndDefineNeedATenant(t *testing.T) {
	ctx := context.Background()
	app, err := orchagent.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close() }()
	if _, err := app.Session(ctx, orchagent.SessionOptions{Agent: "helper"}); err == nil {
		t.Fatal("Session without a tenant succeeded")
	}
	if _, err := app.Define(ctx, helper); err == nil {
		t.Fatal("Define without a tenant succeeded")
	}
}
