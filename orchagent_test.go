package orchagent_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/orchagent"
	"github.com/urmzd/orchagent/flow/event"
	"github.com/urmzd/orchagent/flow/turn/native/agenttest"
)

const helper = `---
apiVersion: saige/v1
name: helper
version: 1.0.0
description: answers in tests
model:
  use: scripted/one
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

func start(t *testing.T, opts ...orchagent.Option) (context.Context, *orchagent.App) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	app, err := orchagent.New(ctx, append([]orchagent.Option{orchagent.WithTenant("acme")}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Close() })
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- app.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v", err)
		}
	})
	if _, err := app.Define(ctx, helper); err != nil {
		t.Fatal(err)
	}
	return ctx, app
}

func TestAskText(t *testing.T) {
	ctx, app := start(t, orchagent.WithModelProvider("scripted", scripted("hello from helper")))
	reply, err := app.Ask(ctx, "helper", "hi") //nolint:staticcheck // Verify the one-release legacy API contract.
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reply.Close() }()
	got, err := reply.Text(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello from helper" {
		t.Fatalf("Text = %q", got)
	}
	if reply.RunID == "" {
		t.Fatal("no run id")
	}
}

func TestAskEventsEndAtTerminal(t *testing.T) {
	ctx, app := start(t, orchagent.WithModelProvider("scripted", scripted("streamed")))
	reply, err := app.Ask(ctx, "helper", "hi") //nolint:staticcheck // Verify the one-release legacy API contract.
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reply.Close() }()
	var text strings.Builder
	var last orchagent.Event
	for e, err := range reply.Events(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		if e.Run != reply.RunID {
			t.Fatalf("event for run %s, want %s", e.Run, reply.RunID)
		}
		if b, ok := e.Data.(event.Text); ok {
			text.WriteString(b.Text)
		}
		last = e
	}
	if _, ok := last.Data.(event.Completed); !ok {
		t.Fatalf("last event %s, want run.completed", last.Type)
	}
	if text.String() != "streamed" {
		t.Fatalf("streamed text = %q", text.String())
	}
	// Text after Events reuses the recorded terminal event.
	if got, err := reply.Text(ctx); err != nil || got != "streamed" {
		t.Fatalf("Text after Events = %q, %v", got, err)
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

func TestAskUnknownAgent(t *testing.T) {
	ctx, app := start(t, orchagent.WithModelProvider("scripted", scripted("x")))
	if _, err := app.Ask(ctx, "nobody", "hi"); err == nil { //nolint:staticcheck // Verify the one-release legacy API contract.
		t.Fatal("Ask of an undefined agent succeeded")
	}
}

func TestAskNeedsTenant(t *testing.T) {
	ctx := context.Background()
	app, err := orchagent.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close() }()
	if _, err := app.Ask(ctx, "helper", "hi"); err == nil { //nolint:staticcheck // Verify the one-release legacy API contract.
		t.Fatal("Ask without a tenant succeeded")
	}
	if _, err := app.Define(ctx, helper); err == nil {
		t.Fatal("Define without a tenant succeeded")
	}
}
