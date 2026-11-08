package flow

import (
	"errors"
	"slices"
	"testing"

	"github.com/urmzd/orchagent/flow/definition"
)

func TestHostDefinitionResolverKeepsPolicyChecksWithoutRegisteringAProvider(t *testing.T) {
	ctx := t.Context()
	store := definition.NewMemory(definition.WithProviders("offline", "policy", "unapproved"))
	for _, provider := range []string{"policy", "unapproved"} {
		_, err := createAgent(store, ctx, "acme", definition.AgentSpec{Name: provider + "-agent", Provider: provider, Model: "steady", SystemPrompt: "Help."}, "host")
		if err != nil {
			t.Fatal(err)
		}
	}
	resolver := definition.NewResolver(store, definition.Checks{Providers: []string{"offline", "policy"}})
	app := newApp(t, WithDefinitionResolver(resolver))
	if app.Definitions() != store || app.defs != resolver || slices.Contains(app.Providers(), "policy") {
		t.Fatal("host resolver was replaced or registered as an executable provider")
	}
	pin, err := app.defs.Resolve(ctx, "acme", "policy-agent")
	if err != nil {
		t.Fatal("host policy rejected", err)
	}
	if _, err := app.defs.Verify(ctx, "acme", pin); err != nil {
		t.Fatal("host policy failed pinned verification", err)
	}
	if _, err := app.defs.Resolve(ctx, "acme", "unapproved-agent"); !errors.Is(err, definition.ErrInvalid) {
		t.Fatal("unapproved provider passed host checks", err)
	}
	// Passing only a source retains the ordinary runtime provider boundary.
	standard := newApp(t, WithDefinitionResolver(resolver), WithDefinitions(store))
	if _, err := standard.defs.Resolve(ctx, "acme", "policy-agent"); !errors.Is(err, definition.ErrInvalid) {
		t.Fatal("plain source silently widened runtime providers", err)
	}
}
