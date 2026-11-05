package sink_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/orchagent/flow/address"
	"github.com/urmzd/orchagent/flow/connectors/internal/sink"
	"github.com/urmzd/orchagent/flow/deliverystate"
	"github.com/urmzd/orchagent/flow/event"
)

func TestReplicaHandoffRetainsMessageAndDeduplicatesLastDelta(t *testing.T) {
	ctx := context.Background()
	store := deliverystate.NewMemory()
	surface := newFakeSurface()
	newReplica := func() *sink.Engine {
		return sink.New(surface, layout, sink.WithState(store, "slack:client1"), sink.WithInterval(0))
	}
	first := newReplica()
	for _, e := range []event.Envelope{text(1, "hello "), text(3, "world")} {
		if err := first.Deliver(ctx, target, e); err != nil {
			t.Fatal(err)
		}
	}
	second := newReplica()
	for _, e := range []event.Envelope{text(3, "world"), text(2, "dear "), ev(4, event.TypeCompleted, "")} {
		if err := second.Deliver(ctx, target, e); err != nil {
			t.Fatal(err)
		}
	}
	if surface.count() != 1 {
		t.Fatalf("created %d messages", surface.count())
	}
	if got := surface.body("m1"); !strings.Contains(got, "hello dear world") {
		t.Fatalf("lost or duplicated text: %s", got)
	}
	// The old replica's delayed flush reloads shared state and cannot regress it.
	if err := first.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := surface.body("m1"); !strings.Contains(got, "hello dear world") {
		t.Fatal(got)
	}
}

func TestReplicaHandoffRetainsCoalescedText(t *testing.T) {
	ctx := context.Background()
	store := deliverystate.NewMemory()
	surface := newFakeSurface()
	newReplica := func() *sink.Engine {
		return sink.New(surface, layout, sink.WithState(store, "slack:client1"), sink.WithInterval(time.Hour))
	}
	first := newReplica()
	for _, e := range []event.Envelope{text(1, "hello "), text(2, "world")} {
		if err := first.Deliver(ctx, target, e); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Contains(surface.body("m1"), "world") {
		t.Fatal("expected unflushed coalesced event")
	}
	if err := newReplica().Deliver(ctx, target, ev(3, event.TypeCompleted, "")); err != nil {
		t.Fatal(err)
	}
	if surface.count() != 1 || !strings.Contains(surface.body("m1"), "hello world") {
		t.Fatal(surface.bodies)
	}
}

func TestConcurrentReplicasCreateOneMessage(t *testing.T) {
	ctx := context.Background()
	store := deliverystate.NewMemory()
	surface := newFakeSurface()
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			eng := sink.New(surface, layout, sink.WithState(store, "slack:client1"))
			if err := eng.Deliver(ctx, target, text(1, "hello")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if surface.count() != 1 || len(surface.calls()) != 1 {
		t.Fatalf("messages=%d calls=%v", surface.count(), surface.calls())
	}
}

func TestTenantScopeAndDestinationIsolateNativeHandles(t *testing.T) {
	ctx := context.Background()
	store := deliverystate.NewMemory()
	surface := newFakeSurface()
	for _, tc := range []struct{ tenant, scope, target string }{
		{"one", "slack:client1", target.String()}, {"two", "slack:client1", target.String()}, {"one", "slack:client2", target.String()}, {"one", "slack:client1", "slack://T01/C03"},
	} {
		eng := sink.New(surface, layout, sink.WithState(store, tc.scope))
		e := text(1, tc.tenant)
		e.Tenant = tc.tenant
		if err := eng.Deliver(ctx, address.MustParse(tc.target), e); err != nil {
			t.Fatal(err)
		}
	}
	if surface.count() != 4 {
		t.Fatalf("messages=%d", surface.count())
	}
}

type unavailableStore struct{}

func (unavailableStore) Pending(context.Context, string, int) ([]deliverystate.Key, error) {
	return nil, errors.New("store unavailable")
}

func (unavailableStore) Update(context.Context, deliverystate.Key, func([]byte) ([]byte, bool, error)) error {
	return errors.New("store unavailable")
}
func TestUnavailableStateDoesNotPostAnUnownedMessage(t *testing.T) {
	surface := newFakeSurface()
	eng := sink.New(surface, layout, sink.WithState(unavailableStore{}, "scope"))
	if err := eng.Deliver(context.Background(), target, text(1, "hello")); err == nil {
		t.Fatal("storage failure accepted")
	}
	if surface.count() != 0 {
		t.Fatal("posted without ownership state")
	}
}

func TestFailedEditIsRetriedAfterHandoff(t *testing.T) {
	ctx := context.Background()
	store := deliverystate.NewMemory()
	surface := newFakeSurface()
	replica := func() *sink.Engine {
		return sink.New(surface, layout, sink.WithState(store, "scope"), sink.WithInterval(0))
	}
	if err := replica().Deliver(ctx, target, text(1, "hello ")); err != nil {
		t.Fatal(err)
	}
	surface.failN = 1
	if err := replica().Deliver(ctx, target, text(2, "world")); !errors.Is(err, errSurface) {
		t.Fatalf("error=%v", err)
	}
	if err := replica().Deliver(ctx, target, text(2, "world")); err != nil {
		t.Fatal(err)
	}
	if surface.count() != 1 || !strings.Contains(surface.body("m1"), "hello world") {
		t.Fatal(surface.bodies)
	}
}

func TestFreshReplicaFlushRecoversFailedEventWithoutRedelivery(t *testing.T) {
	ctx := context.Background()
	store := deliverystate.NewMemory()
	surface := newFakeSurface()
	replica := func() *sink.Engine {
		return sink.New(surface, layout, sink.WithState(store, "scope"), sink.WithInterval(0))
	}
	if err := replica().Deliver(ctx, target, text(1, "hello ")); err != nil {
		t.Fatal(err)
	}
	surface.failN = 1
	if err := replica().Deliver(ctx, target, text(2, "world")); !errors.Is(err, errSurface) {
		t.Fatal(err)
	}
	if err := replica().Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if surface.count() != 1 || !strings.Contains(surface.body("m1"), "hello world") {
		t.Fatal(surface.bodies)
	}
	keys, err := store.Pending(ctx, "scope", 256)
	if err != nil || len(keys) != 0 {
		t.Fatalf("pending=%v err=%v", keys, err)
	}
}

func TestFreshReplicaFlushRecoversCoalescedEventWithoutNewEvent(t *testing.T) {
	ctx := context.Background()
	store := deliverystate.NewMemory()
	surface := newFakeSurface()
	replica := func() *sink.Engine {
		return sink.New(surface, layout, sink.WithState(store, "scope"), sink.WithInterval(time.Hour))
	}
	first := replica()
	for _, e := range []event.Envelope{text(1, "hello "), text(2, "world")} {
		if err := first.Deliver(ctx, target, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := replica().Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if surface.count() != 1 || !strings.Contains(surface.body("m1"), "hello world") {
		t.Fatal(surface.bodies)
	}
}
