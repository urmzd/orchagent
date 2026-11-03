package dispatch_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/humagen/fiber/pkg/bus"
	"github.com/urmzd/humagen/fiber/pkg/dispatch"
	"github.com/urmzd/humagen/fiber/pkg/event"
	"github.com/urmzd/humagen/fiber/pkg/membership"
	"github.com/urmzd/humagen/fiber/pkg/runtime"
)

const channel = "slack://T1/C1"

// fakeRuntime starts a run once per key, as anchor does, and records stops.
type fakeRuntime struct {
	mu      sync.Mutex
	started map[string]string // run id -> subscriber
	accepts int
	stops   []runtime.StopRequest
}

func (f *fakeRuntime) Accept(_ context.Context, e event.Envelope, sub string) (runtime.Acceptance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.started == nil {
		f.started = map[string]string{}
	}
	f.accepts++
	id := runtime.RunKey(e.Tenant, e.ID, sub)
	if _, ok := f.started[id]; !ok {
		f.started[id] = sub
	}
	return runtime.Acceptance{RunID: id, Tenant: e.Tenant, Channel: e.Channel}, nil
}

func (f *fakeRuntime) Stop(_ context.Context, req runtime.StopRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops = append(f.stops, req)
	return nil
}

func (f *fakeRuntime) runs() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for k, v := range f.started {
		out[k] = v
	}
	return out
}

func setup(t *testing.T, opts ...dispatch.Option) (*dispatch.Dispatcher, *membership.Memory, *fakeRuntime, bus.Bus) {
	t.Helper()
	b := bus.NewMemory()
	m := membership.NewMemory()
	rt := &fakeRuntime{}
	d, err := dispatch.New(b, m, rt, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return d, m, rt, b
}

func join(t *testing.T, m membership.Store, agent string, rule membership.Rule) {
	t.Helper()
	if err := m.Join(context.Background(), "acme", channel, agent, rule); err != nil {
		t.Fatal(err)
	}
}

var ambient = membership.Rule{Types: []string{"message"}}.Ambient()

func message(id, text string, mentions ...string) event.Envelope {
	return event.Envelope{
		ID: id, Source: channel, Type: event.TypeMessagePosted, Channel: channel, Tenant: "acme",
		Time: time.Now().UTC(), Data: event.MessagePosted{Request: event.Request{Text: text, Mentions: mentions}},
	}
}

func handle(t *testing.T, d *dispatch.Dispatcher, e event.Envelope) []runtime.Acceptance {
	t.Helper()
	accs, err := d.Handle(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	return accs
}

func TestSelfOutputNeverWakesSelf(t *testing.T) {
	d, m, rt, _ := setup(t)
	join(t, m, "docs-bot", ambient)
	join(t, m, "reviewer", ambient)

	own := message("e1", "here is the answer", "docs-bot")
	own.Agent = "docs-bot"
	handle(t, d, own)

	bot := message("e2", "posted by the bot", "docs-bot")
	bot.Data = event.MessagePosted{Request: event.Request{Text: "posted by the bot", Actor: event.Actor{ID: "docs-bot", Bot: true}}}
	handle(t, d, bot)

	for _, sub := range rt.runs() {
		if sub == "docs-bot" {
			t.Fatal("docs-bot was woken by its own output")
		}
	}
	if got := len(rt.runs()); got != 2 {
		t.Fatalf("%d runs; want one for reviewer per event", got)
	}
}

func TestHopLimitDrops(t *testing.T) {
	d, m, rt, _ := setup(t, dispatch.WithMaxHops(3))
	join(t, m, "docs-bot", ambient)

	at := message("e1", "loop")
	at.Hops = 3
	if accs := handle(t, d, at); len(accs) != 0 {
		t.Fatalf("an event at the hop limit started %d runs", len(accs))
	}
	under := message("e2", "fine")
	under.Hops = 2
	if accs := handle(t, d, under); len(accs) != 1 {
		t.Fatalf("an event under the hop limit started %d runs; want 1", len(accs))
	}
	if len(rt.runs()) != 1 {
		t.Fatalf("runs = %v", rt.runs())
	}
}

func TestRedeliveryJoinsRun(t *testing.T) {
	d, m, rt, _ := setup(t)
	join(t, m, "docs-bot", ambient)
	e := message("Ev-1", "hello")
	first := handle(t, d, e)
	second := handle(t, d, e)
	if len(first) != 1 || len(second) != 1 || first[0].RunID != second[0].RunID {
		t.Fatalf("redelivery got %v then %v; want the same run", first, second)
	}
	if first[0].RunID != runtime.RunKey("acme", "Ev-1", "docs-bot") {
		t.Fatalf("run id %q is not RunKey(tenant, event id, subscriber)", first[0].RunID)
	}
	if n := len(rt.runs()); n != 1 {
		t.Fatalf("%d runs; want 1", n)
	}
}

func TestTwoMembersTwoRuns(t *testing.T) {
	d, m, rt, b := setup(t)
	join(t, m, "docs-bot", ambient)
	join(t, m, "reviewer", ambient)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx, "acme") }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	if err := b.Publish(context.Background(), message("Ev-1", "hello")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(rt.runs()) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("runs = %v; want two", rt.runs())
		}
		time.Sleep(10 * time.Millisecond)
	}
	runs := rt.runs()
	var subs []string
	for id, sub := range runs {
		if id != runtime.RunKey("acme", "Ev-1", sub) {
			t.Errorf("run %q for %s is not keyed by subscriber", id, sub)
		}
		subs = append(subs, sub)
	}
	slices.Sort(subs)
	if !slices.Equal(subs, []string{"docs-bot", "reviewer"}) {
		t.Fatalf("subscribers = %v", subs)
	}
}

func TestNoMembershipNoRun(t *testing.T) {
	d, m, rt, _ := setup(t)
	// A member of another channel, of another tenant, and one with no types.
	if err := m.Join(context.Background(), "acme", "slack://T1/C2", "docs-bot", ambient); err != nil {
		t.Fatal(err)
	}
	if err := m.Join(context.Background(), "globex", channel, "docs-bot", ambient); err != nil {
		t.Fatal(err)
	}
	join(t, m, "lazy", membership.Rule{}.Ambient())

	if accs := handle(t, d, message("Ev-1", "hello", "docs-bot", "lazy")); len(accs) != 0 {
		t.Fatalf("started %v with no matching membership", accs)
	}
	if rt.accepts != 0 {
		t.Fatalf("Accept was called %d times", rt.accepts)
	}
}

func TestAddressedRule(t *testing.T) {
	d, m, rt, _ := setup(t)
	join(t, m, "docs-bot", membership.Rule{Types: []string{"message"}})

	if accs := handle(t, d, message("Ev-1", "nobody in particular")); len(accs) != 0 {
		t.Fatalf("an addressed member woke for an event that names nobody: %v", accs)
	}
	if accs := handle(t, d, message("Ev-2", "someone else", "reviewer")); len(accs) != 0 {
		t.Fatalf("an addressed member woke for another agent's mention: %v", accs)
	}
	if accs := handle(t, d, message("Ev-3", "you", "docs-bot")); len(accs) != 1 {
		t.Fatalf("a mention started %d runs; want 1", len(accs))
	}
	dm := message("Ev-4", "direct")
	dm.Data = event.MessagePosted{Request: event.Request{Text: "direct"}, Slack: &event.SlackRef{ChannelType: "im"}}
	if accs := handle(t, d, dm); len(accs) != 1 {
		t.Fatalf("a direct message started %d runs; want 1", len(accs))
	}
	if n := len(rt.runs()); n != 2 {
		t.Fatalf("%d runs; want 2", n)
	}
}

func TestSessionStopMapsToRuns(t *testing.T) {
	d, m, rt, _ := setup(t)
	join(t, m, "docs-bot", ambient)
	join(t, m, "reviewer", ambient)
	accs := handle(t, d, message("Ev-1", "hello"))
	if len(accs) != 2 {
		t.Fatalf("started %d runs; want 2", len(accs))
	}
	stopped := func(ch string) event.Envelope {
		return event.Envelope{
			ID: "stop-" + ch, Source: ch, Type: event.TypeSlackSessionStopped, Channel: ch, Tenant: "acme",
			Time: time.Now().UTC(), Data: event.SlackSessionStopped{Team: "T1", Channel: "C1", User: "U9"},
		}
	}
	if started := handle(t, d, stopped("slack://T1/C2")); len(started) != 0 || len(rt.stops) != 0 {
		t.Fatalf("a stop on another channel stopped %v", rt.stops)
	}
	if started := handle(t, d, stopped(channel)); len(started) != 0 {
		t.Fatalf("a session stop started runs: %v", started)
	}
	var got []string
	for _, s := range rt.stops {
		if s.Tenant != "acme" || s.By != "slack:U9" {
			t.Errorf("stop %+v; want tenant acme by slack:U9", s)
		}
		got = append(got, s.RunID)
	}
	want := []string{accs[0].RunID, accs[1].RunID}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("stopped %v; want %v", got, want)
	}
}

func sessionStopped(ch string) event.Envelope {
	return event.Envelope{
		ID: "stop-" + ch, Source: ch, Type: event.TypeSlackSessionStopped, Channel: ch, Tenant: "acme",
		Time: time.Now().UTC(), Data: event.SlackSessionStopped{Team: "T1", Channel: "C1", User: "U9"},
	}
}

// A session stop reaches the runs another dispatcher started, the one that
// held the tenant before a failover, through the Runs they share.
func testSessionStopAfterFailover(t *testing.T, runs dispatch.Runs) {
	t.Helper()
	m := membership.NewMemory()
	join(t, m, "docs-bot", ambient)
	rt := &fakeRuntime{}
	before, err := dispatch.New(bus.NewMemory(), m, rt, dispatch.WithRuns(runs))
	if err != nil {
		t.Fatal(err)
	}
	accs := handle(t, before, message("Ev-1", "hello"))
	if len(accs) != 1 {
		t.Fatalf("started %d runs; want 1", len(accs))
	}

	after, err := dispatch.New(bus.NewMemory(), m, rt, dispatch.WithRuns(runs))
	if err != nil {
		t.Fatal(err)
	}
	handle(t, after, sessionStopped(channel))
	if len(rt.stops) != 1 || rt.stops[0].RunID != accs[0].RunID {
		t.Fatalf("stops after failover = %+v; want %s", rt.stops, accs[0].RunID)
	}
}

func TestSessionStopReachesRunsStartedBeforeAFailover(t *testing.T) {
	testSessionStopAfterFailover(t, dispatch.NewMemoryRuns())
}

func TestPostgresRunsSurviveAFailover(t *testing.T) {
	url := os.Getenv("FIBER_POSTGRES_TEST_URL")
	if url == "" {
		t.Skip("set FIBER_POSTGRES_TEST_URL")
	}
	runs := dispatch.NewPostgresRuns(postgresPool(t, url))
	if err := runs.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runs.Migrate(t.Context()); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	testSessionStopAfterFailover(t, runs)

	// The newest DefaultRunsPerChannel runs of a channel are kept.
	for i := range dispatch.DefaultRunsPerChannel + 3 {
		if err := runs.Add(t.Context(), "acme", "slack://T1/C9", fmt.Sprintf("run_%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := runs.On(t.Context(), "acme", "slack://T1/C9")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != dispatch.DefaultRunsPerChannel || slices.Contains(got, "run_000") {
		t.Fatalf("kept %d runs (%v...), want the newest %d", len(got), got[:1], dispatch.DefaultRunsPerChannel)
	}
	if other, _ := runs.On(t.Context(), "globex", "slack://T1/C9"); len(other) != 0 {
		t.Fatalf("another tenant saw %v", other)
	}
}

// Two dispatchers on one bus, as two replicas, wake each member once per
// event: the consumer group gives the tenant to one of them at a time.
func TestTwoDispatchersAcceptEachEventOnce(t *testing.T) {
	b := bus.NewMemory()
	m := membership.NewMemory()
	join(t, m, "docs-bot", ambient)
	rt := &fakeRuntime{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range 2 {
		d, err := dispatch.New(b, m, rt)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			if err := d.Run(ctx, "acme"); err != nil {
				t.Errorf("Run: %v", err)
			}
		})
	}
	defer func() { cancel(); wg.Wait() }()

	for i := range 5 {
		if err := b.Publish(context.Background(), message(fmt.Sprintf("Ev-%d", i), "hello")); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(rt.runs()) < 5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	rt.mu.Lock()
	accepts := rt.accepts
	rt.mu.Unlock()
	if accepts != 5 || len(rt.runs()) != 5 {
		t.Fatalf("accepts = %d, runs = %d; want each of 5 events accepted once", accepts, len(rt.runs()))
	}
}

func postgresPool(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	schema := fmt.Sprintf("fibertest_dispatch_%d", time.Now().UnixNano())
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+ident+" CASCADE")
		pool.Close()
	})
	return pool
}
