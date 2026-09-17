package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T) (*miniredis.Miniredis, *Store, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(mr.Close)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	return mr, New(rc, DefaultPrefix), rc
}

func TestDiscover(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()

	rc.RPush(ctx, st.Key("emails", StatePending), "a", "b", "c")
	rc.RPush(ctx, st.Key("emails", StateFailed), "x")
	rc.RPush(ctx, st.Key("images", StateProcessing), "p")
	// A queue name that itself contains a colon must parse correctly.
	rc.RPush(ctx, st.Key("tenant:42:reports", StatePending), "r1", "r2")
	// A stray non-list key under the prefix must be tolerated, not fatal.
	rc.Set(ctx, st.Key("weird", StateFailed), "not-a-list", 0)
	// A key under the prefix that is not a queue-state key must be ignored.
	rc.Set(ctx, st.prefix+"config", "v", 0)

	infos, err := st.Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	got := map[string]QueueInfo{}
	for _, in := range infos {
		got[in.Name] = in
	}
	if len(got) != 4 {
		t.Fatalf("discovered %d queues, want 4: %+v", len(got), infos)
	}
	if e := got["emails"]; e.Pending != 3 || e.Failed != 1 || e.Processing != 0 {
		t.Errorf("emails counts wrong: %+v", e)
	}
	if r := got["tenant:42:reports"]; r.Pending != 2 {
		t.Errorf("colon-named queue counts wrong: %+v", r)
	}
	if w := got["weird"]; w.Failed != -1 {
		t.Errorf("non-list key should report -1, got %+v", w)
	}
	// Results must be sorted by name.
	for i := 1; i < len(infos); i++ {
		if infos[i-1].Name > infos[i].Name {
			t.Errorf("not sorted: %q before %q", infos[i-1].Name, infos[i].Name)
		}
	}
}

func TestDiscoverEmpty(t *testing.T) {
	_, st, _ := newTestStore(t)
	infos, err := st.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(infos) != 0 {
		t.Fatalf("want no queues, got %+v", infos)
	}
}

func TestPeek(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()
	rc.RPush(ctx, st.Key("emails", StatePending), "j1", "j2", "j3", "j4")

	got, err := st.Peek(ctx, "emails", StatePending, 2)
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	if len(got) != 2 || got[0] != "j1" || got[1] != "j2" {
		t.Errorf("Peek returned %v", got)
	}
	// Peek must not consume.
	if n, _ := rc.LLen(ctx, st.Key("emails", StatePending)).Result(); n != 4 {
		t.Errorf("Peek consumed jobs, len=%d", n)
	}
}

func TestPeekInvalidStateAndBounds(t *testing.T) {
	_, st, _ := newTestStore(t)
	ctx := context.Background()
	if _, err := st.Peek(ctx, "emails", "bogus", 5); !errors.Is(err, ErrInvalidState) {
		t.Errorf("want ErrInvalidState, got %v", err)
	}
	// n < 1 must be rejected so it can never degrade into LRANGE 0 -1 (full dump).
	if _, err := st.Peek(ctx, "emails", StatePending, 0); err == nil {
		t.Errorf("want error for n=0")
	}
}

func TestCounts(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()
	rc.RPush(ctx, st.Key("emails", StatePending), "a", "b")
	rc.RPush(ctx, st.Key("emails", StateFailed), "z")

	info, err := st.Counts(ctx, "emails")
	if err != nil {
		t.Fatalf("Counts: %v", err)
	}
	if info.Pending != 2 || info.Failed != 1 || info.Processing != 0 {
		t.Errorf("counts wrong: %+v", info)
	}
}
