package queue

import (
	"context"
	"errors"
	"testing"
)

func TestPurge(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()
	rc.RPush(ctx, st.Key("q", StateFailed), "a", "b", "c")
	rc.RPush(ctx, st.Key("q", StatePending), "keep")

	n, err := st.Purge(ctx, "q", StateFailed)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n != 3 {
		t.Errorf("purged=%d want 3", n)
	}
	if l, _ := rc.LLen(ctx, st.Key("q", StateFailed)).Result(); l != 0 {
		t.Errorf("failed not empty, len=%d", l)
	}
	// Purging one state must not touch another.
	if l, _ := rc.LLen(ctx, st.Key("q", StatePending)).Result(); l != 1 {
		t.Errorf("pending was affected, len=%d", l)
	}
}

func TestPurgeInvalidState(t *testing.T) {
	_, st, _ := newTestStore(t)
	if _, err := st.Purge(context.Background(), "q", "bogus"); !errors.Is(err, ErrInvalidState) {
		t.Errorf("want ErrInvalidState, got %v", err)
	}
}

func TestPurgeEmpty(t *testing.T) {
	_, st, _ := newTestStore(t)
	n, err := st.Purge(context.Background(), "q", StateFailed)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if n != 0 {
		t.Errorf("purged=%d want 0", n)
	}
}
