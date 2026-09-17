package queue

import (
	"context"
	"encoding/json"
	"testing"
)

func fields(t *testing.T, raw string) map[string]json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("not JSON: %q: %v", raw, err)
	}
	return m
}

// TestRetryAllPreservesBytes is the load-bearing test: retry must bump attempts
// while preserving every other field's exact bytes. A large integer id and an
// empty/array payload are exactly the values that a naive cjson round-trip would
// corrupt, so they are asserted explicitly.
func TestRetryAllPreservesBytes(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()
	job := `{"id":9007199254740993,"payload":[1,2,3],"empty":[],"note":null,"attempts":2,"error":"boom"}`
	rc.RPush(ctx, st.Key("emails", StateFailed), job)

	moved, err := st.RetryAll(ctx, "emails")
	if err != nil {
		t.Fatalf("RetryAll: %v", err)
	}
	if moved != 1 {
		t.Fatalf("moved=%d want 1", moved)
	}
	if n, _ := rc.LLen(ctx, st.Key("emails", StateFailed)).Result(); n != 0 {
		t.Errorf("failed list not drained, len=%d", n)
	}
	got, err := rc.LRange(ctx, st.Key("emails", StatePending), 0, -1).Result()
	if err != nil || len(got) != 1 {
		t.Fatalf("pending=%v err=%v", got, err)
	}
	m := fields(t, got[0])
	if string(m["attempts"]) != "3" {
		t.Errorf("attempts=%s want 3", m["attempts"])
	}
	if string(m["id"]) != "9007199254740993" {
		t.Errorf("large int id mangled: %s", m["id"])
	}
	if string(m["payload"]) != "[1,2,3]" {
		t.Errorf("payload mangled: %s", m["payload"])
	}
	if string(m["empty"]) != "[]" {
		t.Errorf("empty array mangled to %s", m["empty"])
	}
	if string(m["note"]) != "null" {
		t.Errorf("null mangled to %s", m["note"])
	}
	if string(m["error"]) != `"boom"` {
		t.Errorf("error mangled: %s", m["error"])
	}
}

func TestRetryAllMissingAttemptsDefaultsToOne(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()
	rc.RPush(ctx, st.Key("q", StateFailed), `{"id":"a"}`)
	if _, err := st.RetryAll(ctx, "q"); err != nil {
		t.Fatal(err)
	}
	got, _ := rc.LRange(ctx, st.Key("q", StatePending), 0, -1).Result()
	if string(fields(t, got[0])["attempts"]) != "1" {
		t.Errorf("missing attempts should become 1, got %s", got[0])
	}
}

func TestRetryAllMovesNonJSONVerbatim(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()
	rc.RPush(ctx, st.Key("q", StateFailed), "not-json")
	moved, err := st.RetryAll(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Fatalf("moved=%d want 1", moved)
	}
	got, _ := rc.LRange(ctx, st.Key("q", StatePending), 0, -1).Result()
	if len(got) != 1 || got[0] != "not-json" {
		t.Errorf("non-JSON entry not moved verbatim: %v", got)
	}
}

func TestRetryID(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()
	rc.RPush(ctx, st.Key("q", StateFailed),
		`{"id":"a","attempts":0}`,
		`{"id":"b","attempts":1}`,
		`{"id":"c","attempts":5}`,
	)

	moved, err := st.RetryID(ctx, "q", "b")
	if err != nil {
		t.Fatalf("RetryID: %v", err)
	}
	if moved != 1 {
		t.Fatalf("moved=%d want 1", moved)
	}

	// Only b moved, with attempts bumped.
	pend, _ := rc.LRange(ctx, st.Key("q", StatePending), 0, -1).Result()
	if len(pend) != 1 || jobID(pend[0]) != "b" || string(fields(t, pend[0])["attempts"]) != "2" {
		t.Errorf("pending wrong: %v", pend)
	}
	// a and c remain in failed, untouched.
	fail, _ := rc.LRange(ctx, st.Key("q", StateFailed), 0, -1).Result()
	if len(fail) != 2 {
		t.Fatalf("failed len=%d want 2: %v", len(fail), fail)
	}
	for _, f := range fail {
		if jobID(f) == "b" {
			t.Errorf("b still in failed: %v", fail)
		}
	}
}

func TestRetryIDNumericID(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()
	rc.RPush(ctx, st.Key("q", StateFailed), `{"id":42,"attempts":0}`)
	moved, err := st.RetryID(ctx, "q", "42")
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Errorf("numeric id not matched, moved=%d", moved)
	}
}

func TestRetryIDNotFound(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()
	rc.RPush(ctx, st.Key("q", StateFailed), `{"id":"a","attempts":0}`)
	moved, err := st.RetryID(ctx, "q", "does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Errorf("moved=%d want 0", moved)
	}
	if n, _ := rc.LLen(ctx, st.Key("q", StateFailed)).Result(); n != 1 {
		t.Errorf("failed list changed on a no-op retry, len=%d", n)
	}
}

// TestRetryScriptSkipsStaleValue exercises the concurrency guard through the
// shipped NewScript.Run path: if the exact job bytes are no longer in the failed
// list, LREM matches nothing and nothing is pushed to pending.
func TestRetryScriptSkipsStaleValue(t *testing.T) {
	_, st, rc := newTestStore(t)
	ctx := context.Background()
	keys := []string{st.Key("q", StateFailed), st.Key("q", StatePending)}
	rc.RPush(ctx, keys[0], "current")

	moved, err := retryScript.Run(ctx, rc, keys, "stale-value", "stale-new").Int64()
	if err != nil {
		t.Fatalf("script: %v", err)
	}
	if moved != 0 {
		t.Errorf("moved=%d want 0 for a stale value", moved)
	}
	if n, _ := rc.LLen(ctx, keys[1]).Result(); n != 0 {
		t.Errorf("nothing should be pushed to pending, len=%d", n)
	}
}
