package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// runCmd builds a fresh root command pointed at mr and runs it with args,
// returning combined stdout, the returned error, and the raw client for
// assertions.
func runCmd(t *testing.T, mr *miniredis.Miniredis, args ...string) (string, error) {
	t.Helper()
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	full := append([]string{"--addr", "redis://" + mr.Addr()}, args...)
	root.SetArgs(full)
	err := root.Execute()
	return out.String(), err
}

func seed(t *testing.T, mr *miniredis.Miniredis) *redis.Client {
	t.Helper()
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	rc.RPush(context.Background(), "queue:emails:failed", `{"id":"1","attempts":0}`)
	return rc
}

func TestRetryRefusedWithoutWrite(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rc := seed(t, mr)

	_, err = runCmd(t, mr, "retry", "emails", "--all")
	if err == nil {
		t.Fatal("expected an error without --write")
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("error should mention read-only: %v", err)
	}
	// The failed job must be untouched.
	if n, _ := rc.LLen(context.Background(), "queue:emails:failed").Result(); n != 1 {
		t.Errorf("guard failed: failed list changed, len=%d", n)
	}
	if n, _ := rc.LLen(context.Background(), "queue:emails:pending").Result(); n != 0 {
		t.Errorf("guard failed: pending gained jobs, len=%d", n)
	}
}

func TestRetryAppliesWithWrite(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rc := seed(t, mr)

	out, err := runCmd(t, mr, "retry", "emails", "--all", "--write")
	if err != nil {
		t.Fatalf("retry --write failed: %v", err)
	}
	if !strings.Contains(out, "moved 1 job") {
		t.Errorf("unexpected output: %q", out)
	}
	if n, _ := rc.LLen(context.Background(), "queue:emails:failed").Result(); n != 0 {
		t.Errorf("failed not drained, len=%d", n)
	}
	if n, _ := rc.LLen(context.Background(), "queue:emails:pending").Result(); n != 1 {
		t.Errorf("pending should have 1, len=%d", n)
	}
}

func TestPurgeRefusedWithoutWrite(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rc := seed(t, mr)

	_, err = runCmd(t, mr, "purge", "emails", "--state", "failed")
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("expected read-only refusal, got %v", err)
	}
	if n, _ := rc.LLen(context.Background(), "queue:emails:failed").Result(); n != 1 {
		t.Errorf("purge guard failed, len=%d", n)
	}
}

func TestRetryRequiresIDorAll(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	seed(t, mr)

	_, err = runCmd(t, mr, "retry", "emails", "--write")
	if err == nil || !strings.Contains(err.Error(), "--id") {
		t.Errorf("expected --id/--all requirement error, got %v", err)
	}
}

func TestLsTableOutput(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	seed(t, mr)

	out, err := runCmd(t, mr, "ls")
	if err != nil {
		t.Fatalf("ls: %v", err)
	}
	if !strings.Contains(out, "QUEUE") || !strings.Contains(out, "emails") {
		t.Errorf("ls output missing header/queue: %q", out)
	}
}
