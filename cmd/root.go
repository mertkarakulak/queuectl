// Package cmd wires the queuectl command tree.
package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mertkarakulak/queuectl/internal/queue"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

// version is reported by `queuectl --version`.
const version = "0.1.0"

const defaultAddr = "redis://localhost:6379"

// appCtx carries the shared, per-invocation state populated once in
// PersistentPreRunE and read by every subcommand.
type appCtx struct {
	store   *queue.Store
	jsonOut bool
	out     io.Writer
	errOut  io.Writer
}

// resolveAddr applies the precedence: --addr flag, then $REDIS_URL, then the
// local default.
func resolveAddr(flagAddr string) string {
	if flagAddr != "" {
		return flagAddr
	}
	if env := os.Getenv("REDIS_URL"); env != "" {
		return env
	}
	return defaultAddr
}

// NewRootCmd builds the queuectl command tree. It is a constructor with no
// package-level state, so tests can create isolated instances pointed at a
// miniredis.
func NewRootCmd() *cobra.Command {
	ac := &appCtx{}
	var addr, prefix string
	var jsonOut bool

	root := &cobra.Command{
		Use:   "queuectl",
		Short: "Inspect and manage Redis-backed job queues from the terminal",
		Long: "queuectl inspects and manages Redis-backed job queues.\n\n" +
			"Mutating commands (retry, purge) are read-only by default and require --write.",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			resolved := resolveAddr(addr)
			opt, err := redis.ParseURL(resolved)
			if err != nil {
				return fmt.Errorf("invalid redis address %q: %w", resolved, err)
			}
			ac.store = queue.New(redis.NewClient(opt), prefix)
			ac.jsonOut = jsonOut
			ac.out = cmd.OutOrStdout()
			ac.errOut = cmd.ErrOrStderr()
			return nil
		},
	}

	pf := root.PersistentFlags()
	pf.StringVar(&addr, "addr", "", "Redis address as a redis:// URL (falls back to $REDIS_URL, then "+defaultAddr+")")
	pf.StringVar(&prefix, "prefix", queue.DefaultPrefix, "Redis key prefix for queues")
	pf.BoolVar(&jsonOut, "json", false, "Emit machine-readable JSON")

	root.AddCommand(
		newLsCmd(ac),
		newPeekCmd(ac),
		newFailedCmd(ac),
		newRetryCmd(ac),
		newPurgeCmd(ac),
		newStatsCmd(ac),
	)
	return root
}

// emitJSON writes v as indented JSON to the command's output.
func (ac *appCtx) emitJSON(v any) error {
	enc := json.NewEncoder(ac.out)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// rawList renders each stored job as embedded JSON when it is valid JSON, and
// as a plain string otherwise, so --json output stays faithful to what is in
// Redis.
func rawList(items []string) []any {
	out := make([]any, len(items))
	for i, it := range items {
		if json.Valid([]byte(it)) {
			out[i] = json.RawMessage(it)
		} else {
			out[i] = it
		}
	}
	return out
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func count(n int64) string {
	if n < 0 {
		return "-"
	}
	return fmt.Sprintf("%d", n)
}

func oneLine(s string) string {
	return strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(s)
}

func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	if max < 1 {
		return ""
	}
	return string(r[:max-1]) + "…"
}
