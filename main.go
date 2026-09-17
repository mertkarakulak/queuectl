// Command queuectl inspects and manages Redis-backed job queues from the
// terminal. See the README for the queue key layout and usage.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mertkarakulak/queuectl/cmd"
)

func main() {
	// Ctrl-C / SIGTERM cancel the command context (used by `stats --watch`).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := cmd.NewRootCmd().ExecuteContext(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "queuectl: "+err.Error())
		}
		os.Exit(1)
	}
}
