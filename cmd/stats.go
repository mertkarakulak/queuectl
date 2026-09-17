package cmd

import (
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/mertkarakulak/queuectl/internal/queue"
	"github.com/spf13/cobra"
)

func newStatsCmd(ac *appCtx) *cobra.Command {
	var watch bool
	var interval time.Duration
	c := &cobra.Command{
		Use:   "stats <queue>",
		Short: "Show queue counts, optionally refreshing with --watch",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			ctx := cmd.Context()

			if !watch {
				info, err := ac.store.Counts(ctx, name)
				if err != nil {
					return err
				}
				return ac.printStats(info)
			}

			if interval < time.Second {
				interval = time.Second
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				info, err := ac.store.Counts(ctx, name)
				if err != nil {
					return err
				}
				// Clear screen, then redraw. Ctrl-C cancels ctx and exits cleanly.
				fmt.Fprint(ac.out, "\033[H\033[2J")
				fmt.Fprintf(ac.out, "%s  every %s  (Ctrl-C to stop)\n\n", time.Now().Format("15:04:05"), interval)
				if err := ac.printStats(info); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
				}
			}
		},
	}
	c.Flags().BoolVar(&watch, "watch", false, "Continuously refresh counts until Ctrl-C")
	c.Flags().DurationVar(&interval, "interval", 2*time.Second, "Refresh interval for --watch (minimum 1s)")
	return c
}

func (ac *appCtx) printStats(info queue.QueueInfo) error {
	if ac.jsonOut {
		return ac.emitJSON(info)
	}
	tw := tabwriter.NewWriter(ac.out, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "queue\t%s\n", info.Name)
	fmt.Fprintf(tw, "pending\t%s\n", count(info.Pending))
	fmt.Fprintf(tw, "processing\t%s\n", count(info.Processing))
	fmt.Fprintf(tw, "failed\t%s\n", count(info.Failed))
	return tw.Flush()
}
