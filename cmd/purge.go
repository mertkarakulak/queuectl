package cmd

import (
	"fmt"

	"github.com/mertkarakulak/queuectl/internal/queue"
	"github.com/spf13/cobra"
)

func newPurgeCmd(ac *appCtx) *cobra.Command {
	var state string
	var write bool
	c := &cobra.Command{
		Use:   "purge <queue>",
		Short: "Delete every job in a queue state (e.g. clear the dead-letter list)",
		Long: "Deletes every job in the given queue state.\n\n" +
			"Read-only by default: pass --write to apply changes.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := requireWrite(write, fmt.Sprintf("purge %s:%s", name, state)); err != nil {
				return err
			}
			n, err := ac.store.Purge(cmd.Context(), name, state)
			if err != nil {
				return err
			}
			if ac.jsonOut {
				return ac.emitJSON(map[string]any{"queue": name, "state": state, "purged": n})
			}
			fmt.Fprintf(ac.out, "purged %d job(s) from %s:%s\n", n, name, state)
			return nil
		},
	}
	c.Flags().StringVar(&state, "state", queue.StateFailed, "Queue state to purge: pending, processing or failed")
	c.Flags().BoolVar(&write, "write", false, "Apply changes (required; read-only by default)")
	return c
}
