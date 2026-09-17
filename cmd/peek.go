package cmd

import (
	"fmt"

	"github.com/mertkarakulak/queuectl/internal/queue"
	"github.com/spf13/cobra"
)

func newPeekCmd(ac *appCtx) *cobra.Command {
	var state string
	var n int64
	c := &cobra.Command{
		Use:   "peek <queue>",
		Short: "Show the first N jobs of a queue state without removing them",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			jobs, err := ac.store.Peek(cmd.Context(), name, state, n)
			if err != nil {
				return err
			}
			if ac.jsonOut {
				return ac.emitJSON(map[string]any{
					"queue": name,
					"state": state,
					"jobs":  rawList(jobs),
				})
			}
			if len(jobs) == 0 {
				fmt.Fprintf(ac.out, "%s:%s is empty\n", name, state)
				return nil
			}
			for i, j := range jobs {
				fmt.Fprintf(ac.out, "%d) %s\n", i+1, j)
			}
			return nil
		},
	}
	c.Flags().StringVar(&state, "state", queue.StatePending, "Queue state: pending, processing or failed")
	c.Flags().Int64VarP(&n, "num", "n", 10, "Number of jobs to show (>= 1)")
	return c
}
