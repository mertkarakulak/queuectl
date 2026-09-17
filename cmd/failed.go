package cmd

import (
	"encoding/json"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func newFailedCmd(ac *appCtx) *cobra.Command {
	var n int64
	c := &cobra.Command{
		Use:   "failed <queue>",
		Short: "List failed (dead-letter) jobs with their error and attempt count",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if n < 1 {
				n = 1
			}
			jobs, err := ac.store.Failed(cmd.Context(), name, n)
			if err != nil {
				return err
			}
			if ac.jsonOut {
				raws := make([]any, len(jobs))
				for i, j := range jobs {
					if json.Valid([]byte(j.Raw)) {
						raws[i] = json.RawMessage(j.Raw)
					} else {
						raws[i] = j.Raw
					}
				}
				return ac.emitJSON(map[string]any{"queue": name, "failed": raws})
			}
			if len(jobs) == 0 {
				fmt.Fprintf(ac.out, "%s has no failed jobs\n", name)
				return nil
			}
			tw := tabwriter.NewWriter(ac.out, 0, 2, 2, ' ', 0)
			fmt.Fprintln(tw, "ID\tATTEMPTS\tFAILED_AT\tERROR")
			for _, j := range jobs {
				fmt.Fprintf(tw, "%s\t%d\t%s\t%s\n",
					dash(j.ID), j.Attempts, dash(j.FailedAt), dash(truncate(oneLine(j.Error), 60)))
			}
			return tw.Flush()
		},
	}
	c.Flags().Int64VarP(&n, "num", "n", 20, "Number of failed jobs to show")
	return c
}
