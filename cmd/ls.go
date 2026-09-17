package cmd

import (
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func newLsCmd(ac *appCtx) *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List discovered queues with pending/processing/failed counts",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			infos, err := ac.store.Discover(cmd.Context())
			if err != nil {
				return err
			}
			if ac.jsonOut {
				return ac.emitJSON(infos)
			}
			if len(infos) == 0 {
				fmt.Fprintf(ac.out, "no queues found under prefix %q\n", ac.store.Prefix())
				return nil
			}
			tw := tabwriter.NewWriter(ac.out, 0, 2, 2, ' ', 0)
			fmt.Fprintln(tw, "QUEUE\tPENDING\tPROCESSING\tFAILED")
			for _, q := range infos {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", q.Name, count(q.Pending), count(q.Processing), count(q.Failed))
			}
			return tw.Flush()
		},
	}
}
