package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newRetryCmd(ac *appCtx) *cobra.Command {
	var id string
	var all, write bool
	c := &cobra.Command{
		Use:   "retry <queue>",
		Short: "Move failed jobs back to pending (increments attempts)",
		Long: "Atomically moves failed jobs back to the pending list and increments each\n" +
			"job's attempts counter. Every other job field is preserved byte-for-byte.\n\n" +
			"Read-only by default: pass --write to apply changes.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			switch {
			case id == "" && !all:
				return fmt.Errorf("specify --id <ID> or --all")
			case id != "" && all:
				return fmt.Errorf("--id and --all are mutually exclusive")
			}
			if err := requireWrite(write, "retry jobs"); err != nil {
				return err
			}

			var moved int64
			var err error
			if all {
				moved, err = ac.store.RetryAll(cmd.Context(), name)
			} else {
				moved, err = ac.store.RetryID(cmd.Context(), name, id)
			}
			if err != nil {
				return err
			}

			if ac.jsonOut {
				return ac.emitJSON(map[string]any{"queue": name, "moved": moved})
			}
			if !all && moved == 0 {
				fmt.Fprintf(ac.out, "no failed job with id %q found in %s\n", id, name)
				return nil
			}
			fmt.Fprintf(ac.out, "moved %d job(s) from %s:failed to %s:pending\n", moved, name, name)
			return nil
		},
	}
	c.Flags().StringVar(&id, "id", "", "Retry only the failed job with this id")
	c.Flags().BoolVar(&all, "all", false, "Retry every failed job")
	c.Flags().BoolVar(&write, "write", false, "Apply changes (required; read-only by default)")
	return c
}
