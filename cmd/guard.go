package cmd

import "fmt"

// requireWrite enforces queuectl's read-only-by-default safety rule: a mutating
// command refuses to run unless the user explicitly passes --write.
func requireWrite(write bool, action string) error {
	if !write {
		return fmt.Errorf("refusing to %s: queuectl is read-only by default; re-run with --write to apply this change", action)
	}
	return nil
}
