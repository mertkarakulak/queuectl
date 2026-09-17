package queue

import (
	"encoding/json"
	"strings"
)

// Job is a best-effort, display-oriented view of a failed job. Unknown fields
// are ignored; the exact original bytes are kept in Raw for --json output.
type Job struct {
	ID       string `json:"id"`
	Error    string `json:"error"`
	Attempts int    `json:"attempts"`
	FailedAt string `json:"failed_at"`
	Raw      string `json:"-"`
}

// parseFailedJob extracts display fields from a failed-job JSON string. It is
// tolerant: a non-JSON entry yields a Job with only Raw populated, and an "id"
// stored as a number is rendered the same as one stored as a string.
func parseFailedJob(raw string) Job {
	j := Job{Raw: raw}
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return j
	}
	j.ID = jsonScalarString(m["id"])
	j.Error = jsonScalarString(m["error"])
	j.FailedAt = jsonScalarString(m["failed_at"])
	j.Attempts = jsonInt(m["attempts"])
	return j
}

// jsonScalarString renders a JSON scalar (string or number) as a plain string.
// A missing field or JSON null becomes "".
func jsonScalarString(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return ""
	}
	if s[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err == nil {
			return str
		}
	}
	return s
}

// jsonInt reads a JSON number as an int, returning 0 when absent or invalid.
func jsonInt(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0
	}
	iv, err := n.Int64()
	if err != nil {
		return 0
	}
	return int(iv)
}
