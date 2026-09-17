package queue

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// retryScript atomically moves already-prepared jobs from the failed list to the
// pending list. ARGV is a flat sequence of (oldJSON, newJSON) pairs. For each
// pair it removes exactly one copy of oldJSON from KEYS[1] (failed) and, only if
// that removal succeeded, appends newJSON to KEYS[2] (pending); it returns the
// number of jobs actually moved.
//
// Running the whole batch inside one script makes it atomic and safe against
// concurrent workers: if a job was consumed or changed between the moment
// queuectl read it and the moment the script runs, LREM matches nothing, returns
// 0, and the job is skipped rather than duplicated into pending.
var retryScript = redis.NewScript(`
local moved = 0
for i = 1, #ARGV, 2 do
  local removed = redis.call('LREM', KEYS[1], 1, ARGV[i])
  if removed > 0 then
    redis.call('RPUSH', KEYS[2], ARGV[i+1])
    moved = moved + 1
  end
end
return moved
`)

// retryBatchSize bounds how many jobs a single retry EVAL touches so that
// draining a very large dead-letter queue never blocks Redis in one long script.
const retryBatchSize = 500

// bumpAttempts returns job with its "attempts" field incremented by one while
// preserving every other field's exact bytes — payloads, large integer ids,
// nulls and empty arrays are never rewritten. A non-object (e.g. a corrupt
// non-JSON entry) is returned unchanged so it can still be moved verbatim.
func bumpAttempts(job string) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(job), &m); err != nil {
		return job
	}
	m["attempts"] = json.RawMessage(strconv.Itoa(jsonInt(m["attempts"]) + 1))
	out, err := json.Marshal(m)
	if err != nil {
		return job
	}
	return string(out)
}

// scripter is the subset of redis.Cmdable that redis.Script.Run needs. Store's
// client satisfies it.
type scripter interface {
	redis.Scripter
}

// RetryAll moves every failed job back to pending, incrementing attempts, in
// bounded atomic batches. It returns the total number of jobs moved.
func (s *Store) RetryAll(ctx context.Context, name string) (int64, error) {
	failedKey := s.Key(name, StateFailed)
	pendingKey := s.Key(name, StatePending)
	keys := []string{failedKey, pendingKey}

	var total int64
	for {
		batch, err := s.rc.LRange(ctx, failedKey, 0, retryBatchSize-1).Result()
		if err != nil {
			return total, err
		}
		if len(batch) == 0 {
			return total, nil
		}
		argv := make([]any, 0, len(batch)*2)
		for _, j := range batch {
			argv = append(argv, j, bumpAttempts(j))
		}
		moved, err := retryScript.Run(ctx, s.rc.(scripter), keys, argv...).Int64()
		if err != nil {
			return total, err
		}
		total += moved
		// No job we just read still existed (all churned/drained): stop rather
		// than spin forever on a list being rewritten by live workers.
		if moved == 0 {
			return total, nil
		}
	}
}

// RetryID moves the first failed job whose "id" equals id back to pending,
// incrementing its attempts. It returns 1 if a job was moved, 0 otherwise.
func (s *Store) RetryID(ctx context.Context, name, id string) (int64, error) {
	failedKey := s.Key(name, StateFailed)
	pendingKey := s.Key(name, StatePending)
	keys := []string{failedKey, pendingKey}

	var start int64
	for {
		batch, err := s.rc.LRange(ctx, failedKey, start, start+retryBatchSize-1).Result()
		if err != nil {
			return 0, err
		}
		if len(batch) == 0 {
			return 0, nil
		}
		for _, j := range batch {
			if jobID(j) == id {
				return retryScript.Run(ctx, s.rc.(scripter), keys, j, bumpAttempts(j)).Int64()
			}
		}
		start += int64(len(batch))
	}
}

// jobID returns the "id" field of a job JSON string, or "" if absent/non-JSON.
func jobID(job string) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(job), &m); err != nil {
		return ""
	}
	return jsonScalarString(m["id"])
}
