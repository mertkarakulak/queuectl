// Package queue provides read and (guarded) write access to Redis-backed job
// queues that follow queuectl's key layout.
//
// A queue named Q stores its jobs in three Redis LISTs under a configurable
// prefix (default "queue:"):
//
//	<prefix>Q:pending      jobs waiting to be processed
//	<prefix>Q:processing   jobs a worker has claimed
//	<prefix>Q:failed       dead-letter jobs (JSON with id, payload, error, ...)
//
// Each job is stored as a JSON string. The package never mutates a job's bytes
// except to increment its "attempts" field during a retry; every other field is
// preserved exactly.
package queue

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Queue states.
const (
	StatePending    = "pending"
	StateProcessing = "processing"
	StateFailed     = "failed"
)

// DefaultPrefix is the Redis key prefix used when none is supplied.
const DefaultPrefix = "queue:"

// States lists the recognised queue states in display order.
var States = []string{StatePending, StateProcessing, StateFailed}

// ErrInvalidState is returned when a state outside States is requested.
var ErrInvalidState = errors.New("invalid state: want pending, processing or failed")

func validState(s string) bool {
	for _, st := range States {
		if st == s {
			return true
		}
	}
	return false
}

// Store inspects and manages Redis-backed job queues. It is safe to share.
type Store struct {
	rc     redis.Cmdable
	prefix string
}

// New returns a Store backed by rc. An empty prefix falls back to DefaultPrefix.
func New(rc redis.Cmdable, prefix string) *Store {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return &Store{rc: rc, prefix: prefix}
}

// Prefix returns the configured key prefix.
func (s *Store) Prefix() string { return s.prefix }

// Key returns the Redis key for a queue/state pair, e.g. "queue:emails:failed".
func (s *Store) Key(name, state string) string {
	return s.prefix + name + ":" + state
}

// QueueInfo holds per-queue counts. A count of -1 is a sentinel meaning the key
// exists but is not a Redis LIST (WRONGTYPE) and could not be measured.
type QueueInfo struct {
	Name       string `json:"name"`
	Pending    int64  `json:"pending"`
	Processing int64  `json:"processing"`
	Failed     int64  `json:"failed"`
}

// isWrongType reports whether err is a Redis WRONGTYPE reply. Discovery runs
// over keys queuectl did not create, so a stray non-list key must not abort the
// whole listing.
func isWrongType(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "WRONGTYPE")
}

// Discover finds every queue by SCANning keys under the prefix and returns their
// counts, sorted by name. Non-list keys are tolerated and reported as -1.
func (s *Store) Discover(ctx context.Context) ([]QueueInfo, error) {
	names, err := s.discoverNames(ctx)
	if err != nil {
		return nil, err
	}

	pipe := s.rc.Pipeline()
	cmds := make(map[string]*redis.IntCmd, len(names)*len(States))
	for _, n := range names {
		for _, st := range States {
			cmds[n+"\x00"+st] = pipe.LLen(ctx, s.Key(n, st))
		}
	}
	// Exec returns the first command error; per-command WRONGTYPE is resolved
	// below, so a transport failure surfaces there via the default branch.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) && !isWrongType(err) {
		return nil, err
	}

	infos := make([]QueueInfo, 0, len(names))
	for _, n := range names {
		info := QueueInfo{Name: n}
		vals := make([]int64, len(States))
		for i, st := range States {
			v, err := cmds[n+"\x00"+st].Result()
			switch {
			case err == nil:
				vals[i] = v
			case errors.Is(err, redis.Nil):
				vals[i] = 0
			case isWrongType(err):
				vals[i] = -1
			default:
				return nil, err
			}
		}
		info.Pending, info.Processing, info.Failed = vals[0], vals[1], vals[2]
		infos = append(infos, info)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, nil
}

func (s *Store) discoverNames(ctx context.Context) ([]string, error) {
	match := s.prefix + "*"
	seen := map[string]struct{}{}
	var cursor uint64
	for {
		keys, next, err := s.rc.Scan(ctx, cursor, match, 200).Result()
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			if name, ok := s.parseKey(k); ok {
				seen[name] = struct{}{}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	return names, nil
}

// parseKey extracts a queue name from a full key. The queue name may itself
// contain colons, so the state is taken as the final colon-delimited segment.
func (s *Store) parseKey(key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, s.prefix)
	if !ok {
		return "", false
	}
	idx := strings.LastIndex(rest, ":")
	if idx <= 0 {
		return "", false
	}
	name, state := rest[:idx], rest[idx+1:]
	if !validState(state) {
		return "", false
	}
	return name, true
}

// Counts returns the counts for a single queue, tolerating non-list keys (-1).
func (s *Store) Counts(ctx context.Context, name string) (QueueInfo, error) {
	pipe := s.rc.Pipeline()
	cmds := make([]*redis.IntCmd, len(States))
	for i, st := range States {
		cmds[i] = pipe.LLen(ctx, s.Key(name, st))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) && !isWrongType(err) {
		return QueueInfo{}, err
	}
	info := QueueInfo{Name: name}
	vals := make([]int64, len(States))
	for i := range States {
		v, err := cmds[i].Result()
		switch {
		case err == nil:
			vals[i] = v
		case errors.Is(err, redis.Nil):
			vals[i] = 0
		case isWrongType(err):
			vals[i] = -1
		default:
			return QueueInfo{}, err
		}
	}
	info.Pending, info.Processing, info.Failed = vals[0], vals[1], vals[2]
	return info, nil
}

// Peek returns the first n jobs of a queue state without removing them.
func (s *Store) Peek(ctx context.Context, name, state string, n int64) ([]string, error) {
	if !validState(state) {
		return nil, ErrInvalidState
	}
	if n < 1 {
		return nil, fmt.Errorf("n must be >= 1, got %d", n)
	}
	return s.rc.LRange(ctx, s.Key(name, state), 0, n-1).Result()
}

// Failed returns the first n dead-letter jobs, parsed for display. A non-positive
// n returns the entire failed list.
func (s *Store) Failed(ctx context.Context, name string, n int64) ([]Job, error) {
	stop := int64(-1)
	if n >= 1 {
		stop = n - 1
	}
	raws, err := s.rc.LRange(ctx, s.Key(name, StateFailed), 0, stop).Result()
	if err != nil {
		return nil, err
	}
	jobs := make([]Job, len(raws))
	for i, r := range raws {
		jobs[i] = parseFailedJob(r)
	}
	return jobs, nil
}

// Purge deletes every job in a queue state and returns how many were removed.
// The length read and the delete run in one transaction so the reported count
// matches what was actually cleared.
func (s *Store) Purge(ctx context.Context, name, state string) (int64, error) {
	if !validState(state) {
		return 0, ErrInvalidState
	}
	key := s.Key(name, state)
	pipe := s.rc.TxPipeline()
	ln := pipe.LLen(ctx, key)
	pipe.Del(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return ln.Val(), nil
}
