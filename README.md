# queuectl

[![CI](https://github.com/mertkarakulak/queuectl/actions/workflows/ci.yml/badge.svg)](https://github.com/mertkarakulak/queuectl/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mertkarakulak/queuectl.svg)](https://pkg.go.dev/github.com/mertkarakulak/queuectl)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Inspect and manage Redis-backed job queues from the terminal — safely, during an incident.**

`queuectl` is a small Go CLI for the moments when a worker fleet is misbehaving and
you need to see what is stuck, why it failed, and put it back — without hand-writing
`LRANGE` / `LREM` / `RPUSH` against production Redis and hoping you got the arguments right.

## Why

When a queue backs up at 2am, the usual tools are blunt. `redis-cli` shows you raw
keys, not queues. Counting pending vs. failed jobs means remembering three key names
per queue. Retrying a dead-letter job by hand is a multi-command `LREM` + `RPUSH`
dance that is easy to fumble — and a fumble in that state means duplicated or lost work.

`queuectl` gives you queue-level commands (`ls`, `peek`, `failed`, `retry`, `purge`,
`stats`) with two properties that matter under pressure:

- **Read-only by default.** Nothing mutates Redis unless you pass `--write`.
- **Retries never corrupt a job.** Moving a failed job back to pending is atomic and
  preserves the job's bytes exactly — only the `attempts` counter changes.

## Install

```sh
go install github.com/mertkarakulak/queuectl@latest
```

Requires Go 1.25+. This installs the `queuectl` binary into `$(go env GOPATH)/bin`.

## Queue model / key layout

`queuectl` assumes a common Redis queue convention. A queue named `Q` uses three
Redis **LIST**s, under a configurable prefix (default `queue:`):

| Key                    | Type | Meaning                                             |
| ---------------------- | ---- | --------------------------------------------------- |
| `queue:Q:pending`      | LIST | Jobs waiting to be processed                        |
| `queue:Q:processing`   | LIST | Jobs a worker has claimed and is working on          |
| `queue:Q:failed`       | LIST | Dead-letter jobs that exhausted their retries        |

Each job is a **JSON string**. Failed jobs are expected to carry fields such as:

```json
{
  "id": "job-8f31",
  "payload": { "user_id": 42, "action": "send_welcome_email" },
  "error": "SMTP timeout",
  "attempts": 3,
  "failed_at": "2026-09-17T09:12:44Z"
}
```

Queues are **discovered** with `SCAN` over `<prefix>*`, so you do not register
anything — if the keys follow the layout, `queuectl ls` finds them. The queue name
itself may contain colons (e.g. `tenant:42:reports`); the trailing segment is taken
as the state.

> This is a convention, not a framework. `queuectl` does not run workers and does not
> require a specific library — point it at any Redis whose queues use this layout, or
> set `--prefix` to match yours.

## Usage

Connection is resolved from `--addr`, then `$REDIS_URL`, then `redis://localhost:6379`.

```sh
queuectl --addr redis://localhost:6379 ls
export REDIS_URL=redis://prod-redis:6379   # or set it once
```

### List queues

```sh
$ queuectl ls
QUEUE   PENDING  PROCESSING  FAILED
emails  0        0           1
images  2        0           0
```

A `-` in a column means a key with that name exists but is not a Redis list, so it
could not be counted (it is skipped, not fatal).

### Peek at jobs without consuming them

```sh
$ queuectl peek images -n 2
1) {"id":"img-1","payload":{"src":"a.png"}}
2) {"id":"img-2","payload":{"src":"b.png"}}

$ queuectl peek emails --state failed -n 5
```

### Inspect failed jobs

```sh
$ queuectl failed emails
ID                ATTEMPTS  FAILED_AT             ERROR
job-8f31          3         2026-09-17T09:12:44Z  SMTP timeout
```

### Retry failed jobs (mutating — needs `--write`)

```sh
# Refused by default:
$ queuectl retry emails --all
queuectl: refusing to retry jobs: queuectl is read-only by default; re-run with --write to apply this change

# Retry one job by id, or every failed job:
$ queuectl retry emails --id job-8f31 --write
moved 1 job(s) from emails:failed to emails:pending

$ queuectl retry emails --all --write
moved 12 job(s) from emails:failed to emails:pending
```

Each retried job is moved from `failed` to `pending` and its `attempts` counter is
incremented by one. Every other field — payload, large integer ids, nulls, nested
objects — is preserved byte-for-byte.

### Purge a state (mutating — needs `--write`)

```sh
$ queuectl purge emails --state failed --write
purged 12 job(s) from emails:failed
```

### Live stats

```sh
$ queuectl stats emails
queue       emails
pending     134
processing  8
failed      0

$ queuectl stats emails --watch --interval 2s   # refreshes until Ctrl-C
```

### JSON output

Every command accepts `--json` for scripting:

```sh
$ queuectl stats emails --json
{
  "name": "emails",
  "pending": 134,
  "processing": 8,
  "failed": 0
}
```

## Safety

- **Read-only by default.** `retry` and `purge` refuse to run without `--write`, and
  the guard fires *before* any connection is opened.
- **Atomic, non-destructive retry.** The failed→pending move runs as a single Redis
  script that removes the exact job bytes from `failed` and, only if that removal
  succeeded, appends the updated job to `pending`. If a worker changed or consumed the
  job between the read and the write, the move is skipped rather than duplicating work.
- **Bytes are preserved.** `attempts` is incremented in Go by editing only that one
  JSON field; the job is never round-tripped through a lossy encoder, so numeric
  precision, `null`s, and empty arrays survive intact.
- **Bounded work.** Draining a large dead-letter queue is processed in batches so a
  single retry never blocks the Redis server in one long-running script.

## Development

```sh
git clone https://github.com/mertkarakulak/queuectl
cd queuectl
go test ./...      # unit + integration tests (no external Redis needed)
go vet ./...
go build ./...
```

Tests use [`miniredis`](https://github.com/alicebob/miniredis) — an in-process Redis —
so `go test ./...` runs with no dependencies or running server.

### Layout

```
main.go               entry point (signal-aware context)
cmd/                  cobra commands + read-only guard
internal/queue/       Redis access: discovery, peek, atomic retry, purge
```

## License

MIT © 2026 Mert Karakulak. See [LICENSE](LICENSE).
