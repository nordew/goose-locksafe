# goose-locksafe

Runs [goose](https://github.com/pressly/goose) v3 migrations against PostgreSQL without letting DDL stall the table it touches. Your `.sql` migration files stay as they are.

- Every migration connection gets `lock_timeout` (default 2s). A DDL statement stuck behind a long transaction gives up instead of blocking every later query on the table.
- On a lock timeout (SQLSTATE 55P03) the migration is retried with exponential backoff and jitter.
- Each timed-out attempt logs who blocked it (pid, state, transaction age, query from `pg_blocking_pids()` and `pg_stat_activity`). The final error carries the same list.
- An invalid index left by a failed `CREATE INDEX CONCURRENTLY` is dropped, then the migration is retried or the failure is reported. If the drop fails, the error names the index.

It wraps goose's public Provider API (no fork) and uses pgx.

## CLI

```sh
go install github.com/nordew/goose-locksafe/cmd/goose-locksafe@latest

export GOOSE_DBSTRING='postgres://user:pass@localhost:5432/app?sslmode=disable'
goose-locksafe -dir ./migrations up
goose-locksafe -dir ./migrations -lock-timeout 3s -attempts 10 up
```

Commands: `up`, `up-by-one`, `up-to VERSION`, `down`, `status`, `version`. Run `goose-locksafe -h` for flags.

## Library

```go
r, err := locksafe.New(ctx, dsn, os.DirFS("migrations"), locksafe.Config{
	LockTimeout: 2 * time.Second,
	MaxAttempts: 5,
})
if err != nil {
	return err
}
defer r.Close()

if _, err := r.Up(ctx); err != nil {
	var merr *locksafe.MigrationError
	if errors.As(err, &merr) {
		// merr.Blockers, merr.Statement, merr.DroppedIndexes, merr.InvalidIndexes
	}
	return err
}
```

`New` takes a DSN rather than an `*sql.DB` because the runner configures its own connections.

## Retry policy

- **Transactional migrations:** always retried. The timeout aborts the transaction, so each retry runs the whole file from a clean rollback.
- **`-- +goose NO TRANSACTION` migrations:** retried only if no statement of the migration had committed before the timeout. Otherwise the runner stops, because a retry would run those statements again.
- **Other errors:** not retried.

goose's advisory lock is unaffected: goose polls `pg_try_advisory_lock`, which never waits on a lock.

## Limitations

- PostgreSQL only. pgbouncer in transaction mode is not supported.
- Blocker query text is logged as-is, literals included. Other roles' queries need `pg_read_all_stats`.
- Invalid indexes that existed before the run are left alone.
- `CREATE INDEX CONCURRENTLY` also waits for old transactions, so a short `lock_timeout` can make it time out often. Raise `-lock-timeout` for large index builds.

## Tests

```sh
go test -race ./...   # starts postgres:17-alpine via testcontainers (needs Docker)
```

Set `LOCKSAFE_TEST_DSN` to a superuser `postgres://` URL to use an existing server instead.
