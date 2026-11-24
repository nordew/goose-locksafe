// Package locksafe runs goose v3 migrations against PostgreSQL so that DDL
// cannot stall the table it touches.
//
// Every connection the runner opens has lock_timeout set, so a statement stuck
// behind a long transaction gives up instead of sitting at the head of the lock
// queue and blocking every later query on that table. A migration that fails
// with SQLSTATE 55P03 (lock_not_available) is retried with backoff and jitter.
// While a migration waits, a monitor polls pg_blocking_pids and
// pg_stat_activity, so each timed-out attempt is logged with the sessions that
// blocked it, and the final error carries the same list.
//
// Retry policy:
//   - Transactional migrations are always retried: the lock timeout aborts the
//     transaction, goose rolls it back, and the retry re-runs the whole file.
//   - "-- +goose NO TRANSACTION" migrations are retried only when no statement
//     of the migration had committed before the timeout. If an earlier
//     statement committed, the runner stops and reports it, because re-running
//     would repeat that statement.
//
// Invalid indexes: a failed CREATE INDEX CONCURRENTLY (or REINDEX
// CONCURRENTLY) leaves an index with pg_index.indisvalid = false. After every
// failed attempt the runner finds invalid indexes that appeared during the
// attempt, drops them with DROP INDEX CONCURRENTLY, and then retries or
// reports. An index that cannot be dropped is named in the returned error.
//
// Migration files are read by goose unchanged.
package locksafe
