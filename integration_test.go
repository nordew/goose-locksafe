package locksafe_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3/lock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	locksafe "github.com/nordew/goose-locksafe"
)

const addColumn = `-- +goose Up
ALTER TABLE t ADD COLUMN c int;
-- +goose Down
ALTER TABLE t DROP COLUMN c;
`

func setupTable(t *testing.T, dsn string) {
	t.Helper()
	c := connect(t, dsn)
	exec(t, c, `CREATE TABLE t (id int, val int)`)
	exec(t, c, `INSERT INTO t SELECT g, g % 10 FROM generate_series(1, 1000) g`)
}

func hasColumn(t *testing.T, dsn, table, col string) bool {
	t.Helper()
	var ok bool
	err := connect(t, dsn).QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2)`,
		table, col).Scan(&ok)
	require.NoError(t, err)
	return ok
}

func tableExists(t *testing.T, dsn, table string) bool {
	t.Helper()
	var ok bool
	err := connect(t, dsn).QueryRow(context.Background(), `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&ok)
	require.NoError(t, err)
	return ok
}

func invalidIndexCount(t *testing.T, dsn string) int {
	t.Helper()
	var n int
	err := connect(t, dsn).QueryRow(context.Background(), `SELECT count(*) FROM pg_index WHERE NOT indisvalid`).Scan(&n)
	require.NoError(t, err)
	return n
}

func runUpAsync(r *locksafe.Runner) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := r.Up(context.Background())
		done <- err
	}()
	return done
}

func waitDone(t *testing.T, done <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(within):
		t.Fatalf("migration did not finish within %s", within)
		return nil
	}
}

// Scenario 1.
func TestUp_WhenBlockerCommits_SelectsStayFastAndMigrationSucceeds(t *testing.T) {
	dsn := newDB(t)
	setupTable(t, dsn)
	const lockTimeout = 500 * time.Millisecond

	blocker, _ := beginBlocker(t, dsn, `SELECT count(*) FROM t`)
	r, logs := newRunner(t, dsn, migrations(map[string]string{"00001_add_c.sql": addColumn}), locksafe.Config{
		LockTimeout: lockTimeout,
		MaxAttempts: 30,
		BackoffBase: 200 * time.Millisecond,
		BackoffMax:  400 * time.Millisecond,
	})

	// A reader with no lock_timeout of its own, hammering t the whole time.
	reader := connect(t, dsn)
	stop := make(chan struct{})
	var (
		wg         sync.WaitGroup
		maxLatency time.Duration
		reads      int
		readErr    error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			start := time.Now()
			var n int
			if err := reader.QueryRow(context.Background(), `SELECT count(*) FROM t`).Scan(&n); err != nil {
				readErr = err
				return
			}
			maxLatency = max(maxLatency, time.Since(start))
			reads++
		}
	}()

	done := runUpAsync(r)
	logs.waitTimeout(t, 10*time.Second)
	logs.waitTimeout(t, 10*time.Second)
	require.NoError(t, blocker.Commit(context.Background()))

	require.NoError(t, waitDone(t, done, 20*time.Second))
	close(stop)
	wg.Wait()

	require.NoError(t, readErr)
	t.Logf("reads=%d max select latency=%s lock_timeout=%s timeouts=%d",
		reads, maxLatency, lockTimeout, logs.count("migration hit lock_timeout"))
	assert.True(t, hasColumn(t, dsn, "t", "c"))
	assert.GreaterOrEqual(t, logs.count("migration hit lock_timeout, retrying"), 2)
	// SELECTs queued behind the waiting ALTER, so the hazard was real...
	assert.Greater(t, maxLatency, 100*time.Millisecond)
	// ...but no SELECT waited much longer than lock_timeout.
	assert.Less(t, maxLatency, lockTimeout+300*time.Millisecond)
}

// Scenario 2.
func TestUp_WhenBlockerNeverCommits_FailsAfterMaxAttemptsNamingBlocker(t *testing.T) {
	dsn := newDB(t)
	setupTable(t, dsn)
	_, blockerPID := beginBlocker(t, dsn, `SELECT count(*) FROM t /* nightly report */`)
	r, logs := newRunner(t, dsn, migrations(map[string]string{"00001_add_c.sql": addColumn}), locksafe.Config{
		LockTimeout: 300 * time.Millisecond,
		MaxAttempts: 3,
		BackoffBase: 50 * time.Millisecond,
		BackoffMax:  100 * time.Millisecond,
	})

	_, err := r.Up(context.Background())
	require.Error(t, err)
	t.Log(err)

	var merr *locksafe.MigrationError
	require.ErrorAs(t, err, &merr)
	assert.Equal(t, 3, merr.Attempts)
	assert.Equal(t, int64(1), merr.Version)
	assert.Contains(t, merr.Statement, "ALTER TABLE t ADD COLUMN c")
	require.NotEmpty(t, merr.Blockers)
	assert.Equal(t, int32(blockerPID), merr.Blockers[0].PID) //nolint:gosec // pid fits int4
	assert.Equal(t, "idle in transaction", merr.Blockers[0].State)
	assert.Contains(t, merr.Blockers[0].Query, "nightly report")
	assert.Positive(t, merr.Blockers[0].XactAge)
	assert.Contains(t, err.Error(), fmt.Sprintf("pid=%d", blockerPID))
	assert.Contains(t, err.Error(), "nightly report")

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "55P03", pgErr.Code)

	assert.Equal(t, 3, logs.count("migration hit lock_timeout"))
	assert.False(t, hasColumn(t, dsn, "t", "c"))
	v, err := r.Version(context.Background())
	require.NoError(t, err)
	assert.Zero(t, v)
}

// Scenario 3.
func TestUp_WhenLastStatementOfTxMigrationTimesOut_NothingAppliedAndRetryAppliesAll(t *testing.T) {
	dsn := newDB(t)
	setupTable(t, dsn)
	blocker, _ := beginBlocker(t, dsn, `SELECT count(*) FROM t`)
	r, logs := newRunner(t, dsn, migrations(map[string]string{"00001_multi.sql": `-- +goose Up
CREATE TABLE a (id int);
INSERT INTO a VALUES (1);
ALTER TABLE t ADD COLUMN c int;
-- +goose Down
DROP TABLE a;
`}), locksafe.Config{
		LockTimeout: 300 * time.Millisecond,
		MaxAttempts: 5,
		// The first backoff is at least 1s, the window in which the test
		// inspects the state between attempts.
		BackoffBase: 2 * time.Second,
		BackoffMax:  2 * time.Second,
	})

	done := runUpAsync(r)
	rec := logs.waitTimeout(t, 10*time.Second)
	assert.Contains(t, rec.attrs["statement"], "ALTER TABLE t ADD COLUMN c")

	// Between attempts: the whole transaction rolled back.
	assert.False(t, tableExists(t, dsn, "a"), "CREATE TABLE a must have rolled back")
	assert.False(t, hasColumn(t, dsn, "t", "c"))

	require.NoError(t, blocker.Commit(context.Background()))
	require.NoError(t, waitDone(t, done, 20*time.Second))

	var rows int
	require.NoError(t, connect(t, dsn).QueryRow(context.Background(), `SELECT count(*) FROM a`).Scan(&rows))
	assert.Equal(t, 1, rows, "INSERT ran once in the committed attempt, not once per attempt")
	assert.True(t, hasColumn(t, dsn, "t", "c"))
}

// Scenario 4.
func TestUp_WhenUniqueCICFailsOnDuplicates_InvalidIndexDroppedAndReported(t *testing.T) {
	dsn := newDB(t)
	setupTable(t, dsn) // val has duplicates
	r, logs := newRunner(t, dsn, migrations(map[string]string{"00001_uniq.sql": `-- +goose NO TRANSACTION
-- +goose Up
CREATE UNIQUE INDEX CONCURRENTLY t_val_uniq ON t (val);
-- +goose Down
DROP INDEX CONCURRENTLY t_val_uniq;
`}), locksafe.Config{LockTimeout: 300 * time.Millisecond, MaxAttempts: 3})

	_, err := r.Up(context.Background())
	require.Error(t, err)
	t.Log(err)

	var merr *locksafe.MigrationError
	require.ErrorAs(t, err, &merr)
	assert.Equal(t, 1, merr.Attempts, "a unique violation is not retried")
	assert.Equal(t, []string{"public.t_val_uniq"}, merr.DroppedIndexes)
	assert.Empty(t, merr.InvalidIndexes)
	assert.Contains(t, err.Error(), "dropped invalid index(es): public.t_val_uniq")

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "23505", pgErr.Code)

	assert.Zero(t, invalidIndexCount(t, dsn))
	assert.False(t, tableExists(t, dsn, "t_val_uniq"))
	assert.Equal(t, 1, logs.count("dropped invalid index"))
	v, err := r.Version(context.Background())
	require.NoError(t, err)
	assert.Zero(t, v)
}

// CIC that times out waiting for a writer leaves an invalid index; the runner
// drops it and retries until the build succeeds.
func TestUp_WhenCICTimesOutBehindWriter_DropsInvalidIndexAndRetries(t *testing.T) {
	dsn := newDB(t)
	setupTable(t, dsn)
	blocker, _ := beginBlocker(t, dsn, `INSERT INTO t VALUES (0, 0)`)
	r, logs := newRunner(t, dsn, migrations(map[string]string{"00001_idx.sql": `-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY t_val_idx ON t (val);
-- +goose Down
DROP INDEX CONCURRENTLY t_val_idx;
`}), locksafe.Config{
		LockTimeout: 300 * time.Millisecond,
		MaxAttempts: 30,
		BackoffBase: 200 * time.Millisecond,
		BackoffMax:  400 * time.Millisecond,
	})

	done := runUpAsync(r)
	logs.waitTimeout(t, 10*time.Second)
	time.Sleep(time.Second)
	require.NoError(t, blocker.Commit(context.Background()))
	require.NoError(t, waitDone(t, done, 30*time.Second))

	assert.GreaterOrEqual(t, logs.count("dropped invalid index"), 1)
	assert.Zero(t, invalidIndexCount(t, dsn))
	var valid bool
	require.NoError(t, connect(t, dsn).QueryRow(context.Background(),
		`SELECT indisvalid FROM pg_index WHERE indexrelid = 'public.t_val_idx'::regclass`).Scan(&valid))
	assert.True(t, valid)
}

// goose's session locker polls pg_try_advisory_lock, which never waits on a
// lock, so lock_timeout cannot cut it short.
func TestUp_WhenAdvisoryLockHeldLongerThanLockTimeout_WaitsAndSucceeds(t *testing.T) {
	dsn := newDB(t)
	setupTable(t, dsn)
	holder := connect(t, dsn)
	exec(t, holder, `SELECT pg_advisory_lock($1)`, lock.DefaultLockID)

	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 30))
	require.NoError(t, err)
	r, logs := newRunner(t, dsn, migrations(map[string]string{"00001_add_c.sql": addColumn}), locksafe.Config{
		LockTimeout:   200 * time.Millisecond,
		SessionLocker: locker,
	})

	const held = 2500 * time.Millisecond
	start := time.Now()
	done := runUpAsync(r)
	time.Sleep(held)
	assert.False(t, hasColumn(t, dsn, "t", "c"), "runner must wait for the advisory lock")
	exec(t, holder, `SELECT pg_advisory_unlock($1)`, lock.DefaultLockID)

	require.NoError(t, waitDone(t, done, 20*time.Second))
	assert.GreaterOrEqual(t, time.Since(start), held)
	assert.True(t, hasColumn(t, dsn, "t", "c"))
	assert.Zero(t, logs.count("migration hit lock_timeout"))
}

// NO TRANSACTION policy: a timeout after an earlier statement committed is
// not retried, because a retry would run that statement again.
func TestUp_WhenNoTxMigrationTimesOutAfterCommittedStatement_NotRetried(t *testing.T) {
	dsn := newDB(t)
	setupTable(t, dsn)
	_, _ = beginBlocker(t, dsn, `SELECT count(*) FROM t`)
	r, _ := newRunner(t, dsn, migrations(map[string]string{"00001_notx.sql": `-- +goose NO TRANSACTION
-- +goose Up
CREATE TABLE b (id int);
ALTER TABLE t ADD COLUMN c int;
-- +goose Down
DROP TABLE b;
`}), locksafe.Config{LockTimeout: 300 * time.Millisecond, MaxAttempts: 5, BackoffBase: 50 * time.Millisecond})

	_, err := r.Up(context.Background())
	require.Error(t, err)
	t.Log(err)
	var merr *locksafe.MigrationError
	require.ErrorAs(t, err, &merr)
	assert.Equal(t, 1, merr.Attempts)
	assert.Contains(t, merr.Reason, "1 statement(s) of this NO TRANSACTION migration had committed")
	assert.NotEmpty(t, merr.Blockers)
	assert.True(t, tableExists(t, dsn, "b"), "first statement committed and stays")
}

// NO TRANSACTION policy: a timeout on the first statement leaves nothing
// behind, so it is retried.
func TestUp_WhenNoTxMigrationTimesOutOnFirstStatement_Retried(t *testing.T) {
	dsn := newDB(t)
	setupTable(t, dsn)
	blocker, _ := beginBlocker(t, dsn, `SELECT count(*) FROM t`)
	r, logs := newRunner(t, dsn, migrations(map[string]string{"00001_notx.sql": `-- +goose NO TRANSACTION
-- +goose Up
ALTER TABLE t ADD COLUMN c int;
CREATE TABLE b (id int);
-- +goose Down
DROP TABLE b;
`}), locksafe.Config{LockTimeout: 300 * time.Millisecond, MaxAttempts: 20, BackoffBase: 200 * time.Millisecond, BackoffMax: 400 * time.Millisecond})

	done := runUpAsync(r)
	logs.waitTimeout(t, 10*time.Second)
	require.NoError(t, blocker.Commit(context.Background()))
	require.NoError(t, waitDone(t, done, 20*time.Second))
	assert.True(t, hasColumn(t, dsn, "t", "c"))
	assert.True(t, tableExists(t, dsn, "b"))
}

// A session-level SET in one migration must not disable lock_timeout for the
// next one on the same pooled connection.
func TestUp_WhenMigrationDisablesLockTimeout_NextMigrationStillBounded(t *testing.T) {
	dsn := newDB(t)
	setupTable(t, dsn)
	_, _ = beginBlocker(t, dsn, `SELECT count(*) FROM t`)
	r, _ := newRunner(t, dsn, migrations(map[string]string{
		"00001_set.sql":   "-- +goose NO TRANSACTION\n-- +goose Up\nSET lock_timeout = 0;\n",
		"00002_add_c.sql": addColumn,
	}), locksafe.Config{LockTimeout: 300 * time.Millisecond, MaxAttempts: 1})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := r.Up(ctx)
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "55P03", pgErr.Code)
	assert.False(t, errors.Is(err, context.DeadlineExceeded))
	v, err := r.Version(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), v)
}

// When the invalid index cannot be dropped before attempts run out, the error
// names it as left in place, together with the blocker.
func TestUp_WhenInvalidIndexCannotBeDropped_ReportsItWithBlocker(t *testing.T) {
	dsn := newDB(t)
	setupTable(t, dsn)
	_, blockerPID := beginBlocker(t, dsn, `INSERT INTO t VALUES (0, 0)`)
	r, _ := newRunner(t, dsn, migrations(map[string]string{"00001_idx.sql": `-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY t_val_idx ON t (val);
`}), locksafe.Config{LockTimeout: 300 * time.Millisecond, MaxAttempts: 2, BackoffBase: 50 * time.Millisecond})

	_, err := r.Up(context.Background())
	require.Error(t, err)
	t.Log(err)
	var merr *locksafe.MigrationError
	require.ErrorAs(t, err, &merr)
	assert.Equal(t, []string{"public.t_val_idx"}, merr.InvalidIndexes)
	assert.Contains(t, err.Error(), "INVALID INDEX(ES) LEFT IN PLACE, drop manually: public.t_val_idx")
	require.NotEmpty(t, merr.Blockers)
	assert.Equal(t, int32(blockerPID), merr.Blockers[0].PID) //nolint:gosec // pid fits int4
	assert.Equal(t, 1, invalidIndexCount(t, dsn), "the reported index is the only invalid one")
}
