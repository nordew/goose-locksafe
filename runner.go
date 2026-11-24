package locksafe

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"
)

// Defaults for zero Config fields.
const (
	DefaultLockTimeout     = 2 * time.Second
	DefaultMaxAttempts     = 5
	DefaultBackoffBase     = time.Second
	DefaultBackoffMax      = 30 * time.Second
	DefaultTableName       = "goose_db_version"
	DefaultApplicationName = "goose-locksafe"
)

// cleanupTimeout bounds invalid-index cleanup that runs after the caller's
// context is canceled.
const cleanupTimeout = time.Minute

// Config configures a Runner. Zero fields take the defaults above.
type Config struct {
	// LockTimeout is set as lock_timeout on every connection the runner
	// opens. It must not be negative.
	LockTimeout time.Duration
	// MaxAttempts is how many times one migration is tried in total.
	MaxAttempts int
	// BackoffBase is the wait after the first timed-out attempt; it doubles
	// per attempt up to BackoffMax, with jitter.
	BackoffBase time.Duration
	BackoffMax  time.Duration
	// TableName is goose's version table.
	TableName string
	// ApplicationName is set as application_name on migration connections, so
	// they are easy to find in pg_stat_activity.
	ApplicationName string
	// SessionLocker serializes runners across processes. Defaults to goose's
	// Postgres advisory-lock locker, which polls pg_try_advisory_lock and is
	// therefore not affected by lock_timeout.
	SessionLocker lock.SessionLocker
	// Logger receives one warning per timed-out attempt. Defaults to
	// slog.Default().
	Logger *slog.Logger
	// GooseOptions are passed to goose.NewProvider after the runner's own
	// options. WithStore, WithTableName, WithSessionLocker and WithLocker are
	// set by the runner and must not be passed here.
	GooseOptions []goose.ProviderOption
}

func (c *Config) setDefaults() error {
	if c.LockTimeout < 0 || c.MaxAttempts < 0 || c.BackoffBase < 0 || c.BackoffMax < 0 {
		return errors.New("locksafe: negative config value")
	}
	if c.LockTimeout == 0 {
		c.LockTimeout = DefaultLockTimeout
	}
	if c.LockTimeout < time.Millisecond {
		return errors.New("locksafe: lock timeout must be at least 1ms")
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = DefaultMaxAttempts
	}
	if c.BackoffBase == 0 {
		c.BackoffBase = DefaultBackoffBase
	}
	if c.BackoffMax == 0 {
		c.BackoffMax = DefaultBackoffMax
	}
	if c.BackoffMax < c.BackoffBase {
		c.BackoffMax = c.BackoffBase
	}
	if c.TableName == "" {
		c.TableName = DefaultTableName
	}
	if c.ApplicationName == "" {
		c.ApplicationName = DefaultApplicationName
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// Runner applies goose migrations with lock_timeout, retries and invalid
// index cleanup. Methods are safe for concurrent use but run one at a time:
// the statement tracker is shared by every call.
type Runner struct {
	cfg      Config
	provider *goose.Provider
	admin    *pgxpool.Pool
	tr       *tracker
	poll     time.Duration
	randN    func(int64) int64

	mu sync.Mutex
}

// New connects to dsn (a pgx connection string) and prepares a goose
// provider over the migrations in fsys. The runner owns its connection pools
// because lock_timeout and query tracing are set up per connection; call
// Close when done.
func New(ctx context.Context, dsn string, fsys fs.FS, cfg Config) (*Runner, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	tr := newTracker()
	timeout := strconv.FormatInt(cfg.LockTimeout.Milliseconds(), 10) + "ms"
	setTimeout := func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(withoutTrace(ctx), "SELECT set_config('lock_timeout', $1, false)", timeout)
		return err
	}

	connCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("locksafe: parse dsn: %w", err)
	}
	connCfg.Tracer = tr
	connCfg.RuntimeParams["application_name"] = cfg.ApplicationName
	// ResetSession re-applies lock_timeout whenever database/sql reuses a
	// connection, so a session-level SET inside one migration does not leak
	// into the next.
	db := stdlib.OpenDB(*connCfg, stdlib.OptionAfterConnect(setTimeout), stdlib.OptionResetSession(setTimeout))

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("locksafe: parse dsn: %w", err), db.Close())
	}
	poolCfg.MaxConns = 2
	poolCfg.ConnConfig.RuntimeParams["application_name"] = cfg.ApplicationName + "-monitor"
	poolCfg.AfterConnect = setTimeout
	admin, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("locksafe: monitor pool: %w", err), db.Close())
	}
	closeAll := func(err error) error {
		admin.Close()
		return errors.Join(err, db.Close())
	}

	store, err := database.NewStore(database.DialectPostgres, cfg.TableName)
	if err != nil {
		return nil, closeAll(fmt.Errorf("locksafe: goose store: %w", err))
	}
	locker := cfg.SessionLocker
	if locker == nil {
		if locker, err = lock.NewPostgresSessionLocker(); err != nil {
			return nil, closeAll(fmt.Errorf("locksafe: session locker: %w", err))
		}
	}
	opts := append([]goose.ProviderOption{
		goose.WithStore(&markingStore{Store: store, tr: tr}),
		goose.WithSessionLocker(locker),
	}, cfg.GooseOptions...)
	// DialectCustom is what goose requires alongside WithStore; the store
	// itself is goose's Postgres store.
	provider, err := goose.NewProvider(goose.DialectCustom, db, fsys, opts...)
	if err != nil {
		return nil, closeAll(fmt.Errorf("locksafe: goose provider: %w", err))
	}
	return &Runner{
		cfg:      cfg,
		provider: provider,
		admin:    admin,
		tr:       tr,
		poll:     pollInterval(cfg.LockTimeout),
		randN:    defaultRandN,
	}, nil
}

// pollInterval samples a lock wait at least ~5 times before it times out.
func pollInterval(lockTimeout time.Duration) time.Duration {
	return min(max(lockTimeout/5, 10*time.Millisecond), 250*time.Millisecond)
}

// Close closes both connection pools.
func (r *Runner) Close() error {
	r.admin.Close()
	return r.provider.Close()
}

// Up applies all pending migrations, one at a time.
func (r *Runner) Up(ctx context.Context) ([]*goose.MigrationResult, error) {
	return r.UpTo(ctx, math.MaxInt64)
}

// UpTo applies pending migrations up to and including version.
func (r *Runner) UpTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*goose.MigrationResult
	for {
		next, ok, err := r.nextPending(ctx)
		if err != nil {
			return out, err
		}
		if !ok || next > version {
			return out, nil
		}
		res, err := r.runOne(ctx, r.provider.UpByOne)
		if errors.Is(err, goose.ErrNoNextVersion) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, res)
	}
}

// UpByOne applies the next pending migration. It returns
// goose.ErrNoNextVersion when there is none.
func (r *Runner) UpByOne(ctx context.Context) (*goose.MigrationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runOne(ctx, r.provider.UpByOne)
}

// Down rolls back the most recently applied migration with the same
// protections as Up.
func (r *Runner) Down(ctx context.Context) (*goose.MigrationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runOne(ctx, r.provider.Down)
}

// Status reports every migration and whether it is applied.
func (r *Runner) Status(ctx context.Context) ([]*goose.MigrationStatus, error) {
	// Status reads the version table, which moves the tracker's start mark;
	// it must not interleave with a running migration.
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.provider.Status(ctx)
}

// Version returns the current database version.
func (r *Runner) Version(ctx context.Context) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.provider.GetDBVersion(ctx)
}

func (r *Runner) nextPending(ctx context.Context) (int64, bool, error) {
	statuses, err := r.provider.Status(ctx)
	if err != nil {
		return 0, false, err
	}
	for _, s := range statuses { // sorted by version
		if s.State == goose.StatePending {
			return s.Source.Version, true, nil
		}
	}
	return 0, false, nil
}

type migrateFunc func(context.Context) (*goose.MigrationResult, error)

// runOne runs a single-migration goose operation until it succeeds, fails
// with something other than a lock timeout, becomes unsafe to retry, or runs
// out of attempts.
func (r *Runner) runOne(ctx context.Context, op migrateFunc) (*goose.MigrationResult, error) {
	var (
		version  int64
		source   string
		leftover []indexRef // invalid indexes from failed attempts, not dropped yet
		dropped  []string
		// From the latest attempt that hit lock_timeout, so an error raised
		// later (e.g. by index cleanup) still names who blocked the migration.
		statement string
		blockers  []Blocker
	)
	fail := func(attempt int, reason string, err error) *MigrationError {
		return &MigrationError{
			Version: version, Source: source, Attempts: attempt, Reason: reason,
			Statement: statement, Blockers: blockers,
			DroppedIndexes: dropped, InvalidIndexes: refNames(leftover), Err: err,
		}
	}

	for attempt := 1; ; attempt++ {
		// Re-running is unsafe while an invalid index from the last attempt
		// exists: CREATE INDEX would fail on the name, and with IF NOT EXISTS
		// it would succeed and leave the invalid index behind.
		if len(leftover) > 0 {
			var err error
			leftover, dropped, err = r.dropLeftover(ctx, leftover, dropped)
			if err != nil {
				if !isLockTimeout(err) || attempt >= r.cfg.MaxAttempts {
					return nil, fail(attempt, "invalid index could not be dropped", err)
				}
				wait := backoff(attempt, r.cfg.BackoffBase, r.cfg.BackoffMax, r.randN)
				r.cfg.Logger.WarnContext(ctx, "dropping invalid index hit lock_timeout, retrying",
					"version", version, "attempt", attempt, "retry_in", wait, "indexes", refNames(leftover))
				if err := sleepCtx(ctx, wait); err != nil {
					return nil, fail(attempt, "canceled", err)
				}
				continue
			}
		}

		before, err := invalidIndexes(ctx, r.admin)
		if err != nil {
			return nil, fmt.Errorf("locksafe: list invalid indexes: %w", err)
		}

		r.tr.reset()
		w := startWatcher(ctx, r.admin, r.tr, r.poll, func(err error) {
			r.cfg.Logger.DebugContext(ctx, "blocker poll failed", "error", err)
		})
		res, runErr := op(ctx)
		seen := w.stop()
		if runErr == nil {
			return res, nil
		}
		ev := r.tr.lastTimeout()

		var pe *goose.PartialError
		if errors.As(runErr, &pe) && pe.Failed != nil && pe.Failed.Source != nil {
			version, source = pe.Failed.Source.Version, pe.Failed.Source.Path
		}

		// Cleanup outlives ctx so an interrupted CREATE INDEX CONCURRENTLY
		// still has its invalid index removed.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		after, err := invalidIndexes(cctx, r.admin)
		if err != nil {
			cancel()
			return nil, fail(attempt, "failed, and the check for invalid indexes failed", errors.Join(runErr, err))
		}
		leftover = append(leftover, newIndexes(before, after)...)
		if len(leftover) > 0 {
			// An index that fails to drop stays in leftover: it is dropped
			// before the next attempt or reported in the error.
			leftover, dropped, _ = r.dropLeftover(cctx, leftover, dropped)
		}
		cancel()

		lockTimeout := isLockTimeout(runErr)
		statement, blockers = "", nil
		if lockTimeout {
			blockers = seen
			if ev != nil {
				statement = ev.statement
			}
		}
		if pe == nil && !lockTimeout && len(dropped) == 0 && len(leftover) == 0 {
			// goose failed before running a migration (ErrNoNextVersion,
			// missing migrations, bad file): nothing of ours to add.
			return nil, runErr
		}
		merr := fail(attempt, "", runErr)
		if !lockTimeout {
			merr.Reason = "failed (not a lock timeout), not retried"
			return nil, merr
		}

		switch {
		case pe != nil && ev == nil:
			merr.Reason = "lock timeout not seen on a runner connection, not retried"
		case pe != nil && ev.committedBefore > 0:
			merr.Reason = fmt.Sprintf("lock timeout after %d statement(s) of this NO TRANSACTION migration "+
				"had committed; not retried because a retry would run them again", ev.committedBefore)
		case attempt >= r.cfg.MaxAttempts:
			merr.Reason = "lock timeout on every attempt"
		}
		logArgs := []any{
			"version", version, "source", source, "attempt", attempt, "max_attempts", r.cfg.MaxAttempts,
			"statement", oneLine(merr.Statement), "blockers", blockerStrings(blockers),
		}
		if merr.Reason != "" {
			r.cfg.Logger.WarnContext(ctx, "migration hit lock_timeout, giving up", append(logArgs, "reason", merr.Reason)...)
			return nil, merr
		}
		wait := backoff(attempt, r.cfg.BackoffBase, r.cfg.BackoffMax, r.randN)
		r.cfg.Logger.WarnContext(ctx, "migration hit lock_timeout, retrying", append(logArgs, "retry_in", wait)...)
		if err := sleepCtx(ctx, wait); err != nil {
			merr.Reason = "canceled while waiting to retry"
			merr.Err = errors.Join(runErr, err)
			return nil, merr
		}
	}
}

func (r *Runner) dropLeftover(ctx context.Context, leftover []indexRef, dropped []string) ([]indexRef, []string, error) {
	d, left, err := dropIndexes(ctx, r.admin, leftover)
	for _, ix := range d {
		dropped = append(dropped, ix.String())
		r.cfg.Logger.InfoContext(ctx, "dropped invalid index left by failed migration", "index", ix.String())
	}
	if err != nil {
		r.cfg.Logger.WarnContext(ctx, "could not drop invalid index", "indexes", refNames(left), "error", err)
	}
	return left, dropped, err
}

func refNames(ixs []indexRef) []string {
	if len(ixs) == 0 {
		return nil
	}
	out := make([]string, len(ixs))
	for i, ix := range ixs {
		out[i] = ix.String()
	}
	return out
}

func blockerStrings(bs []Blocker) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = b.String()
	}
	return out
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
