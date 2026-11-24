package locksafe

import (
	"context"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3/database"
)

const sqlStateLockNotAvailable = "55P03"

func isLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == sqlStateLockNotAvailable
}

type skipTraceKey struct{}

type querySQLKey struct{}

// withoutTrace marks queries the runner issues itself (session setup) so the
// tracker does not count them as migration work.
func withoutTrace(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipTraceKey{}, true)
}

// timeoutEvent is a lock timeout seen on a migration connection.
type timeoutEvent struct {
	pid       uint32
	statement string
	// committedBefore is the number of statements that committed between the
	// start of the migration and this timeout.
	committedBefore int
}

// tracker observes every query goose sends through the migration pool.
//
// goose does not expose its SQL parser, so the runner cannot count the
// statements of a NO TRANSACTION file before running it. Instead it counts, at
// runtime, statements that completed with the connection outside a
// transaction block: those are committed and would be repeated by a retry.
// The count restarts when goose reads its version table right before running
// a migration (see markingStore).
type tracker struct {
	mu        sync.Mutex
	armed     bool
	committed int
	timeout   *timeoutEvent
	inflight  map[uint32]string
}

func newTracker() *tracker {
	return &tracker{inflight: make(map[uint32]string)}
}

var _ pgx.QueryTracer = (*tracker)(nil)

func (t *tracker) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(skipTraceKey{}) != nil {
		return ctx
	}
	t.mu.Lock()
	t.inflight[conn.PgConn().PID()] = data.SQL
	t.mu.Unlock()
	return context.WithValue(ctx, querySQLKey{}, data.SQL)
}

func (t *tracker) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if ctx.Value(skipTraceKey{}) != nil {
		return
	}
	pid := conn.PgConn().PID()
	stmt, _ := ctx.Value(querySQLKey{}).(string)

	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inflight, pid)
	if data.Err == nil {
		// 'I' after a successful query means its effects are committed: it ran
		// in autocommit mode or it was the COMMIT itself.
		if t.armed && conn.PgConn().TxStatus() == 'I' {
			t.committed++
		}
		return
	}
	if isLockTimeout(data.Err) {
		t.timeout = &timeoutEvent{pid: pid, statement: stmt, committedBefore: t.committed}
	}
}

// reset starts a new attempt.
func (t *tracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.armed = false
	t.committed = 0
	t.timeout = nil
}

// mark records that goose has finished its bookkeeping reads and is about to
// run migration statements.
func (t *tracker) mark() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.armed = true
	t.committed = 0
}

func (t *tracker) lastTimeout() *timeoutEvent {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.timeout == nil {
		return nil
	}
	ev := *t.timeout
	return &ev
}

func (t *tracker) inflightPIDs() []uint32 {
	t.mu.Lock()
	defer t.mu.Unlock()
	pids := make([]uint32, 0, len(t.inflight))
	for pid := range t.inflight {
		pids = append(pids, pid)
	}
	return pids
}

// markingStore wraps goose's Postgres store. goose reads the version table
// (ListMigrations in Up/UpByOne/Down, GetMigration in ApplyVersion) right
// before it runs a migration, so each read moves the tracker's start mark.
type markingStore struct {
	database.Store
	tr *tracker
}

var _ database.StoreExtender = (*markingStore)(nil)

func (s *markingStore) ListMigrations(ctx context.Context, db database.DBTxConn) ([]*database.ListMigrationsResult, error) {
	res, err := s.Store.ListMigrations(ctx, db)
	s.tr.mark()
	return res, err
}

func (s *markingStore) GetMigration(ctx context.Context, db database.DBTxConn, version int64) (*database.GetMigrationResult, error) {
	res, err := s.Store.GetMigration(ctx, db, version)
	s.tr.mark()
	return res, err
}

// TableExists forwards to the wrapped store; embedding the Store interface
// alone would hide this optional method from goose.
func (s *markingStore) TableExists(ctx context.Context, db database.DBTxConn) (bool, error) {
	if ext, ok := s.Store.(database.StoreExtender); ok {
		return ext.TableExists(ctx, db)
	}
	return false, errors.ErrUnsupported
}
