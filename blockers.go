package locksafe

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Blocker is a session that held or queued for a lock the migration waited on.
type Blocker struct {
	// BlockedPID is the migration backend that was waiting.
	BlockedPID int32
	PID        int32
	// State is pg_stat_activity.state, e.g. "idle in transaction".
	State string
	// XactAge is how long the blocker's transaction had been open when seen.
	XactAge time.Duration
	// Query is the blocker's current or last statement. Without
	// pg_read_all_stats (or the same role) Postgres shows "<insufficient
	// privilege>" here.
	Query           string
	ApplicationName string
	User            string
	WaitEventType   string
}

const maxQueryInMessage = 300

func (b Blocker) String() string {
	q := strings.Join(strings.Fields(b.Query), " ")
	if len(q) > maxQueryInMessage {
		q = q[:maxQueryInMessage] + "..."
	}
	return fmt.Sprintf("pid=%d state=%q xact_age=%s app=%q query=%q",
		b.PID, b.State, b.XactAge.Round(time.Millisecond), b.ApplicationName, q)
}

const blockersSQL = `
SELECT a.pid,
       coalesce(a.state, ''),
       coalesce(extract(epoch FROM clock_timestamp() - a.xact_start), 0)::float8,
       coalesce(a.query, ''),
       coalesce(a.application_name, ''),
       coalesce(a.usename::text, ''),
       coalesce(a.wait_event_type, '')
FROM pg_stat_activity a
WHERE a.pid = ANY (pg_blocking_pids($1))
ORDER BY a.xact_start NULLS LAST, a.pid`

func queryBlockers(ctx context.Context, pool *pgxpool.Pool, blocked int32) ([]Blocker, error) {
	rows, err := pool.Query(ctx, blockersSQL, blocked)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Blocker
	for rows.Next() {
		b := Blocker{BlockedPID: blocked}
		var ageSec float64
		if err := rows.Scan(&b.PID, &b.State, &ageSec, &b.Query, &b.ApplicationName, &b.User, &b.WaitEventType); err != nil {
			return nil, err
		}
		b.XactAge = time.Duration(ageSec * float64(time.Second))
		out = append(out, b)
	}
	return out, rows.Err()
}

// watcher polls pg_blocking_pids for every migration backend that has a query
// in flight. Once a statement hits lock_timeout it is no longer waiting, so
// the blockers have to be sampled while the wait is still going on; the last
// non-empty sample is the one reported.
type watcher struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu   sync.Mutex
	last []Blocker
}

func startWatcher(ctx context.Context, pool *pgxpool.Pool, tr *tracker, every time.Duration, onErr func(error)) *watcher {
	wctx, cancel := context.WithCancel(ctx)
	w := &watcher{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(w.done)
		tick := time.NewTicker(every)
		defer tick.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-tick.C:
			}
			for _, pid := range tr.inflightPIDs() {
				bl, err := queryBlockers(wctx, pool, int32(pid)) //nolint:gosec // Postgres pids are int4.
				if err != nil {
					if wctx.Err() == nil {
						onErr(err)
					}
					continue
				}
				if len(bl) > 0 {
					w.mu.Lock()
					w.last = bl
					w.mu.Unlock()
				}
			}
		}
	}()
	return w
}

// stop ends polling and returns the last blockers seen. It waits for the
// polling goroutine to exit.
func (w *watcher) stop() []Blocker {
	w.cancel()
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last
}
