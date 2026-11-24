package locksafe_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/goleak"

	locksafe "github.com/nordew/goose-locksafe"
)

// pgImage is the server under test. LOCKSAFE_TEST_DSN (a superuser
// postgres:// URL) points the tests at an existing server instead.
const pgImage = "postgres:17-alpine"

var baseDSN string

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	if testing.Short() {
		return m.Run()
	}
	if dsn := os.Getenv("LOCKSAFE_TEST_DSN"); dsn != "" {
		baseDSN = dsn
		return m.Run()
	}
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, pgImage,
		tcpostgres.WithDatabase("locksafe"),
		tcpostgres.WithUsername("locksafe"),
		tcpostgres.WithPassword("locksafe"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("start postgres container: %v", err)
		return 1
	}
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			log.Printf("terminate container: %v", err)
		}
	}()
	baseDSN, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return 1
	}
	return m.Run()
}

// newDB creates an empty database for one test and returns its DSN. It also
// registers the goroutine leak check, which must run after every other
// cleanup.
func newDB(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	ignore := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, ignore) })

	ctx := context.Background()
	b := make([]byte, 6)
	_, err := rand.Read(b)
	require.NoError(t, err)
	name := "t_" + hex.EncodeToString(b)

	admin, err := pgx.Connect(ctx, baseDSN)
	require.NoError(t, err)
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	require.NoError(t, err)
	require.NoError(t, admin.Close(ctx))
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), baseDSN)
		if err != nil {
			t.Logf("drop database: %v", err)
			return
		}
		defer func() { _ = c.Close(context.Background()) }()
		if _, err := c.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Logf("drop database: %v", err)
		}
	})

	u, err := url.Parse(baseDSN)
	require.NoError(t, err, "LOCKSAFE_TEST_DSN must be a postgres:// URL")
	u.Path = "/" + name
	return u.String()
}

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func exec(t *testing.T, c *pgx.Conn, sql string, args ...any) {
	t.Helper()
	_, err := c.Exec(context.Background(), sql, args...)
	require.NoError(t, err, sql)
}

// beginBlocker opens a transaction that runs stmt and stays open. It returns
// the transaction and the backend pid.
func beginBlocker(t *testing.T, dsn, stmt string) (pgx.Tx, uint32) {
	t.Helper()
	c := connect(t, dsn)
	tx, err := c.Begin(context.Background())
	require.NoError(t, err)
	_, err = tx.Exec(context.Background(), stmt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	return tx, c.PgConn().PID()
}

func migrations(files map[string]string) fstest.MapFS {
	fs := fstest.MapFS{}
	for name, body := range files {
		fs[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return fs
}

type logRec struct {
	msg   string
	attrs map[string]any
}

// logCapture is an slog.Handler that keeps every record and signals each
// lock-timeout warning on timeouts.
type logCapture struct {
	t        *testing.T
	mu       sync.Mutex
	recs     []logRec
	timeouts chan logRec
}

func newLogCapture(t *testing.T) *logCapture {
	return &logCapture{t: t, timeouts: make(chan logRec, 64)}
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	rec := logRec{msg: r.Message, attrs: map[string]any{}}
	parts := []string{r.Level.String(), r.Message}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.Any()
		parts = append(parts, fmt.Sprintf("%s=%v", a.Key, a.Value.Any()))
		return true
	})
	c.t.Log(strings.Join(parts, " "))
	c.mu.Lock()
	c.recs = append(c.recs, rec)
	c.mu.Unlock()
	if strings.HasPrefix(r.Message, "migration hit lock_timeout") {
		select {
		case c.timeouts <- rec:
		default:
		}
	}
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

func (c *logCapture) count(prefix string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.recs {
		if strings.HasPrefix(r.msg, prefix) {
			n++
		}
	}
	return n
}

func (c *logCapture) waitTimeout(t *testing.T, within time.Duration) logRec {
	t.Helper()
	select {
	case rec := <-c.timeouts:
		return rec
	case <-time.After(within):
		t.Fatalf("no lock timeout logged within %s", within)
		return logRec{}
	}
}

func newRunner(t *testing.T, dsn string, fs fstest.MapFS, cfg locksafe.Config) (*locksafe.Runner, *logCapture) {
	t.Helper()
	logs := newLogCapture(t)
	cfg.Logger = slog.New(logs)
	r, err := locksafe.New(context.Background(), dsn, fs, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	return r, logs
}
