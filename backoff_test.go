package locksafe

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestBackoff_WhenAttemptsGrow_StaysWithinHalfToFullCappedWindow(t *testing.T) {
	base, maxWait := 100*time.Millisecond, time.Second
	lo := func(int64) int64 { return 0 }
	hi := func(n int64) int64 { return n - 1 }
	want := []time.Duration{100, 200, 400, 800, 1000, 1000, 1000}
	for i, w := range want {
		d := w * time.Millisecond
		assert.Equal(t, d/2, backoff(i+1, base, maxWait, lo), "attempt %d min", i+1)
		assert.Equal(t, d, backoff(i+1, base, maxWait, hi), "attempt %d max", i+1)
	}
	assert.Equal(t, maxWait, backoff(200, base, maxWait, hi), "no overflow on large attempt")
}

func TestMigrationError_WhenLockTimeout_NamesBlockersAndUnwraps(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "55P03", Message: "canceling statement due to lock timeout"}
	err := &MigrationError{
		Version: 3, Source: "00003_x.sql", Attempts: 5, Reason: "lock timeout on every attempt",
		Statement:      "ALTER TABLE t\n  ADD COLUMN c int",
		Blockers:       []Blocker{{PID: 42, State: "idle in transaction", XactAge: 90 * time.Second, Query: "SELECT *\nFROM t"}},
		InvalidIndexes: []string{"public.t_idx"},
		Err:            pgErr,
	}
	msg := err.Error()
	for _, want := range []string{"migration 3 (00003_x.sql)", "after 5 attempt(s)", `"ALTER TABLE t ADD COLUMN c int"`,
		"pid=42", `state="idle in transaction"`, "xact_age=1m30s", `query="SELECT * FROM t"`, "LEFT IN PLACE", "public.t_idx"} {
		assert.True(t, strings.Contains(msg, want), "missing %q in %s", want, msg)
	}
	var got *pgconn.PgError
	assert.True(t, errors.As(err, &got))
	assert.True(t, isLockTimeout(err))
}
