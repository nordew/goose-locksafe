package locksafe

import (
	"fmt"
	"strings"
)

// MigrationError is returned when a migration fails and the runner stops
// retrying it. It wraps the last database error, so errors.As with
// *pgconn.PgError works.
type MigrationError struct {
	// Version and Source identify the migration. Version is 0 when goose
	// failed before it picked a migration (for example while reading the
	// version table).
	Version int64
	Source  string
	// Attempts is how many times the runner tried.
	Attempts int
	// Reason says why the runner stopped.
	Reason string
	// Statement is the statement that hit lock_timeout on the last attempt.
	Statement string
	// Blockers are the sessions seen blocking the last timed-out attempt.
	Blockers []Blocker
	// DroppedIndexes are invalid indexes left by failed attempts that the
	// runner dropped, as schema.name.
	DroppedIndexes []string
	// InvalidIndexes are invalid indexes left by failed attempts that the
	// runner could not drop. They are still in the database.
	InvalidIndexes []string
	Err            error
}

func (e *MigrationError) Error() string {
	var b strings.Builder
	b.WriteString("migration")
	if e.Version != 0 {
		fmt.Fprintf(&b, " %d", e.Version)
	}
	if e.Source != "" {
		fmt.Fprintf(&b, " (%s)", e.Source)
	}
	fmt.Fprintf(&b, ": %s after %d attempt(s): %v", e.Reason, e.Attempts, e.Err)
	if e.Statement != "" {
		fmt.Fprintf(&b, "; timed-out statement: %q", oneLine(e.Statement))
	}
	if len(e.Blockers) > 0 {
		b.WriteString("; blocked by: ")
		for i, bl := range e.Blockers {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("[" + bl.String() + "]")
		}
	} else if e.Statement != "" {
		b.WriteString("; blocker not captured")
	}
	if len(e.DroppedIndexes) > 0 {
		fmt.Fprintf(&b, "; dropped invalid index(es): %s", strings.Join(e.DroppedIndexes, ", "))
	}
	if len(e.InvalidIndexes) > 0 {
		fmt.Fprintf(&b, "; INVALID INDEX(ES) LEFT IN PLACE, drop manually: %s", strings.Join(e.InvalidIndexes, ", "))
	}
	return b.String()
}

func (e *MigrationError) Unwrap() error { return e.Err }

func oneLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxQueryInMessage {
		s = s[:maxQueryInMessage] + "..."
	}
	return s
}
