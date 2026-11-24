package locksafe

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type indexRef struct {
	OID    uint32
	Schema string
	Name   string
}

func (i indexRef) String() string { return i.Schema + "." + i.Name }

// Indexes being built right now by any session are also indisvalid = false.
// They are excluded so the runner never drops another session's build in
// progress.
const invalidIndexesSQL = `
SELECT i.indexrelid, n.nspname, c.relname
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE NOT i.indisvalid
  AND NOT EXISTS (SELECT 1 FROM pg_stat_progress_create_index p WHERE p.index_relid = i.indexrelid)`

func invalidIndexes(ctx context.Context, pool *pgxpool.Pool) (map[uint32]indexRef, error) {
	rows, err := pool.Query(ctx, invalidIndexesSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[uint32]indexRef)
	for rows.Next() {
		var ix indexRef
		if err := rows.Scan(&ix.OID, &ix.Schema, &ix.Name); err != nil {
			return nil, err
		}
		out[ix.OID] = ix
	}
	return out, rows.Err()
}

// newIndexes returns indexes in after that were not in before.
func newIndexes(before, after map[uint32]indexRef) []indexRef {
	var out []indexRef
	for oid, ix := range after {
		if _, ok := before[oid]; !ok {
			out = append(out, ix)
		}
	}
	return out
}

// dropIndexes drops each index with DROP INDEX CONCURRENTLY. The pool's
// connections carry the runner's lock_timeout, so a drop stuck behind the same
// blocker gives up rather than queue forever. It returns the indexes that
// were dropped, the ones still present, and the first error.
func dropIndexes(ctx context.Context, pool *pgxpool.Pool, ixs []indexRef) (dropped, left []indexRef, err error) {
	for _, ix := range ixs {
		q := "DROP INDEX CONCURRENTLY IF EXISTS " + pgx.Identifier{ix.Schema, ix.Name}.Sanitize()
		if _, dropErr := pool.Exec(ctx, q); dropErr != nil {
			left = append(left, ix)
			if err == nil {
				err = dropErr
			}
			continue
		}
		dropped = append(dropped, ix)
	}
	return dropped, left, err
}
