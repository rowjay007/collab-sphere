package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Hold is a session-level per-document mutex. It stays held across short
// transactions and document-service RPCs so Materialize cannot emit a node
// at the mutation seq while Append is in flight.
type Hold struct {
	conn *pgxpool.Conn
	docs []uuid.UUID
}

func sortDocs(docs []uuid.UUID) []uuid.UUID {
	sorted := slices.Clone(docs)
	slices.SortFunc(sorted, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
	return slices.Compact(sorted)
}

// Hold acquires pg_advisory_lock for each doc in UUID order on one
// connection, creating the version_docs rows if needed.
func (s *Store) Hold(ctx context.Context, docs []uuid.UUID) (*Hold, error) {
	sorted := sortDocs(docs)
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire: %w", err)
	}
	h := &Hold{conn: conn, docs: sorted}
	for _, doc := range sorted {
		k1, k2 := docKeys(doc)
		if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1, $2)`, k1, k2); err != nil {
			h.Release()
			return nil, fmt.Errorf("advisory lock %s: %w", doc, err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO version_docs (doc) VALUES ($1) ON CONFLICT DO NOTHING`, doc); err != nil {
			h.Release()
			return nil, fmt.Errorf("ensure version_docs %s: %w", doc, err)
		}
	}
	return h, nil
}

func (h *Hold) Covers(docs []uuid.UUID) bool {
	have := make(map[uuid.UUID]struct{}, len(h.docs))
	for _, d := range h.docs {
		have[d] = struct{}{}
	}
	for _, d := range docs {
		if _, ok := have[d]; !ok {
			return false
		}
	}
	return true
}

// Do runs fn in a transaction that row-locks the requested docs. The
// advisory locks stay held after commit, so the next Do still serializes.
func (h *Hold) Do(ctx context.Context, docs []uuid.UUID, fn func(Tx) error) error {
	sorted := sortDocs(docs)
	if !h.Covers(sorted) {
		return fmt.Errorf("hold %v does not cover %v", h.docs, sorted)
	}
	tx, err := h.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	for _, doc := range sorted {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM version_docs WHERE doc = $1 FOR UPDATE`, doc); err != nil {
			return fmt.Errorf("lock %s: %w", doc, err)
		}
	}
	if err := fn(Tx{tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (h *Hold) Release() {
	ctx := context.Background()
	for i := len(h.docs) - 1; i >= 0; i-- {
		k1, k2 := docKeys(h.docs[i])
		_, _ = h.conn.Exec(ctx, `SELECT pg_advisory_unlock($1, $2)`, k1, k2)
	}
	h.conn.Release()
}

func docKeys(id uuid.UUID) (int32, int32) {
	u := binary.BigEndian.Uint64(id[:8])
	return int32(u >> 32), int32(u)
}
