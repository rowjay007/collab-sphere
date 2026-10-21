package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rowjay007/collab-sphere/services/version-service/internal/dag"
)

var ErrNotFound = errors.New("version: not found")

// Meta is a node's unhashed metadata.
type Meta struct {
	Labels       []string `json:"labels"`
	RestoredFrom []byte   `json:"restored_from,omitempty"`
}

type Version struct {
	ID        dag.NodeID
	Node      dag.Node
	Meta      Meta
	CreatedAt time.Time
}

type Checkpoint struct {
	Node      dag.NodeID
	RequestID string
}

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Tx is a transaction that holds the version_docs row locks of its documents.
type Tx struct {
	tx pgx.Tx
}

// WithDoc locks the version_docs rows of docs in UUID order, creating them if
// needed, finishes their pending mutations, then runs fn and commits.
func (s *Store) WithDoc(ctx context.Context, docs []uuid.UUID, fn func(Tx) error) error {
	sorted := slices.Clone(docs)
	slices.SortFunc(sorted, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
	sorted = slices.Compact(sorted)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	for _, doc := range sorted {
		if _, err := tx.Exec(ctx, `INSERT INTO version_docs (doc) VALUES ($1) ON CONFLICT DO NOTHING`, doc); err != nil {
			return fmt.Errorf("ensure version_docs %s: %w", doc, err)
		}
		if _, err := tx.Exec(ctx, `SELECT 1 FROM version_docs WHERE doc = $1 FOR UPDATE`, doc); err != nil {
			return fmt.Errorf("lock %s: %w", doc, err)
		}
	}
	t := Tx{tx: tx}
	if err := t.finishPendingMutations(ctx, sorted); err != nil {
		return err
	}
	if err := fn(t); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (t Tx) finishPendingMutations(context.Context, []uuid.UUID) error {
	return nil
}

// Tip returns the document's highest-seq node; false before its chain starts.
func (t Tx) Tip(ctx context.Context, doc uuid.UUID) (Version, bool, error) {
	return optional(scanVersion(t.tx.QueryRow(ctx, selectVersion+`
		WHERE n.id = (SELECT tip FROM version_docs WHERE doc = $1)`, doc)))
}

func (t Tx) NodeAtOrBelow(ctx context.Context, doc uuid.UUID, seq uint64) (Version, bool, error) {
	return optional(scanVersion(t.tx.QueryRow(ctx, selectVersion+`
		WHERE n.doc = $1 AND n.seq <= $2
		ORDER BY n.seq DESC
		LIMIT 1`, doc, int64(seq))))
}

func (t Tx) Blob(ctx context.Context, hash dag.Hash) ([]byte, error) {
	return blob(ctx, t.tx, hash)
}

// PutNode stores n, its parents' edges and, unless nil, its canonical bytes,
// and moves the tip when n is the document's highest seq. Storing a node that
// exists is a no-op that returns the stored row.
func (t Tx) PutNode(ctx context.Context, n dag.Node, canonical []byte, meta Meta) (Version, error) {
	id := n.ID()
	if canonical != nil {
		if _, err := t.tx.Exec(ctx, `INSERT INTO blobs (hash, bytes) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			n.Content[:], canonical); err != nil {
			return Version{}, fmt.Errorf("insert blob: %w", err)
		}
	}
	if meta.Labels == nil {
		meta.Labels = []string{}
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return Version{}, err
	}
	tag, err := t.tx.Exec(ctx, `
		INSERT INTO nodes (id, doc, seq, content_hash, meta)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`, id[:], n.Doc, int64(n.Seq), n.Content[:], metaJSON)
	if err != nil {
		return Version{}, fmt.Errorf("insert node: %w", err)
	}
	if tag.RowsAffected() == 1 {
		for ord, parent := range n.Parents {
			tag, err := t.tx.Exec(ctx, `
				INSERT INTO node_parents (child, ord, parent, parent_doc, parent_seq)
				SELECT $1, $2, id, doc, seq FROM nodes WHERE id = $3`, id[:], ord, parent[:])
			if err != nil {
				return Version{}, fmt.Errorf("insert parent: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return Version{}, fmt.Errorf("parent %s of %s@%d is not stored", parent, n.Doc, n.Seq)
			}
		}
		if _, err := t.tx.Exec(ctx, `
			UPDATE version_docs SET tip = $2, tip_seq = $3
			WHERE doc = $1 AND (tip_seq IS NULL OR tip_seq < $3)`, n.Doc, id[:], int64(n.Seq)); err != nil {
			return Version{}, fmt.Errorf("move tip: %w", err)
		}
	}
	stored, err := scanVersion(t.tx.QueryRow(ctx, selectVersion+` WHERE n.doc = $1 AND n.seq = $2`, n.Doc, int64(n.Seq)))
	if err != nil {
		return Version{}, fmt.Errorf("re-read node: %w", err)
	}
	if stored.ID != id {
		return Version{}, fmt.Errorf("%s@%d is stored as %s, computed %s", n.Doc, n.Seq, stored.ID, id)
	}
	return stored, nil
}

func (t Tx) AddLabel(ctx context.Context, id dag.NodeID, label string) error {
	if _, err := t.tx.Exec(ctx, `
		UPDATE nodes SET meta = jsonb_set(meta, '{labels}', coalesce(meta->'labels', '[]'::jsonb) || to_jsonb($2::text))
		WHERE id = $1 AND NOT coalesce(meta->'labels', '[]'::jsonb) ? $2`, id[:], label); err != nil {
		return fmt.Errorf("add label: %w", err)
	}
	return nil
}

// PutCheckpoint creates the ref unless the name is taken and returns the
// stored ref either way.
func (t Tx) PutCheckpoint(ctx context.Context, doc uuid.UUID, name string, node dag.NodeID, requestID string) (Checkpoint, error) {
	if _, err := t.tx.Exec(ctx, `
		INSERT INTO checkpoints (doc, name, node, request_id) VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING`, doc, name, node[:], requestID); err != nil {
		return Checkpoint{}, fmt.Errorf("insert checkpoint: %w", err)
	}
	cp, _, err := checkpoint(ctx, t.tx, doc, name)
	return cp, err
}

func (s *Store) Checkpoint(ctx context.Context, doc uuid.UUID, name string) (Checkpoint, bool, error) {
	return checkpoint(ctx, s.pool, doc, name)
}

func (s *Store) Version(ctx context.Context, id dag.NodeID) (Version, error) {
	v, err := scanVersion(s.pool.QueryRow(ctx, selectVersion+` WHERE n.id = $1`, id[:]))
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, fmt.Errorf("%w: version %s", ErrNotFound, id)
	}
	return v, err
}

func (s *Store) VersionAt(ctx context.Context, doc uuid.UUID, seq uint64) (Version, error) {
	v, err := scanVersion(s.pool.QueryRow(ctx, selectVersion+` WHERE n.doc = $1 AND n.seq = $2`, doc, int64(seq)))
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, fmt.Errorf("%w: no version of %s at %d", ErrNotFound, doc, seq)
	}
	return v, err
}

// Versions lists the document's nodes with seq above after, ascending.
func (s *Store) Versions(ctx context.Context, doc uuid.UUID, after int64, limit int) ([]Version, error) {
	rows, err := s.pool.Query(ctx, selectVersion+`
		WHERE n.doc = $1 AND n.seq > $2
		ORDER BY n.seq
		LIMIT $3`, doc, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list versions: %w", err)
	}
	defer rows.Close()
	var out []Version
	for rows.Next() {
		v, err := scanVersion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) Blob(ctx context.Context, hash dag.Hash) ([]byte, error) {
	return blob(ctx, s.pool, hash)
}

// Reach returns every other document that doc's nodes reach through
// cross-document parent edges, each at the highest seq reached. A document
// reached at seq s contributes the edges of all its nodes at or below s,
// because its state at s holds that whole log prefix.
func (s *Store) Reach(ctx context.Context, doc uuid.UUID) (map[uuid.UUID]uint64, error) {
	rows, err := s.pool.Query(ctx, `
		WITH RECURSIVE reach (doc, seq) AS (
			SELECT p.parent_doc, p.parent_seq
			FROM nodes n JOIN node_parents p ON p.child = n.id
			WHERE n.doc = $1 AND p.parent_doc <> n.doc
			UNION
			SELECT p.parent_doc, p.parent_seq
			FROM reach r
			JOIN nodes n ON n.doc = r.doc AND n.seq <= r.seq
			JOIN node_parents p ON p.child = n.id
			WHERE p.parent_doc <> n.doc
		)
		SELECT doc, max(seq) FROM reach WHERE doc <> $1 GROUP BY doc`, doc)
	if err != nil {
		return nil, fmt.Errorf("reach: %w", err)
	}
	defer rows.Close()
	out := map[uuid.UUID]uint64{}
	for rows.Next() {
		var (
			d   uuid.UUID
			seq int64
		)
		if err := rows.Scan(&d, &seq); err != nil {
			return nil, err
		}
		out[d] = uint64(seq)
	}
	return out, rows.Err()
}

const selectVersion = `
	SELECT n.id, n.doc, n.seq, n.content_hash, n.meta, n.created_at,
		coalesce((SELECT array_agg(p.parent ORDER BY p.ord) FROM node_parents p WHERE p.child = n.id), '{}')
	FROM nodes n`

func scanVersion(row pgx.Row) (Version, error) {
	var (
		v           Version
		id, content []byte
		seq         int64
		metaJSON    []byte
		parents     [][]byte
	)
	if err := row.Scan(&id, &v.Node.Doc, &seq, &content, &metaJSON, &v.CreatedAt, &parents); err != nil {
		return Version{}, err
	}
	v.ID = dag.NodeID(id)
	v.Node.Seq = uint64(seq)
	v.Node.Content = dag.Hash(content)
	for _, p := range parents {
		v.Node.Parents = append(v.Node.Parents, dag.NodeID(p))
	}
	if err := json.Unmarshal(metaJSON, &v.Meta); err != nil {
		return Version{}, fmt.Errorf("decode meta: %w", err)
	}
	return v, nil
}

func optional(v Version, err error) (Version, bool, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return Version{}, false, nil
	}
	if err != nil {
		return Version{}, false, err
	}
	return v, true, nil
}

func blob(ctx context.Context, q querier, hash dag.Hash) ([]byte, error) {
	var b []byte
	err := q.QueryRow(ctx, `SELECT bytes FROM blobs WHERE hash = $1`, hash[:]).Scan(&b)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: blob %s", ErrNotFound, hash)
	}
	if err != nil {
		return nil, fmt.Errorf("read blob: %w", err)
	}
	return b, nil
}

func checkpoint(ctx context.Context, q querier, doc uuid.UUID, name string) (Checkpoint, bool, error) {
	var (
		cp   Checkpoint
		node []byte
	)
	err := q.QueryRow(ctx, `SELECT node, request_id FROM checkpoints WHERE doc = $1 AND name = $2`, doc, name).Scan(&node, &cp.RequestID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, fmt.Errorf("read checkpoint: %w", err)
	}
	cp.Node = dag.NodeID(node)
	return cp, true, nil
}
