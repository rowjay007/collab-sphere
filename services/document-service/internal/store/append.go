package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
)

var marshalOp = proto.MarshalOptions{Deterministic: true}

type opKey struct {
	client uint64
	clock  uint64
}

type pendingOp struct {
	key  opKey
	raw  []byte
	skip bool
	seq  uint64
}

type existingOp struct {
	seq uint64
	raw []byte
}

type AppendResult struct {
	OpSeq       uint64
	FirstNewSeq uint64
	Seqs        []uint64
}

const (
	defaultReadLimit = 1000
	maxReadLimit     = 10000
)

func (s *Store) Append(ctx context.Context, id uuid.UUID, ops []*crdtv1.Operation) (AppendResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AppendResult{}, fmt.Errorf("begin append: %w", err)
	}
	defer tx.Rollback(ctx)

	doc, err := lockDocument(ctx, tx, id)
	if err != nil {
		return AppendResult{}, err
	}

	pending, err := classifyOps(ctx, tx, id, ops)
	if err != nil {
		return AppendResult{}, err
	}

	next := doc.OpSeq
	insertSeqs := make([]int64, 0, len(pending))
	clients := make([]string, 0, len(pending))
	clocks := make([]string, 0, len(pending))
	rawOps := make([][]byte, 0, len(pending))
	for i := range pending {
		if pending[i].skip {
			continue
		}
		next++
		pending[i].seq = next
		insertSeqs = append(insertSeqs, int64(next))
		clients = append(clients, strconv.FormatUint(pending[i].key.client, 10))
		clocks = append(clocks, strconv.FormatUint(pending[i].key.clock, 10))
		rawOps = append(rawOps, pending[i].raw)
	}
	result := AppendResult{
		OpSeq: next,
		Seqs:  seqsOf(pending),
	}
	if next > doc.OpSeq {
		result.FirstNewSeq = doc.OpSeq + 1
	}

	if next == doc.OpSeq {
		if err := tx.Commit(ctx); err != nil {
			return AppendResult{}, fmt.Errorf("commit append: %w", err)
		}
		if doc.OpSeq-doc.SnapshotSeq >= domain.SnapshotEvery {
			s.triggerCompaction(id, doc.SnapshotSeq)
		}
		return result, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO operations (document_id, seq, op_client, op_clock, op)
		SELECT $1, seq, op_client, op_clock, op
		FROM unnest($2::bigint[], $3::numeric[], $4::numeric[], $5::bytea[])
			AS pending(seq, op_client, op_clock, op)`,
		id, insertSeqs, clients, clocks, rawOps); err != nil {
		return AppendResult{}, fmt.Errorf("insert ops: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE documents
		SET op_seq = $2, updated_at = now()
		WHERE id = $1`, id, int64(next)); err != nil {
		return AppendResult{}, fmt.Errorf("bump seq: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return AppendResult{}, fmt.Errorf("commit append: %w", err)
	}
	if next-doc.SnapshotSeq >= domain.SnapshotEvery {
		s.triggerCompaction(id, doc.SnapshotSeq)
	}
	return result, nil
}

// Compact holds no lock while it rebuilds: operations and snapshots are
// immutable once written, so a snapshot at op_seq stays valid however far
// Append has moved on, and the conditional UPDATE keeps snapshot_seq monotonic.
func (s *Store) Compact(ctx context.Context, id uuid.UUID) (uint64, error) {
	var opSeq, snapshotSeq int64
	if err := s.pool.QueryRow(ctx, `
		SELECT op_seq, snapshot_seq
		FROM documents
		WHERE id = $1`, id).Scan(&opSeq, &snapshotSeq); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, domain.ErrNotFound
		}
		return 0, fmt.Errorf("load compact metadata: %w", err)
	}
	if uint64(opSeq-snapshotSeq) < domain.SnapshotEvery {
		return uint64(snapshotSeq), nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin compact: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := writeSnapshot(ctx, tx, id, uint64(snapshotSeq), uint64(opSeq)); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE documents
		SET snapshot_seq = $2
		WHERE id = $1 AND snapshot_seq < $2`, id, opSeq); err != nil {
		return 0, fmt.Errorf("advance snapshot seq: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit compact: %w", err)
	}
	return uint64(opSeq), nil
}

func (s *Store) LoadState(ctx context.Context, id uuid.UUID) (domain.Document, *crdtv1.Snapshot, []*crdtv1.Operation, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return domain.Document{}, nil, nil, fmt.Errorf("begin load: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `SELECT `+documentColumns+` FROM documents WHERE id = $1`, id)
	doc, err := scanDocument(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Document{}, nil, nil, domain.ErrNotFound
		}
		return domain.Document{}, nil, nil, err
	}

	snap, err := loadSnapshot(ctx, tx, id, doc.SnapshotSeq)
	if err != nil {
		return domain.Document{}, nil, nil, err
	}
	ops, err := loadOpsAfter(ctx, tx, id, doc.SnapshotSeq)
	if err != nil {
		return domain.Document{}, nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Document{}, nil, nil, fmt.Errorf("commit load: %w", err)
	}
	return doc, snap, ops, nil
}

func (s *Store) ReadOperations(ctx context.Context, id uuid.UUID, after uint64, limit int) (uint64, []*crdtv1.Operation, uint64, error) {
	if limit < 0 {
		return 0, nil, 0, fmt.Errorf("%w: limit %d", domain.ErrInvalidArgument, limit)
	}
	if limit == 0 {
		limit = defaultReadLimit
	}
	if limit > maxReadLimit {
		return 0, nil, 0, fmt.Errorf("%w: limit %d", domain.ErrInvalidArgument, limit)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return 0, nil, 0, fmt.Errorf("begin read ops: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `SELECT `+documentColumns+` FROM documents WHERE id = $1`, id)
	doc, err := scanDocument(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil, 0, domain.ErrNotFound
		}
		return 0, nil, 0, err
	}
	if after > doc.OpSeq {
		return 0, nil, 0, fmt.Errorf("%w: after_seq %d exceeds op_seq %d", domain.ErrInvalidArgument, after, doc.OpSeq)
	}

	rows, err := tx.Query(ctx, `
		SELECT seq, op FROM operations
		WHERE document_id = $1 AND seq > $2
		ORDER BY seq
		LIMIT $3`, id, int64(after), limit)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("read ops: %w", err)
	}
	firstSeq, ops, err := scanOps(rows)
	if err != nil {
		return 0, nil, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, nil, 0, fmt.Errorf("commit read ops: %w", err)
	}
	return firstSeq, ops, doc.OpSeq, nil
}

func lockDocument(ctx context.Context, tx pgx.Tx, id uuid.UUID) (domain.Document, error) {
	row := tx.QueryRow(ctx, `SELECT `+documentColumns+` FROM documents WHERE id = $1 FOR UPDATE`, id)
	doc, err := scanDocument(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Document{}, domain.ErrNotFound
		}
		return domain.Document{}, err
	}
	if doc.Status == domain.StatusArchived {
		return domain.Document{}, domain.ErrArchived
	}
	return doc, nil
}

func classifyOps(ctx context.Context, tx pgx.Tx, id uuid.UUID, ops []*crdtv1.Operation) ([]pendingOp, error) {
	pending := make([]pendingOp, 0, len(ops))
	seen := make(map[opKey][]byte, len(ops))
	for _, protoOp := range ops {
		if _, err := crdt.OpFromProto(protoOp); err != nil {
			return nil, fmt.Errorf("%w: %v", domain.ErrInvalidArgument, err)
		}
		raw, err := marshalOp.Marshal(protoOp)
		if err != nil {
			return nil, fmt.Errorf("marshal op: %w", err)
		}
		key := opKey{client: protoOp.GetId().GetClient(), clock: protoOp.GetId().GetClock()}
		if prev, ok := seen[key]; ok {
			if !bytes.Equal(prev, raw) {
				return nil, domain.ErrConflict
			}
			pending = append(pending, pendingOp{key: key, raw: raw, skip: true})
			continue
		}
		seen[key] = raw
		pending = append(pending, pendingOp{key: key, raw: raw})
	}
	if len(seen) == 0 {
		return pending, nil
	}

	stored, err := existingOps(ctx, tx, id, keysOf(seen))
	if err != nil {
		return nil, err
	}
	for i := range pending {
		if pending[i].skip {
			continue
		}
		held, ok := stored[pending[i].key]
		if !ok {
			continue
		}
		if !bytes.Equal(held.raw, pending[i].raw) {
			return nil, domain.ErrConflict
		}
		pending[i].skip = true
		pending[i].seq = held.seq
	}
	return pending, nil
}

func seqsOf(pending []pendingOp) []uint64 {
	out := make([]uint64, len(pending))
	first := make(map[opKey]uint64, len(pending))
	for i, p := range pending {
		if seq, ok := first[p.key]; ok {
			out[i] = seq
			continue
		}
		out[i] = p.seq
		first[p.key] = p.seq
	}
	return out
}

func keysOf(seen map[opKey][]byte) []opKey {
	keys := make([]opKey, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	return keys
}

func existingOps(ctx context.Context, tx pgx.Tx, id uuid.UUID, keys []opKey) (map[opKey]existingOp, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	clients := make([]string, len(keys))
	clocks := make([]string, len(keys))
	for i, k := range keys {
		clients[i] = strconv.FormatUint(k.client, 10)
		clocks[i] = strconv.FormatUint(k.clock, 10)
	}
	rows, err := tx.Query(ctx, `
		SELECT e.op_client::text, e.op_clock::text, e.seq, e.op
		FROM operations e
		JOIN unnest($2::numeric[], $3::numeric[]) AS v(op_client, op_clock)
		  ON e.op_client = v.op_client AND e.op_clock = v.op_clock
		WHERE e.document_id = $1`, id, clients, clocks)
	if err != nil {
		return nil, fmt.Errorf("lookup ops: %w", err)
	}
	defer rows.Close()

	out := make(map[opKey]existingOp)
	for rows.Next() {
		var clientText, clockText string
		var seq int64
		var raw []byte
		if err := rows.Scan(&clientText, &clockText, &seq, &raw); err != nil {
			return nil, fmt.Errorf("scan stored op: %w", err)
		}
		client, err := strconv.ParseUint(clientText, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse op_client: %w", err)
		}
		clock, err := strconv.ParseUint(clockText, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse op_clock: %w", err)
		}
		out[opKey{client: client, clock: clock}] = existingOp{seq: uint64(seq), raw: raw}
	}
	return out, rows.Err()
}

func writeSnapshot(ctx context.Context, tx pgx.Tx, id uuid.UUID, fromSeq, toSeq uint64) error {
	snap, err := loadSnapshot(ctx, tx, id, fromSeq)
	if err != nil {
		return err
	}
	doc, err := crdt.FromSnapshot(snap)
	if err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	ops, err := loadOpsRange(ctx, tx, id, fromSeq, toSeq)
	if err != nil {
		return err
	}
	for _, protoOp := range ops {
		op, err := crdt.OpFromProto(protoOp)
		if err != nil {
			return fmt.Errorf("%w: %v", domain.ErrInvalidArgument, err)
		}
		if _, err := doc.Apply(op); err != nil {
			return fmt.Errorf("apply for snapshot: %w", err)
		}
	}
	body, err := protojson.Marshal(doc.Snapshot())
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO snapshots (document_id, seq, state)
		VALUES ($1, $2, $3::jsonb)
		ON CONFLICT (document_id, seq) DO NOTHING`, id, int64(toSeq), body); err != nil {
		return fmt.Errorf("insert snapshot: %w", err)
	}
	return nil
}

func loadOpsRange(ctx context.Context, tx pgx.Tx, id uuid.UUID, after, through uint64) ([]*crdtv1.Operation, error) {
	rows, err := tx.Query(ctx, `
		SELECT seq, op FROM operations
		WHERE document_id = $1 AND seq > $2 AND seq <= $3
		ORDER BY seq`, id, int64(after), int64(through))
	if err != nil {
		return nil, fmt.Errorf("load ops: %w", err)
	}
	_, ops, err := scanOps(rows)
	return ops, err
}

func loadSnapshot(ctx context.Context, tx pgx.Tx, id uuid.UUID, seq uint64) (*crdtv1.Snapshot, error) {
	if seq == 0 {
		return crdt.New().Snapshot(), nil
	}
	var raw []byte
	err := tx.QueryRow(ctx, `
		SELECT state FROM snapshots WHERE document_id = $1 AND seq = $2`, id, int64(seq)).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("load snapshot at seq %d: invariant violation, row missing: %w", seq, err)
		}
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	snap := &crdtv1.Snapshot{}
	if err := protojson.Unmarshal(raw, snap); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return snap, nil
}

func loadOpsAfter(ctx context.Context, tx pgx.Tx, id uuid.UUID, seq uint64) ([]*crdtv1.Operation, error) {
	rows, err := tx.Query(ctx, `
		SELECT seq, op FROM operations
		WHERE document_id = $1 AND seq > $2
		ORDER BY seq`, id, int64(seq))
	if err != nil {
		return nil, fmt.Errorf("load ops: %w", err)
	}
	_, ops, err := scanOps(rows)
	return ops, err
}

func scanOps(rows pgx.Rows) (uint64, []*crdtv1.Operation, error) {
	defer rows.Close()
	var firstSeq uint64
	var ops []*crdtv1.Operation
	for rows.Next() {
		var seq int64
		var raw []byte
		if err := rows.Scan(&seq, &raw); err != nil {
			return 0, nil, fmt.Errorf("scan op: %w", err)
		}
		op := &crdtv1.Operation{}
		if err := proto.Unmarshal(raw, op); err != nil {
			return 0, nil, fmt.Errorf("unmarshal op: %w", err)
		}
		if len(ops) == 0 {
			firstSeq = uint64(seq)
		}
		ops = append(ops, op)
	}
	return firstSeq, ops, rows.Err()
}
