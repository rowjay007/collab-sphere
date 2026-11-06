package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"

	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
)

type LiveMeta struct {
	Seq       uint64
	Collected crdt.VersionVector
}

func (s *Store) LiveMeta(ctx context.Context, id uuid.UUID) (LiveMeta, bool, error) {
	var seq int64
	var raw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT seq, state FROM snapshots
		WHERE document_id = $1 AND kind = $2`, id, snapshotKindLive).Scan(&seq, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return LiveMeta{}, false, nil
	}
	if err != nil {
		return LiveMeta{}, false, fmt.Errorf("load live snapshot: %w", err)
	}
	snap := &crdtv1.Snapshot{}
	if err := protojson.Unmarshal(raw, snap); err != nil {
		return LiveMeta{}, false, fmt.Errorf("unmarshal live snapshot: %w", err)
	}
	collected, err := vectorFromSnapshot(snap)
	if err != nil {
		return LiveMeta{}, false, err
	}
	return LiveMeta{Seq: uint64(seq), Collected: collected}, true, nil
}

func vectorFromSnapshot(snap *crdtv1.Snapshot) (crdt.VersionVector, error) {
	out := make(crdt.VersionVector)
	for _, e := range snap.GetCollected() {
		if e.GetClient() == 0 || e.GetClock() == 0 {
			continue
		}
		out[crdt.ClientID(e.GetClient())] = e.GetClock()
	}
	return out, nil
}

func (s *Store) UpsertLiveSnapshot(ctx context.Context, id uuid.UUID, seq uint64, snap *crdtv1.Snapshot) error {
	body, err := protojson.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal live snapshot: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO snapshots (document_id, seq, kind, state)
		VALUES ($1, $2, $3, $4::jsonb)
		ON CONFLICT (document_id) WHERE kind = 'live'
		DO UPDATE SET seq = EXCLUDED.seq, state = EXCLUDED.state`,
		id, int64(seq), snapshotKindLive, body); err != nil {
		return fmt.Errorf("upsert live snapshot: %w", err)
	}
	return nil
}

func (s *Store) ListGCCandidates(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT d.id
		FROM documents d
		LEFT JOIN snapshots live ON live.document_id = d.id AND live.kind = 'live'
		WHERE d.status = $1
		  AND (
		    d.updated_at >= now() - interval '7 days'
		    OR COALESCE(live.seq, 0) < d.op_seq
		  )
		ORDER BY d.updated_at DESC`, domain.StatusActive)
	if err != nil {
		return nil, fmt.Errorf("list gc candidates: %w", err)
	}
	defer rows.Close()
	out := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) BuildDocThrough(ctx context.Context, id uuid.UUID, through uint64) (*crdt.Doc, uint64, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("begin build: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `SELECT `+documentColumns+` FROM documents WHERE id = $1`, id)
	docMeta, err := scanDocument(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, 0, domain.ErrNotFound
		}
		return nil, 0, err
	}
	if through > docMeta.OpSeq {
		through = docMeta.OpSeq
	}
	baseSeq, snap, err := loadStateBase(ctx, tx, id, docMeta)
	if err != nil {
		return nil, 0, err
	}
	cdoc, err := crdt.FromSnapshot(snap)
	if err != nil {
		return nil, 0, fmt.Errorf("restore snapshot: %w", err)
	}
	ops, err := loadOpsRange(ctx, tx, id, baseSeq, through)
	if err != nil {
		return nil, 0, err
	}
	for i, pb := range ops {
		op, err := crdt.OpFromProto(pb)
		if err != nil {
			return nil, 0, fmt.Errorf("decode op %d: %w", i, err)
		}
		if _, err := cdoc.Apply(op); err != nil {
			return nil, 0, fmt.Errorf("apply op %d: %w", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, 0, err
	}
	return cdoc, through, nil
}

func loadStateBase(ctx context.Context, tx pgx.Tx, id uuid.UUID, doc domain.Document) (uint64, *crdtv1.Snapshot, error) {
	var liveSeq int64
	var liveRaw []byte
	err := tx.QueryRow(ctx, `
		SELECT seq, state FROM snapshots
		WHERE document_id = $1 AND kind = $2`, id, snapshotKindLive).Scan(&liveSeq, &liveRaw)
	if err == nil && uint64(liveSeq) >= doc.SnapshotSeq {
		snap := &crdtv1.Snapshot{}
		if err := protojson.Unmarshal(liveRaw, snap); err != nil {
			return 0, nil, fmt.Errorf("unmarshal live snapshot: %w", err)
		}
		return uint64(liveSeq), snap, nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, fmt.Errorf("load live snapshot: %w", err)
	}
	snap, err := loadSnapshot(ctx, tx, id, doc.SnapshotSeq)
	if err != nil {
		return 0, nil, err
	}
	return doc.SnapshotSeq, snap, nil
}

func (s *Store) DocumentCommittedVector(ctx context.Context, id uuid.UUID) (crdt.VersionVector, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)
	var raw []byte
	if err := tx.QueryRow(ctx, `SELECT vector FROM documents WHERE id = $1`, id).Scan(&raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, domain.ErrNotFound
		}
		return nil, false, err
	}
	if len(raw) == 0 {
		return nil, false, nil
	}
	vector, err := unmarshalVector(raw)
	return vector, true, err
}
