package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
)

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Store) Fork(ctx context.Context, src uuid.UUID, seq uint64, title, requestID string) (domain.Document, error) {
	if title == "" || requestID == "" {
		return domain.Document{}, fmt.Errorf("%w: title and request_id are required", domain.ErrInvalidArgument)
	}

	if existing, ok, err := lookupForkByRequest(ctx, s.pool, requestID); err != nil {
		return domain.Document{}, err
	} else if ok {
		return adoptFork(existing, src, seq)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Document{}, fmt.Errorf("begin fork: %w", err)
	}
	defer tx.Rollback(ctx)

	if existing, ok, err := lookupForkByRequest(ctx, tx, requestID); err != nil {
		return domain.Document{}, err
	} else if ok {
		return adoptFork(existing, src, seq)
	}

	source, err := lockDocument(ctx, tx, src)
	if err != nil {
		return domain.Document{}, err
	}
	if seq > source.OpSeq {
		return domain.Document{}, fmt.Errorf("%w: seq %d exceeds op_seq %d", domain.ErrOutOfRange, seq, source.OpSeq)
	}

	snapSeq, err := latestSnapshotSeqAtOrBelow(ctx, tx, src, seq)
	if err != nil {
		return domain.Document{}, err
	}

	id := uuid.New()
	row := tx.QueryRow(ctx, `
		INSERT INTO documents (id, title, owner_id, fork_root, fork_of, fork_seq, fork_request_id, op_seq, snapshot_seq)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $6, $8)
		ON CONFLICT (fork_request_id) DO NOTHING
		RETURNING `+documentColumns,
		id, title, source.OwnerID, source.ForkRoot, src, int64(seq), requestID, int64(snapSeq))
	doc, err := scanDocument(row)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, ok, err := lookupForkByRequest(ctx, tx, requestID)
		if err != nil {
			return domain.Document{}, err
		}
		if !ok {
			return domain.Document{}, fmt.Errorf("fork request %q conflicted but the row is missing", requestID)
		}
		return adoptFork(existing, src, seq)
	}
	if err != nil {
		return domain.Document{}, err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO operations (document_id, seq, op_client, op_clock, op)
		SELECT $1, seq, op_client, op_clock, op
		FROM operations
		WHERE document_id = $2 AND seq >= 1 AND seq <= $3`, id, src, int64(seq)); err != nil {
		return domain.Document{}, fmt.Errorf("copy operations: %w", err)
	}
	if snapSeq > 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO snapshots (document_id, seq, state)
			SELECT $1, seq, state
			FROM snapshots
			WHERE document_id = $2 AND seq = $3`, id, src, int64(snapSeq)); err != nil {
			return domain.Document{}, fmt.Errorf("copy snapshot: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Document{}, fmt.Errorf("commit fork: %w", err)
	}
	return doc, nil
}

func adoptFork(existing domain.Document, src uuid.UUID, seq uint64) (domain.Document, error) {
	if existing.ForkOf == nil || *existing.ForkOf != src || existing.ForkSeq != seq {
		return domain.Document{}, domain.ErrAlreadyExists
	}
	return existing, nil
}

func lookupForkByRequest(ctx context.Context, q rowQuerier, requestID string) (domain.Document, bool, error) {
	doc, err := scanDocument(q.QueryRow(ctx, `SELECT `+documentColumns+` FROM documents WHERE fork_request_id = $1`, requestID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Document{}, false, nil
	}
	if err != nil {
		return domain.Document{}, false, err
	}
	return doc, true, nil
}

func latestSnapshotSeqAtOrBelow(ctx context.Context, tx pgx.Tx, id uuid.UUID, seq uint64) (uint64, error) {
	var snap int64
	err := tx.QueryRow(ctx, `
		SELECT seq FROM snapshots
		WHERE document_id = $1 AND seq <= $2
		ORDER BY seq DESC
		LIMIT 1`, id, int64(seq)).Scan(&snap)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("lookup snapshot: %w", err)
	}
	return uint64(snap), nil
}
