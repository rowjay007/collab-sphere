package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
)

func (s *Store) ClaimReplica(ctx context.Context, documentID uuid.UUID, requested uint64) (uint64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin replica claim: %w", err)
	}
	defer tx.Rollback(ctx)

	var forkRoot uuid.UUID
	var status string
	err = tx.QueryRow(ctx, `
		SELECT fork_root, status
		FROM documents
		WHERE id = $1
		FOR SHARE`, documentID).Scan(&forkRoot, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, domain.ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("load replica document: %w", err)
	}
	if domain.Status(status) == domain.StatusArchived {
		return 0, domain.ErrArchived
	}

	client := requested
	if client == 0 {
		client, err = allocateReplica(ctx, tx, documentID, forkRoot)
	} else {
		err = claimReplica(ctx, tx, documentID, forkRoot, client)
	}
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit replica claim: %w", err)
	}
	return client, nil
}

func allocateReplica(ctx context.Context, tx pgx.Tx, documentID, forkRoot uuid.UUID) (uint64, error) {
	for {
		var next int64
		if err := tx.QueryRow(ctx, `SELECT nextval('replica_ids')`).Scan(&next); err != nil {
			return 0, fmt.Errorf("allocate replica id: %w", err)
		}
		client := uint64(next)
		tag, err := tx.Exec(ctx, `
			INSERT INTO replicas (id, fork_root, document_id)
			VALUES ($1::numeric, $2, $3)
			ON CONFLICT DO NOTHING`, strconv.FormatUint(client, 10), forkRoot, documentID)
		if err != nil {
			return 0, fmt.Errorf("insert allocated replica: %w", err)
		}
		if tag.RowsAffected() == 1 {
			return client, nil
		}
	}
}

func claimReplica(ctx context.Context, tx pgx.Tx, documentID, forkRoot uuid.UUID, client uint64) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO replicas (id, fork_root, document_id)
		VALUES ($1::numeric, $2, $3)
		ON CONFLICT DO NOTHING`, strconv.FormatUint(client, 10), forkRoot, documentID)
	if err != nil {
		return fmt.Errorf("insert claimed replica: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var home uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT document_id
		FROM replicas
		WHERE id = $1::numeric AND fork_root = $2`,
		strconv.FormatUint(client, 10), forkRoot).Scan(&home)
	if err != nil {
		return fmt.Errorf("load replica home: %w", err)
	}
	if home != documentID {
		return domain.ErrReplicaHomedElsewhere
	}
	return nil
}
