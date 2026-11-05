package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
)

func (s *Store) ClaimReplica(ctx context.Context, documentID uuid.UUID, requested uint64, system bool, userID string) (uint64, error) {
	if system && userID != "" {
		return 0, fmt.Errorf("%w: system replica has no user", domain.ErrInvalidArgument)
	}
	if userID != "" && !auth.ValidHandle(userID) {
		return 0, fmt.Errorf("%w: user id", domain.ErrInvalidArgument)
	}
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

	kind := replicaKind(system)
	client := requested
	if client == 0 {
		client, err = allocateReplica(ctx, tx, documentID, forkRoot, kind, userID)
	} else {
		err = claimReplica(ctx, tx, documentID, forkRoot, client, kind, userID)
	}
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit replica claim: %w", err)
	}
	return client, nil
}

func replicaKind(system bool) string {
	if system {
		return "system"
	}
	return "client"
}

func allocateReplica(ctx context.Context, tx pgx.Tx, documentID, forkRoot uuid.UUID, kind, userID string) (uint64, error) {
	for {
		var next int64
		if err := tx.QueryRow(ctx, `SELECT nextval('replica_ids')`).Scan(&next); err != nil {
			return 0, fmt.Errorf("allocate replica id: %w", err)
		}
		client := uint64(next)
		tag, err := tx.Exec(ctx, `
			INSERT INTO replicas (id, fork_root, document_id, kind, user_id)
			VALUES ($1::numeric, $2, $3, $4, $5)
			ON CONFLICT DO NOTHING`, strconv.FormatUint(client, 10), forkRoot, documentID, kind, userArg(userID))
		if err != nil {
			return 0, fmt.Errorf("insert allocated replica: %w", err)
		}
		if tag.RowsAffected() == 1 {
			return client, nil
		}
	}
}

func claimReplica(ctx context.Context, tx pgx.Tx, documentID, forkRoot uuid.UUID, client uint64, kind, userID string) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO replicas (id, fork_root, document_id, kind, user_id)
		VALUES ($1::numeric, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`, strconv.FormatUint(client, 10), forkRoot, documentID, kind, userArg(userID))
	if err != nil {
		return fmt.Errorf("insert claimed replica: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var home uuid.UUID
	var existing string
	var existingUser *string
	err = tx.QueryRow(ctx, `
		SELECT document_id, kind, user_id
		FROM replicas
		WHERE id = $1::numeric AND fork_root = $2`,
		strconv.FormatUint(client, 10), forkRoot).Scan(&home, &existing, &existingUser)
	if err != nil {
		return fmt.Errorf("load replica home: %w", err)
	}
	if home != documentID || existing == "system" && kind != "system" {
		return domain.ErrReplicaHomedElsewhere
	}
	if kind == "system" || existing == "system" {
		return nil
	}
	return bindReplicaUser(ctx, tx, forkRoot, client, userID, existingUser)
}

func bindReplicaUser(ctx context.Context, tx pgx.Tx, forkRoot uuid.UUID, client uint64, userID string, existing *string) error {
	if existing == nil {
		if userID == "" {
			return nil
		}
		tag, err := tx.Exec(ctx, `
			UPDATE replicas SET user_id = $3
			WHERE id = $1::numeric AND fork_root = $2 AND user_id IS NULL`,
			strconv.FormatUint(client, 10), forkRoot, userID)
		if err != nil {
			return fmt.Errorf("bind replica user: %w", err)
		}
		if tag.RowsAffected() == 1 {
			return nil
		}
		var now *string
		err = tx.QueryRow(ctx, `
			SELECT user_id FROM replicas
			WHERE id = $1::numeric AND fork_root = $2`,
			strconv.FormatUint(client, 10), forkRoot).Scan(&now)
		if err != nil {
			return fmt.Errorf("reload replica user: %w", err)
		}
		if now != nil && *now == userID {
			return nil
		}
		return domain.ErrReplicaOwned
	}
	if userID != "" && *existing == userID {
		return nil
	}
	return domain.ErrReplicaOwned
}

func userArg(userID string) any {
	if userID == "" {
		return nil
	}
	return userID
}
