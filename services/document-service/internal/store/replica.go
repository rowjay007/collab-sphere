package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	docgc "github.com/rowjay007/collab-sphere/services/document-service/internal/gc"
)

// ClaimReplica binds the replica to documentID. lapsed is true when the
// replica's report row predates this claim and was released, older than ttl,
// or left with a resync pending. A released replica is revived.
func (s *Store) ClaimReplica(ctx context.Context, documentID uuid.UUID, requested uint64, system bool, userID string, ttl time.Duration) (client uint64, lapsed bool, err error) {
	if system && userID != "" {
		return 0, false, fmt.Errorf("%w: system replica has no user", domain.ErrInvalidArgument)
	}
	if userID != "" && !auth.ValidHandle(userID) {
		return 0, false, fmt.Errorf("%w: user id", domain.ErrInvalidArgument)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("begin replica claim: %w", err)
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
		return 0, false, domain.ErrNotFound
	}
	if err != nil {
		return 0, false, fmt.Errorf("load replica document: %w", err)
	}
	if domain.Status(status) == domain.StatusArchived {
		return 0, false, domain.ErrArchived
	}

	kind := replicaKind(system)
	client = requested
	inserted := true
	if client == 0 {
		client, err = allocateReplica(ctx, tx, documentID, forkRoot, kind, userID)
	} else {
		inserted, err = claimReplica(ctx, tx, documentID, forkRoot, client, kind, userID)
	}
	if err != nil {
		return 0, false, err
	}
	lapsed, err = reviveReport(ctx, tx, forkRoot, client, ttl)
	if err != nil {
		return 0, false, err
	}
	switch {
	case system:
		err = writeSystemReport(ctx, tx, documentID, forkRoot, client)
	case inserted:
		err = writeEmptyReport(ctx, tx, forkRoot, client)
	}
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("commit replica claim: %w", err)
	}
	return client, lapsed, nil
}

// reviveReport locks the replica's report row, decides lapse from the row as
// it stood before this claim, and clears released_at.
func reviveReport(ctx context.Context, tx pgx.Tx, forkRoot uuid.UUID, replica uint64, ttl time.Duration) (bool, error) {
	idArg := strconv.FormatUint(replica, 10)
	var reportedAt time.Time
	var releasedAt *time.Time
	var resyncPending bool
	err := tx.QueryRow(ctx, `
		SELECT reported_at, released_at, resync_pending
		FROM replica_reports
		WHERE fork_root = $1 AND replica_id = $2::numeric
		FOR UPDATE`, forkRoot, idArg).Scan(&reportedAt, &releasedAt, &resyncPending)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock claim report: %w", err)
	}
	if releasedAt != nil {
		if _, err := tx.Exec(ctx, `
			UPDATE replica_reports SET released_at = NULL
			WHERE fork_root = $1 AND replica_id = $2::numeric`, forkRoot, idArg); err != nil {
			return false, fmt.Errorf("revive replica: %w", err)
		}
	}
	return resyncPending || docgc.ReportLapsed(reportedAt, releasedAt, ttl, time.Now()), nil
}

func writeEmptyReport(ctx context.Context, tx pgx.Tx, forkRoot uuid.UUID, replica uint64) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO replica_reports (fork_root, replica_id, vector, home_seq, reported_at)
		VALUES ($1, $2::numeric, '{}'::jsonb, 0, $3)
		ON CONFLICT DO NOTHING`, forkRoot, strconv.FormatUint(replica, 10), time.Now()); err != nil {
		return fmt.Errorf("insert claim report: %w", err)
	}
	return nil
}

func writeSystemReport(ctx context.Context, tx pgx.Tx, documentID, forkRoot uuid.UUID, replica uint64) error {
	vector, seq, err := loadDocumentVector(ctx, tx, documentID, true)
	if err != nil {
		return err
	}
	body, err := marshalVector(vector)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO replica_reports (fork_root, replica_id, vector, home_seq, reported_at)
		VALUES ($1, $2::numeric, $3::jsonb, $4, $5)
		ON CONFLICT DO NOTHING`, forkRoot, strconv.FormatUint(replica, 10), body, int64(seq), time.Now()); err != nil {
		return fmt.Errorf("insert system report: %w", err)
	}
	return nil
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

func claimReplica(ctx context.Context, tx pgx.Tx, documentID, forkRoot uuid.UUID, client uint64, kind, userID string) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO replicas (id, fork_root, document_id, kind, user_id)
		VALUES ($1::numeric, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`, strconv.FormatUint(client, 10), forkRoot, documentID, kind, userArg(userID))
	if err != nil {
		return false, fmt.Errorf("insert claimed replica: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
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
		return false, fmt.Errorf("load replica home: %w", err)
	}
	if home != documentID || existing == "system" && kind != "system" {
		return false, domain.ErrReplicaHomedElsewhere
	}
	if kind == "system" || existing == "system" {
		return false, nil
	}
	return false, bindReplicaUser(ctx, tx, forkRoot, client, userID, existingUser)
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
