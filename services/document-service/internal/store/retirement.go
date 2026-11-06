package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	docgc "github.com/rowjay007/collab-sphere/services/document-service/internal/gc"
)

func (s *Store) ReplicaReportLapsed(ctx context.Context, forkRoot uuid.UUID, replica uint64, ttl time.Duration) (bool, error) {
	if replica == 0 {
		return false, nil
	}
	var reportedAt time.Time
	var releasedAt *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT reported_at, released_at
		FROM replica_reports
		WHERE fork_root = $1 AND replica_id = $2::numeric`,
		forkRoot, strconv.FormatUint(replica, 10)).Scan(&reportedAt, &releasedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load report lapse: %w", err)
	}
	return docgc.ReportLapsed(reportedAt, releasedAt, ttl, time.Now()), nil
}

func (s *Store) ForkTreeForRetirement(ctx context.Context, forkRoot uuid.UUID, replica uint64) (homeArchived bool, tree []docgc.TreeDoc, err error) {
	rows, err := s.pool.Query(ctx, `
		SELECT status, vector
		FROM documents
		WHERE fork_root = $1`, forkRoot)
	if err != nil {
		return false, nil, fmt.Errorf("list fork tree: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var raw []byte
		if err := rows.Scan(&status, &raw); err != nil {
			return false, nil, err
		}
		item := docgc.TreeDoc{Archived: domain.Status(status) == domain.StatusArchived}
		if len(raw) > 0 {
			vec, err := unmarshalVector(raw)
			if err != nil {
				return false, nil, err
			}
			item.Vector = vec
			item.VectorOK = true
		}
		tree = append(tree, item)
	}
	if err := rows.Err(); err != nil {
		return false, nil, err
	}
	var homeStatus string
	err = s.pool.QueryRow(ctx, `
		SELECT d.status
		FROM replicas r
		JOIN documents d ON d.id = r.document_id
	 WHERE r.fork_root = $1 AND r.id = $2::numeric`,
		forkRoot, strconv.FormatUint(replica, 10)).Scan(&homeStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, tree, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("replica home: %w", err)
	}
	return domain.Status(homeStatus) == domain.StatusArchived, tree, nil
}
