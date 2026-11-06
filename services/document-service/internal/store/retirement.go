package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
	docgc "github.com/rowjay007/collab-sphere/services/document-service/internal/gc"
)

// UnarchivedForkTree returns the ids of every unarchived document in the tree.
func (s *Store) UnarchivedForkTree(ctx context.Context, forkRoot uuid.UUID) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM documents
		WHERE fork_root = $1 AND status <> $2
		ORDER BY id`, forkRoot, domain.StatusArchived)
	if err != nil {
		return nil, fmt.Errorf("list unarchived fork tree: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
}

// ForkTreeForRetirement returns the replica's home document and every document
// in the tree. A replica with no replicas row gets a zero home.
func (s *Store) ForkTreeForRetirement(ctx context.Context, forkRoot uuid.UUID, replica uint64) (home docgc.TreeDoc, tree []docgc.TreeDoc, err error) {
	rows, err := s.pool.Query(ctx, `
		SELECT status, vector
		FROM documents
		WHERE fork_root = $1`, forkRoot)
	if err != nil {
		return home, nil, fmt.Errorf("list fork tree: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var raw []byte
		if err := rows.Scan(&status, &raw); err != nil {
			return home, nil, err
		}
		item, err := treeDoc(status, raw)
		if err != nil {
			return home, nil, err
		}
		tree = append(tree, item)
	}
	if err := rows.Err(); err != nil {
		return home, nil, err
	}
	var status string
	var raw []byte
	err = s.pool.QueryRow(ctx, `
		SELECT d.status, d.vector
		FROM replicas r
		JOIN documents d ON d.id = r.document_id
		WHERE r.fork_root = $1 AND r.id = $2::numeric`,
		forkRoot, strconv.FormatUint(replica, 10)).Scan(&status, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return home, tree, nil
	}
	if err != nil {
		return home, nil, fmt.Errorf("replica home: %w", err)
	}
	home, err = treeDoc(status, raw)
	if err != nil {
		return home, nil, err
	}
	return home, tree, nil
}

func treeDoc(status string, raw []byte) (docgc.TreeDoc, error) {
	item := docgc.TreeDoc{Archived: domain.Status(status) == domain.StatusArchived}
	if len(raw) == 0 {
		return item, nil
	}
	vec, err := unmarshalVector(raw)
	if err != nil {
		return item, err
	}
	item.Vector = vec
	item.VectorOK = true
	return item, nil
}
