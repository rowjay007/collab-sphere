package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const documentColumns = `id, title, owner_id, status, op_seq, snapshot_seq, created_at, updated_at`

func (s *Store) Create(ctx context.Context, title, ownerID string) (domain.Document, error) {
	if title == "" || ownerID == "" {
		return domain.Document{}, fmt.Errorf("%w: title and owner_id are required", domain.ErrInvalidArgument)
	}
	row := s.pool.QueryRow(ctx, `
		INSERT INTO documents (title, owner_id)
		VALUES ($1, $2)
		RETURNING `+documentColumns, title, ownerID)
	return scanDocument(row)
}

func (s *Store) Get(ctx context.Context, id uuid.UUID) (domain.Document, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+documentColumns+` FROM documents WHERE id = $1`, id)
	doc, err := scanDocument(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Document{}, domain.ErrNotFound
		}
		return domain.Document{}, err
	}
	return doc, nil
}

func (s *Store) Archive(ctx context.Context, id uuid.UUID) (domain.Document, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE documents
		SET status = $2, updated_at = now()
		WHERE id = $1
		RETURNING `+documentColumns, id, domain.StatusArchived)
	doc, err := scanDocument(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Document{}, domain.ErrNotFound
		}
		return domain.Document{}, err
	}
	return doc, nil
}

func scanDocument(row pgx.Row) (domain.Document, error) {
	var (
		doc            domain.Document
		status         string
		opSeq, snapSeq int64
	)
	if err := row.Scan(
		&doc.ID,
		&doc.Title,
		&doc.OwnerID,
		&status,
		&opSeq,
		&snapSeq,
		&doc.CreatedAt,
		&doc.UpdatedAt,
	); err != nil {
		return domain.Document{}, err
	}
	parsed, err := parseStatus(status)
	if err != nil {
		return domain.Document{}, err
	}
	doc.Status = parsed
	doc.OpSeq = uint64(opSeq)
	doc.SnapshotSeq = uint64(snapSeq)
	return doc, nil
}

func parseStatus(s string) (domain.Status, error) {
	switch domain.Status(s) {
	case domain.StatusActive, domain.StatusArchived:
		return domain.Status(s), nil
	default:
		return "", fmt.Errorf("unknown document status %q", s)
	}
}
