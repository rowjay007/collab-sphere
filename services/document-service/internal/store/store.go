package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
)

type Store struct {
	pool *pgxpool.Pool

	compactionMu sync.Mutex
	compacting   map[uuid.UUID]bool
	closed       bool
	closeCh      chan struct{}
	compactionWG sync.WaitGroup
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{
		pool:       pool,
		compacting: make(map[uuid.UUID]bool),
		closeCh:    make(chan struct{}),
	}
}

const compactionTimeout = 30 * time.Second

var compactionRetryDelays = [...]time.Duration{
	100 * time.Millisecond,
	400 * time.Millisecond,
	1600 * time.Millisecond,
}

func (s *Store) Close() {
	s.compactionMu.Lock()
	if !s.closed {
		s.closed = true
		close(s.closeCh)
	}
	s.compactionMu.Unlock()
	s.compactionWG.Wait()
}

func (s *Store) triggerCompaction(id uuid.UUID, snapshotSeq uint64) {
	s.compactionMu.Lock()
	defer s.compactionMu.Unlock()
	if s.closed {
		return
	}
	if _, ok := s.compacting[id]; ok {
		s.compacting[id] = true
		return
	}
	s.compacting[id] = false
	s.compactionWG.Add(1)
	go s.compactInBackground(id, snapshotSeq)
}

func (s *Store) compactInBackground(id uuid.UUID, snapshotSeq uint64) {
	defer s.compactionWG.Done()
	for {
		next, err := retryCompaction(s.closeCh, func() (uint64, error) {
			ctx, cancel := context.WithTimeout(context.Background(), compactionTimeout)
			defer cancel()
			return s.Compact(ctx, id)
		})
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Warn("compact document", "document_id", id, "err", err)
			}
			s.finishCompaction(id)
			return
		}
		if next != snapshotSeq {
			snapshotSeq = next
			select {
			case <-s.closeCh:
				s.finishCompaction(id)
				return
			default:
			}
			continue
		}

		s.compactionMu.Lock()
		if s.compacting[id] {
			s.compacting[id] = false
			s.compactionMu.Unlock()
			continue
		}
		delete(s.compacting, id)
		s.compactionMu.Unlock()
		return
	}
}

func retryCompaction(closeCh <-chan struct{}, compact func() (uint64, error)) (uint64, error) {
	next, err := compact()
	if err == nil {
		return next, nil
	}
	for _, delay := range compactionRetryDelays {
		timer := time.NewTimer(delay)
		select {
		case <-closeCh:
			if !timer.Stop() {
				<-timer.C
			}
			return 0, context.Canceled
		case <-timer.C:
		}
		next, err = compact()
		if err == nil {
			return next, nil
		}
	}
	return next, err
}

func (s *Store) finishCompaction(id uuid.UUID) {
	s.compactionMu.Lock()
	delete(s.compacting, id)
	s.compactionMu.Unlock()
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
