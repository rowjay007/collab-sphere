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

const documentColumns = `id, title, owner_id, status, op_seq, snapshot_seq, created_at, updated_at, fork_of, fork_seq, fork_root, encryption, write_epoch, epoch_state`

const (
	defaultChangedLimit = 500
	maxChangedLimit     = 5000
)

func (s *Store) Create(ctx context.Context, title, ownerID string) (domain.Document, error) {
	return s.CreateWithEncryption(ctx, title, ownerID, false)
}

func (s *Store) CreateWithEncryption(ctx context.Context, title, ownerID string, encrypted bool) (domain.Document, error) {
	if title == "" || ownerID == "" {
		return domain.Document{}, fmt.Errorf("%w: title and owner_id are required", domain.ErrInvalidArgument)
	}
	encryption := domain.EncryptionPlain
	var writeEpoch int
	epochState := ""
	if encrypted {
		encryption = domain.EncryptionEncrypted
		writeEpoch = 1
		epochState = domain.EpochPending
	}
	id := uuid.New()
	row := s.pool.QueryRow(ctx, `
		INSERT INTO documents (id, title, owner_id, fork_root, encryption, write_epoch, epoch_state)
		VALUES ($1, $2, $3, $1, $4, $5, $6)
		RETURNING `+documentColumns, id, title, ownerID, encryption, writeEpoch, epochState)
	return scanDocument(row)
}

func (s *Store) SetEpoch(ctx context.Context, id uuid.UUID, epoch uint32, state string) (domain.Document, error) {
	if epoch == 0 || (state != domain.EpochPending && state != domain.EpochOpen) {
		return domain.Document{}, fmt.Errorf("%w: epoch", domain.ErrInvalidArgument)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Document{}, fmt.Errorf("begin set epoch: %w", err)
	}
	defer tx.Rollback(ctx)
	doc, err := lockDocument(ctx, tx, id)
	if err != nil {
		return domain.Document{}, err
	}
	if doc.Encryption != domain.EncryptionEncrypted {
		return domain.Document{}, fmt.Errorf("%w: document is plain", domain.ErrInvalidArgument)
	}
	if doc.WriteEpoch == epoch && doc.EpochState == state {
		if err := tx.Commit(ctx); err != nil {
			return domain.Document{}, err
		}
		return doc, nil
	}
	if epoch < doc.WriteEpoch || (epoch == doc.WriteEpoch && !(doc.EpochState == domain.EpochPending && state == domain.EpochOpen)) {
		return domain.Document{}, domain.ErrEpoch
	}
	row := tx.QueryRow(ctx, `
		UPDATE documents
		SET write_epoch = $2, epoch_state = $3, updated_at = now()
		WHERE id = $1
		RETURNING `+documentColumns, id, int(epoch), state)
	doc, err = scanDocument(row)
	if err != nil {
		return domain.Document{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Document{}, fmt.Errorf("commit set epoch: %w", err)
	}
	return doc, nil
}

func (s *Store) Delete(ctx context.Context, id uuid.UUID) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM documents WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete document: %w", err)
	}
	return nil
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

func (s *Store) ListChanged(ctx context.Context, since time.Time, afterID uuid.UUID, limit int) ([]domain.Document, time.Time, uuid.UUID, error) {
	if limit < 0 {
		return nil, time.Time{}, uuid.Nil, fmt.Errorf("%w: limit %d", domain.ErrInvalidArgument, limit)
	}
	if limit == 0 {
		limit = defaultChangedLimit
	}
	if limit > maxChangedLimit {
		return nil, time.Time{}, uuid.Nil, fmt.Errorf("%w: limit %d", domain.ErrInvalidArgument, limit)
	}

	var (
		rows pgx.Rows
		err  error
	)
	if afterID != uuid.Nil {
		rows, err = s.pool.Query(ctx, `
			SELECT `+documentColumns+`
			FROM documents
			WHERE status = $1 AND (updated_at, id) > ($2, $3)
			ORDER BY updated_at, id
			LIMIT $4`, domain.StatusActive, since, afterID, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT `+documentColumns+`
			FROM documents
			WHERE status = $1 AND updated_at >= $2
			ORDER BY updated_at, id
			LIMIT $3`, domain.StatusActive, since, limit)
	}
	if err != nil {
		return nil, time.Time{}, uuid.Nil, fmt.Errorf("list changed: %w", err)
	}
	defer rows.Close()

	docs := make([]domain.Document, 0)
	for rows.Next() {
		doc, err := scanDocument(rows)
		if err != nil {
			return nil, time.Time{}, uuid.Nil, err
		}
		docs = append(docs, doc)
	}
	if err := rows.Err(); err != nil {
		return nil, time.Time{}, uuid.Nil, err
	}
	next := since
	nextAfter := uuid.Nil
	if len(docs) > 0 {
		next = docs[len(docs)-1].UpdatedAt
		nextAfter = docs[len(docs)-1].ID
	}
	return docs, next, nextAfter, nil
}

func scanDocument(row pgx.Row) (domain.Document, error) {
	var (
		doc            domain.Document
		status         string
		opSeq, snapSeq int64
		forkOf         *uuid.UUID
		forkSeq        *int64
		writeEpoch     int32
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
		&forkOf,
		&forkSeq,
		&doc.ForkRoot,
		&doc.Encryption,
		&writeEpoch,
		&doc.EpochState,
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
	doc.WriteEpoch = uint32(writeEpoch)
	doc.ForkOf = forkOf
	if forkSeq != nil {
		doc.ForkSeq = uint64(*forkSeq)
	}
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
