package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/domain"
)

type Report struct {
	Replica    uint64
	Vector     crdt.VersionVector
	HomeSeq    uint64
	ReportedAt time.Time
	ReleasedAt *time.Time
}

// UpsertReport stores the replica's vector clamped to its home document's
// committed vector. A replica with no replicas row is ErrNotFound.
func (s *Store) UpsertReport(ctx context.Context, forkRoot uuid.UUID, replica uint64, vector crdt.VersionVector, homeSeq uint64, resyncPending bool, at time.Time) error {
	if err := validateReport(replica, homeSeq); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin report: %w", err)
	}
	defer tx.Rollback(ctx)
	var home uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT document_id FROM replicas
		WHERE fork_root = $1 AND id = $2::numeric`,
		forkRoot, strconv.FormatUint(replica, 10)).Scan(&home)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("load report home: %w", err)
	}
	homeVector, _, err := loadDocumentVector(ctx, tx, home, false)
	if err != nil {
		return err
	}
	clamped := crdt.Stable(vector, nil, homeVector)
	if err := upsertReportTx(ctx, tx, forkRoot, replica, clamped, homeSeq, resyncPending, at); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit report: %w", err)
	}
	return nil
}

func (s *Store) ListReports(ctx context.Context, forkRoot uuid.UUID) ([]Report, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT replica_id::text, vector, home_seq, reported_at, released_at
		FROM replica_reports
		WHERE fork_root = $1
		ORDER BY replica_id`, forkRoot)
	if err != nil {
		return nil, fmt.Errorf("list reports: %w", err)
	}
	defer rows.Close()

	out := make([]Report, 0)
	for rows.Next() {
		var (
			idText string
			raw    []byte
			seq    int64
			report Report
		)
		if err := rows.Scan(&idText, &raw, &seq, &report.ReportedAt, &report.ReleasedAt); err != nil {
			return nil, fmt.Errorf("scan report: %w", err)
		}
		replica, err := strconv.ParseUint(idText, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse replica id: %w", err)
		}
		vector, err := unmarshalVector(raw)
		if err != nil {
			return nil, err
		}
		report.Replica = replica
		report.Vector = vector
		report.HomeSeq = uint64(seq)
		out = append(out, report)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) ReleaseReplica(ctx context.Context, documentID uuid.UUID, replica uint64) error {
	if replica == 0 {
		return fmt.Errorf("%w: replica id", domain.ErrInvalidArgument)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin release: %w", err)
	}
	defer tx.Rollback(ctx)

	var forkRoot uuid.UUID
	err = tx.QueryRow(ctx, `SELECT fork_root FROM documents WHERE id = $1`, documentID).Scan(&forkRoot)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("load release document: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE replica_reports
		SET released_at = COALESCE(released_at, now())
		WHERE fork_root = $1 AND replica_id = $2::numeric`,
		forkRoot, strconv.FormatUint(replica, 10))
	if err != nil {
		return fmt.Errorf("release replica: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit release: %w", err)
	}
	return nil
}

// BackfillReports inserts a report for every replica that does not have one.
// The vector is the home document's committed vector, or a replay of its
// snapshot and later ops when that column is still null. An existing row is
// left alone.
func (s *Store) BackfillReports(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin backfill: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT r.fork_root, r.id::text, r.document_id
		FROM replicas r
		WHERE NOT EXISTS (
			SELECT 1 FROM replica_reports rr
			WHERE rr.fork_root = r.fork_root AND rr.replica_id = r.id
		)`)
	if err != nil {
		return fmt.Errorf("list replicas without reports: %w", err)
	}
	type missing struct {
		root    uuid.UUID
		replica uint64
		home    uuid.UUID
	}
	pending := make([]missing, 0)
	for rows.Next() {
		var item missing
		var idText string
		if err := rows.Scan(&item.root, &idText, &item.home); err != nil {
			rows.Close()
			return fmt.Errorf("scan replica: %w", err)
		}
		replica, err := strconv.ParseUint(idText, 10, 64)
		if err != nil {
			rows.Close()
			return fmt.Errorf("parse replica id: %w", err)
		}
		item.replica = replica
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	type stamped struct {
		vector crdt.VersionVector
		seq    uint64
	}
	cache := make(map[uuid.UUID]stamped, len(pending))
	now := time.Now()
	for _, item := range pending {
		stamp, ok := cache[item.home]
		if !ok {
			vector, seq, err := loadDocumentVector(ctx, tx, item.home, false)
			if err != nil {
				return err
			}
			stamp = stamped{vector: vector, seq: seq}
			cache[item.home] = stamp
		}
		body, err := marshalVector(stamp.vector)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO replica_reports (fork_root, replica_id, vector, home_seq, reported_at)
			VALUES ($1, $2::numeric, $3::jsonb, $4, $5)
			ON CONFLICT DO NOTHING`,
			item.root, strconv.FormatUint(item.replica, 10), body, int64(stamp.seq), now); err != nil {
			return fmt.Errorf("backfill report: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit backfill: %w", err)
	}
	return nil
}

func validateReport(replica, homeSeq uint64) error {
	if replica == 0 || homeSeq > math.MaxInt64 {
		return fmt.Errorf("%w: report", domain.ErrInvalidArgument)
	}
	return nil
}

func upsertReportTx(ctx context.Context, tx pgx.Tx, forkRoot uuid.UUID, replica uint64, vector crdt.VersionVector, homeSeq uint64, resyncPending bool, at time.Time) error {
	idArg := strconv.FormatUint(replica, 10)
	body, err := marshalVector(vector)
	if err != nil {
		return err
	}
	var raw []byte
	err = tx.QueryRow(ctx, `
		SELECT vector
		FROM replica_reports
		WHERE fork_root = $1 AND replica_id = $2::numeric
		FOR UPDATE`, forkRoot, idArg).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		tag, insErr := tx.Exec(ctx, `
			INSERT INTO replica_reports (fork_root, replica_id, vector, home_seq, reported_at, resync_pending)
			VALUES ($1, $2::numeric, $3::jsonb, $4, $5, $6)
			ON CONFLICT DO NOTHING`, forkRoot, idArg, body, int64(homeSeq), at, resyncPending)
		if insErr != nil {
			return fmt.Errorf("insert report: %w", insErr)
		}
		if tag.RowsAffected() == 1 {
			return nil
		}
		err = tx.QueryRow(ctx, `
			SELECT vector
			FROM replica_reports
			WHERE fork_root = $1 AND replica_id = $2::numeric
			FOR UPDATE`, forkRoot, idArg).Scan(&raw)
	}
	if err != nil {
		return fmt.Errorf("lock report: %w", err)
	}
	stored, err := unmarshalVector(raw)
	if err != nil {
		return err
	}
	if vectorBelow(stored, vector) {
		return domain.ErrStaleReport
	}
	if _, err := tx.Exec(ctx, `
		UPDATE replica_reports
		SET vector = $3::jsonb, home_seq = $4, reported_at = $5, resync_pending = $6
		WHERE fork_root = $1 AND replica_id = $2::numeric`,
		forkRoot, idArg, body, int64(homeSeq), at, resyncPending); err != nil {
		return fmt.Errorf("update report: %w", err)
	}
	return nil
}
