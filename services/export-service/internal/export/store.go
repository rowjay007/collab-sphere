package export

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) ByRequestID(ctx context.Context, requestID string) (Job, error) {
	return s.one(ctx, `
		SELECT id, request_id, doc_id, version_id, format, user_id, state, attempts, object_key, COALESCE(error, '')
		FROM export_jobs WHERE request_id = $1`, requestID)
}

func (s *Store) ByID(ctx context.Context, id uuid.UUID) (Job, error) {
	return s.one(ctx, `
		SELECT id, request_id, doc_id, version_id, format, user_id, state, attempts, object_key, COALESCE(error, '')
		FROM export_jobs WHERE id = $1`, id)
}

func (s *Store) Insert(ctx context.Context, j Job) (Job, error) {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO export_jobs (id, request_id, doc_id, version_id, format, user_id, state, object_key)
		VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7)
		ON CONFLICT (request_id) DO NOTHING`,
		j.ID, j.RequestID, j.DocID, j.pin(), string(j.Format), j.UserID, j.ObjectKey)
	if err != nil {
		return Job{}, err
	}
	return s.ByRequestID(ctx, j.RequestID)
}

// Claim is one statement. It locks the oldest queued or expired running row,
// increments attempts, and either leases it for 30 seconds or fails it.
// Five running claims leave attempts at 5. The next claim fails that row.
func (s *Store) Claim(ctx context.Context) (Job, bool, error) {
	row := s.pool.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id
			FROM export_jobs
			WHERE state IN ('queued', 'running')
			  AND (lease_until IS NULL OR lease_until <= now())
			  AND attempts <= 5
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE export_jobs AS j
		SET attempts = j.attempts + 1,
		    state = CASE WHEN j.attempts >= 5 THEN 'failed' ELSE 'running' END,
		    lease_until = CASE WHEN j.attempts >= 5 THEN NULL ELSE now() + interval '30 seconds' END,
		    error = CASE WHEN j.attempts >= 5 THEN 'attempts exhausted' ELSE NULL END
		FROM candidate
		WHERE j.id = candidate.id
		RETURNING j.id, j.request_id, j.doc_id, j.version_id, j.format, j.user_id, j.state, j.attempts, j.object_key, COALESCE(j.error, '')`)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	return j, true, nil
}

func (s *Store) CountByState(ctx context.Context) (map[State]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT state, count(*) FROM export_jobs GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[State]int64{}
	for rows.Next() {
		var state string
		var n int64
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		out[State(state)] = n
	}
	return out, rows.Err()
}

func (s *Store) MarkReady(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE export_jobs
		SET state = 'ready', error = NULL, lease_until = NULL
		WHERE id = $1 AND state = 'running'`, id)
	return err
}

func (s *Store) MarkFailed(ctx context.Context, id uuid.UUID, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE export_jobs
		SET state = 'failed', error = $2, lease_until = NULL
		WHERE id = $1 AND state = 'running'`, id, reason)
	return err
}

func (s *Store) one(ctx context.Context, sql string, arg any) (Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, sql, arg))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var pin []byte
	var format, state string
	if err := row.Scan(&j.ID, &j.RequestID, &j.DocID, &pin, &format, &j.UserID, &state, &j.Attempts, &j.ObjectKey, &j.Error); err != nil {
		return Job{}, err
	}
	parsed, err := ParseFormat(format)
	if err != nil {
		return Job{}, fmt.Errorf("export: stored format %q", format)
	}
	switch State(state) {
	case Queued, Running, Ready, Failed:
	default:
		return Job{}, fmt.Errorf("export: stored state %q", state)
	}
	if len(pin) != pinLen {
		return Job{}, fmt.Errorf("export: version pin length %d", len(pin))
	}
	j.Format = parsed
	j.State = State(state)
	j.NodeID = append([]byte(nil), pin[:idLen]...)
	j.ContentHash = append([]byte(nil), pin[idLen:]...)
	return j, nil
}
