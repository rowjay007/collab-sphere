package perms

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const uniqueViolation = "23505"

func isUnique(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolation
}

type store struct {
	pool *pgxpool.Pool
}

type Grant struct {
	DocID  uuid.UUID
	UserID string
	Role   Role
}

type Link struct {
	ID        uuid.UUID
	DocID     uuid.UUID
	Role      Role
	ExpiresAt time.Time
	RevokedAt *time.Time
	CreatedBy string
}

type Policy struct {
	Members    map[string]Role
	WriteEpoch uint32
	EpochState string
}

func (s *store) grant(ctx context.Context, doc uuid.UUID, user string) (Grant, error) {
	var role string
	err := s.pool.QueryRow(ctx, `SELECT role FROM grants WHERE doc_id = $1 AND user_id = $2`, doc, user).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return Grant{}, ErrNotFound
	}
	if err != nil {
		return Grant{}, err
	}
	r, ok := parseRole(role)
	if !ok {
		return Grant{}, ErrInvalidArgument
	}
	return Grant{DocID: doc, UserID: user, Role: r}, nil
}

func (s *store) insertOwner(ctx context.Context, doc uuid.UUID, user string, encrypted bool) (Grant, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Grant{}, err
	}
	defer tx.Rollback(ctx)

	var existing string
	err = tx.QueryRow(ctx, `SELECT user_id FROM grants WHERE doc_id = $1 AND role = 'owner'`, doc).Scan(&existing)
	switch {
	case err == nil && existing == user:
		if encrypted {
			if err := ensureEpochPending(ctx, tx, doc); err != nil {
				return Grant{}, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return Grant{}, err
		}
		return Grant{DocID: doc, UserID: user, Role: RoleOwner}, nil
	case err == nil:
		return Grant{}, ErrAlreadyExists
	case !errors.Is(err, pgx.ErrNoRows):
		return Grant{}, err
	}

	_, err = tx.Exec(ctx, `INSERT INTO grants (doc_id, user_id, role) VALUES ($1, $2, 'owner')`, doc, user)
	if isUnique(err) {
		return Grant{}, ErrAlreadyExists
	}
	if err != nil {
		return Grant{}, err
	}
	if encrypted {
		if err := ensureEpochPending(ctx, tx, doc); err != nil {
			return Grant{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Grant{}, err
	}
	return Grant{DocID: doc, UserID: user, Role: RoleOwner}, nil
}

func ensureEpochPending(ctx context.Context, tx pgx.Tx, doc uuid.UUID) error {
	_, err := tx.Exec(ctx, `INSERT INTO doc_epochs (doc_id, epoch, state) VALUES ($1, 1, 'pending') ON CONFLICT DO NOTHING`, doc)
	return err
}

func (s *store) upsertGrant(ctx context.Context, doc uuid.UUID, user string, role Role) (Grant, error) {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO grants (doc_id, user_id, role) VALUES ($1, $2, $3)
		ON CONFLICT (doc_id, user_id) DO UPDATE SET role = EXCLUDED.role
		WHERE grants.role <> 'owner'`, doc, user, string(role))
	if err != nil {
		return Grant{}, err
	}
	return s.grant(ctx, doc, user)
}

func (s *store) deleteGrant(ctx context.Context, doc uuid.UUID, user string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM grants WHERE doc_id = $1 AND user_id = $2 AND role <> 'owner'`, doc, user)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *store) insertLink(ctx context.Context, l Link) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO share_links (id, doc_id, role, expires_at, created_by)
		VALUES ($1, $2, $3, $4, $5)`, l.ID, l.DocID, string(l.Role), l.ExpiresAt, l.CreatedBy)
	return err
}

func (s *store) link(ctx context.Context, id uuid.UUID) (Link, error) {
	var l Link
	var role string
	err := s.pool.QueryRow(ctx, `
		SELECT id, doc_id, role, expires_at, revoked_at, created_by
		FROM share_links WHERE id = $1`, id).Scan(&l.ID, &l.DocID, &role, &l.ExpiresAt, &l.RevokedAt, &l.CreatedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return Link{}, ErrNotFound
	}
	if err != nil {
		return Link{}, err
	}
	r, ok := parseRole(role)
	if !ok {
		return Link{}, ErrLinkBad
	}
	l.Role = r
	return l, nil
}

func (s *store) revokeLink(ctx context.Context, id uuid.UUID) (Link, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE share_links SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return Link{}, err
	}
	if tag.RowsAffected() == 0 {
		l, err := s.link(ctx, id)
		if err != nil {
			return Link{}, err
		}
		if l.RevokedAt == nil {
			return Link{}, ErrNotFound
		}
		return l, nil
	}
	return s.link(ctx, id)
}

func (s *store) redeem(ctx context.Context, doc uuid.UUID, user string, role Role, source uuid.UUID) (Grant, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Grant{}, err
	}
	defer tx.Rollback(ctx)

	var existing string
	err = tx.QueryRow(ctx, `SELECT role FROM grants WHERE doc_id = $1 AND user_id = $2`, doc, user).Scan(&existing)
	switch {
	case err == nil:
		cur, ok := parseRole(existing)
		if !ok {
			return Grant{}, ErrInvalidArgument
		}
		if cur.rank() >= role.rank() {
			if err := tx.Commit(ctx); err != nil {
				return Grant{}, err
			}
			return Grant{DocID: doc, UserID: user, Role: cur}, nil
		}
		_, err = tx.Exec(ctx, `UPDATE grants SET role = $3, source_link = $4 WHERE doc_id = $1 AND user_id = $2`, doc, user, string(role), source)
		if err != nil {
			return Grant{}, err
		}
	case errors.Is(err, pgx.ErrNoRows):
		_, err = tx.Exec(ctx, `INSERT INTO grants (doc_id, user_id, role, source_link) VALUES ($1, $2, $3, $4)`, doc, user, string(role), source)
		if isUnique(err) {
			_ = tx.Rollback(ctx)
			return s.redeem(ctx, doc, user, role, source)
		}
		if err != nil {
			return Grant{}, err
		}
	default:
		return Grant{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Grant{}, err
	}
	return Grant{DocID: doc, UserID: user, Role: role}, nil
}

func (s *store) copyGrants(ctx context.Context, src, dst uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO grants (doc_id, user_id, role)
		SELECT $2, user_id, role FROM grants WHERE doc_id = $1
		ON CONFLICT (doc_id, user_id) DO NOTHING`, src, dst)
	return err
}

func (s *store) policy(ctx context.Context, doc uuid.UUID) (Policy, error) {
	rows, err := s.pool.Query(ctx, `SELECT user_id, role FROM grants WHERE doc_id = $1`, doc)
	if err != nil {
		return Policy{}, err
	}
	defer rows.Close()
	members := map[string]Role{}
	for rows.Next() {
		var user, role string
		if err := rows.Scan(&user, &role); err != nil {
			return Policy{}, err
		}
		r, ok := parseRole(role)
		if !ok {
			return Policy{}, ErrInvalidArgument
		}
		members[user] = r
	}
	if err := rows.Err(); err != nil {
		return Policy{}, err
	}
	var epoch int32
	var state string
	err = s.pool.QueryRow(ctx, `SELECT epoch, state FROM doc_epochs WHERE doc_id = $1 ORDER BY epoch DESC LIMIT 1`, doc).Scan(&epoch, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return Policy{Members: members}, nil
	}
	if err != nil {
		return Policy{}, err
	}
	return Policy{Members: members, WriteEpoch: uint32(epoch), EpochState: state}, nil
}
