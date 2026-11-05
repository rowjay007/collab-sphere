package perms

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rowjay007/collab-sphere/pkg/auth"
)

type PublicKey struct {
	ID     uuid.UUID
	UserID string
	Key    []byte
}

type KeyWrap struct {
	Epoch      uint32
	UserID     string
	KeyID      uuid.UUID
	EphPub     []byte
	Nonce      []byte
	Ciphertext []byte
}

type rotation struct {
	Epoch   uint32
	State   string
	Rotated bool
}

func (s *Service) PutKey(ctx context.Context, user string, pub []byte) (PublicKey, error) {
	if !auth.ValidHandle(user) {
		return PublicKey{}, fmt.Errorf("%w: user id", ErrInvalidArgument)
	}
	if len(pub) != 32 {
		return PublicKey{}, fmt.Errorf("%w: public key", ErrInvalidArgument)
	}
	return s.db.putKey(ctx, user, pub)
}

func (s *Service) ListKeys(ctx context.Context, user string) ([]PublicKey, error) {
	if !auth.ValidHandle(user) {
		return nil, fmt.Errorf("%w: user id", ErrInvalidArgument)
	}
	return s.db.listKeys(ctx, user)
}

func (s *Service) GetWraps(ctx context.Context, user string, doc uuid.UUID) ([]KeyWrap, error) {
	if _, err := s.db.grant(ctx, doc, user); err != nil {
		if err == ErrNotFound {
			return nil, ErrPermissionDenied
		}
		return nil, err
	}
	return s.db.wrapsFor(ctx, doc, user)
}

func (s *Service) PublishWraps(ctx context.Context, actor string, doc uuid.UUID, requestID string, wraps []KeyWrap) (uint32, string, error) {
	if err := s.requireOwner(ctx, actor, doc); err != nil {
		return 0, "", err
	}
	epoch, state, replay, err := s.db.publish(ctx, doc, requestID, wraps)
	if err != nil {
		return 0, "", err
	}
	if err := s.pushEpoch(ctx, doc, epoch, state); err != nil {
		if replay && status.Code(err) == codes.FailedPrecondition {
			return epoch, state, nil
		}
		return 0, "", err
	}
	return epoch, state, nil
}

func (s *Service) pushEpoch(ctx context.Context, doc uuid.UUID, epoch uint32, state string) error {
	if s.epochs == nil {
		return errors.New("permission: epoch client is not configured")
	}
	return s.epochs.SetEpoch(ctx, doc, epoch, state)
}

func (s *Service) repairEpoch(ctx context.Context, doc uuid.UUID) {
	epoch, state, ok, err := s.db.currentEpoch(ctx, doc)
	if err != nil || !ok {
		return
	}
	_ = s.pushEpoch(ctx, doc, epoch, state)
}

func (s *store) putKey(ctx context.Context, user string, pub []byte) (PublicKey, error) {
	var out PublicKey
	err := s.pool.QueryRow(ctx, `
		INSERT INTO user_keys (key_id, user_id, public_key)
		VALUES ($1, $2, $3)
		ON CONFLICT (user_id, public_key) DO UPDATE SET public_key = EXCLUDED.public_key
		RETURNING key_id, user_id, public_key`, uuid.New(), user, pub).Scan(&out.ID, &out.UserID, &out.Key)
	return out, err
}

func (s *store) listKeys(ctx context.Context, user string) ([]PublicKey, error) {
	rows, err := s.pool.Query(ctx, `SELECT key_id, user_id, public_key FROM user_keys WHERE user_id = $1 ORDER BY key_id`, user)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PublicKey
	for rows.Next() {
		var k PublicKey
		if err := rows.Scan(&k.ID, &k.UserID, &k.Key); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (s *store) wrapsFor(ctx context.Context, doc uuid.UUID, user string) ([]KeyWrap, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT epoch, user_id, key_id, eph_pub, nonce, ciphertext
		FROM doc_key_wraps WHERE doc_id = $1 AND user_id = $2 ORDER BY epoch`, doc, user)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWraps(rows)
}

func scanWraps(rows pgx.Rows) ([]KeyWrap, error) {
	var out []KeyWrap
	for rows.Next() {
		var w KeyWrap
		var epoch int32
		if err := rows.Scan(&epoch, &w.UserID, &w.KeyID, &w.EphPub, &w.Nonce, &w.Ciphertext); err != nil {
			return nil, err
		}
		w.Epoch = uint32(epoch)
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *store) hasEpoch(ctx context.Context, doc uuid.UUID) (bool, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM doc_epochs WHERE doc_id = $1`, doc).Scan(&n)
	return n > 0, err
}

func (s *store) currentEpoch(ctx context.Context, doc uuid.UUID) (uint32, string, bool, error) {
	var epoch int32
	var state string
	err := s.pool.QueryRow(ctx, `SELECT epoch, state FROM doc_epochs WHERE doc_id = $1 ORDER BY epoch DESC LIMIT 1`, doc).Scan(&epoch, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, err
	}
	return uint32(epoch), state, true, nil
}

func (s *store) grantMember(ctx context.Context, doc uuid.UUID, user string, role Role, wraps []KeyWrap) (Grant, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Grant{}, err
	}
	defer tx.Rollback(ctx)
	if err := lockOwner(ctx, tx, doc); err != nil {
		return Grant{}, err
	}
	epochs, err := epochList(ctx, tx, doc)
	if err != nil {
		return Grant{}, err
	}
	if len(epochs) == 0 {
		if len(wraps) != 0 {
			return Grant{}, fmt.Errorf("%w: wraps", ErrInvalidArgument)
		}
	} else if err := requireWraps(ctx, tx, user, epochs, wraps); err != nil {
		return Grant{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO grants (doc_id, user_id, role) VALUES ($1, $2, $3)
		ON CONFLICT (doc_id, user_id) DO UPDATE SET role = EXCLUDED.role
		WHERE grants.role <> 'owner'`, doc, user, string(role)); err != nil {
		return Grant{}, err
	}
	for _, w := range wraps {
		if err := insertWrap(ctx, tx, doc, w); err != nil {
			return Grant{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Grant{}, err
	}
	return s.grant(ctx, doc, user)
}

func (s *store) revokeMember(ctx context.Context, doc uuid.UUID, user string) (rotation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return rotation{}, err
	}
	defer tx.Rollback(ctx)
	if err := lockOwner(ctx, tx, doc); err != nil {
		return rotation{}, err
	}
	var role string
	err = tx.QueryRow(ctx, `SELECT role FROM grants WHERE doc_id = $1 AND user_id = $2 FOR UPDATE`, doc, user).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return rotation{}, ErrNotFound
	}
	if err != nil {
		return rotation{}, err
	}
	if role == string(RoleOwner) {
		return rotation{}, ErrOwnerNotRevocable
	}
	if _, err := tx.Exec(ctx, `DELETE FROM grants WHERE doc_id = $1 AND user_id = $2`, doc, user); err != nil {
		return rotation{}, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM doc_key_wraps WHERE doc_id = $1 AND user_id = $2`, doc, user); err != nil {
		return rotation{}, err
	}
	epochs, err := epochList(ctx, tx, doc)
	if err != nil {
		return rotation{}, err
	}
	if len(epochs) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return rotation{}, err
		}
		return rotation{}, nil
	}
	next := epochs[len(epochs)-1] + 1
	if _, err := tx.Exec(ctx, `INSERT INTO doc_epochs (doc_id, epoch, state) VALUES ($1, $2, 'pending')`, doc, next); err != nil {
		return rotation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return rotation{}, err
	}
	return rotation{Epoch: uint32(next), State: "pending", Rotated: true}, nil
}

func (s *store) publish(ctx context.Context, doc uuid.UUID, requestID string, wraps []KeyWrap) (uint32, string, bool, error) {
	if requestID == "" {
		return 0, "", false, fmt.Errorf("%w: request id", ErrInvalidArgument)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, "", false, err
	}
	defer tx.Rollback(ctx)
	var stored int32
	err = tx.QueryRow(ctx, `SELECT epoch FROM wrap_publishes WHERE doc_id = $1 AND request_id = $2`, doc, requestID).Scan(&stored)
	if err == nil {
		var state string
		if err := tx.QueryRow(ctx, `SELECT state FROM doc_epochs WHERE doc_id = $1 AND epoch = $2`, doc, stored).Scan(&state); err != nil {
			return 0, "", false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, "", false, err
		}
		return uint32(stored), state, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, "", false, err
	}
	if err := lockOwner(ctx, tx, doc); err != nil {
		return 0, "", false, err
	}
	epochs, err := epochList(ctx, tx, doc)
	if err != nil {
		return 0, "", false, err
	}
	if len(epochs) == 0 {
		return 0, "", false, fmt.Errorf("%w: document is plain", ErrInvalidArgument)
	}
	epoch := epochs[len(epochs)-1]
	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM doc_epochs WHERE doc_id = $1 AND epoch = $2 FOR UPDATE`, doc, epoch).Scan(&state); err != nil {
		return 0, "", false, err
	}
	if state != "pending" {
		return 0, "", false, ErrAlreadyExists
	}
	members, err := grantUsers(ctx, tx, doc)
	if err != nil {
		return 0, "", false, err
	}
	if len(wraps) != len(members) {
		return 0, "", false, fmt.Errorf("%w: wrap set", ErrInvalidArgument)
	}
	seen := map[string]bool{}
	for _, w := range wraps {
		if int(w.Epoch) != epoch || seen[w.UserID] || !members[w.UserID] {
			return 0, "", false, fmt.Errorf("%w: wrap set", ErrInvalidArgument)
		}
		seen[w.UserID] = true
		if err := validateWrap(ctx, tx, w); err != nil {
			return 0, "", false, err
		}
		if err := insertWrap(ctx, tx, doc, w); err != nil {
			return 0, "", false, err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE doc_epochs SET state = 'open' WHERE doc_id = $1 AND epoch = $2`, doc, epoch); err != nil {
		return 0, "", false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO wrap_publishes (doc_id, request_id, epoch) VALUES ($1, $2, $3)`, doc, requestID, epoch); err != nil {
		return 0, "", false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, "", false, err
	}
	return uint32(epoch), "open", false, nil
}

func lockOwner(ctx context.Context, tx pgx.Tx, doc uuid.UUID) error {
	var user string
	err := tx.QueryRow(ctx, `SELECT user_id FROM grants WHERE doc_id = $1 AND role = 'owner' FOR UPDATE`, doc).Scan(&user)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func epochList(ctx context.Context, tx pgx.Tx, doc uuid.UUID) ([]int, error) {
	rows, err := tx.Query(ctx, `SELECT epoch FROM doc_epochs WHERE doc_id = $1 ORDER BY epoch FOR UPDATE`, doc)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var epoch int
		if err := rows.Scan(&epoch); err != nil {
			return nil, err
		}
		out = append(out, epoch)
	}
	return out, rows.Err()
}

func grantUsers(ctx context.Context, tx pgx.Tx, doc uuid.UUID) (map[string]bool, error) {
	rows, err := tx.Query(ctx, `SELECT user_id FROM grants WHERE doc_id = $1`, doc)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var user string
		if err := rows.Scan(&user); err != nil {
			return nil, err
		}
		out[user] = true
	}
	return out, rows.Err()
}

func requireWraps(ctx context.Context, tx pgx.Tx, user string, epochs []int, wraps []KeyWrap) error {
	if len(wraps) != len(epochs) {
		return fmt.Errorf("%w: wraps", ErrInvalidArgument)
	}
	want := map[int]bool{}
	for i, epoch := range epochs {
		if epoch != i+1 {
			return fmt.Errorf("%w: epochs", ErrInvalidArgument)
		}
		want[epoch] = true
	}
	seen := map[int]bool{}
	for _, w := range wraps {
		if w.UserID != user || !want[int(w.Epoch)] || seen[int(w.Epoch)] {
			return fmt.Errorf("%w: wraps", ErrInvalidArgument)
		}
		seen[int(w.Epoch)] = true
		if err := validateWrap(ctx, tx, w); err != nil {
			return err
		}
	}
	return nil
}

func validateWrap(ctx context.Context, tx pgx.Tx, w KeyWrap) error {
	if len(w.EphPub) != 32 || len(w.Nonce) != 12 || len(w.Ciphertext) != 48 {
		return fmt.Errorf("%w: wrap", ErrInvalidArgument)
	}
	var owner string
	err := tx.QueryRow(ctx, `SELECT user_id FROM user_keys WHERE key_id = $1`, w.KeyID).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && owner != w.UserID) {
		return fmt.Errorf("%w: key", ErrInvalidArgument)
	}
	return err
}

func insertWrap(ctx context.Context, tx pgx.Tx, doc uuid.UUID, w KeyWrap) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO doc_key_wraps (doc_id, epoch, user_id, key_id, eph_pub, nonce, ciphertext)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (doc_id, epoch, user_id) DO UPDATE SET
			key_id = EXCLUDED.key_id,
			eph_pub = EXCLUDED.eph_pub,
			nonce = EXCLUDED.nonce,
			ciphertext = EXCLUDED.ciphertext`,
		doc, int(w.Epoch), w.UserID, w.KeyID, w.EphPub, w.Nonce, w.Ciphertext)
	return err
}
