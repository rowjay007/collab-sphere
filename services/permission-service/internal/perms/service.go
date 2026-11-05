package perms

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rowjay007/collab-sphere/pkg/auth"
)

const (
	minLinkTTL = time.Minute
	maxLinkTTL = 30 * 24 * time.Hour
)

type Epochs interface {
	SetEpoch(ctx context.Context, doc uuid.UUID, epoch uint32, state string) error
}

type Service struct {
	db        *store
	sharePriv ed25519.PrivateKey
	sharePub  ed25519.PublicKey
	now       func() time.Time
	epochs    Epochs
}

func New(pool *pgxpool.Pool, sharePriv ed25519.PrivateKey) *Service {
	return &Service{
		db:        &store{pool: pool},
		sharePriv: sharePriv,
		sharePub:  sharePriv.Public().(ed25519.PublicKey),
		now:       time.Now,
	}
}

func (s *Service) UseEpochs(epochs Epochs) { s.epochs = epochs }

func (s *Service) EnsureOwner(ctx context.Context, doc uuid.UUID, user string) (Grant, error) {
	return s.EnsureDocument(ctx, doc, user, false)
}

func (s *Service) EnsureDocument(ctx context.Context, doc uuid.UUID, user string, encrypted bool) (Grant, error) {
	if !auth.ValidHandle(user) {
		return Grant{}, fmt.Errorf("%w: user id", ErrInvalidArgument)
	}
	g, err := s.db.insertOwner(ctx, doc, user, encrypted)
	if err != nil || !encrypted {
		return g, err
	}
	_ = s.pushCurrent(ctx, doc)
	return g, nil
}

func (s *Service) Grant(ctx context.Context, actor string, doc uuid.UUID, user string, role Role) (Grant, error) {
	if err := s.requireOwner(ctx, actor, doc); err != nil {
		return Grant{}, err
	}
	if !auth.ValidHandle(user) {
		return Grant{}, fmt.Errorf("%w: user id", ErrInvalidArgument)
	}
	if role == RoleOwner || role.rank() == 0 {
		return Grant{}, fmt.Errorf("%w: role", ErrInvalidArgument)
	}
	existing, err := s.db.grant(ctx, doc, user)
	if err == nil && existing.Role == RoleOwner {
		return Grant{}, ErrAlreadyExists
	}
	if err != nil && err != ErrNotFound {
		return Grant{}, err
	}
	return s.db.grantMember(ctx, doc, user, role, nil)
}

func (s *Service) GrantWithWraps(ctx context.Context, actor string, doc uuid.UUID, user string, role Role, wraps []KeyWrap) (Grant, error) {
	if err := s.requireOwner(ctx, actor, doc); err != nil {
		return Grant{}, err
	}
	if !auth.ValidHandle(user) {
		return Grant{}, fmt.Errorf("%w: user id", ErrInvalidArgument)
	}
	if role == RoleOwner || role.rank() == 0 {
		return Grant{}, fmt.Errorf("%w: role", ErrInvalidArgument)
	}
	existing, err := s.db.grant(ctx, doc, user)
	if err == nil && existing.Role == RoleOwner {
		return Grant{}, ErrAlreadyExists
	}
	if err != nil && err != ErrNotFound {
		return Grant{}, err
	}
	return s.db.grantMember(ctx, doc, user, role, wraps)
}

func (s *Service) Revoke(ctx context.Context, actor string, doc uuid.UUID, user string) error {
	if err := s.requireOwner(ctx, actor, doc); err != nil {
		return err
	}
	if !auth.ValidHandle(user) {
		return fmt.Errorf("%w: user id", ErrInvalidArgument)
	}
	rot, err := s.db.revokeMember(ctx, doc, user)
	if err != nil {
		if err == ErrNotFound {
			_ = s.pushCurrent(ctx, doc)
		}
		return err
	}
	if rot.Rotated {
		_ = s.pushCurrent(ctx, doc)
	}
	return nil
}

func (s *Service) CreateLink(ctx context.Context, actor string, doc uuid.UUID, role Role, exp time.Time) (Link, string, error) {
	if err := s.requireOwner(ctx, actor, doc); err != nil {
		return Link{}, "", err
	}
	actorGrant, err := s.db.grant(ctx, doc, actor)
	if err != nil {
		return Link{}, "", err
	}
	if !role.linkOK() {
		return Link{}, "", fmt.Errorf("%w: role", ErrInvalidArgument)
	}
	if role.rank() > actorGrant.Role.rank() {
		return Link{}, "", fmt.Errorf("%w: role exceeds creator", ErrInvalidArgument)
	}
	now := s.now()
	ttl := exp.Sub(now)
	if ttl < minLinkTTL || ttl > maxLinkTTL {
		return Link{}, "", fmt.Errorf("%w: expiry", ErrInvalidArgument)
	}
	id := uuid.New()
	l := Link{ID: id, DocID: doc, Role: role, ExpiresAt: exp.UTC(), CreatedBy: actor}
	if err := s.db.insertLink(ctx, l); err != nil {
		return Link{}, "", err
	}
	tok, err := signLink(s.sharePriv, id, doc, role, exp.UTC())
	if err != nil {
		return Link{}, "", err
	}
	return l, tok, nil
}

func (s *Service) RevokeLink(ctx context.Context, actor string, id uuid.UUID) (Link, error) {
	l, err := s.db.link(ctx, id)
	if err != nil {
		return Link{}, err
	}
	if err := s.requireOwner(ctx, actor, l.DocID); err != nil {
		return Link{}, err
	}
	return s.db.revokeLink(ctx, id)
}

func (s *Service) Redeem(ctx context.Context, actor string, token string) (Grant, error) {
	if !auth.ValidHandle(actor) {
		return Grant{}, fmt.Errorf("%w: user id", ErrInvalidArgument)
	}
	id, doc, role, exp, err := parseLink(s.sharePub, token)
	if err != nil {
		return Grant{}, ErrLinkBad
	}
	row, err := s.db.link(ctx, id)
	if err != nil {
		if err == ErrNotFound {
			return Grant{}, ErrLinkBad
		}
		return Grant{}, err
	}
	if row.DocID != doc || row.Role != role || !row.ExpiresAt.UTC().Truncate(time.Second).Equal(exp.UTC().Truncate(time.Second)) {
		return Grant{}, ErrLinkBad
	}
	if row.RevokedAt != nil {
		return Grant{}, ErrLinkRevoked
	}
	if !s.now().Before(row.ExpiresAt) {
		return Grant{}, ErrLinkExpired
	}
	if encrypted, err := s.db.hasEpoch(ctx, doc); err != nil {
		return Grant{}, err
	} else if encrypted {
		return Grant{}, ErrEncrypted
	}
	return s.db.redeem(ctx, doc, actor, role, id)
}

func (s *Service) PolicyDump(ctx context.Context, doc uuid.UUID) (Policy, error) {
	return s.db.policy(ctx, doc)
}

func (s *Service) CopyGrants(ctx context.Context, src, dst uuid.UUID) error {
	if src == dst {
		return nil
	}
	return s.db.copyGrants(ctx, src, dst)
}

func (s *Service) SetNow(now func() time.Time) {
	if now == nil {
		s.now = time.Now
		return
	}
	s.now = now
}

func (s *Service) Lookup(ctx context.Context, doc uuid.UUID, user string) (Grant, error) {
	return s.db.grant(ctx, doc, user)
}

func (s *Service) PutLink(ctx context.Context, l Link) error {
	return s.db.insertLink(ctx, l)
}

func SignLink(priv ed25519.PrivateKey, id, doc uuid.UUID, role Role, exp time.Time) (string, error) {
	return signLink(priv, id, doc, role, exp)
}

func (s *Service) ShareKey() ed25519.PrivateKey { return s.sharePriv }

func (s *Service) requireOwner(ctx context.Context, actor string, doc uuid.UUID) error {
	if !auth.ValidHandle(actor) {
		return fmt.Errorf("%w: actor", ErrInvalidArgument)
	}
	g, err := s.db.grant(ctx, doc, actor)
	if err != nil {
		if err == ErrNotFound {
			return ErrPermissionDenied
		}
		return err
	}
	if g.Role != RoleOwner {
		return ErrPermissionDenied
	}
	return nil
}
