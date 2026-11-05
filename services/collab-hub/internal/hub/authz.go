package hub

import (
	"errors"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/access"
)

func (h *Hub) admitFresh(p *Peer, asOf time.Time) error {
	if h.registry.Policy() == nil {
		return nil
	}
	if snap := h.policy.Load(); h.admitSnap(p, snap) == nil {
		return nil
	}
	snap, err := h.waitPolicy(asOf)
	if err != nil {
		return err
	}
	return h.admitSnap(p, snap)
}

func (h *Hub) admit(p *Peer) error {
	if h.registry.Policy() == nil {
		return nil
	}
	return h.admitSnap(p, h.policy.Load())
}

// deny sends the admission error frame. ErrStopped has no frame: the caller retries or the handler writes one.
func (h *Hub) deny(p *Peer, err error) {
	if err == nil || errors.Is(err, ErrStopped) {
		return
	}
	code := collabv1.ErrorMessage_CODE_REVOKED
	if errors.Is(err, errPolicyDown) || errors.Is(err, ErrUnavailable) {
		code = collabv1.ErrorMessage_CODE_UNAVAILABLE
	}
	p.TrySend(EncodeError(code, err.Error()))
}

func (h *Hub) admitSnap(p *Peer, snap *policyView) error {
	if p.User() == "" {
		return errNoGrant
	}
	if !h.policyFresh(snap) {
		return errPolicyDown
	}
	role, ok := snap.members[p.User()]
	if !ok {
		return errNoGrant
	}
	if access.Rank(role) < access.Rank(permissionv1.Role_ROLE_VIEWER) {
		return errNoGrant
	}
	return nil
}

func (h *Hub) gateWrite(p *Peer) bool {
	if h.registry.Policy() == nil {
		return true
	}
	snap := h.policy.Load()
	if !h.policyFresh(snap) {
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "permission service unavailable")
		return false
	}
	role, ok := snap.members[p.User()]
	if !ok {
		p.TrySend(EncodeError(collabv1.ErrorMessage_CODE_REVOKED, "access revoked"))
		p.CloseWithReason("revoked")
		h.dropPeer(p)
		return false
	}
	if access.Rank(role) < access.Rank(permissionv1.Role_ROLE_EDITOR) {
		p.TrySend(EncodeError(collabv1.ErrorMessage_CODE_FORBIDDEN, "forbidden"))
		return false
	}
	return true
}

func (h *Hub) gatePresence(p *Peer) bool {
	if h.registry.Policy() == nil {
		return true
	}
	snap := h.policy.Load()
	if !h.policyFresh(snap) {
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "permission service unavailable")
		return false
	}
	if _, ok := snap.members[p.User()]; !ok {
		p.TrySend(EncodeError(collabv1.ErrorMessage_CODE_REVOKED, "access revoked"))
		p.CloseWithReason("revoked")
		h.dropPeer(p)
		return false
	}
	return true
}
