package hub

import (
	"context"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/access"
)

func (h *Hub) admit(p *Peer) error {
	if h.registry.Policy() == nil {
		return nil
	}
	if p.User() == "" {
		return errNoGrant
	}
	h.loadMembers()
	role, ok := h.members[p.User()]
	if !ok {
		h.loadMembers()
		role, ok = h.members[p.User()]
	}
	if !ok {
		if h.policyOK.IsZero() {
			return errPolicyDown
		}
		return errNoGrant
	}
	if access.Rank(role) < access.Rank(permissionv1.Role_ROLE_VIEWER) {
		return errNoGrant
	}
	if !h.policyFresh() {
		return errPolicyDown
	}
	return nil
}

func (h *Hub) pollPolicy() {
	if h.dead || h.registry.Policy() == nil || len(h.peers) == 0 {
		return
	}
	h.loadMembers()
	if !h.policyFresh() {
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "permission service unavailable")
		return
	}
	if !h.policyOK.IsZero() && time.Since(h.policyOK) < policyPeriod*2 {
		h.revokeAbsent()
	}
}

func (h *Hub) loadMembers() {
	if h.policyNeed.IsZero() {
		h.policyNeed = time.Now()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	roles, epoch, state, err := h.registry.Policy().Dump(ctx, h.docID)
	if err != nil {
		return
	}
	if roles == nil {
		roles = map[string]permissionv1.Role{}
	}
	h.members = roles
	h.writeEpoch = epoch
	h.epochState = state
	h.policyOK = time.Now()
}

func (h *Hub) policyFresh() bool {
	if !h.policyOK.IsZero() {
		return time.Since(h.policyOK) <= policyStale
	}
	if h.policyNeed.IsZero() {
		return true
	}
	return time.Since(h.policyNeed) <= policyStale
}

func (h *Hub) revokeAbsent() {
	for _, p := range h.peers {
		if p.User() == "" {
			continue
		}
		if _, ok := h.members[p.User()]; !ok {
			p.TrySend(EncodeError(collabv1.ErrorMessage_CODE_REVOKED, "access revoked"))
			p.CloseWithReason("revoked")
			h.dropPeer(p)
		}
	}
}

func (h *Hub) gateWrite(p *Peer) bool {
	if h.registry.Policy() == nil {
		return true
	}
	if !h.policyFresh() {
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "permission service unavailable")
		return false
	}
	role, ok := h.members[p.User()]
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
	if !h.policyFresh() {
		h.failStop(collabv1.ErrorMessage_CODE_UNAVAILABLE, "permission service unavailable")
		return false
	}
	if _, ok := h.members[p.User()]; !ok {
		p.TrySend(EncodeError(collabv1.ErrorMessage_CODE_REVOKED, "access revoked"))
		p.CloseWithReason("revoked")
		h.dropPeer(p)
		return false
	}
	return true
}
