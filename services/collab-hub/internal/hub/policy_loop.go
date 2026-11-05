package hub

import (
	"context"
	"maps"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
)

// policyView is one PolicyDump result. freshAt is when the request was sent.
// The poller publishes it by swapping the pointer. The loop never writes it.
type policyView struct {
	members    map[string]permissionv1.Role
	writeEpoch uint32
	epochState string
	freshAt    time.Time
	ok         bool
}

// peerSnap is the loop's peer set, published so the poller can close sockets
// without taking the peer map.
type peerSnap struct {
	peers []*Peer
}

func (h *Hub) pollMembers() {
	for {
		if h.pollDone() {
			return
		}
		h.dumpPolicy()
		if h.stopApply.Load() {
			return
		}
		timer := time.NewTimer(policyPeriod)
		select {
		case <-h.pollStop:
			timer.Stop()
			return
		case <-h.stopped:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (h *Hub) pollDone() bool {
	select {
	case <-h.pollStop:
		return true
	case <-h.stopped:
		return true
	default:
		return false
	}
}

func (h *Hub) dumpPolicy() {
	if h.pollDone() || h.registry.Policy() == nil {
		return
	}
	sent := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type dumpResult struct {
		roles map[string]permissionv1.Role
		epoch uint32
		state string
		err   error
	}
	ch := make(chan dumpResult, 1)
	go func() {
		roles, epoch, state, err := h.registry.Policy().Dump(ctx, h.docID)
		ch <- dumpResult{roles: roles, epoch: epoch, state: state, err: err}
	}()

	var stale <-chan time.Time
	var staleTimer *time.Timer
	if prev := h.policy.Load(); prev != nil && prev.ok {
		remain := policyStale - time.Since(prev.freshAt)
		if remain <= 0 {
			h.markUnavailable()
			h.notePolicy()
			return
		}
		staleTimer = time.NewTimer(remain)
		defer staleTimer.Stop()
		stale = staleTimer.C
	}

	var res dumpResult
	select {
	case <-h.pollStop:
		return
	case <-h.stopped:
		return
	case <-stale:
		h.markUnavailable()
		h.notePolicy()
		return
	case res = <-ch:
	}
	h.attempted.Store(true)
	h.notePolicy()
	if res.err != nil || h.stopApply.Load() {
		if prev := h.policy.Load(); prev != nil && prev.ok && time.Since(prev.freshAt) > policyStale {
			h.markUnavailable()
		}
		return
	}
	if time.Since(sent) > policyStale {
		h.markUnavailable()
		return
	}
	if res.roles == nil {
		res.roles = map[string]permissionv1.Role{}
	}
	view := &policyView{
		members:    maps.Clone(res.roles),
		writeEpoch: res.epoch,
		epochState: res.state,
		freshAt:    sent,
		ok:         true,
	}
	h.policy.Store(view)
	h.closeAbsent(view)
}

func (h *Hub) notePolicy() {
	if h.policyNote == nil {
		return
	}
	select {
	case h.policyNote <- struct{}{}:
	default:
	}
}

func (h *Hub) waitFirstPolicy() {
	timer := time.NewTimer(policyStale)
	defer timer.Stop()
	for {
		if h.policy.Load() != nil || h.attempted.Load() || h.stopApply.Load() {
			return
		}
		select {
		case <-h.policyNote:
		case <-h.stopped:
			return
		case <-timer.C:
			return
		}
	}
}

func (h *Hub) waitPolicy(asOf time.Time) (*policyView, error) {
	timer := time.NewTimer(policyStale)
	defer timer.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if h.stopApply.Load() {
			return nil, errPolicyDown
		}
		select {
		case <-h.stopped:
			return nil, ErrStopped
		default:
		}
		if snap := h.policy.Load(); snap != nil && snap.ok && !snap.freshAt.Before(asOf) && time.Since(snap.freshAt) <= policyStale {
			return snap, nil
		}
		select {
		case <-h.stopped:
			return nil, ErrStopped
		case <-timer.C:
			return nil, errPolicyDown
		case <-h.policyNote:
		case <-tick.C:
		}
	}
}

func (h *Hub) publishPeers() {
	list := make([]*Peer, 0, len(h.peers))
	for _, p := range h.peers {
		list = append(list, p)
	}
	h.peerView.Store(&peerSnap{peers: list})
}

func (h *Hub) closeAbsent(view *policyView) {
	snap := h.peerView.Load()
	if snap == nil || view == nil {
		return
	}
	frame := EncodeError(collabv1.ErrorMessage_CODE_REVOKED, "access revoked")
	for _, p := range snap.peers {
		if p.User() == "" {
			continue
		}
		if _, ok := view.members[p.User()]; ok {
			continue
		}
		p.TrySend(frame)
		p.CloseWithReason("revoked")
	}
}

func (h *Hub) markUnavailable() {
	if !h.stopApply.CompareAndSwap(false, true) {
		return
	}
	frame := EncodeError(collabv1.ErrorMessage_CODE_UNAVAILABLE, "permission service unavailable")
	if snap := h.peerView.Load(); snap != nil {
		for _, p := range snap.peers {
			p.TrySend(frame)
			p.CloseWithReason("permission service unavailable")
		}
	}
	select {
	case h.events <- wakeEvent{}:
	default:
	}
}

func (h *Hub) currentEpoch() uint32 {
	snap := h.policy.Load()
	if snap == nil {
		return 0
	}
	return snap.writeEpoch
}

func (h *Hub) epochOpen() bool {
	snap := h.policy.Load()
	return snap != nil && snap.ok && snap.epochState == "open"
}

func (h *Hub) peerVisible(p *Peer) bool {
	if h.registry.Policy() == nil {
		return true
	}
	snap := h.policy.Load()
	if snap == nil || !snap.ok {
		return false
	}
	_, ok := snap.members[p.User()]
	return ok
}

func (h *Hub) policyFresh(snap *policyView) bool {
	if h.stopApply.Load() || snap == nil || !snap.ok {
		return false
	}
	return time.Since(snap.freshAt) <= policyStale
}
