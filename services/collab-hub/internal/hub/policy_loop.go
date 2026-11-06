package hub

import (
	"context"
	"maps"
	"time"

	collabv1 "github.com/rowjay007/collab-sphere/gen/collab/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
)

type observedPolicy struct {
	next Policy
	dump interface{ Observe(float64) }
}

// ObservePolicy times each batch dump. A nil dump returns p unchanged.
func ObservePolicy(p Policy, dump interface{ Observe(float64) }) Policy {
	if p == nil || dump == nil {
		return p
	}
	return observedPolicy{next: p, dump: dump}
}

func (o observedPolicy) DumpBatch(ctx context.Context, docIDs []string) (map[string]PolicyEntry, error) {
	start := time.Now()
	entries, err := o.next.DumpBatch(ctx, docIDs)
	o.dump.Observe(time.Since(start).Seconds())
	return entries, err
}

func (r *Registry) noteStale(now time.Time) {
	if r.met == nil || r.met.PolicyStale == nil {
		return
	}
	r.met.PolicyStale.Set(float64(r.staleDocuments(now)))
}

func (r *Registry) staleDocuments(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, h := range r.hubs {
		snap := h.peerView.Load()
		if snap == nil || len(snap.peers) == 0 {
			continue
		}
		view := h.policy.Load()
		if view != nil && view.ok && now.Sub(view.freshAt) > policyStale {
			n++
		}
	}
	return n
}

// policyView is one policy dump result. freshAt is when the request was sent.
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

func (h *Hub) applyPolicy(sent time.Time, entry *PolicyEntry, batchErr error) {
	if h.registry.Policy() == nil || h.stopApply.Load() {
		return
	}
	h.attempted.Store(true)
	h.notePolicy()
	if batchErr != nil || entry == nil {
		if prev := h.policy.Load(); prev != nil && prev.ok && time.Since(prev.freshAt) > policyStale {
			h.markUnavailable()
		}
		return
	}
	if prev := h.policy.Load(); prev != nil && prev.ok && !sent.After(prev.freshAt) {
		return
	}
	if time.Since(sent) > policyStale {
		h.markUnavailable()
		return
	}
	roles := entry.Members
	if roles == nil {
		roles = map[string]permissionv1.Role{}
	}
	view := &policyView{
		members:    maps.Clone(roles),
		writeEpoch: entry.WriteEpoch,
		epochState: entry.EpochState,
		freshAt:    sent,
		ok:         true,
	}
	h.policy.Store(view)
	h.closeAbsent(view)
}

// notePolicy wakes every waiter by closing the current note channel.
func (h *Hub) notePolicy() {
	h.noteMu.Lock()
	defer h.noteMu.Unlock()
	if h.policyNote == nil {
		return
	}
	close(h.policyNote)
	h.policyNote = make(chan struct{})
}

// policyChanged must be taken before the condition is checked, so a note
// between the check and the select is not lost.
func (h *Hub) policyChanged() <-chan struct{} {
	h.noteMu.Lock()
	defer h.noteMu.Unlock()
	return h.policyNote
}

func (h *Hub) waitFirstPolicy() {
	timer := time.NewTimer(policyStale)
	defer timer.Stop()
	for {
		changed := h.policyChanged()
		if h.policy.Load() != nil || h.attempted.Load() || h.stopApply.Load() {
			return
		}
		select {
		case <-changed:
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
	for {
		changed := h.policyChanged()
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
		case <-changed:
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
