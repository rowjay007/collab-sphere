package hub

import (
	"context"
	"time"
)

func (r *Registry) pollPolicy(stop <-chan struct{}) {
	for {
		if policyPollDone(stop) {
			return
		}
		r.runPolicyBatch(stop)
		r.noteStale(time.Now())
		timer := time.NewTimer(policyPeriod)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		case <-r.policyKick:
			timer.Stop()
		}
	}
}

func policyPollDone(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

func (r *Registry) runPolicyBatch(stop <-chan struct{}) {
	if r.Policy() == nil {
		return
	}
	r.mu.Lock()
	ids := make([]string, 0, len(r.hubs))
	hubs := make([]*Hub, 0, len(r.hubs))
	for id, h := range r.hubs {
		ids = append(ids, id)
		hubs = append(hubs, h)
	}
	r.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	sent := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	type batchResult struct {
		entries map[string]PolicyEntry
		err     error
	}
	ch := make(chan batchResult, 1)
	go func() {
		entries, err := r.Policy().DumpBatch(ctx, ids)
		ch <- batchResult{entries: entries, err: err}
	}()

	var res batchResult
	select {
	case <-stop:
		return
	case res = <-ch:
	}
	for _, h := range hubs {
		var entry *PolicyEntry
		if res.err == nil {
			if e, ok := res.entries[h.docID]; ok {
				entry = &e
			}
		}
		h.applyPolicy(sent, entry, res.err)
	}
}
