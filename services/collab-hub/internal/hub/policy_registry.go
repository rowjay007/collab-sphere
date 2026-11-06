package hub

import (
	"context"
	"sync"
	"time"
)

// pollPolicy starts a batch every period without waiting for the last one,
// so the view stays fresh while batch latency is under policyStale-policyPeriod.
func (r *Registry) pollPolicy(stop <-chan struct{}) {
	inFlight := make(chan struct{}, policyInFlight)
	var polls sync.WaitGroup
	defer polls.Wait()
	tick := time.NewTicker(policyPeriod)
	defer tick.Stop()
	for {
		select {
		case inFlight <- struct{}{}:
			polls.Go(func() {
				defer func() { <-inFlight }()
				r.runPolicyBatch(stop)
				r.noteStale(time.Now())
			})
		default:
		}
		select {
		case <-stop:
			return
		case <-tick.C:
		case <-r.policyKick:
		}
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
	case <-ctx.Done():
		res.err = ctx.Err()
	}
	r.policyApply.Lock()
	defer r.policyApply.Unlock()
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
