package history

import (
	"context"
	"log/slog"
	"time"
)

const (
	sweepOverlap = 10 * time.Second
	sweepPage    = 500
)

// Sweep materializes recently changed documents every interval until ctx
// ends. Each pass asks for documents changed since the previous pass's
// newest updated_at minus an overlap that absorbs commit-order skew.
func (h *History) Sweep(ctx context.Context, interval time.Duration) {
	var since time.Time
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		since = h.sweepOnce(ctx, since)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (h *History) sweepOnce(ctx context.Context, since time.Time) time.Time {
	from := since
	if !from.IsZero() {
		from = from.Add(-sweepOverlap)
	}
	newest := since
	for ctx.Err() == nil {
		ids, next, err := h.docs.ListChanged(ctx, from, sweepPage)
		if err != nil {
			slog.Error("sweep: list changed documents", "err", err)
			return newest
		}
		for _, id := range ids {
			if _, err := h.Materialize(ctx, id); err != nil && ctx.Err() == nil {
				slog.Error("sweep: materialize", "doc", id, "err", err)
			}
		}
		if next.After(newest) {
			newest = next
		}
		if len(ids) < sweepPage || !next.After(from) {
			return newest
		}
		from = next
	}
	return newest
}
