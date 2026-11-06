package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/pkg/metrics"
	docgc "github.com/rowjay007/collab-sphere/services/document-service/internal/gc"
	"github.com/rowjay007/collab-sphere/services/document-service/internal/store"
)

type FloorClient interface {
	RestoreFloor(ctx context.Context, docID uuid.UUID) (crdt.VersionVector, error)
}

type PinsClient interface {
	AnchorPins(ctx context.Context, forkRoot uuid.UUID) (map[crdt.ID]struct{}, error)
}

type GC struct {
	Store   *store.Store
	Floor   FloorClient
	Pins    PinsClient
	Metrics metrics.Doc
	TTL     time.Duration
}

type retireInputs struct {
	homeArchived bool
	tree         []docgc.TreeDoc
}

func (g *GC) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.pass(ctx)
		}
	}
}

func (g *GC) pass(ctx context.Context) {
	ids, err := g.Store.ListGCCandidates(ctx)
	if err != nil {
		slog.Warn("gc list", "err", err)
		return
	}
	for _, id := range ids {
		g.RunDocument(ctx, id)
	}
}

// RunDocument runs one GC pass for id.
func (g *GC) RunDocument(ctx context.Context, id uuid.UUID) {
	start := time.Now()
	docRow, err := g.Store.Get(ctx, id)
	if err != nil {
		slog.Warn("gc load document", "document_id", id, "err", err)
		return
	}
	vector, ok, err := g.Store.DocumentCommittedVector(ctx, id)
	if err != nil {
		slog.Warn("gc vector", "document_id", id, "err", err)
		return
	}
	if !ok {
		slog.Debug("gc skip null vector", "document_id", id)
		return
	}
	floor, err := g.Floor.RestoreFloor(ctx, id)
	if err != nil {
		slog.Warn("gc floor", "document_id", id, "err", err)
		return
	}
	pins, err := g.Pins.AnchorPins(ctx, docRow.ForkRoot)
	if err != nil {
		slog.Warn("gc pins", "document_id", id, "err", err)
		return
	}
	reports, err := g.Store.ListReports(ctx, docRow.ForkRoot)
	if err != nil {
		slog.Warn("gc reports", "document_id", id, "err", err)
		return
	}
	now := time.Now()
	retireCache := make(map[uint64]retireInputs)
	gcReports := make([]docgc.Report, 0, len(reports))
	for _, r := range reports {
		in, ok := retireCache[r.Replica]
		if !ok {
			homeArchived, tree, err := g.Store.ForkTreeForRetirement(ctx, docRow.ForkRoot, r.Replica)
			if err != nil {
				slog.Warn("gc retirement inputs", "document_id", id, "replica", r.Replica, "err", err)
				return
			}
			in = retireInputs{homeArchived: homeArchived, tree: tree}
			retireCache[r.Replica] = in
		}
		retired := docgc.ReportRetired(r.Replica, r.Vector, r.ReportedAt, r.ReleasedAt, in.homeArchived, in.tree, g.TTL, now)
		gcReports = append(gcReports, docgc.Report{
			Replica: r.Replica, Vector: r.Vector, ReportedAt: r.ReportedAt, ReleasedAt: r.ReleasedAt, Retired: retired,
		})
	}
	stable := docgc.Stable(docgc.Input{
		Doc:     vector,
		Reports: gcReports,
		Floor:   floor,
	})
	cdoc, builtSeq, err := g.Store.BuildDocThrough(ctx, id, docRow.OpSeq)
	if err != nil {
		slog.Warn("gc build", "document_id", id, "err", err)
		return
	}
	removed, err := cdoc.GCWithPins(stable, pins)
	if err != nil {
		slog.Warn("gc apply", "document_id", id, "err", err)
		return
	}
	snap := cdoc.Snapshot()
	if err := g.Store.UpsertLiveSnapshot(ctx, id, builtSeq, snap); err != nil {
		slog.Warn("gc upsert", "document_id", id, "err", err)
		return
	}
	if g.Metrics.GCPass != nil {
		g.Metrics.GCPass.Observe(time.Since(start).Seconds())
	}
	if g.Metrics.GCNodes != nil {
		g.Metrics.GCNodes.Add(float64(removed))
	}
	if g.Metrics.GCPinned != nil {
		g.Metrics.GCPinned.Set(docgc.PinnedReplicaCount(gcReports))
	}
	if g.Metrics.GCLastSuccess != nil {
		g.Metrics.GCLastSuccess.Set(float64(now.Unix()))
	}
	if g.Metrics.GCStableAge != nil {
		g.Metrics.GCStableAge.Set(docgc.StableAge(gcReports, stable, now).Seconds())
	}
}
