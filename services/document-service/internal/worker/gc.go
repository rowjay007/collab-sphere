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
	home docgc.TreeDoc
	tree []docgc.TreeDoc
}

// DocResult is what one committed document GC contributes to its pass.
type DocResult struct {
	ForkRoot  uuid.UUID
	Pinned    []uint64
	StableAge time.Duration
}

type pinnedReplica struct {
	forkRoot uuid.UUID
	replica  uint64
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
	pinned := make(map[pinnedReplica]struct{})
	var maxAge time.Duration
	for _, id := range ids {
		res, ok := g.RunDocument(ctx, id)
		if !ok {
			continue
		}
		for _, replica := range res.Pinned {
			pinned[pinnedReplica{forkRoot: res.ForkRoot, replica: replica}] = struct{}{}
		}
		maxAge = max(maxAge, res.StableAge)
	}
	if g.Metrics.GCPinned != nil {
		g.Metrics.GCPinned.Set(float64(len(pinned)))
	}
	if g.Metrics.GCStableAge != nil {
		g.Metrics.GCStableAge.Set(maxAge.Seconds())
	}
}

// RunDocument runs one GC pass for id. It reports false when the pass did not
// commit a live snapshot.
func (g *GC) RunDocument(ctx context.Context, id uuid.UUID) (DocResult, bool) {
	start := time.Now()
	docRow, err := g.Store.Get(ctx, id)
	if err != nil {
		slog.Warn("gc load document", "document_id", id, "err", err)
		return DocResult{}, false
	}
	vector, ok, err := g.Store.DocumentCommittedVector(ctx, id)
	if err != nil {
		slog.Warn("gc vector", "document_id", id, "err", err)
		return DocResult{}, false
	}
	if !ok {
		slog.Debug("gc skip null vector", "document_id", id)
		return DocResult{}, false
	}
	floor, err := g.treeFloor(ctx, docRow.ForkRoot)
	if err != nil {
		slog.Warn("gc floor", "document_id", id, "err", err)
		return DocResult{}, false
	}
	pins, err := g.Pins.AnchorPins(ctx, docRow.ForkRoot)
	if err != nil {
		slog.Warn("gc pins", "document_id", id, "err", err)
		return DocResult{}, false
	}
	reports, err := g.Store.ListReports(ctx, docRow.ForkRoot)
	if err != nil {
		slog.Warn("gc reports", "document_id", id, "err", err)
		return DocResult{}, false
	}
	now := time.Now()
	retireCache := make(map[uint64]retireInputs)
	gcReports := make([]docgc.Report, 0, len(reports))
	for _, r := range reports {
		in, ok := retireCache[r.Replica]
		if !ok {
			home, tree, err := g.Store.ForkTreeForRetirement(ctx, docRow.ForkRoot, r.Replica)
			if err != nil {
				slog.Warn("gc retirement inputs", "document_id", id, "replica", r.Replica, "err", err)
				return DocResult{}, false
			}
			in = retireInputs{home: home, tree: tree}
			retireCache[r.Replica] = in
		}
		retired := docgc.ReportRetired(r.Replica, r.ReportedAt, r.ReleasedAt, in.home, in.tree, g.TTL, now)
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
		return DocResult{}, false
	}
	removed, err := cdoc.GCWithPins(stable, pins)
	if err != nil {
		slog.Warn("gc apply", "document_id", id, "err", err)
		return DocResult{}, false
	}
	snap := cdoc.Snapshot()
	if err := g.Store.UpsertLiveSnapshot(ctx, id, builtSeq, snap); err != nil {
		slog.Warn("gc upsert", "document_id", id, "err", err)
		return DocResult{}, false
	}
	if g.Metrics.GCPass != nil {
		g.Metrics.GCPass.Observe(time.Since(start).Seconds())
	}
	if g.Metrics.GCNodes != nil {
		g.Metrics.GCNodes.Add(float64(removed))
	}
	if g.Metrics.GCLastSuccess != nil {
		g.Metrics.GCLastSuccess.Set(float64(now.Unix()))
	}
	return DocResult{
		ForkRoot:  docRow.ForkRoot,
		Pinned:    docgc.PinnedReplicas(gcReports),
		StableAge: docgc.StableAge(gcReports, stable, now),
	}, true
}

// treeFloor is the pointwise minimum of RestoreFloor across every unarchived
// document in the tree. A client absent from any floor is 0.
func (g *GC) treeFloor(ctx context.Context, forkRoot uuid.UUID) (crdt.VersionVector, error) {
	ids, err := g.Store.UnarchivedForkTree(ctx, forkRoot)
	if err != nil {
		return nil, err
	}
	floors := make([]crdt.VersionVector, 0, len(ids))
	for _, doc := range ids {
		floor, err := g.Floor.RestoreFloor(ctx, doc)
		if err != nil {
			return nil, err
		}
		floors = append(floors, floor)
	}
	if len(floors) == 0 {
		return crdt.VersionVector{}, nil
	}
	return crdt.Stable(floors[0], floors[1:], floors[0]), nil
}
