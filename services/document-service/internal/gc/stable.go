package gc

import (
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

type Report struct {
	Replica    uint64
	Vector     crdt.VersionVector
	ReportedAt time.Time
	ReleasedAt *time.Time
	Retired    bool
}

type Input struct {
	Doc     crdt.VersionVector
	Reports []Report
	Pins    map[crdt.ID]struct{}
	Floor   crdt.VersionVector
}

func Stable(in Input) crdt.VersionVector {
	active := make([]crdt.VersionVector, 0, len(in.Reports))
	for _, r := range in.Reports {
		if r.Retired {
			continue
		}
		active = append(active, r.Vector)
	}
	return crdt.Stable(in.Doc, active, in.Floor)
}

func StableAge(reports []Report, stable crdt.VersionVector, now time.Time) time.Duration {
	if len(stable) == 0 {
		return 0
	}
	var oldest time.Time
	for _, r := range reports {
		if r.Retired {
			continue
		}
		for client, clock := range stable {
			if clock == 0 {
				continue
			}
			if r.Vector[client] != clock {
				continue
			}
			if oldest.IsZero() || r.ReportedAt.Before(oldest) {
				oldest = r.ReportedAt
			}
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return now.Sub(oldest)
}

func PinnedReplicas(reports []Report) []uint64 {
	out := make([]uint64, 0, len(reports))
	for _, r := range reports {
		if !r.Retired {
			out = append(out, r.Replica)
		}
	}
	return out
}
