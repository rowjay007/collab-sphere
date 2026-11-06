package gc

import (
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

type Report struct {
	Vector     crdt.VersionVector
	ReportedAt time.Time
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

func PinnedReplicaCount(reports []Report) float64 {
	n := 0
	for _, r := range reports {
		if !r.Retired {
			n++
		}
	}
	return float64(n)
}

func ReportRetired(r Report, ttl time.Duration, now time.Time) bool {
	if r.Retired {
		return true
	}
	if ttl > 0 && now.Sub(r.ReportedAt) > ttl {
		return true
	}
	return false
}
