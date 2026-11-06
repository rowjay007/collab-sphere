package gc

import (
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

// TreeDoc is one document in a fork tree for the retirement rule.
type TreeDoc struct {
	Archived bool
	Vector   crdt.VersionVector
	VectorOK bool
}

// ReportLapsed is true when released_at is set or reported_at is older than ttl.
func ReportLapsed(reportedAt time.Time, releasedAt *time.Time, ttl time.Duration, now time.Time) bool {
	if releasedAt != nil {
		return true
	}
	if ttl > 0 && now.Sub(reportedAt) > ttl {
		return true
	}
	return false
}

// ReportRetired is true when the report is lapsed and either the home document
// is archived or every unarchived document in the tree covers this replica's
// clock in the report. A sibling with a null vector means not retired.
func ReportRetired(replica uint64, vector crdt.VersionVector, reportedAt time.Time, releasedAt *time.Time, homeArchived bool, tree []TreeDoc, ttl time.Duration, now time.Time) bool {
	if !ReportLapsed(reportedAt, releasedAt, ttl, now) {
		return false
	}
	if homeArchived {
		return true
	}
	self := crdt.ID{Client: crdt.ClientID(replica), Clock: vector[crdt.ClientID(replica)]}
	for _, doc := range tree {
		if doc.Archived {
			continue
		}
		if !doc.VectorOK {
			return false
		}
		if !doc.Vector.Covers(self) {
			return false
		}
	}
	return true
}
