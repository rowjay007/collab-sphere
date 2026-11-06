package gc_test

import (
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	docgc "github.com/rowjay007/collab-sphere/services/document-service/internal/gc"
)

func TestStableHonorsFloor(t *testing.T) {
	doc := crdt.VersionVector{1: 5, 2: 4}
	floor := crdt.VersionVector{1: 3}
	stable := docgc.Stable(docgc.Input{Doc: doc, Reports: []docgc.Report{{Vector: doc}}, Floor: floor})
	if stable[1] != 3 {
		t.Fatalf("stable=%v", stable)
	}
}

func TestReportRetiredByTTL(t *testing.T) {
	r := docgc.Report{ReportedAt: time.Now().Add(-400 * time.Hour)}
	if !docgc.ReportRetired(r, 336*time.Hour, time.Now()) {
		t.Fatal("expected retired")
	}
}
