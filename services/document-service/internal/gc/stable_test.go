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

func TestReportLapsedByTTL(t *testing.T) {
	if !docgc.ReportLapsed(time.Now().Add(-400*time.Hour), nil, 336*time.Hour, time.Now()) {
		t.Fatal("expected lapsed")
	}
}
