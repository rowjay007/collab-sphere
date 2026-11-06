package gc_test

import (
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	docgc "github.com/rowjay007/collab-sphere/services/document-service/internal/gc"
)

func TestReportLapsedReleased(t *testing.T) {
	now := time.Now()
	rel := now.Add(-time.Hour)
	if !docgc.ReportLapsed(now.Add(-time.Minute), &rel, time.Hour, now) {
		t.Fatal("released report should lapse")
	}
}

func TestReportRetiredCases(t *testing.T) {
	now := time.Now()
	old := now.Add(-400 * time.Hour)
	ttl := 336 * time.Hour
	vec := crdt.VersionVector{3: 2}

	t.Run("not lapsed", func(t *testing.T) {
		if docgc.ReportRetired(3, vec, now, nil, false, nil, ttl, now) {
			t.Fatal("fresh report")
		}
	})
	t.Run("lapsed home archived", func(t *testing.T) {
		if !docgc.ReportRetired(3, vec, old, nil, true, nil, ttl, now) {
			t.Fatal("archived home")
		}
	})
	t.Run("lapsed all siblings cover", func(t *testing.T) {
		tree := []docgc.TreeDoc{{Vector: crdt.VersionVector{3: 2}, VectorOK: true}}
		if !docgc.ReportRetired(3, vec, old, nil, false, tree, ttl, now) {
			t.Fatal("covered tree")
		}
	})
	t.Run("lapsed sibling behind", func(t *testing.T) {
		tree := []docgc.TreeDoc{{Vector: crdt.VersionVector{3: 1}, VectorOK: true}}
		if docgc.ReportRetired(3, vec, old, nil, false, tree, ttl, now) {
			t.Fatal("behind sibling keeps report active")
		}
	})
	t.Run("lapsed sibling null vector", func(t *testing.T) {
		tree := []docgc.TreeDoc{{VectorOK: false}}
		if docgc.ReportRetired(3, vec, old, nil, false, tree, ttl, now) {
			t.Fatal("null sibling vector")
		}
	})
}
