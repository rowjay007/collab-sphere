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
	home := docgc.TreeDoc{Vector: crdt.VersionVector{3: 2}, VectorOK: true}

	t.Run("not lapsed", func(t *testing.T) {
		if docgc.ReportRetired(3, now, nil, home, nil, ttl, now) {
			t.Fatal("fresh report")
		}
	})
	t.Run("lapsed home archived", func(t *testing.T) {
		if !docgc.ReportRetired(3, old, nil, docgc.TreeDoc{Archived: true}, nil, ttl, now) {
			t.Fatal("archived home")
		}
	})
	t.Run("lapsed all siblings cover", func(t *testing.T) {
		tree := []docgc.TreeDoc{home, {Vector: crdt.VersionVector{3: 2}, VectorOK: true}}
		if !docgc.ReportRetired(3, old, nil, home, tree, ttl, now) {
			t.Fatal("covered tree")
		}
	})
	t.Run("lapsed sibling behind", func(t *testing.T) {
		tree := []docgc.TreeDoc{home, {Vector: crdt.VersionVector{3: 1}, VectorOK: true}}
		if docgc.ReportRetired(3, old, nil, home, tree, ttl, now) {
			t.Fatal("behind sibling keeps report active")
		}
	})
	t.Run("lapsed sibling null vector", func(t *testing.T) {
		tree := []docgc.TreeDoc{home, {VectorOK: false}}
		if docgc.ReportRetired(3, old, nil, home, tree, ttl, now) {
			t.Fatal("null sibling vector")
		}
	})
	t.Run("lapsed home null vector", func(t *testing.T) {
		if docgc.ReportRetired(3, old, nil, docgc.TreeDoc{}, nil, ttl, now) {
			t.Fatal("null home vector")
		}
	})
}

func TestReportRetiredUsesHomeClock(t *testing.T) {
	now := time.Now()
	old := now.Add(-400 * time.Hour)
	ttl := 336 * time.Hour
	home := docgc.TreeDoc{Vector: crdt.VersionVector{7: 8}, VectorOK: true}

	behind := []docgc.TreeDoc{home, {Vector: crdt.VersionVector{7: 5}, VectorOK: true}}
	if docgc.ReportRetired(7, old, nil, home, behind, ttl, now) {
		t.Fatal("sibling at the report's clock 5 retired a replica whose home holds clock 8")
	}
	caughtUp := []docgc.TreeDoc{home, {Vector: crdt.VersionVector{7: 8}, VectorOK: true}}
	if !docgc.ReportRetired(7, old, nil, home, caughtUp, ttl, now) {
		t.Fatal("sibling at the home clock 8 did not retire the replica")
	}
}
