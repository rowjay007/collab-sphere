package crdt_test

import (
	"strings"
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/oracle"
)

func TestDiffMatchesBruteForceOracle(t *testing.T) {
	var changed, restoresChecked int
	for seed := uint64(1); seed <= 500; seed++ {
		h := newHistory(t, seed).run(50)
		a, b := h.pick(), h.pick()
		got, err := crdt.Diff(a, b)
		if err != nil {
			t.Fatal(err)
		}
		want, err := oracle.Diff(replay(t, a), replay(t, b))
		if err != nil {
			t.Fatal(err)
		}
		for name, spans := range map[string][]crdt.Span{"crdt": got, "oracle": want} {
			if s := spell(spans, crdt.Retained, crdt.Deleted); s != a.Text() {
				t.Fatalf("seed %d %s: retained+deleted = %q, want a = %q", seed, name, s, a.Text())
			}
			if s := spell(spans, crdt.Retained, crdt.Inserted); s != b.Text() {
				t.Fatalf("seed %d %s: retained+inserted = %q, want b = %q", seed, name, s, b.Text())
			}
		}
		for _, kind := range []crdt.SpanKind{crdt.Retained, crdt.Inserted, crdt.Deleted} {
			if g, w := len([]rune(spell(got, kind))), len([]rune(spell(want, kind))); g != w {
				t.Fatalf("seed %d: %s runes = %d, oracle %d", seed, kind, g, w)
			}
		}
		for i := 1; i < len(got); i++ {
			if got[i].Kind == got[i-1].Kind {
				t.Fatalf("seed %d: adjacent %s spans are not coalesced", seed, got[i].Kind)
			}
		}
		if spell(got, crdt.Inserted, crdt.Deleted) != "" {
			changed++
		}
		for _, r := range h.restores {
			spans, err := crdt.Diff(r.target, r.result)
			if err != nil {
				t.Fatal(err)
			}
			if s := spell(spans, crdt.Inserted, crdt.Deleted); s != "" {
				t.Fatalf("seed %d: Diff(target, restore) changed %q", seed, s)
			}
			restoresChecked++
		}
	}
	if changed < 250 || restoresChecked < 300 {
		t.Fatalf("only %d diffs had changes and %d restores were checked", changed, restoresChecked)
	}
}

func spell(spans []crdt.Span, kinds ...crdt.SpanKind) string {
	var b strings.Builder
	for _, s := range spans {
		for _, k := range kinds {
			if s.Kind == k {
				b.WriteString(s.Text)
			}
		}
	}
	return b.String()
}

func replay(t testing.TB, d *crdt.Doc) oracle.State {
	t.Helper()
	s, err := oracle.Replay(d.Ops())
	if err != nil {
		t.Fatal(err)
	}
	return s
}
