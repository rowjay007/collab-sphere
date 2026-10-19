package crdt_test

import (
	"testing"

	"github.com/rowjay007/collab-sphere/tests/internal/oracle"
)

func TestResolveRangeMatchesBruteForceOracle(t *testing.T) {
	var attached, detached int
	for seed := uint64(1); seed <= 300; seed++ {
		h := newHistory(t, seed).run(20)
		anchored := h.pick()
		n := anchored.Len()
		if n == 0 {
			continue
		}
		i := h.rng.IntN(n)
		j := i + h.rng.IntN(n-i)
		start, err := anchored.IDAt(i)
		if err != nil {
			t.Fatal(err)
		}
		end, err := anchored.IDAt(j)
		if err != nil {
			t.Fatal(err)
		}
		h.run(40)
		for _, d := range h.states() {
			if !d.Has(start) || !d.Has(end) {
				continue
			}
			got, err := d.ResolveRange(start, end)
			if err != nil {
				t.Fatal(err)
			}
			want, ok := oracle.ResolveRange(replay(t, d), start, end)
			if !ok {
				t.Fatalf("seed %d: oracle lost the anchor", seed)
			}
			if got != want {
				t.Fatalf("seed %d: ResolveRange(%s, %s) = %+v, oracle %+v", seed, start, end, got, want)
			}
			if got.Detached {
				detached++
			} else {
				attached++
			}
		}
	}
	if attached < 500 || detached < 20 {
		t.Fatalf("exercised %d attached and %d detached ranges", attached, detached)
	}
}
