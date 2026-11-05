package hub

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/pkg/metrics"
)

func TestApplyAppendDeliverCounts(t *testing.T) {
	prom := metrics.NewRegistry()
	reg := localRegistry(NewMemoryStore())
	reg.SetMetrics(metrics.NewHub(prom))
	t.Cleanup(reg.Close)

	peer := NewPeer(1, 0)
	session, err := reg.Join(t.Context(), "doc", peer, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Leave(context.Background()) })
	if msg := readServerMessage(t, t.Context(), peer); msg.GetDocumentSync() == nil {
		t.Fatalf("first frame %T", msg.GetPayload())
	}
	session.SubmitOps(t.Context(), []crdt.Op{insert(1, 1)})
	msg := readServerMessage(t, t.Context(), peer)
	if msg.GetOperationBroadcast() == nil {
		t.Fatalf("broadcast %T", msg.GetPayload())
	}
	if msg.GetOperationBroadcast().GetReadUnixNano() == 0 {
		t.Fatal("broadcast missing read-pump stamp")
	}

	want := map[string]uint64{
		"collab_hub_apply_seconds":   1,
		"collab_hub_append_seconds":  1,
		"collab_hub_deliver_seconds": 1,
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		ok := true
		for name, n := range want {
			if histogramCount(prom, name) != n {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			for name, n := range want {
				if got := histogramCount(prom, name); got != n {
					t.Errorf("%s count %d, want %d", name, got, n)
				}
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, name := range []string{
		"collab_hub_apply_seconds",
		"collab_hub_append_seconds",
		"collab_hub_deliver_seconds",
		"collab_hub_fanout_lag_seconds",
		"collab_hub_connections",
		"collab_hub_policy_dump_seconds",
		"collab_hub_policy_stale_documents",
	} {
		if !hasFamily(prom, name) {
			t.Errorf("registry missing %s", name)
		}
	}
}

func histogramCount(g prometheus.Gatherer, name string) uint64 {
	families, _ := g.Gather()
	var n uint64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if h := m.GetHistogram(); h != nil {
				n += h.GetSampleCount()
			}
		}
	}
	return n
}

func hasFamily(g prometheus.Gatherer, name string) bool {
	families, _ := g.Gather()
	for _, f := range families {
		if f.GetName() == name {
			return true
		}
	}
	return false
}

var _ = dto.Metric{}
