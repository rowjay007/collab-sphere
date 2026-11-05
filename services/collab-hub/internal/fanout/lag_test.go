package fanout

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/rowjay007/collab-sphere/pkg/metrics"
)

func TestStampedBatchRecordsLag(t *testing.T) {
	reg := prometheus.NewRegistry()
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "collab_hub_fanout_lag_seconds",
		Help:    "Publish stamp to Offer.",
		Buckets: prometheus.DefBuckets,
	})
	reg.MustRegister(hist)

	stamped := batchAt(1)
	stamped.OriginUnixNano = time.Now().Add(-25 * time.Millisecond).UnixNano()
	stamped.ReadUnixNano = stamped.OriginUnixNano - int64(time.Millisecond)
	raw, err := encodeBatch("hub-a", stamped)
	if err != nil {
		t.Fatal(err)
	}
	_, got, err := decodeBatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	ready := NewSequencer(0).Offer(got)
	if len(ready) != 1 || ready[0].OriginUnixNano != stamped.OriginUnixNano || ready[0].ReadUnixNano != stamped.ReadUnixNano {
		t.Fatalf("offer dropped stamps: %+v", ready)
	}
	metrics.ObserveUnix(hist, got.OriginUnixNano)
	if n := histogramCount(t, reg, "collab_hub_fanout_lag_seconds"); n != 1 {
		t.Fatalf("stamped lag count %d", n)
	}

	raw, err = encodeBatch("hub-a", batchAt(2))
	if err != nil {
		t.Fatal(err)
	}
	_, got, err = decodeBatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.OriginUnixNano != 0 || got.ReadUnixNano != 0 {
		t.Fatalf("unstamped batch decoded with stamps %+v", got)
	}
	metrics.ObserveUnix(hist, got.OriginUnixNano)
	if n := histogramCount(t, reg, "collab_hub_fanout_lag_seconds"); n != 1 {
		t.Fatalf("unstamped batch changed lag count to %d", n)
	}
}

func histogramCount(t *testing.T, reg prometheus.Gatherer, name string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if len(families) == 0 && err != nil {
		t.Fatal(err)
	}
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

var _ = dto.Metric{}
