package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var secondsBuckets = prometheus.DefBuckets

// Addr is METRICS_ADDR when that variable is set, including an empty value,
// and fallback otherwise. An empty result disables Serve.
func Addr(fallback string) string {
	if v, ok := os.LookupEnv("METRICS_ADDR"); ok {
		return v
	}
	return fallback
}

// NewRegistry is a registry with the Go and process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// Serve exposes reg at /metrics. An empty addr does not listen.
func Serve(addr string, reg *prometheus.Registry) (func(), error) {
	if addr == "" {
		return func() {}, nil
	}
	if reg == nil {
		return nil, errors.New("metrics: nil registry")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	return func() { _ = srv.Close() }, nil
}

// ObserveSince records time.Since(start) when obs is set.
func ObserveSince(obs interface{ Observe(float64) }, start time.Time) {
	if obs == nil {
		return
	}
	obs.Observe(time.Since(start).Seconds())
}

// ObserveUnix records the seconds from a unix-nano stamp until now.
// A zero stamp records nothing.
func ObserveUnix(obs interface{ Observe(float64) }, unixNano int64) {
	if obs == nil || unixNano == 0 {
		return
	}
	obs.Observe(float64(time.Now().UnixNano()-unixNano) / 1e9)
}

// UnaryServer records collab_grpc_server_seconds{method,code}.
// Chain it with the auth interceptor.
func UnaryServer(h *prometheus.HistogramVec) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if h == nil {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		h.WithLabelValues(info.FullMethod, status.Code(err).String()).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

// NewGRPC registers collab_grpc_server_seconds.
func NewGRPC(reg prometheus.Registerer) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "collab_grpc_server_seconds",
		Help:    "Unary gRPC handler duration in seconds.",
		Buckets: secondsBuckets,
	}, []string{"method", "code"})
	reg.MustRegister(h)
	return h
}

// Hub collectors owned by collab-hub.
type Hub struct {
	Apply       prometheus.Observer
	Append      prometheus.Observer
	Deliver     prometheus.Observer
	FanoutLag   prometheus.Observer
	Connections prometheus.Gauge
	PolicyDump  prometheus.Observer
	PolicyStale prometheus.Gauge
}

// NewHub registers the hub metrics on reg.
func NewHub(reg prometheus.Registerer) Hub {
	apply := histogram(reg, "collab_hub_apply_seconds", "Hub loop from an ops event through the sender send, including Append.")
	appendHist := histogram(reg, "collab_hub_append_seconds", "Append RPC inside hub persist.")
	deliver := histogram(reg, "collab_hub_deliver_seconds", "Accepting hub read-pump time to this hub's send.")
	lag := histogram(reg, "collab_hub_fanout_lag_seconds", "Publish stamp to Offer on a receiver.")
	conns := gauge(reg, "collab_hub_connections", "Open sockets on this process.")
	dump := histogram(reg, "collab_hub_policy_dump_seconds", "PolicyDump duration.")
	stale := gauge(reg, "collab_hub_policy_stale_documents", "Open documents whose last good dump is older than 2s.")
	return Hub{
		Apply: apply, Append: appendHist, Deliver: deliver, FanoutLag: lag,
		Connections: conns, PolicyDump: dump, PolicyStale: stale,
	}
}

// Doc collectors owned by document-service.
type Doc struct {
	Append        prometheus.Observer
	GCPass        prometheus.Observer
	GCNodes       prometheus.Counter
	GCPinned      prometheus.Gauge
	GCLastSuccess prometheus.Gauge
	GCStableAge   prometheus.Gauge
}

// NewDoc registers the document-service metrics on reg.
func NewDoc(reg prometheus.Registerer) Doc {
	appendHist := histogram(reg, "collab_doc_append_seconds", "document-service AppendOperations duration.")
	gcPass := histogram(reg, "collab_doc_gc_pass_seconds", "One document GC pass.")
	gcNodes := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "collab_doc_gc_nodes_removed_total",
		Help: "Nodes removed by GC.",
	})
	gcPinned := gauge(reg, "collab_doc_gc_pinned_replicas", "Distinct replicas (fork root, replica) included in a stable minimum during the last GC pass.")
	gcLast := gauge(reg, "collab_doc_gc_last_success_unixtime", "Unix time of the last pass that committed a live snapshot.")
	gcAge := gauge(reg, "collab_doc_gc_stable_age_seconds", "Maximum stable frontier age across documents in the last GC pass.")
	reg.MustRegister(gcNodes)
	return Doc{
		Append: appendHist, GCPass: gcPass, GCNodes: gcNodes,
		GCPinned: gcPinned, GCLastSuccess: gcLast, GCStableAge: gcAge,
	}
}

// Export collectors owned by export-service.
type Export struct {
	Counts  StateGauge
	Seconds prometheus.Observer
}

// StateGauge sets collab_export_jobs{state}.
type StateGauge struct{ g *prometheus.GaugeVec }

// Set records the count for one job state.
func (s StateGauge) Set(state string, v float64) {
	if s.g == nil {
		return
	}
	s.g.WithLabelValues(state).Set(v)
}

// NewExport registers the export-service metrics on reg.
func NewExport(reg prometheus.Registerer) Export {
	jobs := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "collab_export_jobs",
		Help: "Export jobs by state.",
	}, []string{"state"})
	seconds := histogram(reg, "collab_export_job_seconds", "Insert to a terminal export job state.")
	reg.MustRegister(jobs)
	return Export{Counts: StateGauge{g: jobs}, Seconds: seconds}
}

func histogram(reg prometheus.Registerer, name, help string) prometheus.Histogram {
	h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: secondsBuckets})
	reg.MustRegister(h)
	return h
}

func gauge(reg prometheus.Registerer, name, help string) prometheus.Gauge {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	reg.MustRegister(g)
	return g
}
