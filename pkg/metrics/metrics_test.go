package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServe(t *testing.T) {
	reg := NewRegistry()
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "collab_hub_connections", Help: "sockets"})
	reg.MustRegister(g)
	g.Set(3)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	stop, err := Serve(addr, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)

	body := getMetrics(t, addr)
	if !strings.Contains(body, "collab_hub_connections") || !strings.Contains(body, "go_goroutines") {
		t.Fatalf("metrics body missing collectors:\n%s", body)
	}

	noop, err := Serve("", reg)
	if err != nil {
		t.Fatal(err)
	}
	noop()
}

func TestUnaryServerLabels(t *testing.T) {
	reg := NewRegistry()
	hist := NewGRPC(reg)
	outer := UnaryServer(hist)
	inner := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		return nil, status.Error(codes.InvalidArgument, "bad")
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/document.v1.DocumentService/AppendOperations"}
	_, err := outer(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		return inner(ctx, req, info, func(context.Context, any) (any, error) { return nil, nil })
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code %v", status.Code(err))
	}
	method, code := grpcLabels(t, reg)
	if method != info.FullMethod || code != "InvalidArgument" {
		t.Fatalf("labels method=%q code=%q", method, code)
	}

	_, err = outer(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/document.v1.DocumentService/GetDocument"}, func(context.Context, any) (any, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasCode(t, reg, "OK") {
		t.Fatal("missing OK code label")
	}
}

func getMetrics(t *testing.T, addr string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last error
	for {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK {
				return string(body)
			}
			err = readErr
		}
		last = err
		if time.Now().After(deadline) {
			t.Fatalf("scrape %s: %v", addr, last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func grpcLabels(t *testing.T, reg *prometheus.Registry) (method, code string) {
	t.Helper()
	for _, m := range family(t, reg, "collab_grpc_server_seconds") {
		var gotMethod, gotCode string
		for _, lp := range m.GetLabel() {
			switch lp.GetName() {
			case "method":
				gotMethod = lp.GetValue()
			case "code":
				gotCode = lp.GetValue()
			}
		}
		if gotCode == "InvalidArgument" {
			return gotMethod, gotCode
		}
	}
	t.Fatal("missing InvalidArgument sample")
	return "", ""
}

func hasCode(t *testing.T, reg *prometheus.Registry, code string) bool {
	t.Helper()
	for _, m := range family(t, reg, "collab_grpc_server_seconds") {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "code" && lp.GetValue() == code {
				return true
			}
		}
	}
	return false
}

func family(t *testing.T, reg *prometheus.Registry, name string) []*dto.Metric {
	t.Helper()
	families, err := reg.Gather()
	if len(families) == 0 && err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()
		}
	}
	t.Fatalf("missing family %s (gather err %v)", name, err)
	return nil
}
