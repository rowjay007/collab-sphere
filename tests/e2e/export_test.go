package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	exportv1 "github.com/rowjay007/collab-sphere/gen/export/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/exportcheck"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func TestExportFormats(t *testing.T) {
	s, ctx := newExportStack(t, 3*time.Minute)
	text := "Hello export.\nSecond line."
	docID, version := authorDocument(t, ctx, s, text)
	s.Grant(ctx, docID, "viewer", permissionv1.Role_ROLE_VIEWER)

	head := requestExport(t, ctx, s, docID, nil, "pdf", t.Name()+"/pdf", "viewer")
	if !bytes.Equal(head.GetVersionId(), version.GetId()) {
		t.Fatalf("head pin %x, checkpoint %x", head.GetVersionId(), version.GetId())
	}
	pdf := waitReady(t, ctx, s, head.GetId(), "viewer", 15*time.Second)
	assertBody(t, "pdf", fetch(t, pdf.GetUrl()), text)

	for _, format := range []string{"md", "docx"} {
		job := requestExport(t, ctx, s, docID, version.GetId(), format, t.Name()+"/"+format, "viewer")
		if !bytes.Equal(job.GetVersionId(), version.GetId()) {
			t.Fatalf("%s pin %x, checkpoint %x", format, job.GetVersionId(), version.GetId())
		}
		ready := waitReady(t, ctx, s, job.GetId(), "viewer", 15*time.Second)
		assertBody(t, format, fetch(t, ready.GetUrl()), text)
	}

	_, err := s.Export.Client.RequestExport(s.AuthCtx(ctx, "viewer"), &exportv1.RequestExportRequest{
		DocId: docID, VersionId: bytes.Repeat([]byte{9}, 32), Format: "pdf", RequestId: t.Name() + "/missing",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("missing version: %v", err)
	}
	_, err = s.Export.Client.RequestExport(s.AuthCtx(ctx, "viewer"), &exportv1.RequestExportRequest{
		DocId: docID, VersionId: []byte{1}, Format: "pdf", RequestId: t.Name() + "/short",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("short version id: %v", err)
	}
}

func TestExportNonMember(t *testing.T) {
	s, ctx := newExportStack(t, 3*time.Minute)
	docID, version := authorDocument(t, ctx, s, "members only")
	_, err := s.Export.Client.RequestExport(s.AuthCtx(ctx, "outsider"), &exportv1.RequestExportRequest{
		DocId: docID, VersionId: version.GetId(), Format: "md", RequestId: t.Name() + "/req",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("RequestExport outsider: %v", err)
	}
	job := requestExport(t, ctx, s, docID, version.GetId(), "md", t.Name()+"/owner", s.Owner())
	_, err = s.Export.Client.GetExport(s.AuthCtx(ctx, "outsider"), &exportv1.GetExportRequest{Id: job.GetId()})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("GetExport outsider: %v", err)
	}
}

func TestExportIdempotentRequestID(t *testing.T) {
	s, ctx := newExportStack(t, 3*time.Minute)
	docID, version := authorDocument(t, ctx, s, "same request")
	first := requestExport(t, ctx, s, docID, version.GetId(), "pdf", t.Name(), s.Owner())
	second := requestExport(t, ctx, s, docID, nil, "docx", t.Name(), s.Owner())
	if first.GetId() != second.GetId() || first.GetFormat() != second.GetFormat() || first.GetFormat() != "pdf" {
		t.Fatalf("first %+v second %+v", first, second)
	}
	if !bytes.Equal(first.GetVersionId(), second.GetVersionId()) {
		t.Fatalf("version %x then %x", first.GetVersionId(), second.GetVersionId())
	}
}

func TestExportCrashAfterUpload(t *testing.T) {
	assertCrashExportsOneObject(t, "upload")
}

func TestExportCrashAfterClaim(t *testing.T) {
	assertCrashExportsOneObject(t, "claimed")
}

func TestExportTiming(t *testing.T) {
	s := stack.New(t, stack.WithExport())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	text := fixture100()
	docID, version := authorDocument(t, ctx, s, text)

	samples := make([]time.Duration, 5)
	for i := range samples {
		job, err := s.Export.Client.RequestExport(s.AuthCtx(ctx, s.Owner()), &exportv1.RequestExportRequest{
			DocId: docID, VersionId: version.GetId(), Format: "pdf", RequestId: fmt.Sprintf("%s-%d", t.Name(), i),
		})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		ready := waitReady(t, ctx, s, job.GetId(), s.Owner(), 30*time.Second)
		body := fetch(t, ready.GetUrl())
		pdf, err := exportcheck.ReadPDF(body)
		if err != nil {
			t.Fatal(err)
		}
		samples[i] = time.Since(start)
		if pdf.Pages != 100 {
			t.Fatalf("sample %d pages = %d", i, pdf.Pages)
		}
		if strings.Join(pdf.Text, "\n") != text {
			t.Fatalf("sample %d extracted %d bytes, want %d", i, len(strings.Join(pdf.Text, "\n")), len(text))
		}
	}
	logged := append([]time.Duration(nil), samples...)
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	min, median, max := samples[0], samples[len(samples)/2], samples[len(samples)-1]
	spread := max - min
	t.Logf("export timing samples=%s min=%s median=%s max=%s spread=%s cpus=%d", logged, min, median, max, spread, runtime.NumCPU())
	if median > 8*time.Second {
		writeWorkerProfile(t, ctx, s, docID, version.GetId())
	}
	if median >= 10*time.Second || spread >= 2*time.Second || max >= 12*time.Second {
		t.Fatalf("budget median<10s each<12s spread<2s, got min=%s median=%s max=%s spread=%s", min, median, max, spread)
	}
}

func assertCrashExportsOneObject(t *testing.T, point string) {
	t.Helper()
	s, ctx := newExportStack(t, 4*time.Minute)
	text := "crash " + point
	docID, version := authorDocument(t, ctx, s, text)
	s.Export.Env = []string{"EXPORT_CRASH_AFTER=" + point}
	s.Export.Restart()
	job := requestExport(t, ctx, s, docID, version.GetId(), "pdf", t.Name(), s.Owner())
	s.Export.WaitExit(20 * time.Second)
	s.Export.Env = nil
	s.Export.Restart()
	ready := waitReady(t, ctx, s, job.GetId(), s.Owner(), 50*time.Second)
	prefix := "exports/" + job.GetId()
	keys := listKeys(t, ctx, s.Export, prefix)
	if len(keys) != 1 || keys[0] != prefix {
		t.Fatalf("objects under %s: %v", prefix, keys)
	}
	assertBody(t, "pdf", fetch(t, ready.GetUrl()), text)
}

func newExportStack(t *testing.T, budget time.Duration) (*stack.Stack, context.Context) {
	t.Helper()
	s := stack.New(t, stack.WithExport())
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	t.Cleanup(cancel)
	return s, ctx
}

func authorDocument(t *testing.T, ctx context.Context, s *stack.Stack, text string) (string, *versionv1.Version) {
	t.Helper()
	docID := s.CreateDocument(ctx)
	c := s.Dial(ctx, docID, 1)
	for off := 0; off < len(text); {
		end := off + 1024
		if end > len(text) {
			end = len(text)
		}
		chunk := text[off:end]
		at := utf8.RuneCountInString(text[:off])
		if err := c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
			ins, err := r.InsertText(at, chunk)
			if err != nil {
				return nil, err
			}
			ops := make([]crdt.Op, len(ins))
			for i := range ins {
				ops[i] = ins[i]
			}
			return ops, nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := c.WaitAcked(ctx); err != nil {
			t.Fatalf("ack: %v hub errors %v", err, c.Errors())
		}
		off = end
	}
	if got := stack.Settle(t, ctx, s.Docs, docID, []*collabclient.Client{c}); got != text {
		t.Fatalf("stored %d bytes, want %d", len(got), len(text))
	}
	return docID, checkpoint(t, ctx, s.Versions.Client, docID, "v")
}

func requestExport(t *testing.T, ctx context.Context, s *stack.Stack, docID string, versionID []byte, format, requestID, user string) *exportv1.ExportJob {
	t.Helper()
	job, err := s.Export.Client.RequestExport(s.AuthCtx(ctx, user), &exportv1.RequestExportRequest{
		DocId:     docID,
		VersionId: versionID,
		Format:    format,
		RequestId: requestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func waitReady(t *testing.T, ctx context.Context, s *stack.Stack, id, user string, timeout time.Duration) *exportv1.ExportJob {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *exportv1.ExportJob
	for time.Now().Before(deadline) {
		got, err := s.Export.Client.GetExport(s.AuthCtx(ctx, user), &exportv1.GetExportRequest{Id: id})
		if err != nil {
			t.Fatal(err)
		}
		last = got
		switch got.GetState() {
		case "failed":
			t.Fatalf("export %s failed: %s", id, got.GetError())
		case "ready":
			if got.GetUrl() == "" {
				t.Fatal("ready export has no url")
			}
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("export %s not ready: state %s", id, last.GetState())
	return nil
}

func assertBody(t *testing.T, format string, body []byte, text string) {
	t.Helper()
	switch format {
	case "md":
		if !bytes.Equal(body, []byte(text+"\n")) {
			t.Fatalf("markdown = %q, want %q", body, text+"\n")
		}
	case "docx":
		paras, err := exportcheck.ReadDOCX(body)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(paras, "\n"); got != text {
			t.Fatalf("docx = %q, want %q", got, text)
		}
	case "pdf":
		pdf, err := exportcheck.ReadPDF(body)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(pdf.Text, "\n"); got != text {
			t.Fatalf("pdf = %q, want %q", got, text)
		}
	default:
		t.Fatalf("format %s", format)
	}
}

func fetch(t *testing.T, rawURL string) []byte {
	t.Helper()
	resp, err := http.Get(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status %d body %s", resp.StatusCode, body)
	}
	return body
}

func listKeys(t *testing.T, ctx context.Context, exp *stack.ExportService, prefix string) []string {
	t.Helper()
	keys, err := exp.List(ctx, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func writeWorkerProfile(t *testing.T, ctx context.Context, s *stack.Stack, docID string, versionID []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+s.Export.Pprof+"/debug/pprof/profile?seconds=12", nil)
	if err != nil {
		t.Fatal(err)
	}
	var body []byte
	var profErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			profErr = err
			return
		}
		defer resp.Body.Close()
		body, profErr = io.ReadAll(resp.Body)
		if profErr == nil && resp.StatusCode != http.StatusOK {
			profErr = fmt.Errorf("pprof status %d", resp.StatusCode)
		}
	}()
	time.Sleep(200 * time.Millisecond)
	job := requestExport(t, ctx, s, docID, versionID, "pdf", t.Name()+"/profile", s.Owner())
	waitReady(t, ctx, s, job.GetId(), s.Owner(), 30*time.Second)
	<-done
	if profErr != nil {
		t.Errorf("worker profile: %v", profErr)
		return
	}
	path := "/tmp/collab-export-worker.pprof"
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Error(err)
		return
	}
	t.Logf("median exceeds 8s, wrote worker CPU profile %s (%d bytes)", path, len(body))
}

func fixture100() string {
	var b strings.Builder
	b.Grow(4600 * 10)
	for i := 1; i <= 4600; i++ {
		if i > 1 {
			b.WriteByte('\n')
		}
		fmt.Fprintf(&b, "line %04d", i)
	}
	return b.String()
}
