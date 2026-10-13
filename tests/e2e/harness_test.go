package e2e

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

var (
	buildOnce sync.Once
	binDir    string
	buildErr  error
)

func binaries(t *testing.T) (docsvc, hub string) {
	t.Helper()
	buildOnce.Do(func() {
		binDir, buildErr = os.MkdirTemp("", "collab-e2e-bin")
		if buildErr != nil {
			return
		}
		for name, pkg := range map[string]string{
			"document-service": "./services/document-service/cmd/document-service",
			"collab-hub":       "./services/collab-hub/cmd/collab-hub",
		} {
			cmd := exec.Command("go", "build", "-race", "-o", filepath.Join(binDir, name), pkg)
			cmd.Dir = filepath.Join("..", "..")
			if out, err := cmd.CombinedOutput(); err != nil {
				buildErr = fmt.Errorf("build %s: %v\n%s", name, err, out)
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return filepath.Join(binDir, "document-service"), filepath.Join(binDir, "collab-hub")
}

func freshDatabase(t *testing.T) string {
	t.Helper()
	admin := os.Getenv("COLLAB_TEST_DATABASE_URL")
	if admin == "" {
		t.Skip("COLLAB_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("e2e_%d", rand.Uint64())
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer conn.Close(ctx)
		if _, err := conn.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database: %v", err)
		}
	})
	u, err := url.Parse(admin)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

type process struct {
	t    *testing.T
	name string
	cmd  *exec.Cmd
	out  *lockedBuffer
	done chan struct{}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func start(t *testing.T, name, bin string, env ...string) *process {
	t.Helper()
	p := &process{t: t, name: name, cmd: exec.Command(bin), out: &lockedBuffer{}, done: make(chan struct{})}
	p.cmd.Env = append(os.Environ(), env...)
	p.cmd.Stdout = p.out
	p.cmd.Stderr = p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		p.stop()
		if t.Failed() {
			t.Logf("%s output:\n%s", name, p.out.String())
		}
	})
	return p
}

func (p *process) stop() {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		p.t.Errorf("%s did not stop on SIGTERM", p.name)
	}
}

type stack struct {
	t       *testing.T
	dbURL   string
	grpc    string
	http    string
	docsvc  *process
	hub     *process
	docs    documentv1.DocumentServiceClient
	hubBase string
}

func newStack(t *testing.T) *stack {
	t.Helper()
	s := &stack{t: t, dbURL: freshDatabase(t), grpc: freeAddr(t), http: freeAddr(t)}
	s.hubBase = "ws://" + s.http
	conn, err := grpc.NewClient(s.grpc, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	s.docs = documentv1.NewDocumentServiceClient(conn)
	s.startDocumentService()
	s.startHub()
	return s
}

func (s *stack) startDocumentService() {
	docsvc, _ := binaries(s.t)
	s.docsvc = start(s.t, "document-service", docsvc, "DATABASE_URL="+s.dbURL, "GRPC_ADDR="+s.grpc)
	waitFor(s.t, "document-service", func(ctx context.Context) error {
		_, err := s.docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: uuid.NewString()})
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("not ready: %v", err)
	})
}

func (s *stack) startHub() {
	_, hub := binaries(s.t)
	s.hub = start(s.t, "collab-hub", hub, "HTTP_ADDR="+s.http, "DOCUMENT_SERVICE_ADDR="+s.grpc)
	waitFor(s.t, "collab-hub", func(ctx context.Context) error {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+s.http+"/healthz", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("healthz status %d", resp.StatusCode)
		}
		return nil
	})
}

func (s *stack) createDocument(ctx context.Context) string {
	s.t.Helper()
	doc, err := s.docs.CreateDocument(ctx, &documentv1.CreateDocumentRequest{Title: s.t.Name(), OwnerId: "e2e"})
	if err != nil {
		s.t.Fatal(err)
	}
	return doc.GetId()
}

func (s *stack) dial(ctx context.Context, docID string, client crdt.ClientID) *collabclient.Client {
	s.t.Helper()
	c, err := collabclient.Dial(ctx, s.hubBase, docID, client)
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { c.Close() })
	return c
}

func waitFor(t *testing.T, what string, ready func(ctx context.Context) error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		last = ready(ctx)
		cancel()
		if last == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s never became ready: %v", what, last)
}
