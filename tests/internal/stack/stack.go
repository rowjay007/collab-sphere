package stack

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
	"runtime"
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
	presencev1 "github.com/rowjay007/collab-sphere/gen/presence/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

var (
	buildOnce sync.Once
	binDir    string
	buildErr  error
)

func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func binary(t *testing.T, name string) string {
	t.Helper()
	buildOnce.Do(func() {
		binDir, buildErr = os.MkdirTemp("", "collab-e2e-bin")
		if buildErr != nil {
			return
		}
		root := repoRoot()
		for name, pkg := range map[string]string{
			"document-service": "./services/document-service/cmd/document-service",
			"collab-hub":       "./services/collab-hub/cmd/collab-hub",
			"presence-service": "./services/presence-service/cmd/presence-service",
		} {
			args := append(append([]string{"build"}, buildFlags...), "-o", filepath.Join(binDir, name), pkg)
			cmd := exec.Command("go", args...)
			cmd.Dir = root
			if out, err := cmd.CombinedOutput(); err != nil {
				buildErr = fmt.Errorf("build %s: %v\n%s", name, err, out)
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return filepath.Join(binDir, name)
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

func (p *process) kill() {
	_ = p.cmd.Process.Kill()
	<-p.done
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

type hubInstance struct {
	id   string
	http string
	proc *process
}

type Stack struct {
	t           *testing.T
	dbURL       string
	redisURL    string
	hubRedisURL string
	redisProxy  *tcpProxy
	grpc        string
	presence    string
	presenceTTL time.Duration
	docsvc      *process
	hubs        []*hubInstance
	Docs        documentv1.DocumentServiceClient
}

type Option func(*config)

type config struct {
	hubs        int
	presenceTTL time.Duration
	redisProxy  bool
}

// WithRedisProxy routes the hubs' Redis connections, but not
// presence-service's, through a proxy that CutRedis and RestoreRedis control.
func WithRedisProxy() Option {
	return func(c *config) { c.redisProxy = true }
}

// WithHubs runs n collab-hub instances. More than one shares ops over the
// Redis at COLLAB_TEST_REDIS_URL, and the test skips when it is unset.
func WithHubs(n int) Option {
	return func(c *config) { c.hubs = n }
}

// WithPresenceTTL sets presence-service's TTL to d, its sweep to d/10 and
// the hubs' heartbeat to d/4.
func WithPresenceTTL(d time.Duration) Option {
	return func(c *config) { c.presenceTTL = d }
}

// New starts document-service and the hubs. With Redis, which more than one
// hub needs, it also starts presence-service and points every hub at it;
// a single hub without Redis keeps presence in process.
func New(t *testing.T, opts ...Option) *Stack {
	t.Helper()
	cfg := config{hubs: 1}
	for _, opt := range opts {
		opt(&cfg)
	}
	var redisURL string
	if cfg.hubs > 1 {
		if redisURL = os.Getenv("COLLAB_TEST_REDIS_URL"); redisURL == "" {
			t.Skip("COLLAB_TEST_REDIS_URL not set")
		}
	}
	s := &Stack{t: t, dbURL: freshDatabase(t), redisURL: redisURL, hubRedisURL: redisURL, grpc: freeAddr(t), presenceTTL: cfg.presenceTTL}
	if cfg.redisProxy && redisURL != "" {
		u, err := url.Parse(redisURL)
		if err != nil {
			t.Fatal(err)
		}
		s.redisProxy = newTCPProxy(t, u.Host)
		u.Host = s.redisProxy.addr
		s.hubRedisURL = u.String()
	}
	for i := range cfg.hubs {
		s.hubs = append(s.hubs, &hubInstance{id: fmt.Sprintf("hub-%d", i), http: freeAddr(t)})
	}
	conn, err := grpc.NewClient(s.grpc, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	s.Docs = documentv1.NewDocumentServiceClient(conn)
	s.StartDocumentService()
	if redisURL != "" {
		s.startPresenceService()
	}
	for i := range s.hubs {
		s.StartHub(i)
	}
	return s
}

func (s *Stack) startPresenceService() {
	s.presence = freeAddr(s.t)
	env := []string{"REDIS_URL=" + s.redisURL, "GRPC_ADDR=" + s.presence}
	if s.presenceTTL > 0 {
		env = append(env, "PRESENCE_TTL="+s.presenceTTL.String(), "PRESENCE_SWEEP="+(s.presenceTTL/10).String())
	}
	start(s.t, "presence-service", binary(s.t, "presence-service"), env...)
	conn, err := grpc.NewClient(s.presence, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.t.Fatal(err)
	}
	defer conn.Close()
	client := presencev1.NewPresenceServiceClient(conn)
	waitFor(s.t, "presence-service", func(ctx context.Context) error {
		_, err := client.List(ctx, &presencev1.ListRequest{DocumentId: uuid.NewString()})
		return err
	})
}

func (s *Stack) StartDocumentService() {
	s.docsvc = start(s.t, "document-service", binary(s.t, "document-service"), "DATABASE_URL="+s.dbURL, "GRPC_ADDR="+s.grpc)
	waitFor(s.t, "document-service", func(ctx context.Context) error {
		_, err := s.Docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: uuid.NewString()})
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("not ready: %v", err)
	})
}

// StartHub starts hub i on its own port and HUB_ID, which stay the same
// across restarts.
func (s *Stack) StartHub(i int) {
	h := s.hubs[i]
	env := []string{"HTTP_ADDR=" + h.http, "DOCUMENT_SERVICE_ADDR=" + s.grpc, "HUB_ID=" + h.id}
	if s.hubRedisURL != "" {
		env = append(env, "REDIS_URL="+s.hubRedisURL)
	}
	if s.presence != "" {
		env = append(env, "PRESENCE_SERVICE_ADDR="+s.presence)
	}
	if s.presenceTTL > 0 {
		env = append(env, "PRESENCE_HEARTBEAT="+(s.presenceTTL/4).String())
	}
	h.proc = start(s.t, h.id, binary(s.t, "collab-hub"), env...)
	waitFor(s.t, h.id, func(ctx context.Context) error {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+h.http+"/healthz", nil)
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

func (s *Stack) StopDocumentService() {
	s.docsvc.stop()
}

// StopHub sends hub i SIGTERM and waits for it to drain and exit.
func (s *Stack) StopHub(i int) {
	s.hubs[i].proc.stop()
}

// CutRedis closes every hub connection to Redis and refuses new ones until
// RestoreRedis. It needs WithRedisProxy.
func (s *Stack) CutRedis() { s.redisProxy.cut() }

func (s *Stack) RestoreRedis() { s.redisProxy.restore() }

// KillHub sends hub i SIGKILL, so it drains nothing.
func (s *Stack) KillHub(i int) {
	s.hubs[i].proc.kill()
}

func (s *Stack) Hubs() int { return len(s.hubs) }

func (s *Stack) CreateDocument(ctx context.Context) string {
	s.t.Helper()
	doc, err := s.Docs.CreateDocument(ctx, &documentv1.CreateDocumentRequest{Title: s.t.Name(), OwnerId: "e2e"})
	if err != nil {
		s.t.Fatal(err)
	}
	return doc.GetId()
}

// HubURL is hub i's WebSocket base URL.
func (s *Stack) HubURL(i int) string { return "ws://" + s.hubs[i].http }

func (s *Stack) HubURLs() []string {
	urls := make([]string, len(s.hubs))
	for i := range s.hubs {
		urls[i] = s.HubURL(i)
	}
	return urls
}

// Dial joins hub 0 without auto-reconnect.
func (s *Stack) Dial(ctx context.Context, docID string, client crdt.ClientID) *collabclient.Client {
	s.t.Helper()
	return s.DialHub(ctx, docID, client, 0)
}

// DialHub joins hub i without auto-reconnect.
func (s *Stack) DialHub(ctx context.Context, docID string, client crdt.ClientID, i int) *collabclient.Client {
	s.t.Helper()
	return s.dial(ctx, docID, client, collabclient.Options{URLs: []string{s.HubURL(i)}})
}

// DialAuto joins hub i with auto-reconnect rotating through every hub after it.
func (s *Stack) DialAuto(ctx context.Context, docID string, client crdt.ClientID, i int) *collabclient.Client {
	s.t.Helper()
	urls := s.HubURLs()
	return s.dial(ctx, docID, client, collabclient.Options{
		URLs:          append(urls[i:], urls[:i]...),
		AutoReconnect: true,
	})
}

func (s *Stack) dial(ctx context.Context, docID string, client crdt.ClientID, opts collabclient.Options) *collabclient.Client {
	s.t.Helper()
	c, err := collabclient.DialWith(ctx, docID, client, opts)
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
