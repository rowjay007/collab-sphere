package stack

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
)

var (
	versionBuildOnce sync.Once
	versionBin       string
	versionBuildErr  error
)

func versionBinary(t *testing.T) string {
	t.Helper()
	versionBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "collab-e2e-version")
		if err != nil {
			versionBuildErr = err
			return
		}
		versionBin = filepath.Join(dir, "version-service")
		args := append(append([]string{"build"}, buildFlags...), "-o", versionBin, "./services/version-service/cmd/version-service")
		cmd := exec.Command("go", args...)
		cmd.Dir = repoRoot()
		if out, err := cmd.CombinedOutput(); err != nil {
			versionBuildErr = fmt.Errorf("build version-service: %v\n%s", err, out)
		}
	})
	if versionBuildErr != nil {
		t.Fatal(versionBuildErr)
	}
	return versionBin
}

// VersionService is a version-service process with its own database, which
// survives Kill and Start.
type VersionService struct {
	s      *Stack
	addr   string
	dbURL  string
	sweep  time.Duration
	proc   *process
	db     *pgxpool.Pool
	Client versionv1.VersionServiceClient
	// Env is extra process environment (VERSION_CRASH_AT=after_append).
	Env []string
}

// StartVersionService starts version-service against the stack's
// document-service. A sweep of zero disables the sweeper, so only RPCs
// materialize.
func StartVersionService(s *Stack, sweep time.Duration) *VersionService {
	s.t.Helper()
	v := &VersionService{s: s, addr: freeAddr(s.t), dbURL: freshDatabase(s.t), sweep: sweep}
	conn, err := grpc.NewClient(v.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { conn.Close() })
	v.Client = versionv1.NewVersionServiceClient(conn)
	v.Start()
	if v.db, err = pgxpool.New(context.Background(), v.dbURL); err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(v.db.Close)
	return v
}

func (v *VersionService) Start() {
	v.s.t.Helper()
	env := []string{
		"DATABASE_URL=" + v.dbURL,
		"GRPC_ADDR=" + v.addr,
		"DOCUMENT_ADDR=" + v.s.grpc,
		"SWEEP_INTERVAL=" + v.sweep.String(),
	}
	v.proc = start(v.s.t, "version-service", versionBinary(v.s.t), append(env, v.Env...)...)
	waitFor(v.s.t, "version-service", func(ctx context.Context) error {
		_, err := v.Client.ListVersions(ctx, &versionv1.ListVersionsRequest{DocId: uuid.NewString()})
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("not ready: %v", err)
	})
}

// Kill sends SIGKILL, so nothing in flight commits.
func (v *VersionService) Kill() { v.proc.kill() }

// DB is version-service's own database, for assertions that must not touch
// a document through an RPC.
func (v *VersionService) DB() *pgxpool.Pool { return v.db }
