package stack

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
)

var (
	permissionBuildOnce sync.Once
	permissionBin       string
	permissionBuildErr  error
)

func permissionBinary(t *testing.T) string {
	t.Helper()
	permissionBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "collab-e2e-permission")
		if err != nil {
			permissionBuildErr = err
			return
		}
		permissionBin = filepath.Join(dir, "permission-service")
		args := append(append([]string{"build"}, buildFlags...), "-o", permissionBin, "./services/permission-service/cmd/permission-service")
		cmd := exec.Command("go", args...)
		cmd.Dir = repoRoot()
		if out, err := cmd.CombinedOutput(); err != nil {
			permissionBuildErr = fmt.Errorf("build permission-service: %v\n%s", err, out)
		}
	})
	if permissionBuildErr != nil {
		t.Fatal(permissionBuildErr)
	}
	return permissionBin
}

// PermissionService is a permission-service process with its own database, which
// survives Kill and Start.
type PermissionService struct {
	s             *Stack
	addr          string
	dbURL         string
	proc          *process
	db            *pgxpool.Pool
	Client        permissionv1.PermissionServiceClient
	PublicKey     string
	PrivateKey    string
	InternalToken string
	sharePriv     string
}

// StartPermissionService starts permission-service on its own database.
func StartPermissionService(s *Stack) *PermissionService {
	s.t.Helper()
	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		s.t.Fatal(err)
	}
	_, share, err := auth.GenerateKeypair()
	if err != nil {
		s.t.Fatal(err)
	}
	secret := randomInternal(s.t)
	p := &PermissionService{
		s:             s,
		addr:          freeAddr(s.t),
		dbURL:         freshDatabase(s.t),
		PublicKey:     auth.EncodeKey(pub),
		PrivateKey:    auth.EncodeKey(priv.Seed()),
		InternalToken: secret,
		sharePriv:     auth.EncodeKey(share.Seed()),
	}
	conn, err := grpc.NewClient(p.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { conn.Close() })
	p.Client = permissionv1.NewPermissionServiceClient(conn)
	p.Start()
	if p.db, err = pgxpool.New(context.Background(), p.dbURL); err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(p.db.Close)
	return p
}

func (p *PermissionService) Start() {
	p.s.t.Helper()
	env := []string{
		"DATABASE_URL=" + p.dbURL,
		"GRPC_ADDR=" + p.addr,
		"AUTH_PUBLIC_KEY=" + p.PublicKey,
		"SHARE_PRIVATE_KEY=" + p.sharePriv,
		"INTERNAL_TOKEN=" + p.InternalToken,
		"DOCUMENT_ADDR=" + p.s.grpc,
	}
	p.proc = start(p.s.t, "permission-service", permissionBinary(p.s.t), env...)
	waitFor(p.s.t, "permission-service", func(ctx context.Context) error {
		_, err := p.Client.PolicyDump(p.Internal(ctx), &permissionv1.PolicyDumpRequest{DocId: uuid.NewString()})
		return err
	})
}

// Kill sends SIGKILL, so nothing in flight commits.
func (p *PermissionService) Kill() { p.proc.kill() }

// DB is permission-service's own database.
func (p *PermissionService) DB() *pgxpool.Pool { return p.db }

// Internal attaches the process internal bearer.
func (p *PermissionService) Internal(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+auth.FormatInternal(p.InternalToken))
}

func randomInternal(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}
