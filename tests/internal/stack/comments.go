package stack

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
)

var (
	commentBuildOnce sync.Once
	commentBin       string
	commentBuildErr  error
)

func commentBinary(t *testing.T) string {
	t.Helper()
	commentBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "collab-e2e-comment")
		if err != nil {
			commentBuildErr = err
			return
		}
		commentBin = filepath.Join(dir, "comment-service")
		args := append(append([]string{"build"}, buildFlags...), "-o", commentBin, "./services/comment-service/cmd/comment-service")
		cmd := exec.Command("go", args...)
		cmd.Dir = repoRoot()
		if out, err := cmd.CombinedOutput(); err != nil {
			commentBuildErr = fmt.Errorf("build comment-service: %v\n%s", err, out)
		}
	})
	if commentBuildErr != nil {
		t.Fatal(commentBuildErr)
	}
	return commentBin
}

// StartCommentService starts comment-service against the stack's
// document-service and vs, on its own database.
func StartCommentService(s *Stack, vs *VersionService) commentv1.CommentServiceClient {
	s.t.Helper()
	addr := freeAddr(s.t)
	env := []string{
		"DATABASE_URL=" + freshDatabase(s.t),
		"GRPC_ADDR=" + addr,
		"DOCUMENT_ADDR=" + s.grpc,
		"VERSION_ADDR=" + vs.addr,
	}
	start(s.t, "comment-service", commentBinary(s.t), env...)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { conn.Close() })
	client := commentv1.NewCommentServiceClient(conn)
	waitFor(s.t, "comment-service", func(ctx context.Context) error {
		_, err := client.ListThreads(ctx, &commentv1.ListThreadsRequest{DocumentId: uuid.NewString()})
		if status.Code(err) == codes.NotFound || status.Code(err) == codes.InvalidArgument || err == nil {
			return nil
		}
		return err
	})
	return client
}
