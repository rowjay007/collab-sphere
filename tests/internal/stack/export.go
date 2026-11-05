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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	exportv1 "github.com/rowjay007/collab-sphere/gen/export/v1"
)

var (
	exportBuildOnce sync.Once
	exportBin       string
	exportBuildErr  error
)

func exportBinary(t *testing.T) string {
	t.Helper()
	exportBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "collab-e2e-export")
		if err != nil {
			exportBuildErr = err
			return
		}
		exportBin = filepath.Join(dir, "export-service")
		args := append(append([]string{"build"}, buildFlags...), "-o", exportBin, "./services/export-service/cmd/export-service")
		cmd := exec.Command("go", args...)
		cmd.Dir = repoRoot()
		if out, err := cmd.CombinedOutput(); err != nil {
			exportBuildErr = fmt.Errorf("build export-service: %v\n%s", err, out)
		}
	})
	if exportBuildErr != nil {
		t.Fatal(exportBuildErr)
	}
	return exportBin
}

// ExportService is an export-service process with its own database and bucket.
type ExportService struct {
	s         *Stack
	addr      string
	dbURL     string
	proc      *process
	Client    exportv1.ExportServiceClient
	Endpoint  string
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
	Pprof     string
	// Env is extra process environment, such as EXPORT_CRASH_AFTER.
	Env []string
}

// StartExportService starts export-service against the stack's document,
// version, and permission services. It skips when COLLAB_TEST_S3_URL is unset.
func StartExportService(s *Stack) *ExportService {
	s.t.Helper()
	endpoint := os.Getenv("COLLAB_TEST_S3_URL")
	if endpoint == "" {
		s.t.Skip("COLLAB_TEST_S3_URL not set; start MinIO with docker compose up -d --wait minio and set COLLAB_TEST_S3_URL=http://localhost:9000")
	}
	if s.Versions == nil {
		s.t.Fatal("export-service needs version-service")
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		s.t.Fatal(err)
	}
	e := &ExportService{
		s:         s,
		addr:      freeAddr(s.t),
		dbURL:     freshDatabase(s.t),
		Endpoint:  endpoint,
		Bucket:    "e2e" + hex.EncodeToString(nonce[:]),
		Region:    "us-east-1",
		AccessKey: "collab",
		SecretKey: "collabsecret",
		Pprof:     freeAddr(s.t),
	}
	conn, err := grpc.NewClient(e.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { conn.Close() })
	e.Client = exportv1.NewExportServiceClient(conn)
	e.Start()
	return e
}

func (e *ExportService) Start() {
	e.s.t.Helper()
	env := append([]string{
		"DATABASE_URL=" + e.dbURL,
		"GRPC_ADDR=" + e.addr,
		"DOCUMENT_ADDR=" + e.s.grpc,
		"VERSION_ADDR=" + e.s.Versions.addr,
		"S3_ENDPOINT=" + e.Endpoint,
		"S3_BUCKET=" + e.Bucket,
		"S3_REGION=" + e.Region,
		"S3_ACCESS_KEY=" + e.AccessKey,
		"S3_SECRET_KEY=" + e.SecretKey,
		"EXPORT_PPROF_ADDR=" + e.Pprof,
	}, e.s.authEnv()...)
	e.proc = start(e.s.t, "export-service", exportBinary(e.s.t), append(env, e.Env...)...)
	waitFor(e.s.t, "export-service", func(ctx context.Context) error {
		_, err := e.Client.GetExport(e.s.AuthCtx(ctx, e.s.Owner()), &exportv1.GetExportRequest{Id: uuid.NewString()})
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("not ready: %v", err)
	})
}

// Restart stops the process and starts it again with the current Env.
func (e *ExportService) Restart() {
	e.s.t.Helper()
	if e.proc != nil {
		e.proc.stop()
	}
	e.Start()
}

// WaitExit waits until the process has exited.
func (e *ExportService) WaitExit(timeout time.Duration) {
	e.s.t.Helper()
	if err := e.proc.waitExit(timeout); err != nil {
		e.s.t.Fatal(err)
	}
}

// List returns object keys under prefix in this test's bucket.
func (e *ExportService) List(ctx context.Context, prefix string) ([]string, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(e.Region),
		awsconfig.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: e.AccessKey, SecretAccessKey: e.SecretKey, Source: "static"}, nil
		})),
	)
	if err != nil {
		return nil, err
	}
	endpoint := e.Endpoint
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &e.Bucket, Prefix: &prefix})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(out.Contents))
	for _, obj := range out.Contents {
		if obj.Key != nil {
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}
