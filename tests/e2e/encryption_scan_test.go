package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commentv1 "github.com/rowjay007/collab-sphere/gen/comment/v1"
	crdtv1 "github.com/rowjay007/collab-sphere/gen/crdt/v1"
	exportv1 "github.com/rowjay007/collab-sphere/gen/export/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

func TestEncryptedStorage(t *testing.T) {
	s := stack.New(t, stack.WithExport())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cs := stack.StartCommentService(s, s.Versions)

	marker := newMarker(t)
	docID := createEncrypted(t, ctx, s)
	doc := uuid.MustParse(docID)
	owner := memberKey(t, ctx, s, s.Owner())
	key := randomKey(t)
	if _, err := s.Perm.Client.PublishWraps(s.AuthCtx(ctx, s.Owner()), &permissionv1.PublishWrapsRequest{
		DocId: docID, RequestId: t.Name() + "-open", Wraps: []*permissionv1.KeyWrap{owner.wrap(t, doc, 1, key)},
	}); err != nil {
		t.Fatal(err)
	}
	var epoch atomic.Uint32
	epoch.Store(1)
	keys := map[uint32][]byte{1: mustUnwrap(t, ctx, s, doc, owner, 1)}
	c := dialEncrypted(t, ctx, s, docID, s.Owner(), 0, &epoch, keys)
	insertText(t, c, marker)
	if err := c.WaitAcked(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{DocId: docID, Name: "sealed", RequestId: t.Name() + "-cp"}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Export.Client.RequestExport(s.AuthCtx(ctx, s.Owner()), &exportv1.RequestExportRequest{DocId: docID, Format: "md", RequestId: t.Name() + "-export"})
	if status.Code(err) != codes.FailedPrecondition || linkReason(err) != "ENCRYPTED" {
		t.Fatalf("export: %v", err)
	}
	var anchor crdt.ID
	c.Read(func(r *crdt.Replica) {
		anchor, err = r.IDAt(0)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CreateThread(s.AuthCtx(ctx, s.Owner()), &commentv1.CreateThreadRequest{
		DocumentId: docID,
		Anchor:     &commentv1.Anchor{Start: opID(anchor), End: opID(anchor)},
		Author:     s.Owner(),
		Body:       "anchor only",
		RequestId:  t.Name() + "-thread",
	}); err != nil {
		t.Fatal(err)
	}

	if hit, leak := scanStack(t, ctx, s, marker); hit != "" || leak != "" {
		t.Fatalf("encrypted storage hit %q leak %q", hit, leak)
	}

	plain := s.CreateDocument(ctx)
	plainClient := s.Dial(ctx, plain, 0)
	insertText(t, plainClient, marker)
	if got := stack.Settle(t, ctx, s.Docs, plain, []*collabclient.Client{plainClient}); got != marker {
		t.Fatalf("plain text %q", got)
	}
	if _, err := s.Versions.Client.Checkpoint(ctx, &versionv1.CheckpointRequest{DocId: plain, Name: "plain", RequestId: t.Name() + "-plain"}); err != nil {
		t.Fatal(err)
	}
	hit, _ := scanStack(t, ctx, s, marker)
	if hit == "" {
		t.Fatal("scan missed the plain document marker")
	}
}

func newMarker(t *testing.T) string {
	t.Helper()
	var raw [23]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(raw[:]) + "ZZ"
}

type storedRune struct {
	seq uint64
	r   rune
}

func scanStack(t *testing.T, ctx context.Context, s *stack.Stack, marker string) (hit, leak string) {
	t.Helper()
	for _, url := range s.PostgresURLs() {
		h, l := scanPostgres(t, ctx, url, marker)
		if hit == "" {
			hit = h
		}
		if leak == "" {
			leak = l
		}
	}
	redisURL := s.RedisURL()
	if redisURL == "" {
		redisURL = os.Getenv("COLLAB_TEST_REDIS_URL")
	}
	if redisURL != "" {
		if h := scanRedis(t, ctx, redisURL, marker); hit == "" {
			hit = h
		}
	}
	if s.Export != nil {
		h := scanMinIO(t, ctx, s, marker)
		if hit == "" {
			hit = h
		}
	}
	return hit, leak
}

func scanPostgres(t *testing.T, ctx context.Context, url, marker string) (string, string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `
		SELECT table_name, column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND data_type IN ('text', 'character varying', 'json', 'jsonb', 'bytea')
		ORDER BY table_name, ordinal_position`)
	if err != nil {
		t.Fatal(err)
	}
	type col struct{ table, name, kind string }
	var cols []col
	for rows.Next() {
		var c col
		if err := rows.Scan(&c.table, &c.name, &c.kind); err != nil {
			t.Fatal(err)
		}
		cols = append(cols, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	extra := map[string][2]bool{}
	for _, c := range cols {
		if _, ok := extra[c.table]; ok {
			continue
		}
		var docID, seq bool
		if err := conn.QueryRow(ctx, `
			SELECT
				EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'document_id'),
				EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'seq')`,
			c.table).Scan(&docID, &seq); err != nil {
			t.Fatal(err)
		}
		extra[c.table] = [2]bool{docID, seq}
	}
	groups := map[string][]storedRune{}
	var hit, leak string
	for _, c := range cols {
		h, l := scanColumn(ctx, conn, c.table, c.name, c.kind, extra[c.table][0], extra[c.table][1], marker, groups)
		if hit == "" {
			hit = h
		}
		if leak == "" {
			leak = l
		}
	}
	for id, ops := range groups {
		sort.Slice(ops, func(i, j int) bool { return ops[i].seq < ops[j].seq })
		var b strings.Builder
		for _, op := range ops {
			b.WriteRune(op.r)
		}
		if strings.Contains(b.String(), marker) && hit == "" {
			hit = url + " runes " + id
		}
	}
	return hit, leak
}

func scanColumn(ctx context.Context, conn *pgx.Conn, table, column, kind string, hasDoc, hasSeq bool, marker string, groups map[string][]storedRune) (string, string) {
	docSel, seqSel := "''", "0::bigint"
	if hasDoc {
		docSel = pgx.Identifier{"document_id"}.Sanitize() + "::text"
	}
	if hasSeq {
		seqSel = pgx.Identifier{"seq"}.Sanitize() + "::bigint"
	}
	value := pgx.Identifier{column}.Sanitize()
	if kind != "bytea" {
		value += "::text"
	}
	q := fmt.Sprintf("SELECT %s, %s, %s FROM %s", docSel, seqSel, value, pgx.Identifier{table}.Sanitize())
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return "", ""
	}
	defer rows.Close()
	var hit, leak string
	for rows.Next() {
		var docID string
		var seq int64
		if kind == "bytea" {
			var raw []byte
			if err := rows.Scan(&docID, &seq, &raw); err != nil {
				continue
			}
			h, l := inspectBytes(table+"."+column, docID, uint64(seq), raw, marker, groups)
			if hit == "" {
				hit = h
			}
			if leak == "" {
				leak = l
			}
			continue
		}
		var text string
		if err := rows.Scan(&docID, &seq, &text); err != nil {
			continue
		}
		where := table + "." + column
		if strings.Contains(text, marker) && hit == "" {
			hit = where
		}
		if h, l := inspectText(where, text, marker); hit == "" || leak == "" {
			if hit == "" {
				hit = h
			}
			if leak == "" {
				leak = l
			}
		}
	}
	return hit, leak
}

func inspectText(where, text, marker string) (string, string) {
	var hit, leak string
	var snap crdtv1.Snapshot
	if protojson.Unmarshal([]byte(text), &snap) == nil && (len(snap.GetNodes()) > 0 || snap.GetEncrypted()) {
		if h, l := inspectSnapshot(where, &snap, marker); h != "" || l != "" {
			return h, l
		}
	}
	var op crdtv1.Operation
	if protojson.Unmarshal([]byte(text), &op) == nil && op.GetInsert() != nil {
		h, l := inspectOp(where, "", 0, &op, marker, nil)
		return h, l
	}
	for _, field := range base64Fields(text) {
		if bytes.Contains(field, []byte(marker)) {
			hit = where + " base64"
		}
	}
	return hit, leak
}

func inspectBytes(where, docID string, seq uint64, raw []byte, marker string, groups map[string][]storedRune) (string, string) {
	var hit, leak string
	if bytes.Contains(raw, []byte(marker)) {
		hit = where
	}
	var op crdtv1.Operation
	if proto.Unmarshal(raw, &op) == nil && op.GetId() != nil {
		h, l := inspectOp(where, docID, seq, &op, marker, groups)
		if hit == "" {
			hit = h
		}
		leak = l
	}
	if doc, err := crdt.FromCanonical(raw); err == nil {
		if text, err := doc.Text(); err == nil && strings.Contains(text, marker) && hit == "" {
			hit = where + " canonical"
		}
	}
	return hit, leak
}

func inspectSnapshot(where string, snap *crdtv1.Snapshot, marker string) (string, string) {
	for _, n := range snap.GetNodes() {
		if len(n.GetCipher()) > 0 && n.GetValue() != 0 {
			return "", where + " node rune"
		}
	}
	doc, err := crdt.FromSnapshot(snap)
	if err != nil {
		return "", ""
	}
	text, err := doc.Text()
	if err == nil && strings.Contains(text, marker) {
		return where + " snapshot", ""
	}
	return "", ""
}

func inspectOp(where, docID string, seq uint64, op *crdtv1.Operation, marker string, groups map[string][]storedRune) (string, string) {
	ins := op.GetInsert()
	if ins == nil {
		return "", ""
	}
	if len(ins.GetCipher()) >= 17 && len(ins.GetCipher()) <= 20 && ins.GetValue() != 0 {
		return "", where + " insert rune"
	}
	if groups != nil && len(ins.GetCipher()) == 0 && ins.GetValue() != 0 {
		if docID == "" {
			docID = where
		}
		groups[docID] = append(groups[docID], storedRune{seq: seq, r: rune(ins.GetValue())})
	}
	if bytes.Contains(ins.GetCipher(), []byte(marker)) {
		return where + " cipher", ""
	}
	return "", ""
}

func base64Fields(text string) [][]byte {
	var out [][]byte
	for _, part := range strings.FieldsFunc(text, func(r rune) bool {
		return r == '"' || r == ',' || r == ':' || r == ' ' || r == '\n'
	}) {
		if len(part) < 16 {
			continue
		}
		if decoded, err := base64.StdEncoding.DecodeString(part); err == nil {
			out = append(out, decoded)
		}
	}
	return out
}

func scanRedis(t *testing.T, ctx context.Context, url, marker string) string {
	t.Helper()
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	var cursor uint64
	var hit string
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "*", 200).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if strings.Contains(key, marker) && hit == "" {
				hit = "redis key " + key
			}
			dump, err := rdb.Dump(ctx, key).Bytes()
			if err != nil || len(dump) == 0 {
				continue
			}
			if h, _ := inspectBytes("redis "+key, "", 0, dump, marker, nil); hit == "" {
				hit = h
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	return hit
}

func scanMinIO(t *testing.T, ctx context.Context, s *stack.Stack, marker string) string {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(s.Export.Region),
		awsconfig.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: s.Export.AccessKey, SecretAccessKey: s.Export.SecretKey, Source: "static"}, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := s.Export.Endpoint
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})
	buckets, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range buckets.Buckets {
		if b.Name == nil {
			continue
		}
		var token *string
		for {
			page, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: b.Name, ContinuationToken: token})
			if err != nil {
				break
			}
			for _, obj := range page.Contents {
				if obj.Key == nil {
					continue
				}
				if strings.Contains(*obj.Key, marker) {
					return "minio key " + *b.Name + "/" + *obj.Key
				}
				body, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: b.Name, Key: obj.Key})
				if err != nil {
					continue
				}
				raw, err := io.ReadAll(body.Body)
				body.Body.Close()
				if err != nil {
					continue
				}
				if h, _ := inspectBytes("minio "+*b.Name+"/"+*obj.Key, "", 0, raw, marker, nil); h != "" {
					return h
				}
			}
			if page.IsTruncated == nil || !*page.IsTruncated {
				break
			}
			token = page.NextContinuationToken
		}
	}
	return ""
}
