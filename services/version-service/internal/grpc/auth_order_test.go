package versiongrpc

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/dag"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/history"
	"github.com/rowjay007/collab-sphere/services/version-service/internal/store"
)

type denyPerm struct{}

func (denyPerm) Require(context.Context, string, string, permissionv1.Role) error {
	return status.Error(codes.PermissionDenied, "permission denied")
}

type probeHistory struct {
	known    uuid.UUID
	version  dag.NodeID
	hash     dag.Hash
	versions int
	bodies   int
	blobs    int
}

func (p *probeHistory) Versions(_ context.Context, id uuid.UUID, _ int64, _ int) ([]store.Version, error) {
	p.versions++
	if id != p.known {
		return nil, history.ErrNotFound
	}
	return []store.Version{{Node: dag.Node{Doc: id}}}, nil
}

func (p *probeHistory) Version(_ context.Context, id dag.NodeID) (store.Version, error) {
	p.bodies++
	if id != p.version {
		return store.Version{}, history.ErrNotFound
	}
	return store.Version{Node: dag.Node{Doc: p.known}}, nil
}

func (p *probeHistory) DocOf(_ context.Context, id dag.NodeID) (uuid.UUID, error) {
	if id != p.version {
		return uuid.UUID{}, history.ErrNotFound
	}
	return p.known, nil
}

func (p *probeHistory) DocsForBlob(_ context.Context, hash dag.Hash) ([]uuid.UUID, error) {
	if hash != p.hash {
		return nil, nil
	}
	return []uuid.UUID{p.known}, nil
}

func (p *probeHistory) Blob(_ context.Context, hash dag.Hash) ([]byte, error) {
	p.blobs++
	if hash != p.hash {
		return nil, history.ErrNotFound
	}
	return []byte("blob"), nil
}

func (p *probeHistory) Head(context.Context, uuid.UUID) (store.Version, error) {
	panic("unused")
}
func (p *probeHistory) Checkpoint(context.Context, uuid.UUID, string, string) (store.Version, error) {
	panic("unused")
}
func (p *probeHistory) Branch(context.Context, dag.NodeID, string, string) (uuid.UUID, store.Version, error) {
	panic("unused")
}
func (p *probeHistory) Diff(context.Context, dag.NodeID, dag.NodeID) ([]crdt.Span, error) {
	panic("unused")
}
func (p *probeHistory) Merge(context.Context, uuid.UUID, uuid.UUID, string) (history.MergeResult, error) {
	panic("unused")
}
func (p *probeHistory) Restore(context.Context, uuid.UUID, dag.NodeID, string) (store.Version, error) {
	panic("unused")
}
func (p *probeHistory) Frontier(context.Context, uuid.UUID) (map[uuid.UUID]uint64, error) {
	panic("unused")
}

func TestReadRPCsSameCodeForMissingAndForbidden(t *testing.T) {
	known := uuid.New()
	var version dag.NodeID
	version[0] = 1
	var missingVersion dag.NodeID
	missingVersion[0] = 2
	var hash dag.Hash
	hash[0] = 3
	var missingHash dag.Hash
	missingHash[0] = 4
	h := &probeHistory{known: known, version: version, hash: hash}
	s := &Server{history: h, perm: denyPerm{}}
	user := auth.WithSubject(context.Background(), "ada")

	_, missingList := s.ListVersions(user, &versionv1.ListVersionsRequest{DocId: uuid.NewString()})
	_, forbiddenList := s.ListVersions(user, &versionv1.ListVersionsRequest{DocId: known.String()})
	if status.Code(missingList) != codes.PermissionDenied || status.Code(forbiddenList) != codes.PermissionDenied {
		t.Fatalf("list missing %v forbidden %v", missingList, forbiddenList)
	}
	if h.versions != 0 {
		t.Fatalf("list looked up %d version pages", h.versions)
	}

	_, missingVer := s.GetVersion(user, &versionv1.GetVersionRequest{Id: missingVersion[:]})
	_, forbiddenVer := s.GetVersion(user, &versionv1.GetVersionRequest{Id: version[:]})
	if status.Code(missingVer) != codes.PermissionDenied || status.Code(forbiddenVer) != codes.PermissionDenied {
		t.Fatalf("version missing %v forbidden %v", missingVer, forbiddenVer)
	}
	if h.bodies != 0 {
		t.Fatalf("get version read %d bodies", h.bodies)
	}

	_, missingBlob := s.GetBlob(user, &versionv1.GetBlobRequest{Hash: missingHash[:]})
	_, forbiddenBlob := s.GetBlob(user, &versionv1.GetBlobRequest{Hash: hash[:]})
	if status.Code(missingBlob) != codes.PermissionDenied || status.Code(forbiddenBlob) != codes.PermissionDenied {
		t.Fatalf("blob missing %v forbidden %v", missingBlob, forbiddenBlob)
	}
	if h.blobs != 0 {
		t.Fatalf("get blob read %d payloads", h.blobs)
	}

	_, err := s.GetBlob(auth.WithInternal(context.Background()), &versionv1.GetBlobRequest{Hash: missingHash[:]})
	if status.Code(err) != codes.NotFound || h.blobs != 1 {
		t.Fatalf("internal blob %v calls %d", err, h.blobs)
	}
}
