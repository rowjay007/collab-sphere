package export

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	documentv1 "github.com/rowjay007/collab-sphere/gen/document/v1"
	permissionv1 "github.com/rowjay007/collab-sphere/gen/permission/v1"
	versionv1 "github.com/rowjay007/collab-sphere/gen/version/v1"
	"github.com/rowjay007/collab-sphere/pkg/access"
	"github.com/rowjay007/collab-sphere/pkg/auth"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/pkg/render"
)

const pollInterval = 50 * time.Millisecond

type Service struct {
	store    *Store
	docs     documentv1.DocumentServiceClient
	versions versionv1.VersionServiceClient
	perms    *access.Client
	bucket   *Bucket
	crash    string
}

func New(poolStore *Store, docs documentv1.DocumentServiceClient, versions versionv1.VersionServiceClient, perms *access.Client, bucket *Bucket, crash string) *Service {
	return &Service{store: poolStore, docs: docs, versions: versions, perms: perms, bucket: bucket, crash: crash}
}

func (s *Service) Request(ctx context.Context, in Request) (Job, error) {
	user, err := subject(ctx)
	if err != nil {
		return Job{}, err
	}
	if in.RequestID == "" || (len(in.VersionID) != 0 && len(in.VersionID) != idLen) {
		return Job{}, ErrInvalidArgument
	}
	if _, err := ParseFormat(string(in.Format)); err != nil {
		return Job{}, err
	}
	existing, err := s.store.ByRequestID(ctx, in.RequestID)
	if err == nil {
		if err := s.perms.Require(ctx, existing.DocID.String(), user, permissionv1.Role_ROLE_VIEWER); err != nil {
			return Job{}, err
		}
		return existing, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Job{}, err
	}
	if err := s.perms.Require(ctx, in.DocID.String(), user, permissionv1.Role_ROLE_VIEWER); err != nil {
		return Job{}, err
	}
	doc, err := s.docs.GetDocument(ctx, &documentv1.GetDocumentRequest{Id: in.DocID.String()})
	if err != nil {
		return Job{}, err
	}
	if doc.GetEncryption() == "encrypted" {
		return Job{}, ErrEncrypted
	}
	nodeID, hash, err := s.resolve(ctx, in)
	if err != nil {
		return Job{}, err
	}
	blob, err := s.blob(ctx, hash)
	if err != nil {
		return Job{}, err
	}
	if _, err := render.Text(blob); err != nil {
		if errors.Is(err, crdt.ErrEncrypted) {
			return Job{}, ErrEncrypted
		}
		return Job{}, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return Job{}, err
	}
	stored, err := s.store.Insert(ctx, Job{
		ID:          id,
		RequestID:   in.RequestID,
		DocID:       in.DocID,
		NodeID:      nodeID,
		ContentHash: hash,
		Format:      in.Format,
		UserID:      user,
		ObjectKey:   objectKey(id),
	})
	if err != nil {
		return Job{}, err
	}
	if stored.DocID != in.DocID {
		if err := s.perms.Require(ctx, stored.DocID.String(), user, permissionv1.Role_ROLE_VIEWER); err != nil {
			return Job{}, err
		}
	}
	return stored, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Job, error) {
	user, err := subject(ctx)
	if err != nil {
		return Job{}, err
	}
	job, err := s.store.ByID(ctx, id)
	if err != nil {
		return Job{}, err
	}
	if err := s.perms.Require(ctx, job.DocID.String(), user, permissionv1.Role_ROLE_VIEWER); err != nil {
		return Job{}, err
	}
	if job.State != Ready {
		return job, nil
	}
	url, err := s.bucket.PresignGet(ctx, job.ObjectKey)
	if err != nil {
		return Job{}, err
	}
	job.URL = url
	return job, nil
}

func (s *Service) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		job, ok, err := s.store.Claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("claim", "err", err)
			if err := sleep(ctx, pollInterval); err != nil {
				return err
			}
			continue
		}
		if !ok {
			if err := sleep(ctx, pollInterval); err != nil {
				return err
			}
			continue
		}
		if job.State == Failed {
			continue
		}
		s.crashAt("claimed")
		if err := s.render(ctx, job); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("render", "job", job.ID.String(), "err", err)
		}
	}
}

func (s *Service) render(ctx context.Context, job Job) error {
	blob, err := s.blob(ctx, job.ContentHash)
	if err != nil {
		return err
	}
	text, err := render.Text(blob)
	if err != nil {
		if errors.Is(err, crdt.ErrEncrypted) {
			return s.store.MarkFailed(ctx, job.ID, err.Error())
		}
		return err
	}
	body, err := renderFormat(job.Format, text)
	if err != nil {
		if errors.Is(err, render.ErrCharset) {
			return s.store.MarkFailed(ctx, job.ID, err.Error())
		}
		return err
	}
	path, err := writeTemp(body)
	if err != nil {
		return err
	}
	defer os.Remove(path)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := s.bucket.Put(ctx, job.ObjectKey, f); err != nil {
		return err
	}
	s.crashAt("upload")
	return s.store.MarkReady(ctx, job.ID)
}

func (s *Service) resolve(ctx context.Context, in Request) (nodeID, hash []byte, err error) {
	var v *versionv1.Version
	if len(in.VersionID) == 0 {
		v, err = s.versions.Head(ctx, &versionv1.HeadRequest{DocId: in.DocID.String()})
	} else {
		v, err = s.versions.GetVersion(ctx, &versionv1.GetVersionRequest{Id: in.VersionID})
	}
	if err != nil {
		return nil, nil, err
	}
	if v.GetDocId() != in.DocID.String() {
		return nil, nil, fmt.Errorf("%w: version is not in this document", ErrInvalidArgument)
	}
	if len(v.GetId()) != idLen || len(v.GetContentHash()) != idLen {
		return nil, nil, fmt.Errorf("export: version %s has no content hash", in.DocID)
	}
	return append([]byte(nil), v.GetId()...), append([]byte(nil), v.GetContentHash()...), nil
}

func (s *Service) blob(ctx context.Context, hash []byte) ([]byte, error) {
	resp, err := s.versions.GetBlob(ctx, &versionv1.GetBlobRequest{Hash: hash})
	if err != nil {
		return nil, err
	}
	return resp.GetBytes(), nil
}

func (s *Service) crashAt(point string) {
	if s.crash == point {
		os.Exit(1)
	}
}

func renderFormat(format Format, text string) ([]byte, error) {
	switch format {
	case FormatMD:
		return render.Markdown(text), nil
	case FormatPDF:
		return render.PDF(text)
	case FormatDOCX:
		return render.DOCX(text)
	default:
		return nil, fmt.Errorf("%w: format %q", ErrInvalidArgument, format)
	}
}

func writeTemp(body []byte) (string, error) {
	f, err := os.CreateTemp("", "export-*")
	if err != nil {
		return "", err
	}
	name := f.Name()
	_, werr := f.Write(body)
	cerr := f.Close()
	if werr != nil {
		os.Remove(name)
		return "", werr
	}
	if cerr != nil {
		os.Remove(name)
		return "", cerr
	}
	return name, nil
}

func subject(ctx context.Context) (string, error) {
	user := auth.Subject(ctx)
	if user == "" {
		return "", status.Error(codes.Unauthenticated, "unauthenticated")
	}
	return user, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
