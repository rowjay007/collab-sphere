package comments

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

const (
	defaultNotificationLimit = 50
	maxNotificationLimit     = 200
)

type Service struct {
	db       *store
	docs     Docs
	versions Versions
}

func New(pool *pgxpool.Pool, docs Docs, versions Versions) *Service {
	return &Service{db: &store{pool: pool}, docs: docs, versions: versions}
}

func (s *Service) Create(ctx context.Context, home uuid.UUID, a Anchor, author, body, reqID string) (Thread, error) {
	if err := requireWrite(author, body, reqID); err != nil {
		return Thread{}, err
	}
	if th, err := s.db.replayCreate(ctx, reqID, createFingerprint(home, a, author, body)); !errors.Is(err, ErrNotFound) {
		return th, err
	}
	doc, seq, err := s.docs.LoadState(ctx, home)
	if err != nil {
		return Thread{}, err
	}
	if err := validateAnchor(doc, a); err != nil {
		return Thread{}, err
	}
	mentions := ParseMentions(body)
	return s.db.createThread(ctx, home, seq, a, author, body, reqID, mentions, mentionNotes(mentions))
}

func (s *Service) Reply(ctx context.Context, threadID uuid.UUID, author, body, reqID string) (Comment, error) {
	if err := requireWrite(author, body, reqID); err != nil {
		return Comment{}, err
	}
	if c, err := s.db.replayReply(ctx, reqID, replyFingerprint(threadID, author, body)); !errors.Is(err, ErrNotFound) {
		return c, err
	}
	th, err := s.db.loadThread(ctx, threadID)
	if err != nil {
		return Comment{}, err
	}
	mentions := ParseMentions(body)
	notes := mentionNotes(mentions)
	notes = append(notes, replyNotes(author, participants(th))...)
	return s.db.addReply(ctx, threadID, author, body, reqID, mentions, notes)
}

func (s *Service) SetResolved(ctx context.Context, threadID uuid.UUID, resolved bool, actor, reqID string) (Thread, error) {
	if actor == "" || reqID == "" {
		return Thread{}, fmt.Errorf("%w: actor and request_id are required", ErrInvalidArgument)
	}
	if th, err := s.db.replayResolved(ctx, reqID, resolveFingerprint(threadID, resolved, actor)); !errors.Is(err, ErrNotFound) {
		return th, err
	}
	return s.db.setResolved(ctx, threadID, resolved, actor, reqID)
}

func (s *Service) List(ctx context.Context, docID uuid.UUID, includeResolved bool) ([]ResolvedThread, error) {
	frontier, err := s.versions.Frontier(ctx, docID)
	if err != nil {
		return nil, err
	}
	doc, _, err := s.docs.LoadState(ctx, docID)
	if err != nil {
		return nil, err
	}
	threads, err := s.db.listThreads(ctx)
	if err != nil {
		return nil, err
	}
	var out []ResolvedThread
	for _, th := range threads {
		if !includeResolved && th.Resolved {
			continue
		}
		if !Visible(frontier, th.Home, th.CreatedSeq) {
			continue
		}
		res, err := resolutionOf(doc, th.Anchor)
		if err != nil {
			return nil, err
		}
		out = append(out, ResolvedThread{Thread: th, Resolution: res})
	}
	return out, nil
}

func (s *Service) ListNotifications(ctx context.Context, user string, afterID uint64, limit int) ([]Notification, error) {
	if user == "" {
		return nil, fmt.Errorf("%w: user is required", ErrInvalidArgument)
	}
	if limit <= 0 {
		limit = defaultNotificationLimit
	}
	if limit > maxNotificationLimit {
		limit = maxNotificationLimit
	}
	return s.db.listNotifications(ctx, user, afterID, limit)
}

func requireWrite(author, body, reqID string) error {
	if author == "" || body == "" || reqID == "" {
		return fmt.Errorf("%w: author, body, and request_id are required", ErrInvalidArgument)
	}
	return nil
}

func validateAnchor(doc *crdt.Doc, a Anchor) error {
	if doc == nil || a.Start.IsRoot() || a.End.IsRoot() {
		return ErrInvalidAnchor
	}
	if id, ok := doc.Resolve(a.Start); !ok || id != a.Start {
		return ErrInvalidAnchor
	}
	if id, ok := doc.Resolve(a.End); !ok || id != a.End {
		return ErrInvalidAnchor
	}
	if a.Start == a.End {
		return nil
	}
	seenStart := false
	for i := 0; i < doc.Len(); i++ {
		id, err := doc.IDAt(i)
		if err != nil {
			return err
		}
		if id == a.Start {
			seenStart = true
		}
		if id == a.End {
			if !seenStart {
				return ErrInvalidAnchor
			}
			return nil
		}
	}
	return ErrInvalidAnchor
}

func resolutionOf(doc *crdt.Doc, a Anchor) (Resolution, error) {
	r, err := doc.ResolveRange(a.Start, a.End)
	if err != nil {
		return Resolution{}, err
	}
	if r.Detached {
		return Resolution{State: Detached}, nil
	}
	return Resolution{State: Attached, First: r.First, Last: r.Last, Text: r.Text}, nil
}

func mentionNotes(mentions []string) []Notification {
	var out []Notification
	for _, user := range mentions {
		out = append(out, Notification{User: user, Kind: Mention})
	}
	return out
}

func participants(th Thread) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, c := range th.Comments {
		if _, ok := seen[c.Author]; ok {
			continue
		}
		seen[c.Author] = struct{}{}
		out = append(out, c.Author)
	}
	return out
}

func replyNotes(author string, prior []string) []Notification {
	skip := map[string]struct{}{author: {}}
	var out []Notification
	for _, user := range prior {
		if _, ok := skip[user]; ok {
			continue
		}
		skip[user] = struct{}{}
		out = append(out, Notification{User: user, Kind: ReplyKind})
	}
	return out
}
