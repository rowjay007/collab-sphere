package comments

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

type store struct {
	pool *pgxpool.Pool
}

type requestRow struct {
	Kind        string
	Fingerprint string
	ThreadID    uuid.UUID
	CommentID   uuid.UUID
	Resolved    bool
	HasResolved bool
}

const uniqueViolation = "23505"

func fingerprint(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func createFingerprint(home uuid.UUID, a Anchor, author, body string) string {
	return fingerprint("create", home.String(), a.Start.String(), a.End.String(), author, body)
}

func replyFingerprint(thread uuid.UUID, author, body string) string {
	return fingerprint("reply", thread.String(), author, body)
}

func resolveFingerprint(thread uuid.UUID, resolved bool, actor string) string {
	flag := "0"
	if resolved {
		flag = "1"
	}
	return fingerprint("set_resolved", thread.String(), flag, actor)
}

func isUnique(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolation
}

func (s *store) createThread(ctx context.Context, home uuid.UUID, createdSeq uint64, a Anchor, author, body, reqID string, mentions []string, notes []Notification) (Thread, error) {
	fp := createFingerprint(home, a, author, body)
	threadID := uuid.New()
	commentID := uuid.New()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Thread{}, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO threads (id, home, created_seq, start_client, start_clock, end_client, end_clock, author)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		threadID, home, int64(createdSeq),
		uint64(a.Start.Client), a.Start.Clock, uint64(a.End.Client), a.End.Clock, author,
	); err != nil {
		return Thread{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO comments (id, thread_id, author, body) VALUES ($1, $2, $3, $4)`,
		commentID, threadID, author, body,
	); err != nil {
		return Thread{}, err
	}
	if err := insertMentions(ctx, tx, commentID, mentions); err != nil {
		return Thread{}, err
	}
	if err := insertNotifications(ctx, tx, threadID, commentID, notes); err != nil {
		return Thread{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO requests (request_id, kind, fingerprint, thread_id, comment_id)
		VALUES ($1, 'create', $2, $3, $4)`, reqID, fp, threadID, commentID); err != nil {
		if isUnique(err) {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				return Thread{}, rollbackErr
			}
			return s.replayCreate(ctx, reqID, fp)
		}
		return Thread{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Thread{}, err
	}
	return s.loadThread(ctx, threadID)
}

func (s *store) addReply(ctx context.Context, threadID uuid.UUID, author, body, reqID string, mentions []string, notes []Notification) (Comment, error) {
	fp := replyFingerprint(threadID, author, body)
	commentID := uuid.New()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Comment{}, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO comments (id, thread_id, author, body) VALUES ($1, $2, $3, $4)`,
		commentID, threadID, author, body,
	); err != nil {
		return Comment{}, err
	}
	if err := insertMentions(ctx, tx, commentID, mentions); err != nil {
		return Comment{}, err
	}
	if err := insertNotifications(ctx, tx, threadID, commentID, notes); err != nil {
		return Comment{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO requests (request_id, kind, fingerprint, thread_id, comment_id)
		VALUES ($1, 'reply', $2, $3, $4)`, reqID, fp, threadID, commentID); err != nil {
		if isUnique(err) {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				return Comment{}, rollbackErr
			}
			return s.replayReply(ctx, reqID, fp)
		}
		return Comment{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Comment{}, err
	}
	return s.loadComment(ctx, commentID)
}

func (s *store) setResolved(ctx context.Context, threadID uuid.UUID, resolved bool, actor, reqID string) (Thread, error) {
	fp := resolveFingerprint(threadID, resolved, actor)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Thread{}, err
	}
	defer tx.Rollback(ctx)

	var current bool
	var author string
	if err := tx.QueryRow(ctx, `
		SELECT resolved, author FROM threads WHERE id = $1 FOR UPDATE`, threadID,
	).Scan(&current, &author); errors.Is(err, pgx.ErrNoRows) {
		return Thread{}, ErrNotFound
	} else if err != nil {
		return Thread{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE threads SET resolved = $2 WHERE id = $1`, threadID, resolved); err != nil {
		return Thread{}, err
	}
	if current != resolved && author != actor {
		kind := ResolvedKind
		if !resolved {
			kind = UnresolvedKind
		}
		if err := insertNotifications(ctx, tx, threadID, uuid.Nil, []Notification{{User: author, Kind: kind}}); err != nil {
			return Thread{}, err
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO requests (request_id, kind, fingerprint, thread_id, resolved)
		VALUES ($1, 'set_resolved', $2, $3, $4)`, reqID, fp, threadID, resolved); err != nil {
		if isUnique(err) {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				return Thread{}, rollbackErr
			}
			return s.replayResolved(ctx, reqID, fp)
		}
		return Thread{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Thread{}, err
	}
	return s.loadThread(ctx, threadID)
}

func (s *store) replayCreate(ctx context.Context, reqID, fp string) (Thread, error) {
	row, err := s.loadRequest(ctx, reqID)
	if err != nil {
		return Thread{}, err
	}
	if row.Fingerprint != fp {
		return Thread{}, ErrAlreadyExists
	}
	return s.loadThread(ctx, row.ThreadID)
}

func (s *store) replayReply(ctx context.Context, reqID, fp string) (Comment, error) {
	row, err := s.loadRequest(ctx, reqID)
	if err != nil {
		return Comment{}, err
	}
	if row.Fingerprint != fp {
		return Comment{}, ErrAlreadyExists
	}
	return s.loadComment(ctx, row.CommentID)
}

func (s *store) replayResolved(ctx context.Context, reqID, fp string) (Thread, error) {
	row, err := s.loadRequest(ctx, reqID)
	if err != nil {
		return Thread{}, err
	}
	if row.Fingerprint != fp {
		return Thread{}, ErrAlreadyExists
	}
	th, err := s.loadThread(ctx, row.ThreadID)
	if err != nil {
		return Thread{}, err
	}
	if row.HasResolved {
		th.Resolved = row.Resolved
	}
	return th, nil
}

func (s *store) loadRequest(ctx context.Context, reqID string) (requestRow, error) {
	var row requestRow
	var commentID *uuid.UUID
	var resolved *bool
	err := s.pool.QueryRow(ctx, `
		SELECT kind, fingerprint, thread_id, comment_id, resolved
		FROM requests WHERE request_id = $1`, reqID).Scan(
		&row.Kind, &row.Fingerprint, &row.ThreadID, &commentID, &resolved,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return requestRow{}, ErrNotFound
	}
	if err != nil {
		return requestRow{}, err
	}
	if commentID != nil {
		row.CommentID = *commentID
	}
	if resolved != nil {
		row.Resolved = *resolved
		row.HasResolved = true
	}
	return row, nil
}

func (s *store) loadThread(ctx context.Context, id uuid.UUID) (Thread, error) {
	th, err := scanThread(s.pool.QueryRow(ctx, `
		SELECT id, home, created_seq, start_client, start_clock, end_client, end_clock, resolved, author, created_at
		FROM threads WHERE id = $1`, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Thread{}, ErrNotFound
		}
		return Thread{}, err
	}
	comments, err := s.loadComments(ctx, id)
	if err != nil {
		return Thread{}, err
	}
	th.Comments = comments
	return th, nil
}

func (s *store) loadComment(ctx context.Context, id uuid.UUID) (Comment, error) {
	c, err := scanComment(s.pool.QueryRow(ctx, `
		SELECT id, author, body, created_at FROM comments WHERE id = $1`, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Comment{}, ErrNotFound
		}
		return Comment{}, err
	}
	mentions, err := s.loadMentions(ctx, id)
	if err != nil {
		return Comment{}, err
	}
	c.Mentions = mentions
	return c, nil
}

func (s *store) loadComments(ctx context.Context, threadID uuid.UUID) ([]Comment, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, author, body, created_at FROM comments
		WHERE thread_id = $1 ORDER BY created_at, id`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Comment
	for rows.Next() {
		c, err := scanComment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		mentions, err := s.loadMentions(ctx, out[i].ID)
		if err != nil {
			return nil, err
		}
		out[i].Mentions = mentions
	}
	return out, nil
}

func (s *store) loadMentions(ctx context.Context, commentID uuid.UUID) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT username FROM mentions WHERE comment_id = $1 ORDER BY ord`, commentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func (s *store) listThreads(ctx context.Context) ([]Thread, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, home, created_seq, start_client, start_clock, end_client, end_clock, resolved, author, created_at
		FROM threads ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Thread
	for rows.Next() {
		th, err := scanThread(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, th)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		comments, err := s.loadComments(ctx, out[i].ID)
		if err != nil {
			return nil, err
		}
		out[i].Comments = comments
	}
	return out, nil
}

func (s *store) listNotifications(ctx context.Context, user string, afterID uint64, limit int) ([]Notification, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, username, thread_id, comment_id, kind, created_at
		FROM notifications
		WHERE username = $1 AND id > $2
		ORDER BY id
		LIMIT $3`, user, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		var n Notification
		var commentID *uuid.UUID
		var kind string
		if err := rows.Scan(&n.ID, &n.User, &n.ThreadID, &commentID, &kind, &n.CreatedAt); err != nil {
			return nil, err
		}
		if commentID != nil {
			n.CommentID = *commentID
		}
		parsed, err := parseKind(kind)
		if err != nil {
			return nil, err
		}
		n.Kind = parsed
		out = append(out, n)
	}
	return out, rows.Err()
}

func insertMentions(ctx context.Context, tx pgx.Tx, commentID uuid.UUID, mentions []string) error {
	for i, name := range mentions {
		if _, err := tx.Exec(ctx, `
			INSERT INTO mentions (comment_id, username, ord) VALUES ($1, $2, $3)`,
			commentID, name, i); err != nil {
			return err
		}
	}
	return nil
}

func insertNotifications(ctx context.Context, tx pgx.Tx, threadID, commentID uuid.UUID, notes []Notification) error {
	for _, n := range notes {
		kind, err := formatKind(n.Kind)
		if err != nil {
			return err
		}
		var cid any
		if commentID != uuid.Nil {
			cid = commentID
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO notifications (username, thread_id, comment_id, kind)
			VALUES ($1, $2, $3, $4)`, n.User, threadID, cid, kind); err != nil {
			return err
		}
	}
	return nil
}

func scanThread(row pgx.Row) (Thread, error) {
	var (
		th                        Thread
		startClient, endClient    uint64
		startClock, endClock, seq int64
	)
	if err := row.Scan(
		&th.ID, &th.Home, &seq, &startClient, &startClock, &endClient, &endClock,
		&th.Resolved, &th.Author, &th.CreatedAt,
	); err != nil {
		return Thread{}, err
	}
	th.CreatedSeq = uint64(seq)
	th.Anchor = Anchor{
		Start: crdt.ID{Client: crdt.ClientID(startClient), Clock: uint64(startClock)},
		End:   crdt.ID{Client: crdt.ClientID(endClient), Clock: uint64(endClock)},
	}
	return th, nil
}

func scanComment(row pgx.Row) (Comment, error) {
	var c Comment
	if err := row.Scan(&c.ID, &c.Author, &c.Body, &c.At); err != nil {
		return Comment{}, err
	}
	return c, nil
}

func formatKind(k Kind) (string, error) {
	switch k {
	case Mention:
		return "mention", nil
	case ReplyKind:
		return "reply", nil
	case ResolvedKind:
		return "resolved", nil
	case UnresolvedKind:
		return "unresolved", nil
	default:
		return "", fmt.Errorf("%w: unknown notification kind", ErrInvalidArgument)
	}
}

func parseKind(s string) (Kind, error) {
	switch s {
	case "mention":
		return Mention, nil
	case "reply":
		return ReplyKind, nil
	case "resolved":
		return ResolvedKind, nil
	case "unresolved":
		return UnresolvedKind, nil
	default:
		return 0, fmt.Errorf("%w: unknown notification kind %q", ErrInvalidArgument, s)
	}
}
