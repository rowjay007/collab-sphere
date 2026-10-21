package comments

import (
	"time"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

type Anchor struct {
	Start, End crdt.ID
}

type ResolutionState uint8

const (
	Attached ResolutionState = iota + 1
	Detached
)

type Resolution struct {
	State       ResolutionState
	First, Last crdt.ID
	Text        string
}

type Comment struct {
	ID       uuid.UUID
	Author   string
	Body     string
	Mentions []string
	At       time.Time
}

type Thread struct {
	ID         uuid.UUID
	Home       uuid.UUID
	CreatedSeq uint64
	Anchor     Anchor
	Resolved   bool
	Author     string
	Comments   []Comment
	CreatedAt  time.Time
}

type ResolvedThread struct {
	Thread
	Resolution Resolution
}

type Kind uint8

const (
	Mention Kind = iota + 1
	ReplyKind
	ResolvedKind
	UnresolvedKind
)

type Notification struct {
	ID        uint64
	User      string
	ThreadID  uuid.UUID
	CommentID uuid.UUID
	Kind      Kind
	CreatedAt time.Time
}
