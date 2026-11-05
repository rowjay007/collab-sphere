package export

import (
	"errors"

	"github.com/google/uuid"
)

type Format string

const (
	FormatPDF  Format = "pdf"
	FormatMD   Format = "md"
	FormatDOCX Format = "docx"
)

type State string

const (
	Queued  State = "queued"
	Running State = "running"
	Ready   State = "ready"
	Failed  State = "failed"
)

const (
	presignTTL = 60
	pinLen     = 64
	idLen      = 32
)

var (
	ErrNotFound        = errors.New("export: not found")
	ErrEncrypted       = errors.New("document is encrypted")
	ErrInvalidArgument = errors.New("export: invalid argument")
)

func ParseFormat(s string) (Format, error) {
	switch Format(s) {
	case FormatPDF, FormatMD, FormatDOCX:
		return Format(s), nil
	default:
		return "", ErrInvalidArgument
	}
}

type Job struct {
	ID          uuid.UUID
	RequestID   string
	DocID       uuid.UUID
	NodeID      []byte
	ContentHash []byte
	Format      Format
	UserID      string
	State       State
	Attempts    int
	ObjectKey   string
	Error       string
	URL         string
}

func (j Job) pin() []byte {
	out := make([]byte, 0, pinLen)
	out = append(out, j.NodeID...)
	out = append(out, j.ContentHash...)
	return out
}

func objectKey(id uuid.UUID) string {
	return "exports/" + id.String()
}

type Request struct {
	DocID     uuid.UUID
	VersionID []byte
	Format    Format
	RequestID string
}
