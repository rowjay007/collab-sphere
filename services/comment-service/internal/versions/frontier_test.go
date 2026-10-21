package versions

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/rowjay007/collab-sphere/services/comment-service/internal/comments"
)

func TestFrontierClientReportsUnbound(t *testing.T) {
	c := NewFrontierClient("127.0.0.1:1")
	got, err := c.Frontier(t.Context(), uuid.New())
	if !errors.Is(err, comments.ErrUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if got != nil {
		t.Fatalf("fallback frontier %v", got)
	}
}
