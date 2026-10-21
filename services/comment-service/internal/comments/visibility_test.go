package comments

import (
	"testing"

	"github.com/google/uuid"
)

func TestVisibleEqualityIsIncluded(t *testing.T) {
	home := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	other := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	frontier := map[uuid.UUID]uint64{home: 10}
	if !Visible(frontier, home, 10) {
		t.Fatal("frontier[home] == createdSeq must be visible")
	}
	if !Visible(frontier, home, 9) {
		t.Fatal("frontier[home] > createdSeq must be visible")
	}
	if Visible(frontier, home, 11) {
		t.Fatal("frontier[home] < createdSeq must be hidden")
	}
	if Visible(frontier, other, 1) {
		t.Fatal("missing home must be hidden")
	}
	if Visible(nil, home, 0) {
		t.Fatal("nil frontier must hide")
	}
}
