package comments

import "github.com/google/uuid"

func Visible(frontier map[uuid.UUID]uint64, home uuid.UUID, createdSeq uint64) bool {
	seq, ok := frontier[home]
	return ok && seq >= createdSeq
}
