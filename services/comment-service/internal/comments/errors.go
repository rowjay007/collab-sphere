package comments

import "errors"

var (
	ErrNotFound        = errors.New("comment: not found")
	ErrAlreadyExists   = errors.New("comment: already exists")
	ErrInvalidArgument = errors.New("comment: invalid argument")
	ErrInvalidAnchor   = errors.New("comment: invalid anchor")
	ErrUnavailable     = errors.New("comment: unavailable")
)
