package perms

import "errors"

var (
	ErrNotFound          = errors.New("permission: not found")
	ErrAlreadyExists     = errors.New("permission: already exists")
	ErrInvalidArgument   = errors.New("permission: invalid argument")
	ErrPermissionDenied  = errors.New("permission: permission denied")
	ErrOwnerNotRevocable = errors.New("permission: owner not revocable")
	ErrLinkBad           = errors.New("LINK_BAD")
	ErrLinkRevoked       = errors.New("LINK_REVOKED")
	ErrLinkExpired       = errors.New("LINK_EXPIRED")
)

const (
	ReasonLinkBad     = "LINK_BAD"
	ReasonLinkRevoked = "LINK_REVOKED"
	ReasonLinkExpired = "LINK_EXPIRED"
)
