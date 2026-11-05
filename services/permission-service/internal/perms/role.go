package perms

type Role string

const (
	RoleOwner     Role = "owner"
	RoleEditor    Role = "editor"
	RoleCommenter Role = "commenter"
	RoleViewer    Role = "viewer"
)

func (r Role) rank() int {
	switch r {
	case RoleOwner:
		return 4
	case RoleEditor:
		return 3
	case RoleCommenter:
		return 2
	case RoleViewer:
		return 1
	default:
		return 0
	}
}

func (r Role) linkOK() bool {
	return r == RoleEditor || r == RoleCommenter || r == RoleViewer
}

func parseRole(s string) (Role, bool) {
	switch Role(s) {
	case RoleOwner, RoleEditor, RoleCommenter, RoleViewer:
		return Role(s), true
	default:
		return "", false
	}
}
