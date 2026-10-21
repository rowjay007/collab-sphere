package comments

import "regexp"

var mentionRE = regexp.MustCompile(`(?:^|\s)@([a-z0-9][a-z0-9._-]{0,63})`)

func ParseMentions(body string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, m := range mentionRE.FindAllStringSubmatch(body, -1) {
		name := m[1]
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}
