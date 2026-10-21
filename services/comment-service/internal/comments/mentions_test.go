package comments

import (
	"reflect"
	"testing"
)

func TestParseMentions(t *testing.T) {
	cases := []struct {
		body string
		want []string
	}{
		{"@bob look here", []string{"bob"}},
		{"hi @bob", []string{"bob"}},
		{"hi @bob @alice @bob", []string{"bob", "alice"}},
		{"x@bob", nil},
		{"mail@bob.com", nil},
		{"@Bob", nil},
		{"hello @user.name_1-x", []string{"user.name_1-x"}},
		{"  @a @b", []string{"a", "b"}},
		{"no mentions", nil},
		{"@", nil},
	}
	for _, tc := range cases {
		got := ParseMentions(tc.body)
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("ParseMentions(%q) = %v, want %v", tc.body, got, tc.want)
		}
	}
}
