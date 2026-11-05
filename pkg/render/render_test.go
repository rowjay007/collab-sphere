package render

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

func TestTextFromCanonical(t *testing.T) {
	for _, text := range []string{"", "x", "hello\nworld", "a<b>&c"} {
		got, err := Text(mustCanonical(t, text))
		if err != nil {
			t.Fatalf("%q: %v", text, err)
		}
		if got != text {
			t.Fatalf("Text = %q, want %q", got, text)
		}
	}
	if _, err := Text([]byte("not-canonical")); err == nil {
		t.Fatal("Text accepted invalid canonical bytes")
	}
}

func TestMarkdownTrailingNewline(t *testing.T) {
	for _, text := range []string{"", "x", "a\nb", "already\n"} {
		got := Markdown(text)
		want := []byte(text + "\n")
		if !bytes.Equal(got, want) {
			t.Fatalf("Markdown(%q) = %q, want %q", text, got, want)
		}
	}
}

func TestCharsetRejected(t *testing.T) {
	if _, err := PDF("café"); !errors.Is(err, ErrCharset) {
		t.Fatalf("PDF error = %v, want ErrCharset", err)
	}
	if _, err := DOCX("ok\x01"); !errors.Is(err, ErrCharset) {
		t.Fatalf("DOCX error = %v, want ErrCharset", err)
	}
}

func TestPDFFixtureDuration(t *testing.T) {
	text := fixture100()
	start := time.Now()
	pdf, err := PDF(text)
	d := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("100-page fixture PDF render: %s (%d bytes)", d, len(pdf))
}

var benchPDF []byte

func BenchmarkPDFFixture(b *testing.B) {
	text := fixture100()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		pdf, err := PDF(text)
		if err != nil {
			b.Fatal(err)
		}
		benchPDF = pdf
	}
}

func fixture100() string {
	var b strings.Builder
	b.Grow(4600 * 10)
	for i := 1; i <= 4600; i++ {
		if i > 1 {
			b.WriteByte('\n')
		}
		fmt.Fprintf(&b, "line %04d", i)
	}
	return b.String()
}

func mustCanonical(t *testing.T, text string) []byte {
	t.Helper()
	r, err := crdt.NewReplica(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.InsertText(0, text); err != nil {
		t.Fatal(err)
	}
	b, err := crdt.CanonicalBytes(r.Doc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
