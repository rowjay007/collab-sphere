package exportcheck_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/rowjay007/collab-sphere/pkg/render"
	"github.com/rowjay007/collab-sphere/tests/internal/exportcheck"
)

func TestFormatRoundTrips(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"empty", ""},
		{"one char", "x"},
		{"pdf escapes", `a(b)c\d`},
		{"pdf unbalanced parens", `x) y (z`},
		{"backticks and quotes", strings.Repeat("`'", 60)},
		{"xml escapes", `a<b>&"c'`},
		{"blank lines", "a\n\nb"},
		{"long word", strings.Repeat("W", 50)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !bytes.Equal(render.Markdown(tc.text), []byte(tc.text+"\n")) {
				t.Fatal("markdown bytes")
			}
			pdf, err := render.PDF(tc.text)
			if err != nil {
				t.Fatal(err)
			}
			gotPDF, err := exportcheck.ReadPDF(pdf)
			if err != nil {
				t.Fatal(err)
			}
			if !visualMatches(tc.text, gotPDF.Text) {
				t.Fatalf("pdf text = %#v for %q", gotPDF.Text, tc.text)
			}
			docx, err := render.DOCX(tc.text)
			if err != nil {
				t.Fatal(err)
			}
			gotDOCX, err := exportcheck.ReadDOCX(docx)
			if err != nil {
				t.Fatal(err)
			}
			if !visualMatches(tc.text, gotDOCX) {
				t.Fatalf("docx text = %#v for %q", gotDOCX, tc.text)
			}
			if len(gotPDF.Text) != len(gotDOCX) {
				t.Fatalf("pdf lines %d, docx lines %d", len(gotPDF.Text), len(gotDOCX))
			}
			for i := range gotPDF.Text {
				if gotPDF.Text[i] != gotDOCX[i] {
					t.Fatalf("line %d: pdf %q docx %q", i, gotPDF.Text[i], gotDOCX[i])
				}
			}
		})
	}
}

func TestPDF100Pages(t *testing.T) {
	text := fixture100()
	pdf, err := render.PDF(text)
	if err != nil {
		t.Fatal(err)
	}
	got, err := exportcheck.ReadPDF(pdf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Pages != 100 {
		t.Fatalf("pages = %d, want 100", got.Pages)
	}
	if joined := strings.Join(got.Text, "\n"); joined != text {
		t.Fatalf("extracted %d bytes, want %d", len(joined), len(text))
	}
	docx, err := render.DOCX(text)
	if err != nil {
		t.Fatal(err)
	}
	paras, err := exportcheck.ReadDOCX(docx)
	if err != nil {
		t.Fatal(err)
	}
	if joined := strings.Join(paras, "\n"); joined != text {
		t.Fatalf("docx extracted %d bytes, want %d", len(joined), len(text))
	}
}

func TestPDFQpdfCheck(t *testing.T) {
	if _, err := exec.LookPath("qpdf"); err != nil {
		t.Skip("qpdf not installed")
	}
	pdf, err := render.PDF(fixture100())
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "export-*.pdf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(pdf); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("qpdf", "--check", f.Name()).CombinedOutput()
	if err != nil {
		t.Fatalf("qpdf --check: %v\n%s", err, out)
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

func visualMatches(input string, extracted []string) bool {
	if strings.Join(extracted, "\n") == input {
		return true
	}
	return strings.Join(extracted, "") == input
}
