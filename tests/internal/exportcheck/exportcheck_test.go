package exportcheck

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestReadPDFExtractsTjAndCountsPages(t *testing.T) {
	raw := minimalPDF(t, []string{"hello", "wo(rld"})
	got, err := ReadPDF(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Pages != 1 {
		t.Fatalf("pages = %d, want 1", got.Pages)
	}
	if len(got.Text) != 2 || got.Text[0] != "hello" || got.Text[1] != "wo(rld" {
		t.Fatalf("text = %#v", got.Text)
	}
}

func TestReadPDFRejectsWrongXrefOffset(t *testing.T) {
	raw := minimalPDF(t, []string{"x"})
	if _, err := ReadPDF(raw); err != nil {
		t.Fatal(err)
	}
	bad := corruptXref(t, raw, 1)
	if _, err := ReadPDF(bad); err == nil {
		t.Fatal("ReadPDF accepted a wrong xref offset")
	}
}

func TestReadDOCXReturnsParagraphs(t *testing.T) {
	raw := minimalDOCX(t, []string{"a<b", "c&d"})
	got, err := ReadDOCX(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "a<b" || got[1] != "c&d" {
		t.Fatalf("paragraphs = %#v", got)
	}
}

func minimalPDF(t *testing.T, lines []string) []byte {
	t.Helper()
	var w pdfBuf
	w.buf.WriteString("%PDF-1.4\n")
	w.offsets = []int{0}
	var stream strings.Builder
	stream.WriteString("BT\n")
	for i, line := range lines {
		if i > 0 {
			stream.WriteString("T*\n")
		}
		stream.WriteByte('(')
		for j := 0; j < len(line); j++ {
			if line[j] == '\\' || line[j] == '(' || line[j] == ')' {
				stream.WriteByte('\\')
			}
			stream.WriteByte(line[j])
		}
		stream.WriteString(") Tj\n")
	}
	stream.WriteString("ET")
	data := []byte(stream.String())
	w.obj(1, "<< /Type /Catalog /Pages 2 0 R >>")
	w.obj(2, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
	w.obj(3, "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>")
	w.stream(4, data)
	return w.finish()
}

func corruptXref(t *testing.T, raw []byte, id int) []byte {
	t.Helper()
	idx := bytes.LastIndex(raw, []byte("\nxref\n"))
	if idx < 0 {
		t.Fatal("missing xref")
	}
	line := idx + 1
	for n := 0; n < 2+id; n++ {
		next := bytes.IndexByte(raw[line:], '\n')
		if next < 0 {
			t.Fatal("truncated xref")
		}
		line += next + 1
	}
	if line+10 > len(raw) {
		t.Fatal("xref entry too short")
	}
	out := append([]byte(nil), raw...)
	off := 0
	fmt.Sscanf(string(out[line:line+10]), "%d", &off)
	copy(out[line:line+10], []byte(fmt.Sprintf("%010d", off+1)))
	return out
}

func minimalDOCX(t *testing.T, paras []string) []byte {
	t.Helper()
	var body strings.Builder
	body.WriteString(`<?xml version="1.0" encoding="UTF-8"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>`)
	for _, p := range paras {
		body.WriteString(`<w:p><w:r><w:t>`)
		body.WriteString(xmlEscape(p))
		body.WriteString(`</w:t></w:r></w:p>`)
	}
	body.WriteString(`</w:body></w:document>`)
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("word/document.xml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(w, body.String()); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func xmlEscape(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return r.Replace(s)
}

type pdfBuf struct {
	buf     bytes.Buffer
	offsets []int
}

func (w *pdfBuf) obj(id int, body string) {
	w.offsets = append(w.offsets, w.buf.Len())
	fmt.Fprintf(&w.buf, "%d 0 obj\n%s\nendobj\n", id, body)
}

func (w *pdfBuf) stream(id int, data []byte) {
	w.offsets = append(w.offsets, w.buf.Len())
	fmt.Fprintf(&w.buf, "%d 0 obj\n<< /Length %d >>\nstream\n", id, len(data))
	w.buf.Write(data)
	w.buf.WriteString("\nendstream\nendobj\n")
}

func (w *pdfBuf) finish() []byte {
	xref := w.buf.Len()
	n := len(w.offsets)
	fmt.Fprintf(&w.buf, "xref\n0 %d\n", n)
	w.buf.WriteString("0000000000 65535 f\r\n")
	for id := 1; id < n; id++ {
		fmt.Fprintf(&w.buf, "%010d 00000 n\r\n", w.offsets[id])
	}
	fmt.Fprintf(&w.buf, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", n, xref)
	return append([]byte(nil), w.buf.Bytes()...)
}
