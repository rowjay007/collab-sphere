package render

import (
	"bytes"
	"fmt"
	"strings"
)

// PDF renders text as a Helvetica WinAnsi PDF.
func PDF(text string) ([]byte, error) {
	lines, err := wrap(text)
	if err != nil {
		return nil, err
	}
	pages := paginate(lines)
	w := newPDF()
	nPages := len(pages)
	kids := make([]string, nPages)
	for i := range pages {
		kids[i] = fmt.Sprintf("%d 0 R", 4+2*i)
	}
	w.obj(1, "<< /Type /Catalog /Pages 2 0 R >>")
	w.obj(2, fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", strings.Join(kids, " "), nPages))
	w.obj(3, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>")
	for i, pageLines := range pages {
		pageID := 4 + 2*i
		contentID := 5 + 2*i
		w.obj(pageID, fmt.Sprintf(
			"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 %d %d] /Contents %d 0 R /Resources << /Font << /F1 3 0 R >> >> >>",
			pageWidth, pageHeight, contentID,
		))
		w.stream(contentID, pageStream(pageLines))
	}
	return w.finish(1), nil
}

type pdfWriter struct {
	buf     bytes.Buffer
	offsets []int
}

func newPDF() *pdfWriter {
	w := &pdfWriter{offsets: []int{0}}
	w.buf.WriteString("%PDF-1.4\n")
	return w
}

func (w *pdfWriter) obj(id int, body string) {
	w.place(id)
	fmt.Fprintf(&w.buf, "%d 0 obj\n%s\nendobj\n", id, body)
}

func (w *pdfWriter) stream(id int, data []byte) {
	w.place(id)
	fmt.Fprintf(&w.buf, "%d 0 obj\n<< /Length %d >>\nstream\n", id, len(data))
	w.buf.Write(data)
	w.buf.WriteString("\nendstream\nendobj\n")
}

func (w *pdfWriter) place(id int) {
	if id != len(w.offsets) {
		panic("render: pdf objects must be written in id order")
	}
	w.offsets = append(w.offsets, w.buf.Len())
}

func (w *pdfWriter) finish(root int) []byte {
	xref := w.buf.Len()
	n := len(w.offsets)
	fmt.Fprintf(&w.buf, "xref\n0 %d\n", n)
	w.buf.WriteString("0000000000 65535 f\r\n")
	for id := 1; id < n; id++ {
		fmt.Fprintf(&w.buf, "%010d 00000 n\r\n", w.offsets[id])
	}
	fmt.Fprintf(&w.buf, "trailer\n<< /Size %d /Root %d 0 R >>\nstartxref\n%d\n%%%%EOF\n", n, root, xref)
	return append([]byte(nil), w.buf.Bytes()...)
}

func pageStream(lines []string) []byte {
	var b strings.Builder
	b.WriteString("BT\n/F1 10 Tf\n14 TL\n")
	if len(lines) > 0 {
		fmt.Fprintf(&b, "72 %d Td\n", firstBaseY)
	}
	for i, line := range lines {
		if i > 0 {
			b.WriteString("T*\n")
		}
		b.WriteByte('(')
		b.WriteString(escapePDF(line))
		b.WriteString(") Tj\n")
	}
	b.WriteString("ET")
	return []byte(b.String())
}

func escapePDF(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\', '(', ')':
			b.WriteByte('\\')
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
