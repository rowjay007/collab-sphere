// Package render turns visible document text into Markdown, DOCX, and PDF bytes.
package render

import (
	"errors"

	"github.com/rowjay007/collab-sphere/pkg/crdt"
)

const (
	fontSize     = 10
	leading      = 14
	pageWidth    = 612
	pageHeight   = 792
	margin       = 72
	lineWidth    = pageWidth - 2*margin
	linesPerPage = 46
	maxEM        = lineWidth * 1000 / fontSize
	firstBaseY   = pageHeight - margin - leading
)

// ErrCharset is returned when PDF or DOCX sees a rune outside WinAnsi 32..126
// (newline is allowed).
var ErrCharset = errors.New("render: rune outside WinAnsi 32..126")

// Text returns the visible text of a CSS1 canonical snapshot.
func Text(canonical []byte) (string, error) {
	d, err := crdt.FromCanonical(canonical)
	if err != nil {
		return "", err
	}
	return d.Text(), nil
}

// Markdown returns text plus one trailing newline.
func Markdown(text string) []byte {
	return []byte(text + "\n")
}

func wrap(text string) ([]string, error) {
	var lines []string
	start := 0
	for i := 0; i <= len(text); i++ {
		if i < len(text) && text[i] != '\n' {
			continue
		}
		para, err := wrapParagraph(text[start:i])
		if err != nil {
			return nil, err
		}
		lines = append(lines, para...)
		start = i + 1
	}
	return lines, nil
}

func wrapParagraph(p string) ([]string, error) {
	if p == "" {
		return []string{""}, nil
	}
	var lines []string
	var line []byte
	lineW := 0
	lastSpace := -1
	for i := 0; i < len(p); i++ {
		r := rune(p[i])
		if r < 32 || r > 126 {
			return nil, ErrCharset
		}
		w := helveticaWX[p[i]-32]
		if lineW+w <= maxEM {
			if p[i] == ' ' {
				lastSpace = len(line)
			}
			line = append(line, p[i])
			lineW += w
			continue
		}
		if lastSpace >= 0 {
			lines = append(lines, string(line[:lastSpace]))
			next := append([]byte{}, line[lastSpace+1:]...)
			line = next
			lineW = widthEM(line)
			lastSpace = -1
			for j, c := range line {
				if c == ' ' {
					lastSpace = j
				}
			}
			i--
			continue
		}
		if len(line) == 0 {
			line = append(line, p[i])
			lineW = w
			continue
		}
		lines = append(lines, string(line))
		line = line[:0]
		lineW = 0
		lastSpace = -1
		i--
	}
	if len(line) > 0 {
		lines = append(lines, string(line))
	}
	return lines, nil
}

func widthEM(s []byte) int {
	n := 0
	for _, c := range s {
		n += helveticaWX[c-32]
	}
	return n
}

func paginate(lines []string) [][]string {
	if len(lines) == 0 {
		return [][]string{{}}
	}
	n := (len(lines) + linesPerPage - 1) / linesPerPage
	pages := make([][]string, 0, n)
	for i := 0; i < len(lines); i += linesPerPage {
		end := i + linesPerPage
		if end > len(lines) {
			end = len(lines)
		}
		pages = append(pages, lines[i:end])
	}
	return pages
}
