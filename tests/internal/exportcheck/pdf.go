// Package exportcheck reads export files with parsers that do not import pkg/render.
package exportcheck

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// PDF is a parsed PDF: page object count and Tj strings in document order.
type PDF struct {
	Pages int
	Text  []string
}

// ReadPDF validates classic xref offsets, counts page objects, and extracts Tj strings.
func ReadPDF(raw []byte) (PDF, error) {
	offsets, err := parseXref(raw)
	if err != nil {
		return PDF{}, err
	}
	objs, err := loadObjects(raw, offsets)
	if err != nil {
		return PDF{}, err
	}
	pageIDs, err := pageOrder(objs)
	if err != nil {
		return PDF{}, err
	}
	var text []string
	for _, id := range pageIDs {
		contentID, ok := contentsRef(objs[id])
		if !ok {
			continue
		}
		stream, ok := objs[contentID]
		if !ok {
			return PDF{}, fmt.Errorf("exportcheck: missing content object %d", contentID)
		}
		data, err := streamData(stream)
		if err != nil {
			return PDF{}, err
		}
		extracted, err := extractTj(data)
		if err != nil {
			return PDF{}, err
		}
		text = append(text, extracted...)
	}
	return PDF{Pages: len(pageIDs), Text: text}, nil
}

type xrefEnt struct {
	offset int
	free   bool
}

func parseXref(raw []byte) ([]xrefEnt, error) {
	idx := bytes.LastIndex(raw, []byte("startxref"))
	if idx < 0 {
		return nil, fmt.Errorf("exportcheck: missing startxref")
	}
	rest := bytes.TrimSpace(raw[idx+len("startxref"):])
	nl := bytes.IndexByte(rest, '\n')
	if nl < 0 {
		return nil, fmt.Errorf("exportcheck: truncated startxref")
	}
	off, err := strconv.Atoi(strings.TrimSpace(string(rest[:nl])))
	if err != nil || off < 0 || off >= len(raw) {
		return nil, fmt.Errorf("exportcheck: bad startxref")
	}
	xref := raw[off:]
	if !bytes.HasPrefix(xref, []byte("xref")) {
		return nil, fmt.Errorf("exportcheck: startxref does not point at xref")
	}
	lineEnd := bytes.IndexByte(xref, '\n')
	if lineEnd < 0 {
		return nil, fmt.Errorf("exportcheck: truncated xref")
	}
	header := xref[lineEnd+1:]
	eol := bytes.IndexByte(header, '\n')
	if eol < 0 {
		return nil, fmt.Errorf("exportcheck: truncated xref header")
	}
	fields := strings.Fields(string(header[:eol]))
	if len(fields) != 2 {
		return nil, fmt.Errorf("exportcheck: bad xref subsection")
	}
	first, err1 := strconv.Atoi(fields[0])
	count, err2 := strconv.Atoi(fields[1])
	if err1 != nil || err2 != nil || first != 0 || count < 1 {
		return nil, fmt.Errorf("exportcheck: bad xref subsection")
	}
	entries := header[eol+1:]
	out := make([]xrefEnt, count)
	for i := 0; i < count; i++ {
		if len(entries) < 20 {
			return nil, fmt.Errorf("exportcheck: truncated xref entry %d", i)
		}
		rec := entries[:20]
		entries = entries[20:]
		off, err := strconv.Atoi(strings.TrimSpace(string(rec[0:10])))
		if err != nil {
			return nil, fmt.Errorf("exportcheck: bad xref offset %d", i)
		}
		flag := rec[17]
		out[i] = xrefEnt{offset: off, free: flag == 'f'}
	}
	return out, nil
}

func loadObjects(raw []byte, offsets []xrefEnt) (map[int][]byte, error) {
	objs := make(map[int][]byte, len(offsets))
	for id, e := range offsets {
		if e.free {
			continue
		}
		if e.offset < 0 || e.offset >= len(raw) {
			return nil, fmt.Errorf("exportcheck: xref object %d offset %d out of range", id, e.offset)
		}
		got, body, err := readObj(raw[e.offset:])
		if err != nil {
			return nil, fmt.Errorf("exportcheck: xref object %d: %w", id, err)
		}
		if got != id {
			return nil, fmt.Errorf("exportcheck: xref object %d offset points at %d", id, got)
		}
		objs[id] = body
	}
	return objs, nil
}

func readObj(src []byte) (int, []byte, error) {
	eol := bytes.IndexByte(src, '\n')
	if eol < 0 {
		return 0, nil, fmt.Errorf("not an object")
	}
	fields := strings.Fields(string(src[:eol]))
	if len(fields) != 3 || fields[2] != "obj" {
		return 0, nil, fmt.Errorf("not an object")
	}
	id, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, nil, fmt.Errorf("not an object")
	}
	body := src[eol+1:]
	if len(body) > 0 && body[0] == '\r' {
		body = body[1:]
	}
	end := bytes.Index(body, []byte("endobj"))
	if end < 0 {
		return 0, nil, fmt.Errorf("missing endobj")
	}
	return id, body[:end], nil
}

func pageOrder(objs map[int][]byte) ([]int, error) {
	var pagesObj []byte
	for _, body := range objs {
		if bytes.Contains(body, []byte("/Type /Pages")) {
			pagesObj = body
			break
		}
	}
	if pagesObj == nil {
		return nil, fmt.Errorf("exportcheck: missing pages tree")
	}
	kids, err := parseKids(pagesObj)
	if err != nil {
		return nil, err
	}
	var pages []int
	for _, id := range kids {
		body, ok := objs[id]
		if !ok {
			return nil, fmt.Errorf("exportcheck: missing page object %d", id)
		}
		if !isPage(body) {
			return nil, fmt.Errorf("exportcheck: object %d is not a page", id)
		}
		pages = append(pages, id)
	}
	return pages, nil
}

func isPage(body []byte) bool {
	i := bytes.Index(body, []byte("/Type /Page"))
	if i < 0 {
		return false
	}
	rest := body[i+len("/Type /Page"):]
	return len(rest) == 0 || rest[0] != 's'
}

func parseKids(body []byte) ([]int, error) {
	i := bytes.Index(body, []byte("/Kids"))
	if i < 0 {
		return nil, fmt.Errorf("exportcheck: pages missing /Kids")
	}
	rest := body[i+len("/Kids"):]
	lb := bytes.IndexByte(rest, '[')
	rb := bytes.IndexByte(rest, ']')
	if lb < 0 || rb < 0 || rb < lb {
		return nil, fmt.Errorf("exportcheck: bad /Kids")
	}
	fields := strings.Fields(string(rest[lb+1 : rb]))
	var ids []int
	for i := 0; i+2 < len(fields); i += 3 {
		if fields[i+2] != "R" {
			return nil, fmt.Errorf("exportcheck: bad kid ref")
		}
		id, err := strconv.Atoi(fields[i])
		if err != nil {
			return nil, fmt.Errorf("exportcheck: bad kid id")
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func contentsRef(body []byte) (int, bool) {
	i := bytes.Index(body, []byte("/Contents"))
	if i < 0 {
		return 0, false
	}
	fields := strings.Fields(string(body[i+len("/Contents"):]))
	if len(fields) < 3 || fields[2] != "R" {
		return 0, false
	}
	id, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, false
	}
	return id, true
}

func streamData(body []byte) ([]byte, error) {
	length, err := dictLength(body)
	if err != nil {
		return nil, err
	}
	i := bytes.Index(body, []byte("stream"))
	if i < 0 {
		return nil, fmt.Errorf("exportcheck: missing stream")
	}
	data := body[i+len("stream"):]
	if len(data) > 0 && data[0] == '\r' {
		data = data[1:]
	}
	if len(data) > 0 && data[0] == '\n' {
		data = data[1:]
	}
	if length > len(data) {
		return nil, fmt.Errorf("exportcheck: stream length exceeds object")
	}
	return data[:length], nil
}

func dictLength(body []byte) (int, error) {
	i := bytes.Index(body, []byte("/Length"))
	if i < 0 {
		return 0, fmt.Errorf("exportcheck: stream missing /Length")
	}
	fields := strings.Fields(string(body[i+len("/Length"):]))
	if len(fields) == 0 {
		return 0, fmt.Errorf("exportcheck: bad /Length")
	}
	n, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, fmt.Errorf("exportcheck: bad /Length")
	}
	return n, nil
}

func extractTj(data []byte) ([]string, error) {
	var out []string
	i := 0
	for i < len(data) {
		if data[i] != '(' {
			i++
			continue
		}
		s, next, err := parsePDFString(data, i)
		if err != nil {
			return nil, err
		}
		j := next
		for j < len(data) && (data[j] == ' ' || data[j] == '\n' || data[j] == '\r' || data[j] == '\t') {
			j++
		}
		if j+2 <= len(data) && string(data[j:j+2]) == "Tj" {
			out = append(out, s)
			i = j + 2
			continue
		}
		i = next
	}
	return out, nil
}

func parsePDFString(data []byte, i int) (string, int, error) {
	if i >= len(data) || data[i] != '(' {
		return "", 0, fmt.Errorf("exportcheck: not a string")
	}
	var b strings.Builder
	depth := 1
	i++
	for i < len(data) {
		c := data[i]
		if c == '\\' {
			if i+1 >= len(data) {
				return "", 0, fmt.Errorf("exportcheck: truncated escape")
			}
			n := data[i+1]
			switch n {
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case '(', ')', '\\':
				b.WriteByte(n)
			case '\n':
			case '\r':
				if i+2 < len(data) && data[i+2] == '\n' {
					i++
				}
			default:
				if n >= '0' && n <= '7' {
					v := int(n - '0')
					used := 1
					for used < 3 && i+1+used < len(data) {
						d := data[i+1+used]
						if d < '0' || d > '7' {
							break
						}
						v = v*8 + int(d-'0')
						used++
					}
					b.WriteByte(byte(v))
					i += used + 1
					continue
				}
				b.WriteByte(n)
			}
			i += 2
			continue
		}
		if c == '(' {
			depth++
			b.WriteByte(c)
			i++
			continue
		}
		if c == ')' {
			depth--
			if depth == 0 {
				return b.String(), i + 1, nil
			}
			b.WriteByte(c)
			i++
			continue
		}
		b.WriteByte(c)
		i++
	}
	return "", 0, fmt.Errorf("exportcheck: unterminated string")
}
