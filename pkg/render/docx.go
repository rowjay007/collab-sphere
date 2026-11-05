package render

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"io"
)

const (
	contentTypesXML = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">` +
		`<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>` +
		`<Default Extension="xml" ContentType="application/xml"/>` +
		`<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>` +
		`</Types>`
	relsXML = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
		`<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>` +
		`</Relationships>`
	docXMLHead = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>`
	docXMLTail = `</w:body></w:document>`
)

// DOCX renders text as a zip of Content_Types, relationships, and document.xml.
func DOCX(text string) ([]byte, error) {
	lines, err := wrap(text)
	if err != nil {
		return nil, err
	}
	var body bytes.Buffer
	body.WriteString(docXMLHead)
	for _, line := range lines {
		body.WriteString(`<w:p><w:r><w:t xml:space="preserve">`)
		if err := xml.EscapeText(&body, []byte(line)); err != nil {
			return nil, err
		}
		body.WriteString(`</w:t></w:r></w:p>`)
	}
	body.WriteString(docXMLTail)

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	if err := zipString(zw, "[Content_Types].xml", contentTypesXML); err != nil {
		return nil, err
	}
	if err := zipString(zw, "_rels/.rels", relsXML); err != nil {
		return nil, err
	}
	if err := zipBytes(zw, "word/document.xml", body.Bytes()); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func zipString(zw *zip.Writer, name, s string) error {
	return zipBytes(zw, name, []byte(s))
}

func zipBytes(zw *zip.Writer, name string, data []byte) error {
	w, err := zw.Create(name)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, bytes.NewReader(data))
	return err
}
