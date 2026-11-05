package exportcheck

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

// ReadDOCX returns the text of each paragraph in word/document.xml.
func ReadDOCX(raw []byte) ([]string, error) {
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return nil, err
	}
	var doc []byte
	for _, f := range zr.File {
		if f.Name != "word/document.xml" {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		doc, err = io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}
		break
	}
	if doc == nil {
		return nil, fmt.Errorf("exportcheck: missing word/document.xml")
	}
	return paragraphs(doc)
}

func paragraphs(doc []byte) ([]string, error) {
	dec := xml.NewDecoder(bytes.NewReader(doc))
	var out []string
	var cur []string
	inP, inT := false, false
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "p":
				inP = true
				cur = nil
			case "t":
				inT = true
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "t":
				inT = false
			case "p":
				out = append(out, strings.Join(cur, ""))
				inP = false
			}
		case xml.CharData:
			if inP && inT {
				cur = append(cur, string(t))
			}
		}
	}
	return out, nil
}
