package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/jpappel/atlas/pkg/index"
)

const DefaultOutputFormat string = "%p %T %d authors:%a tags:%t"

type OutputToken uint64

const (
	OUT_TOK_STR      OutputToken = iota
	OUT_TOK_PATH                 // %p %path
	OUT_TOK_TITLE                // %T %title
	OUT_TOK_DATE                 // %d %date
	OUT_TOK_FILETIME             // %f %filetime
	OUT_TOK_AUTHORS              // %a %authors
	OUT_TOK_TAGS                 // %t %tags
	OUT_TOK_LINKS                // %l %links
	OUT_TOK_META                 // %m %meta
)

type Outputer interface {
	OutputOne(doc *index.Document) (string, error)
	OutputOneTo(w io.Writer, doc *index.Document) (int, error)
	Output(docs []*index.Document) (string, error)
	OutputTo(w io.Writer, docs []*index.Document) (int, error)
}

type DefaultOutput struct{}
type JsonOutput struct{}
type YamlOutput struct{}
type CustomOutput struct {
	stringTokens   []string
	tokens         []OutputToken
	datetimeFormat string
	docSeparator   string
	listSeparator  string
}

// compile time interface check
var _ Outputer = &DefaultOutput{}
var _ Outputer = &JsonOutput{}
var _ Outputer = &CustomOutput{}
var _ Outputer = &YamlOutput{}

// Returns "<path> <title> <date> authors:<authors...> tags:<tags>"
// and a nil error
func (o DefaultOutput) OutputOne(doc *index.Document) (string, error) {
	b := strings.Builder{}
	o.WriteDoc(&b, doc)

	return b.String(), nil
}

func (o DefaultOutput) OutputOneTo(w io.Writer, doc *index.Document) (int, error) {
	return o.WriteDoc(w, doc)
}

func (o DefaultOutput) Output(docs []*index.Document) (string, error) {
	b := strings.Builder{}

	for i, doc := range docs {
		o.WriteDoc(&b, doc)
		if i != len(docs)-1 {
			b.WriteRune('\n')
		}
	}

	return b.String(), nil
}

func (o DefaultOutput) OutputTo(w io.Writer, docs []*index.Document) (int, error) {
	n := 0
	for _, doc := range docs {
		nn, err := o.WriteDoc(w, doc)
		if err != nil {
			return n, err
		}

		n += nn
	}

	return n, nil
}

func (o DefaultOutput) WriteDoc(w io.Writer, doc *index.Document) (int, error) {
	var n int
	s := [][]byte{
		[]byte(doc.Path),
		{' '},
		[]byte(doc.Title),
		{' '},
		[]byte(doc.Date.String()),
		{' '},
		[]byte("authors:"),
		[]byte(strings.Join(doc.Authors, ",")),
		[]byte(" tags:"),
		[]byte(strings.Join(doc.Tags, ",")),
		{'\n'},
	}
	for _, b := range s {
		cnt, err := w.Write(b)
		if err != nil {
			return n, err
		}

		n += cnt
	}

	return n, nil
}

func (o JsonOutput) OutputOne(doc *index.Document) (string, error) {
	b, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (o JsonOutput) OutputOneTo(w io.Writer, doc *index.Document) (int, error) {
	b, err := json.Marshal(doc)
	if err != nil {
		return 0, err
	}

	return w.Write(b)
}

func (o JsonOutput) Output(docs []*index.Document) (string, error) {
	b, err := json.Marshal(docs)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (o JsonOutput) OutputTo(w io.Writer, docs []*index.Document) (int, error) {
	b, err := json.Marshal(docs)
	if err != nil {
		return 0, err
	}
	return w.Write(b)
}

func (o YamlOutput) OutputOne(doc *index.Document) (string, error) {
	b, err := doc.MarshalYAML()
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func (o YamlOutput) OutputOneTo(w io.Writer, doc *index.Document) (int, error) {
	b, err := doc.MarshalYAML()
	if err != nil {
		return 0, err
	}
	return w.Write(b)
}

func (o YamlOutput) Output(docs []*index.Document) (string, error) {
	b, err := yaml.Marshal(docs)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (o YamlOutput) OutputTo(w io.Writer, docs []*index.Document) (int, error) {
	b, err := yaml.Marshal(docs)
	if err != nil {
		return 0, err
	}

	return w.Write(b)
}

func ParseOutputFormat(formatStr string) ([]OutputToken, []string, error) {
	toks := make([]OutputToken, 0, 16)
	curTok := make([]rune, 0, 16)
	strToks := make([]string, 0, 8)

	for _, c := range formatStr {
		if c == '%' && len(curTok) > 0 && curTok[0] != '%' {
			strToks = append(strToks, string(curTok))
			toks = append(toks, OUT_TOK_STR)
			curTok = curTok[:0]
			curTok = append(curTok, c)
			continue
		}

		curTok = append(curTok, c)
		if curTok[0] == '%' && len(curTok) == 2 {
			switch string(curTok) {
			case "%%":
				strToks = append(strToks, "%")
				toks = append(toks, OUT_TOK_STR)
			case "%p":
				toks = append(toks, OUT_TOK_PATH)
			case "%T":
				toks = append(toks, OUT_TOK_TITLE)
			case "%d":
				toks = append(toks, OUT_TOK_DATE)
			case "%f":
				toks = append(toks, OUT_TOK_FILETIME)
			case "%a":
				toks = append(toks, OUT_TOK_AUTHORS)
			case "%t":
				toks = append(toks, OUT_TOK_TAGS)
			case "%l":
				toks = append(toks, OUT_TOK_LINKS)
			case "%m":
				toks = append(toks, OUT_TOK_META)
			default:
				return nil, nil, ErrUnrecognizedOutputToken
			}
			curTok = curTok[:0]
		}
	}

	if len(curTok) != 0 && curTok[len(curTok)-1] == '%' {
		fmt.Println("2")
		return nil, nil, ErrUnrecognizedOutputToken
	} else if len(curTok) != 0 {
		strToks = append(strToks, string(curTok))
		toks = append(toks, OUT_TOK_STR)
	}

	return toks, strToks, nil
}

func NewCustomOutput(
	formatStr string, datetimeFormat string,
	docSeparator string, listSeparator string,
) (CustomOutput, error) {
	outToks, strToks, err := ParseOutputFormat(formatStr)
	if err != nil {
		return CustomOutput{}, err
	}

	return CustomOutput{
		strToks,
		outToks,
		datetimeFormat,
		docSeparator,
		listSeparator,
	}, nil
}

func (o CustomOutput) OutputOne(doc *index.Document) (string, error) {
	b := strings.Builder{}

	if _, err := o.writeDoc(&b, doc); err != nil {
		return "", err
	}

	return b.String(), nil
}

func (o CustomOutput) OutputOneTo(w io.Writer, doc *index.Document) (int, error) {
	return o.writeDoc(w, doc)
}

func (o CustomOutput) Output(docs []*index.Document) (string, error) {
	b := strings.Builder{}

	for i, doc := range docs {
		if _, err := o.writeDoc(&b, doc); err != nil {
			return "", err
		}
		if i != len(docs)-1 {
			b.WriteRune('\n')
		}
	}

	return b.String(), nil
}

func (o CustomOutput) OutputTo(w io.Writer, docs []*index.Document) (int, error) {
	n := 0

	for _, doc := range docs {
		nn, err := o.writeDoc(w, doc)
		if err != nil {
			return n, err
		}
		n += nn
	}

	return n, nil
}

func (o CustomOutput) writeDoc(w io.Writer, doc *index.Document) (int, error) {
	curStrTok := 0
	var b bytes.Buffer
	for _, token := range o.tokens {
		switch token {
		case OUT_TOK_STR:
			if curStrTok >= len(o.stringTokens) {
				return 0, ErrExpectedMoreStringTokens
			}
			b.WriteString(o.stringTokens[curStrTok])
			curStrTok++
		case OUT_TOK_PATH:
			b.WriteString(doc.Path)
		case OUT_TOK_TITLE:
			b.WriteString(doc.Title)
		case OUT_TOK_DATE:
			b.WriteString(doc.Date.Format(o.datetimeFormat))
		case OUT_TOK_FILETIME:
			b.WriteString(doc.FileTime.Format(o.datetimeFormat))
		case OUT_TOK_AUTHORS:
			b.WriteString(strings.Join(doc.Authors, o.listSeparator))
		case OUT_TOK_TAGS:
			b.WriteString(strings.Join(doc.Tags, o.listSeparator))
		case OUT_TOK_LINKS:
			b.WriteString(strings.Join(doc.Links, o.listSeparator))
		case OUT_TOK_META:
			b.WriteString(doc.OtherMeta)
		default:
			return 0, ErrUnrecognizedOutputToken
		}
	}

	b.WriteString(o.docSeparator)
	n, err := io.Copy(w, &b)
	return int(n), err
}
