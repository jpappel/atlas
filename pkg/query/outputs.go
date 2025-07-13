package query

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/jpappel/atlas/pkg/index"
)

const DefaultOutputFormat string = "%p %T %d authors:%a tags:%t"

type OutputToken uint64

// TODO: support long token names
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

// TODO: change interface to use byte slices
type Outputer interface {
	OutputOne(doc *index.Document) (string, error)
	OutputOneTo(w io.Writer, doc *index.Document) (int, error)
	Output(docs []*index.Document) (string, error)
	OutputTo(w io.Writer, docs []*index.Document) (int, error)
}

type DefaultOutput struct{}
type JsonOutput struct{}
type CustomOutput struct {
	stringTokens   []string
	tokens         []OutputToken
	datetimeFormat string
}

// compile time interface check
var _ Outputer = &DefaultOutput{}
var _ Outputer = &JsonOutput{}
var _ Outputer = &CustomOutput{}

// Returns "<path> <title> <date> authors:<authors...> tags:<tags>"
// and a nil error
func (o DefaultOutput) OutputOne(doc *index.Document) (string, error) {
	b := strings.Builder{}
	o.writeDoc(&b, doc)

	return b.String(), nil
}

func (o DefaultOutput) OutputOneTo(w io.Writer, doc *index.Document) (int, error) {
	return o.writeDoc(w, doc)
}

func (o DefaultOutput) Output(docs []*index.Document) (string, error) {
	b := strings.Builder{}

	for i, doc := range docs {
		o.writeDoc(&b, doc)
		if i != len(docs)-1 {
			b.WriteRune('\n')
		}
	}

	return b.String(), nil
}

func (o DefaultOutput) OutputTo(w io.Writer, docs []*index.Document) (int, error) {
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

func (o DefaultOutput) writeDoc(w io.Writer, doc *index.Document) (int, error) {
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

func NewCustomOutput(formatStr string, datetimeFormat string) (CustomOutput, error) {
	outToks, strToks, err := ParseOutputFormat(formatStr)
	if err != nil {
		return CustomOutput{}, err
	}

	return CustomOutput{strToks, outToks, datetimeFormat}, nil
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
	n := 0
	for _, token := range o.tokens {
		switch token {
		case OUT_TOK_STR:
			if curStrTok >= len(o.stringTokens) {
				return n, ErrExpectedMoreStringTokens
			}
			cnt, err := w.Write([]byte(o.stringTokens[curStrTok]))
			if err != nil {
				return n, err
			}
			n += cnt
			curStrTok++
		case OUT_TOK_PATH:
			cnt, err := w.Write([]byte(doc.Path))
			if err != nil {
				return n, err
			}
			n += cnt
		case OUT_TOK_TITLE:
			cnt, err := w.Write([]byte(doc.Title))
			if err != nil {
				return n, err
			}
			n += cnt
		case OUT_TOK_DATE:
			cnt, err := w.Write([]byte(doc.Date.Format(o.datetimeFormat)))
			if err != nil {
				return n, err
			}
			n += cnt
		case OUT_TOK_FILETIME:
			cnt, err := w.Write([]byte(doc.FileTime.Format(o.datetimeFormat)))
			if err != nil {
				return n, err
			}
			n += cnt
		case OUT_TOK_AUTHORS:
			cnt, err := w.Write([]byte(strings.Join(doc.Authors, ", ")))
			if err != nil {
				return n, err
			}
			n += cnt
		case OUT_TOK_TAGS:
			cnt, err := w.Write([]byte(strings.Join(doc.Tags, ", ")))
			if err != nil {
				return n, err
			}
			n += cnt
		case OUT_TOK_LINKS:
			cnt, err := w.Write([]byte(strings.Join(doc.Links, ", ")))
			if err != nil {
				return n, err
			}
			n += cnt
		case OUT_TOK_META:
			cnt, err := w.Write([]byte(doc.OtherMeta))
			if err != nil {
				return n, err
			}
			n += cnt
		default:
			return n, ErrUnrecognizedOutputToken
		}
	}
	return n, nil
}
