package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jpappel/atlas/pkg/index"
)

var ErrUnrecognizedOutputToken = errors.New("Unrecognized output token")
var ErrExpectedMoreStringTokens = errors.New("Expected more string tokens")

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
	Output(docs []*index.Document) (string, error)
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

func (o DefaultOutput) writeDoc(b *strings.Builder, doc *index.Document) bool {
	if b == nil {
		return false
	}

	b.WriteString(doc.Path)
	b.WriteRune(' ')
	b.WriteString(doc.Title)
	b.WriteRune(' ')
	b.WriteString(doc.Date.String())
	b.WriteRune(' ')
	b.WriteString("authors:")
	b.WriteString(strings.Join(doc.Authors, ","))
	b.WriteString(" tags:")
	b.WriteString(strings.Join(doc.Tags, ","))

	return true
}

func (o JsonOutput) OutputOne(doc *index.Document) (string, error) {
	b, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (o JsonOutput) Output(docs []*index.Document) (string, error) {
	b, err := json.Marshal(docs)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parseOutputFormat(formatStr string) ([]OutputToken, []string, error) {
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
	outToks, strToks, err := parseOutputFormat(formatStr)
	if err != nil {
		return CustomOutput{}, err
	}

	return CustomOutput{strToks, outToks, datetimeFormat}, nil
}

func (o CustomOutput) OutputOne(doc *index.Document) (string, error) {
	b := strings.Builder{}
	// TODO: determine realistic initial capacity

	if err := o.writeDoc(&b, doc); err != nil {
		return "", err
	}

	return b.String(), nil
}

func (o CustomOutput) Output(docs []*index.Document) (string, error) {
	b := strings.Builder{}
	// TODO: determine realistic initial capacity

	for i, doc := range docs {
		if err := o.writeDoc(&b, doc); err != nil {
			return "", err
		}
		if i != len(docs)-1 {
			b.WriteRune('\n')
		}
	}

	return b.String(), nil
}

func (o CustomOutput) writeDoc(b *strings.Builder, doc *index.Document) error {
	curStrTok := 0
	for _, token := range o.tokens {
		switch token {
		case OUT_TOK_STR:
			if curStrTok >= len(o.stringTokens) {
				return ErrExpectedMoreStringTokens
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
			b.WriteString(strings.Join(doc.Authors, ", "))
		case OUT_TOK_TAGS:
			b.WriteString(strings.Join(doc.Tags, ", "))
		case OUT_TOK_LINKS:
			b.WriteString(strings.Join(doc.Links, ", "))
		case OUT_TOK_META:
			b.WriteString(doc.OtherMeta)
		default:
			return ErrUnrecognizedOutputToken
		}
	}
	return nil
}
