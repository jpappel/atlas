package query

import (
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
	OutputOne(doc index.Document) (string, error)
	Output(docs []index.Document) (string, error)
}

type JsonOutput struct{}
type CustomOutput struct {
	stringTokens   []string
	tokens         []OutputToken
	datetimeFormat string
}

func (o JsonOutput) OutputOne(doc index.Document) (string, error) {
	// TODO: implement
	return "", nil
}

func (o JsonOutput) Output(docs []index.Document) (string, error) {
	// TODO: implement
	return "", nil
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
			s := string(curTok)
			if s == "%%" {
				strToks = append(strToks, "%")
				toks = append(toks, OUT_TOK_STR)
			} else if s == "%p" {
				toks = append(toks, OUT_TOK_PATH)
			} else if s == "%T" {
				toks = append(toks, OUT_TOK_TITLE)
			} else if s == "%d" {
				toks = append(toks, OUT_TOK_DATE)
			} else if s == "%f" {
				toks = append(toks, OUT_TOK_FILETIME)
			} else if s == "%a" {
				toks = append(toks, OUT_TOK_AUTHORS)
			} else if s == "%t" {
				toks = append(toks, OUT_TOK_TAGS)
			} else if s == "%l" {
				toks = append(toks, OUT_TOK_LINKS)
			} else if s == "%m" {
				toks = append(toks, OUT_TOK_META)
			} else {
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

func (o CustomOutput) OutputOne(doc index.Document) (string, error) {
	b := strings.Builder{}
	// TODO: determine realistic initial capacity

	if err := o.writeDoc(&b, doc); err != nil {
		return "", err
	}

	return b.String(), nil
}

func (o CustomOutput) Output(docs []index.Document) (string, error) {
	b := strings.Builder{}
	// TODO: determine realistic initial capacity

	for i := range len(docs) - 1 {
		if err := o.writeDoc(&b, docs[i]); err != nil {
			return "", err
		}
		b.WriteRune('\n')
	}
	if err := o.writeDoc(&b, docs[len(docs)-1]); err != nil {
		return "", err
	}
	b.WriteRune('\n')

	return b.String(), nil
}

func (o CustomOutput) writeDoc(b *strings.Builder, doc index.Document) error {
	curStrTok := 0
	for _, token := range o.tokens {
		if token == OUT_TOK_STR {
			if curStrTok >= len(o.stringTokens) {
				return ErrExpectedMoreStringTokens
			}
			b.WriteString(o.stringTokens[curStrTok])
			curStrTok++
		} else if token == OUT_TOK_PATH {
			b.WriteString(doc.Path)
		} else if token == OUT_TOK_TITLE {
			b.WriteString(doc.Title)
		} else if token == OUT_TOK_DATE {
			b.WriteString(doc.Date.Format(o.datetimeFormat))
		} else if token == OUT_TOK_FILETIME {
			b.WriteString(doc.FileTime.Format(o.datetimeFormat))
		} else if token == OUT_TOK_AUTHORS {
			b.WriteString(strings.Join(doc.Authors, ", "))
		} else if token == OUT_TOK_TAGS {
			b.WriteString(strings.Join(doc.Tags, ", "))
		} else if token == OUT_TOK_LINKS {
			b.WriteString(strings.Join(doc.Links, ", "))
		} else if token == OUT_TOK_META {
			b.WriteString(doc.OtherMeta)
		} else {
			return ErrUnrecognizedOutputToken
		}
	}
	return nil
}
