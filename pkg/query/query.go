package query

import "strings"

func Generate(ir *QueryIR) (any, error) {
	// TODO: implement
	return nil, nil
}

func Compile(query string) (any, error) {
	// TODO: logging
	clause, err := Parse(Lex(query))
	if err != nil {
		return nil, err
	}

	ir, err := NewIR(*clause)
	if err != nil {
		return nil, err
	}

	ir, err = Optimize(ir)
	if err != nil {
		return nil, err
	}

	return Generate(ir)
}

func writeIndent(b *strings.Builder, level int) {
	for range level {
		b.WriteByte('\t')
	}
}
