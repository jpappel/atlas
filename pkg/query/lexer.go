package query

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

type queryTokenType int

var LexRegex *regexp.Regexp
var LexRegexPattern string

const (
	TOK_UNKNOWN queryTokenType = iota

	// clause tokens
	TOK_CLAUSE_OR  // clause or
	TOK_CLAUSE_AND // clause and
	TOK_CLAUSE_START
	TOK_CLAUSE_END

	// statement tokens
	TOK_OP_NEG // negation
	TOK_OP_EQ  // equal
	TOK_OP_AP  // approximate/fuzzy
	TOK_OP_NE  // not equal
	TOK_OP_LT  // less than
	TOK_OP_LE  // less than or equal
	TOK_OP_GE  // greater than or equal
	TOK_OP_GT  // greater than
	TOK_OP_RE  // regex match
	// categories
	TOK_CAT_PATH
	TOK_CAT_TITLE
	TOK_CAT_AUTHOR
	TOK_CAT_DATE
	TOK_CAT_FILETIME
	TOK_CAT_TAGS
	TOK_CAT_LINKS
	TOK_CAT_META
	// TODO: add headings
	// values
	TOK_VAL_STR
	TOK_VAL_DATETIME
)

type Token struct {
	Type  queryTokenType
	Value string
}

func (tokType queryTokenType) String() string {
	switch tokType {
	case TOK_UNKNOWN:
		return "Unknown"
	case TOK_CLAUSE_OR:
		return "Or"
	case TOK_CLAUSE_AND:
		return "And"
	case TOK_CLAUSE_START:
		return "Start Clause"
	case TOK_CLAUSE_END:
		return "End Clause"
	case TOK_OP_NEG:
		return "Negation"
	case TOK_OP_EQ:
		return "Equal"
	case TOK_OP_AP:
		return "Approximate"
	case TOK_OP_RE:
		return "Regular Expression"
	case TOK_OP_NE:
		return "Not Equal"
	case TOK_OP_LT:
		return "Less Than"
	case TOK_OP_LE:
		return "Less Than or Equal"
	case TOK_OP_GE:
		return "Greater Than or Equal"
	case TOK_OP_GT:
		return "Greater Than"
	case TOK_CAT_PATH:
		return "Filepath Category"
	case TOK_CAT_TITLE:
		return "Title Category"
	case TOK_CAT_AUTHOR:
		return "Author Category"
	case TOK_CAT_DATE:
		return "Date Category"
	case TOK_CAT_FILETIME:
		return "Filetime Category"
	case TOK_CAT_TAGS:
		return "Tags Category"
	case TOK_CAT_LINKS:
		return "Links Category"
	case TOK_CAT_META:
		return "Metadata Category"
	case TOK_VAL_DATETIME:
		return "Datetime Value"
	case TOK_VAL_STR:
		return "String Value"
	default:
		return "Invalid"
	}
}

func (t Token) String() string {
	return fmt.Sprint(t.Type.String(), ": ", t.Value)
}

func (t Token) Equal(other Token) bool {
	if t.Type.isValue() {
		return t.Type == other.Type && t.Value == other.Value
	}
	return t.Type == other.Type
}

// if a token type is one of any
func (tokType queryTokenType) Any(expected ...queryTokenType) bool {
	return slices.Contains(expected, tokType)
}

func (t queryTokenType) isCategory() bool {
	return t.Any(TOK_CAT_PATH, TOK_CAT_TITLE, TOK_CAT_AUTHOR, TOK_CAT_DATE, TOK_CAT_FILETIME, TOK_CAT_TAGS, TOK_CAT_LINKS, TOK_CAT_META)
}

func (t queryTokenType) isDateOperation() bool {
	return t.Any(TOK_OP_EQ, TOK_OP_AP, TOK_OP_NE, TOK_OP_LT, TOK_OP_LE, TOK_OP_GE, TOK_OP_GT)
}

func (t queryTokenType) isStringOperation() bool {
	return t.Any(TOK_OP_EQ, TOK_OP_AP, TOK_OP_NE, TOK_OP_RE)
}

func (t queryTokenType) isValue() bool {
	return t == TOK_VAL_STR || t == TOK_VAL_DATETIME
}

func Lex(query string) []Token {
	const (
		MATCH = iota
		CLAUSE_START
		CLAUSE_OPERATOR
		STATEMENT
		NEGATION
		CATEGORY
		OPERATOR
		VALUE
		UNKNOWN
		CLAUSE_END
	)

	matches := LexRegex.FindAllStringSubmatch(query, -1)
	tokens := make([]Token, 0, 4*len(matches))

	tokens = append(tokens, Token{Type: TOK_CLAUSE_START})
	tokens = append(tokens, Token{TOK_CLAUSE_AND, "and"}) // default to and'ing all args
	clauseLevel := 1
	for _, match := range matches {
		if match[CLAUSE_START] != "" {
			tokens = append(tokens, Token{Type: TOK_CLAUSE_START})
			clauseLevel += 1
		}
		if match[CLAUSE_OPERATOR] != "" {
			if len(tokens) == 0 || tokens[len(tokens)-1].Type != TOK_CLAUSE_START {
				tokens = append(tokens, Token{Type: TOK_CLAUSE_START})
				clauseLevel += 1
			}
			tokens = append(tokens, tokenizeClauseOperation(match[CLAUSE_OPERATOR]))
		}

		if t, ok := tokenizeNegation(match[NEGATION]); ok {
			tokens = append(tokens, t)
		}

		if match[CATEGORY] != "" {
			tokens = append(tokens, tokenizeCategory(match[CATEGORY]))
		}
		if match[OPERATOR] != "" {
			tokens = append(tokens, tokenizeOperation(match[OPERATOR]))
		}
		if match[VALUE] != "" {
			tokens = append(tokens, tokenizeValue(match[VALUE], tokens[len(tokens)-2].Type))
		}

		if match[UNKNOWN] != "" {
			tokens = append(tokens, Token{Value: match[UNKNOWN]})
		}

		if match[CLAUSE_END] != "" {
			tokens = append(tokens, Token{Type: TOK_CLAUSE_END})
			clauseLevel -= 1
		}
	}

	for range clauseLevel {
		tokens = append(tokens, Token{Type: TOK_CLAUSE_END})
	}

	return tokens
}

func tokenizeClauseOperation(s string) Token {
	t := Token{Value: s}
	switch s {
	case "and", "AND":
		t.Type = TOK_CLAUSE_AND
	case "or", "OR":
		t.Type = TOK_CLAUSE_OR
	}
	return t
}

func tokenizeNegation(s string) (Token, bool) {
	t := Token{Value: s}
	if s == "-" {
		t.Type = TOK_OP_NEG
	}

	return t, len(s) > 0
}

func tokenizeOperation(s string) Token {
	t := Token{Value: s}
	switch s {
	case "!=":
		t.Type = TOK_OP_NE
	case "<=":
		t.Type = TOK_OP_LE
	case ">=":
		t.Type = TOK_OP_GE
	case "=":
		t.Type = TOK_OP_EQ
	case ":", "~":
		t.Type = TOK_OP_AP
	case "<":
		t.Type = TOK_OP_LT
	case ">":
		t.Type = TOK_OP_GT
	case "!re!":
		t.Type = TOK_OP_RE
	}

	return t
}

func tokenizeCategory(s string) Token {
	t := Token{Value: s}
	switch s {
	case "p", "path":
		t.Type = TOK_CAT_PATH
	case "T", "title":
		t.Type = TOK_CAT_TITLE
	case "a", "author":
		t.Type = TOK_CAT_AUTHOR
	case "d", "date":
		t.Type = TOK_CAT_DATE
	case "f", "filetime":
		t.Type = TOK_CAT_FILETIME
	case "t", "tags":
		t.Type = TOK_CAT_TAGS
	case "l", "links":
		t.Type = TOK_CAT_LINKS
	case "m", "meta":
		t.Type = TOK_CAT_META
	}
	return t
}

func tokenizeValue(s string, catType queryTokenType) Token {
	t := Token{}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		t.Value = s[1 : len(s)-1]
	} else {
		t.Value = s
	}
	switch catType {
	case TOK_CAT_DATE, TOK_CAT_FILETIME:
		t.Type = TOK_VAL_DATETIME
	case TOK_CAT_PATH, TOK_CAT_TITLE, TOK_CAT_AUTHOR, TOK_CAT_TAGS, TOK_CAT_LINKS, TOK_CAT_META:
		t.Type = TOK_VAL_STR
	}
	return t
}

func TokensStringify(tokens []Token) string {
	b := strings.Builder{}

	indentLvl := 0
	writeToken := func(t Token) {
		b.WriteByte('`')
		b.WriteString(t.String())
		b.WriteByte('`')
	}

	for i, token := range tokens {
		switch token.Type {
		case TOK_CLAUSE_START:
			writeIndent(&b, indentLvl)
			b.WriteByte('(')
		case TOK_CLAUSE_END:
			indentLvl -= 1
			writeIndent(&b, indentLvl)
			b.WriteString(")\n")
		case TOK_CLAUSE_OR:
			b.WriteString("or\n")
			indentLvl += 1
		case TOK_CLAUSE_AND:
			b.WriteString("and\n")
			indentLvl += 1
		case TOK_CAT_PATH, TOK_CAT_TITLE, TOK_CAT_AUTHOR, TOK_CAT_DATE, TOK_CAT_FILETIME, TOK_CAT_TAGS, TOK_CAT_LINKS, TOK_CAT_META, TOK_OP_NEG:
			if i == 0 || tokens[i-1].Type != TOK_OP_NEG {
				writeIndent(&b, indentLvl)
			}
			writeToken(token)
		case TOK_VAL_STR, TOK_VAL_DATETIME, TOK_UNKNOWN:
			writeToken(token)
			b.WriteByte('\n')
		default:
			writeToken(token)
		}
	}

	return b.String()
}

func init() {
	negPattern := `(?<negation>-?)`
	categoryPattern := `(?<category>T|p(?:ath)?|a(?:uthor)?|d(?:ate)?|f(?:iletime)?|t(?:ags|itle)?|l(?:inks)?|m(?:eta)?)`
	opPattern := `(?<operator>!re!|!=|<=|>=|=|:|~|<|>)`
	valPattern := `(?<value>".*?"|\S*[^\s\)])`
	statementPattern := `(?<statement>` + negPattern + categoryPattern + opPattern + valPattern + `)`
	unknownPattern := `(?<unknown>\S*".*?"[^\s)]*|\S*[^\s\)])`

	clauseOpPattern := `(?<clause_operator>(?i)and|or)?`
	clauseStart := `(?<clause_start>\()?`
	clauseEnd := `(?<clause_end>\))?`
	clausePattern := clauseStart + `\s*` + clauseOpPattern + `\s*(?:` + statementPattern + `|` + unknownPattern + `)\s*` + clauseEnd + `\s*`
	LexRegexPattern = clausePattern

	// FIXME: fails to match start of clauses with no values
	//        example: (and (or ... )) fails
	LexRegex = regexp.MustCompile(LexRegexPattern)
}
