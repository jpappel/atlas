package query

type TokenType uint64

const (
    TOKEN_ERROR TokenType = iota
    TOKEN_EOF
	TOKEN_AND
	TOKEN_OR
	TOKEN_NOT
	TOKEN_SIMILAR
	TOKEN_STATEMENT
	// TODO: consider adding regex token
)

type Token struct {
	Type    TokenType
	Content string
}
