package query

import (
	"errors"
	"fmt"
)

var ErrQueryFormat = errors.New("Incorrect query format")
var ErrDatetimeTokenParse = errors.New("Unrecognized format for datetime")

// output errors
var ErrUnrecognizedOutputToken = errors.New("Unrecognized output token")
var ErrExpectedMoreStringTokens = errors.New("Expected more string tokens")

// optimizer errors
var ErrUnexpectedValueType = errors.New("Unexpected value type")
var ErrEmptyResult = errors.New("Queries are contradictory, will lead to an empty result")


type TokenError struct {
	got      Token
	gotPrev  Token
	wantPrev string
}

type CompileError struct {
	s string
}

func (e *TokenError) Error() string {
	if e.wantPrev != "" {
		return fmt.Sprintf("Unexpected token: got %s, got previous %s want previous %s", e.got, e.gotPrev, e.wantPrev)
	}

	return fmt.Sprintf("Unexpected token: got %s, got previous %s", e.got, e.gotPrev)
}

func (e *CompileError) Error() string {
	return fmt.Sprintf("Compile error: %s", e.s)
}
