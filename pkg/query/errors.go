package query

import (
	"errors"
	"fmt"
)

var ErrQueryFormat = errors.New("Incorrect query format")
var ErrDatetimeTokenParse = errors.New("Unrecognized format for datetime token")

type TokenError struct {
	got      Token
	gotPrev  Token
	wantPrev string
}

func (e *TokenError) Error() string {
	if e.wantPrev != "" {
		return fmt.Sprintf("Unexpected token: got %s, got previous %s want previous %s", e.got, e.gotPrev, e.wantPrev)
	}

	return fmt.Sprintf("Unexpected token: got %s, got previous %s", e.got, e.gotPrev)
}
