package query

import (
	"errors"
	"slices"
	"testing"
)

func Test_parseOutputFormat(t *testing.T) {
	tests := []struct {
		name        string
		formatStr   string
		wantToks    []OutputToken
		wantStrToks []string
		wantErr     error
	}{
		{
			"one big string",
			"here is a string with no placeholders",
			[]OutputToken{OUT_TOK_STR},
			[]string{"here is a string with no placeholders"},
			nil,
		},
		{
			"default format",
			"%p %T %d authors:%a tags:%t",
			[]OutputToken{OUT_TOK_PATH, OUT_TOK_STR, OUT_TOK_TITLE, OUT_TOK_STR, OUT_TOK_DATE, OUT_TOK_STR, OUT_TOK_AUTHORS, OUT_TOK_STR, OUT_TOK_TAGS},
			[]string{" ", " ", " authors:", " tags:"},
			nil,
		},
		{
			"literal percents",
			"%%%p%%%T%%",
			[]OutputToken{OUT_TOK_STR, OUT_TOK_PATH, OUT_TOK_STR, OUT_TOK_TITLE, OUT_TOK_STR},
			[]string{"%", "%", "%"},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotToks, gotStrToks, gotErr := parseOutputFormat(tt.formatStr)

			if !errors.Is(gotErr, tt.wantErr) {
				t.Errorf("Recieved unexpected error: got %v want %v", gotErr, tt.wantErr)
			} else if gotErr != nil {
				return
			}

			if !slices.Equal(gotToks, tt.wantToks) {
				t.Error("Unequal output tokens")
				t.Log("Got:", gotToks)
				t.Log("Want:", tt.wantToks)
			}

			if !slices.Equal(gotStrToks, tt.wantStrToks) {
				t.Error("Unequal string tokens")
				t.Log("Got:", gotStrToks)
				t.Log("Want:", tt.wantStrToks)
			}
		})
	}
}
