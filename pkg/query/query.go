package query

import "strings"

func writeIndent(b *strings.Builder, level int) {
	for range level {
		b.WriteByte('\t')
	}
}
