package query

import (
	"fmt"
	"strings"
)

func writeIndent(b *strings.Builder, level int) {
	for range level {
		b.WriteByte('\t')
	}
}

func Compile(userQuery string, optimizationLevel int, numWorkers uint) (CompilationArtifact, error) {
	if numWorkers == 0 {
		return CompilationArtifact{}, fmt.Errorf("Cannot compile with 0 workers")
	}

	clause, err := Parse(Lex(userQuery))
	if err != nil {
		return CompilationArtifact{}, err
	}

	NewOptimizer(clause, numWorkers).Optimize(optimizationLevel)

	return clause.Compile()
}
