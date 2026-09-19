package cmd

import (
	_ "embed"
	"fmt"
)

//go:embed completions/atlas.zsh
var zshCompletions string

func ZshCompletions() {
	fmt.Print(zshCompletions)
}
