package shell

import (
	"fmt"
	"os"

	"golang.org/x/term"
)

var commands = []string{
	"help",
	"clear",
	"let",
	"del",
	"slice",
	"rematch",
	"repattern",
	"tokenize",
	"opt_simplify", "opt_tighten", "opt_flatten", "opt_sort", "opt_tidy", "opt_contradictions", "opt_compact", "opt_strictEq",
	"parse",
	"compile",
}

func (inter *Interpreter) Run() error {
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		panic(err)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)
	inter.term = term.NewTerminal(os.Stdin, "atlasi> ")
	inter.term.SetPrompt(
		string(inter.term.Escape.Yellow) + "atlasi> " +
			string(inter.term.Escape.Reset),
	)

	for {
		line, err := inter.term.ReadLine()
		if err != nil {
			return err
		}
		tokens := inter.Tokenize(line)
		fatal, err := inter.Eval(inter.term, tokens)
		if fatal {
			return err
		} else if err != nil {
			fmt.Fprintln(inter.term, string(inter.term.Escape.Red), "Error:",
				string(inter.term.Escape.Reset), err)
		}
	}
}
