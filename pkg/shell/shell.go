package shell

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"golang.org/x/term"
)

func (inter *Interpreter) runNonInteractive() error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		err := scanner.Err()
		if err != nil {
			return err
		}
		tokens := inter.Tokenize(scanner.Text())
		fatal, err := inter.Eval(os.Stdout, tokens)
		if fatal {
			return err
		} else if err != nil {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
	}
	return io.EOF
}

func (inter *Interpreter) Run() error {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return inter.runNonInteractive()
	}

	defer fmt.Println("\nLeaving atlasi.")

	width, height, err := term.GetSize(int(os.Stdin.Fd()))
	if err != nil {
		panic(err)
	}
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		panic(err)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)
	inter.term = term.NewTerminal(os.Stdin, "atlasi> ")

	if err := inter.term.SetSize(width, height); err != nil {
		panic(err)
	}
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
