package main

import "os"

func main() {
	// TODO: command line args
	state := make(State)
	interpreter := NewInterpreter(state, os.Stdin)

	interpreter.Run()
}
