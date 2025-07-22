package cmd

import (
	"fmt"
	"os"
)

var CommandHelp map[string]string

func PrintHelp() {
	fmt.Println("atlas is a note indexing and querying tool")
	fmt.Printf("\nUsage:\n  %s [global-flags] <command>\n\n", os.Args[0])
	fmt.Println("Commands:")
	fmt.Println("  index        - build, update, or modify an index")
	fmt.Println("  query        - search against an index")
	fmt.Println("  shell        - start a debug shell")
	fmt.Println("  server       - start an http query server (EXPERIMENTAL)")
	fmt.Println("  help         - print this help then exit")
}

func init() {
	CommandHelp = make(map[string]string)
	CommandHelp["query"] = ""
	CommandHelp["index"] = ""
	CommandHelp["server"] = ""
	CommandHelp["completions"] = ""
	CommandHelp["shell"] = ""
	CommandHelp["help"] = ""
}
