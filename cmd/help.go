package cmd

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jpappel/atlas/pkg/shell"
	"github.com/jpappel/atlas/pkg/util"
)

var helpTopics = []string{
	"index", "i",
	"index build", "i build",
	"index update", "i update",
	"index tidy", "i tidy",
	"query", "q",
	"shell",
	"server",
}

func PrintHelp(w io.Writer) {
	fmt.Fprintln(w, "atlas is a note indexing and querying tool")
	fmt.Fprintf(w, "\nUsage:\n  %s [global-flags] <command>\n\n", os.Args[0])
	fmt.Fprintln(w, "Commands:")
	fmt.Fprintln(w, "  index <subcommand>    - build, update, or modify an index")
	fmt.Fprintln(w, "  query <subcommand>    - search against an index")
	fmt.Fprintln(w, "  shell                 - start a debug shell")
	fmt.Fprintln(w, "  server                - start an http query server (EXPERIMENTAL)")
	fmt.Fprintln(w, "  help  <help-topic>    - print help info")
}

func PrintGlobalFlags(w io.Writer) {
	fmt.Fprintln(w, "\nGlobal Flags:")
	PrintFlagSet(w, flag.CommandLine)
}

func PrintFlagSet(w io.Writer, fs *flag.FlagSet) {
	w_ := fs.Output()
	fs.SetOutput(w)
	fs.PrintDefaults()
	fs.SetOutput(w_)
}

func Help(topic string, w io.Writer) {
	fs := flag.NewFlagSet(topic, flag.ExitOnError)
	switch topic {
	case "index", "i":
		SetupIndexFlags(nil, fs, &IndexFlags{})
		fmt.Fprintf(w, "%s [global-flags] index [index-flags] <subcommand>\n\n", os.Args[0])
		fmt.Fprintln(w, "Subcommands:")
		fmt.Fprintln(w, "  build  - create a new index")
		fmt.Fprintln(w, "  update - update an existing index")
		fmt.Fprintln(w, "  tidy   - cleanup an index")
		fmt.Fprintf(w, "\nSee %s help index <subcommand> for subcommand help\n\n", os.Args[0])
		fmt.Fprintln(w, "Index Flags:")
		PrintFlagSet(w, fs)
	case "i build", "index build":
		fmt.Fprintf(w, "%s [global-flags] index [index-flags] build\n\n", os.Args[0])
		fmt.Fprintln(w, "Crawl files starting at `-root` to build an index stored in `-db`")
		fmt.Fprintln(w, "Use this subcommand to generate the initial index, then update it with `atlas index update`")
	case "i update", "index update":
		fmt.Fprintf(w, "%s [global-flags] index [index-flags] update\n\n", os.Args[0])
		fmt.Fprintln(w, "Crawl files starting at `-root` to update an index stored in `-db`")
		fmt.Fprintln(w, "Use this subcommand to update an existing index.")
		fmt.Fprintln(w, "Deleted documents are removed from the index. To remove unused authors and tags run `atlas index tidy`")
	case "i tidy", "index tidy":
		fmt.Fprintf(w, "%s [global-flags] index tidy\n\n", os.Args[0])
		fmt.Fprintln(w, "Remove unused authors or tags and optimize the database")
	case "query", "q":
		SetupQueryFlags(nil, fs, &QueryFlags{}, "")
		fmt.Fprintf(w, "%s [global-flags] query [query-flags] <query>...\n\n", os.Args[0])
		fmt.Fprintln(w, "Execute a query against the connected database")
		fmt.Fprintln(w, "Query Flags:")
		PrintFlagSet(w, fs)
		fmt.Fprintln(w, "\nOutput Format:")
		help := `The output format of query results can be customized by setting -outCustomFormat.

  The output of each document has the value of -docSeparator appended to it.
  Dates are formated using -dateFormat
  Lists use -listSeparator to delimit elements

  Placeholder - Type - Value
       %p     - Str  - path
       %T     - Str  - title
       %d     - Date - date
       %f     - Date - filetime
       %a     - List - authors
       %t     - List - tags
	   %h     - Str  - headings (newline separated)
       %l     - List - links
       %m     - Str  - meta

  Examples:
    "%p %T %d tags:%t" -> '/a/path/to/document A Title 2006-01-02T15:04:05Z07:00 tags:tag1, tag2\n'
    "<h1><a href="%p">%T</a></h1>" -> '<h1><a href="/a/path/to/document">A Title</a></h1>\n'

`
		fmt.Fprint(w, help)
	case "shell":
		fmt.Fprintf(w, "%s [global-flags] shell\n", os.Args[0])
		fmt.Fprintln(w, "Simple shell for debugging queries")
		fmt.Fprintln(w, "\nShell Help:")
		shell.PrintHelp(w)
	case "server":
		SetupServerFlags(nil, fs, &ServerFlags{})
		fmt.Fprintf(w, "%s [global-flags] server [server-flags]\n", os.Args[0])
		fmt.Fprintln(w, "Run a server to execute queries over HTTP or a unix domain socket")
		fmt.Fprintln(w, "HTTP Server:")
		fmt.Fprintln(w, "  To execute a query POST it in the request body to /search")
		fmt.Fprintln(w, "  ex. curl -d 'T:notes d>=\"January 1, 2025\"' 127.0.0.1:8080/search")
		fmt.Fprintln(w, "  To have the backend use the query params `sortBy` and `sortOrder`")
		fmt.Fprintln(w, "    sortBy: path, title, date, filetime, meta")
		fmt.Fprintln(w, "    sortOrder: desc, descending")
		fmt.Fprintln(w, "Server Flags:")
		PrintFlagSet(w, fs)
	case "help", "":
		PrintHelp(w)
		fmt.Fprintln(w, "\nHelp Topics:")
		curLineLen := 2
		fmt.Fprint(w, "  ")
		for i, topic := range helpTopics {
			if curLineLen+len(topic) < 80 {
				curLineLen += len(topic)
				fmt.Fprint(w, topic)
			} else {
				fmt.Fprintln(w, topic)
				fmt.Fprint(w, "  ")
				curLineLen = 2
			}
			if i == len(helpTopics)-1 {
				fmt.Fprintln(w)
			} else if curLineLen != 2 {
				fmt.Fprint(w, ", ")
				curLineLen += 3
			}
		}
		PrintGlobalFlags(w)
	default:
		fmt.Fprintln(os.Stderr, "Unrecognized topic: ", topic)
		if suggestion, ok := util.Nearest(topic, helpTopics, util.LevensteinDistance, 3); ok {
			fmt.Fprintf(w, "Did you mean %s?\n", suggestion)
		}
		fmt.Fprintln(w, "See `atlas help`")
	}
}
