package cmd

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/index"
	"github.com/jpappel/atlas/pkg/query"
)

type QueryFlags struct {
	Outputer          query.Outputer
	DocumentSeparator string
	ListSeparator     string
	CustomFormat      string
	OptimizationLevel int
	SortBy            string
	SortDesc          bool
}

func SetupQueryFlags(args []string, fs *flag.FlagSet, flags *QueryFlags, dateFormat string) {
	// NOTE: providing `-outFormat` before `-outCustomFormat` might ignore user specified format
	fs.Func("outFormat", "output `format` for queries (default, json, pathonly, custom)",
		func(arg string) error {
			switch arg {
			case "default":
				flags.Outputer = query.DefaultOutput{}
				return nil
			case "json":
				flags.Outputer = query.JsonOutput{}
				return nil
			case "pathonly":
				flags.Outputer, _ = query.NewCustomOutput("%p", dateFormat, "\n", "")
				return nil
			case "custom":
				var err error
				flags.Outputer, err = query.NewCustomOutput(flags.CustomFormat, dateFormat, flags.DocumentSeparator, flags.ListSeparator)
				return err
			}
			return fmt.Errorf("Unrecognized output format: %s", arg)
		})

	fs.StringVar(&flags.SortBy, "sortBy", "", "category to sort by (path,title,date,filetime,meta)")
	fs.BoolVar(&flags.SortDesc, "sortDesc", false, "sort in descending order")
	fs.StringVar(&flags.CustomFormat, "outCustomFormat", query.DefaultOutputFormat, "format string for --outFormat custom, see Output Format for more details")
	fs.IntVar(&flags.OptimizationLevel, "optLevel", 0, "optimization `level` for queries, 0 is automatic, <0 to disable")
	fs.StringVar(&flags.DocumentSeparator, "docSeparator", "\n", "separator for custom output format")
	fs.StringVar(&flags.ListSeparator, "listSeparator", ", ", "separator for list fields")

	fs.Usage = func() {
		f := fs.Output()
		fmt.Fprintf(f, "Usage of %s %s\n", os.Args[0], fs.Name())
		fmt.Fprintf(f, "  %s [global-flags] %s [query-flags]\n\n",
			os.Args[0], fs.Name())
		fmt.Fprintln(f, "Query Flags:")
		fs.PrintDefaults()
		fmt.Fprintln(f, "\nOutput Format:")
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
       %l     - List - links
       %m     - Str  - meta

  Examples:
    "%p %T %d tags:%t" -> '/a/path/to/document A Title 2006-01-02T15:04:05Z07:00 tags:tag1, tag2\n'
    "<h1><a href="%p">%T</a></h1>" -> '<h1><a href="/a/path/to/document">A Title</a></h1>\n'

`
		fmt.Fprint(f, help)
		fmt.Fprintln(f, "Global Flags:")
		flag.PrintDefaults()
	}

	fs.Parse(args)
}

func RunQuery(gFlags GlobalFlags, qFlags QueryFlags, db *data.Query, searchQuery string) byte {
	tokens := query.Lex(searchQuery)
	clause, err := query.Parse(tokens)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to parse query: ", err)
		return 1
	}

	o := query.NewOptimizer(clause, gFlags.NumWorkers)
	o.Optimize(qFlags.OptimizationLevel)

	artifact, err := clause.Compile()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to compile query: ", err)
		return 1
	}

	results, err := db.Execute(artifact)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to execute query: ", err)
		return 1
	}

	if len(results) == 0 {
		fmt.Println("No results.")
		return 0
	}

	outputableResults := make([]*index.Document, 0, len(results))
	for _, v := range results {
		outputableResults = append(outputableResults, v)
	}

	var docCmp func(a, b *index.Document) int
	descMod := 1
	if qFlags.SortDesc {
		descMod = -1
	}
	switch qFlags.SortBy {
	case "":
	case "path":
		docCmp = func(a, b *index.Document) int {
			return descMod * strings.Compare(a.Path, b.Path)
		}
	case "title":
		docCmp = func(a, b *index.Document) int {
			return descMod * strings.Compare(a.Title, b.Title)
		}
	case "date":
		docCmp = func(a, b *index.Document) int {
			return descMod * a.Date.Compare(b.Date)
		}
	case "filetime":
		docCmp = func(a, b *index.Document) int {
			return descMod * a.FileTime.Compare(b.FileTime)
		}
	case "meta":
		docCmp = func(a, b *index.Document) int {
			return descMod * strings.Compare(a.OtherMeta, b.OtherMeta)
		}
	default:
		slog.Error("Unrecognized category to sort by, leaving documents unsorted")
		qFlags.SortBy = ""
	}

	if qFlags.SortBy != "" {
		slices.SortFunc(outputableResults, docCmp)
	}

	_, err = qFlags.Outputer.OutputTo(os.Stdout, outputableResults)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error while outputting results: ", err)
		return 1
	}
	return 0
}
