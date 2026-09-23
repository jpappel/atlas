package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"

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
	DryRun            bool
}

func NewQueryFlagSet(flags *QueryFlags, dateFormat string) *flag.FlagSet {

	fs := flag.NewFlagSet("query", flag.ExitOnError)

	// NOTE: providing `-outFormat` before `-outCustomFormat` might ignore user specified format
	fs.Func("outFormat", "output `format` for queries (default, json, yaml, pathonly, custom)",
		func(arg string) error {
			switch arg {
			case "default":
				flags.Outputer = query.DefaultOutput{}
				return nil
			case "json":
				flags.Outputer = query.JsonOutput{}
				return nil
			case "yaml":
				flags.Outputer = query.YamlOutput{}
				return nil
			case "pathonly":
				flags.Outputer, _ = query.NewCustomOutput("%p", dateFormat, "\n", "")
				return nil
			case "custom":
				var err error
				flags.Outputer, err = query.NewCustomOutput(flags.CustomFormat, dateFormat, flags.DocumentSeparator, flags.ListSeparator)
				return err
			default:
				return fmt.Errorf("Unrecognized output format: %s", arg)
			}
		})

	fs.StringVar(&flags.SortBy, "sortBy", "", "category to sort by (path,title,date,filetime,meta)")
	fs.BoolVar(&flags.SortDesc, "sortDesc", false, "sort in descending order")
	fs.StringVar(&flags.CustomFormat, "outCustomFormat", query.DefaultOutputFormat, "`format` string for -outFormat custom, see `atlas help query` for more details")
	fs.IntVar(&flags.OptimizationLevel, "optLevel", 0, "optimization `level` for queries, 0 is automatic, <0 to disable")
	fs.StringVar(&flags.DocumentSeparator, "docSeparator", "\n", "separator for custom output format")
	fs.StringVar(&flags.ListSeparator, "listSeparator", ", ", "separator for list fields")
	fs.BoolVar(&flags.DryRun, "dry", false, "parse query for errors but do not execute")

	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintf(w, "%s [global-flags] query [query-flags] <query>...\n\n", os.Args[0])
		fmt.Fprintf(w, "See %s help query for more detailed usage information\n\n", os.Args[0])
		fmt.Fprintln(w, "Query Flags:")
		PrintFlagSet(w, fs)
		PrintGlobalFlags(w)
	}
	return fs
}

func RunQuery(gFlags GlobalFlags, qFlags QueryFlags, db *data.Query, searchQuery string) byte {
	tokens := query.Lex(searchQuery)
	clause, err := query.Parse(tokens)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to parse query: ", err)
		return 1
	}

	if qFlags.DryRun {
		return 0
	}

	o := query.NewOptimizer(clause, gFlags.NumWorkers)
	o.Optimize(qFlags.OptimizationLevel)

	artifact, err := clause.Compile()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to compile query: ", err)
		return 1
	}

	results, err := db.Execute(context.Background(), artifact)
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

	if qFlags.SortBy != "" {
		docCmp, ok := index.NewDocCmp(qFlags.SortBy, qFlags.SortDesc)
		if ok {
			slices.SortFunc(outputableResults, docCmp)
		}
	}

	_, err = qFlags.Outputer.OutputTo(os.Stdout, outputableResults)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error while outputting results: ", err)
		return 1
	}
	return 0
}
