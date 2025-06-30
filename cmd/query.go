package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/index"
	"github.com/jpappel/atlas/pkg/query"
)

type QueryFlags struct {
	Outputer          query.Outputer
	CustomFormat      string
	OptimizationLevel int
}

func setupQueryFlags(args []string, fs *flag.FlagSet, flags *QueryFlags) {
	// NOTE: providing `-outFormat` before `-outCustomFormat` might ignore user specified format
	fs.Func("outFormat", "output `format` for queries (default, json, custom)",
		func(arg string) error {
			switch arg {
			case "default":
				flags.Outputer = query.DefaultOutput{}
				return nil
			case "json":
				flags.Outputer = query.JsonOutput{}
				return nil
			case "custom":
				var err error
				flags.Outputer, err = query.NewCustomOutput(flags.CustomFormat, dateFormat)
				return err
			}
			return fmt.Errorf("Unrecognized output format: %s", arg)
		})
	fs.StringVar(&flags.CustomFormat, "outCustomFormat", query.DefaultOutputFormat, "format string for --outFormat custom, see EXAMPLES for more details")
	fs.IntVar(&flags.OptimizationLevel, "optLevel", 0, "optimization `level` for queries, 0 is automatic, <0 to disable")

	fs.Parse(args[1:])
}

func runQuery(gFlags GlobalFlags, qFlags QueryFlags, db *data.Query, searchQuery string) byte {
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

	s, err := qFlags.Outputer.Output(outputableResults)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to output results: ", err)
		return 1
	}

	fmt.Println(s)
	return 0
}
