package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/index"
)

type IndexFlags struct {
	Filters []index.DocFilter
	index.ParseOpts
}

func setupIndexFlags(args []string, fs *flag.FlagSet, flags *IndexFlags) {
	fs.BoolVar(&flags.IgnoreDateError, "ignoreBadDates", false, "ignore malformed dates while indexing")
	fs.BoolVar(&flags.IgnoreMetaError, "ignoreMetaError", false, "ignore errors while parsing general YAML header info")
	fs.BoolVar(&flags.ParseMeta, "parseMeta", true, "parse YAML header values other title, authors, date, tags")
	fs.BoolVar(&flags.ParseLinks, "parseLinks", true, "parse file contents for links")

	customFilters := false
	flags.Filters = index.DefaultFilters()
	fs.Func("filter",
		"accept or reject files from indexing, applied in supplied order"+
			"\n(default Ext_.md, MaxSize_204800, YAMLHeader, ExcludeParent_templates)\n"+
			index.FilterHelp,
		func(s string) error {
			if !customFilters {
				flags.Filters = flags.Filters[:0]
			}

			filter, err := index.ParseFilter(s)
			if err != nil {
				return err
			}
			flags.Filters = append(flags.Filters, filter)

			return nil
		})

	fs.Parse(args[1:])
}

func runIndex(gFlags GlobalFlags, iFlags IndexFlags, db *data.Query) byte {
	idx := index.Index{Root: gFlags.IndexRoot, Filters: iFlags.Filters}
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		filterNames := make([]string, 0, len(iFlags.Filters))
		for _, filter := range iFlags.Filters {
			filterNames = append(filterNames, filter.Name)
		}
		slog.Default().Debug("index",
			slog.String("indexRoot", gFlags.IndexRoot),
			slog.String("filters", strings.Join(filterNames, ", ")),
		)
	}

	traversedFiles := idx.Traverse(gFlags.NumWorkers)
	fmt.Print("Crawled ", len(traversedFiles))

	filteredFiles := idx.Filter(traversedFiles, gFlags.NumWorkers)
	fmt.Print(", Filtered ", len(filteredFiles))

	idx.Documents = index.ParseDocs(filteredFiles, gFlags.NumWorkers, iFlags.ParseOpts)
	fmt.Print(", Parsed ", len(idx.Documents), "\n")

	if err := db.Put(idx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
