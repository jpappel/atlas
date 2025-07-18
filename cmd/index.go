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
	Filters    []index.DocFilter
	Subcommand string
	index.ParseOpts
}

func setupIndexFlags(args []string, fs *flag.FlagSet, flags *IndexFlags) {
	flags.ParseLinks = true
	flags.ParseMeta = true
	fs.BoolVar(&flags.IgnoreDateError, "ignoreBadDates", false, "ignore malformed dates while indexing")
	fs.BoolVar(&flags.IgnoreMetaError, "ignoreMetaError", false, "ignore errors while parsing general YAML header info")
	fs.BoolFunc("ignoreMeta", "don't parse YAML header values other title, authors, date, tags", func(s string) error {
		flags.ParseMeta = false
		return nil
	})
	fs.BoolFunc("ignoreLinks", "don't parse file contents for links", func(s string) error {
		flags.ParseLinks = false
		return nil
	})
	fs.BoolVar(&flags.IgnoreHidden, "ignoreHidden", false, "ignore hidden files while crawling")

	fs.Usage = func() {
		f := fs.Output()
		fmt.Fprintf(f, "Usage of %s %s\n", os.Args[0], fs.Name())
		fmt.Fprintf(f, "\t%s [global-flags] %s [index-flags] <subcommand>\n\n", os.Args[0], fs.Name())
		fmt.Fprintln(f, "Subcommands:")
		fmt.Fprintln(f, "build  - create a new index")
		fmt.Fprintln(f, "update - update an existing index")
		fmt.Fprintln(f, "tidy   - cleanup an index")
		fmt.Fprintln(f, "\nIndex Flags:")
		fs.PrintDefaults()
		fmt.Fprintln(f, "\nGlobal Flags:")
		flag.PrintDefaults()
	}

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

	fs.Parse(args)

	remainingArgs := fs.Args()
	if len(remainingArgs) == 0 {
		flags.Subcommand = "build"
	} else if len(remainingArgs) == 1 {
		flags.Subcommand = remainingArgs[0]
	}
}

func runIndex(gFlags GlobalFlags, iFlags IndexFlags, db *data.Query) byte {

	switch iFlags.Subcommand {
	case "build", "update":
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

		traversedFiles := idx.Traverse(gFlags.NumWorkers, iFlags.IgnoreHidden)
		fmt.Print("Crawled ", len(traversedFiles))

		filteredFiles := idx.Filter(traversedFiles, gFlags.NumWorkers)
		fmt.Print(", Filtered ", len(filteredFiles))

		idx.Documents = index.ParseDocs(filteredFiles, gFlags.NumWorkers, iFlags.ParseOpts)
		fmt.Print(", Parsed ", len(idx.Documents), "\n")

		var err error
		// switch in order to appease gopls...
		switch iFlags.Subcommand {
		case "index":
			err = db.Put(idx)
		case "update":
			err = db.Update(idx)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	case "tidy":
		if err := db.Tidy(); err != nil {
			fmt.Fprintln(os.Stderr, "Error while tidying:", err)
			return 1
		}
	default:
		fmt.Fprintln(os.Stderr, "Unrecognised index subcommands: ", iFlags.Subcommand)
		return 2
	}

	return 0
}
