package cmd

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

func SetupIndexFlags(args []string, fs *flag.FlagSet, flags *IndexFlags) {
	flags.ParseLinks = true
	flags.ParseMeta = true
	fs.BoolVar(&flags.IgnoreDateError, "ignoreBadDates", false, "ignore malformed dates while indexing")
	fs.BoolVar(&flags.IgnoreMetaError, "ignoreMetaError", false, "ignore errors while parsing general YAML header info")
	fs.BoolFunc("ignoreMeta", "only parse title, authors, date, tags from YAML headers", func(s string) error {
		flags.ParseMeta = false
		return nil
	})
	fs.BoolFunc("ignoreLinks", "don't parse file contents for links", func(s string) error {
		flags.ParseLinks = false
		return nil
	})
	fs.BoolVar(&flags.IgnoreHidden, "ignoreHidden", false, "ignore hidden files while crawling")

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

	fs.Usage = func() {
		f := fs.Output()
		Help("index", f)
		PrintGlobalFlags(f)
	}

	fs.Parse(args)

	remainingArgs := fs.Args()
	if len(remainingArgs) == 0 {
		flags.Subcommand = "build"
	} else if len(remainingArgs) == 1 {
		flags.Subcommand = remainingArgs[0]
	}
}

func RunIndex(gFlags GlobalFlags, iFlags IndexFlags, db *data.Query) byte {

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

		var errCnt uint64
		idx.Documents, errCnt = index.ParseDocs(filteredFiles, gFlags.NumWorkers, iFlags.ParseOpts)
		fmt.Print(", Parsed ", len(idx.Documents), "\n")
		if errCnt > 0 {
			fmt.Printf("Encountered %d document parse errors", errCnt)
			if !slog.Default().Enabled(context.Background(), slog.LevelWarn) {
				fmt.Print(" (set log level to warn for more info)")
			}
			fmt.Println()
		}

		var err error
		// switch in order to appease gopls...
		switch iFlags.Subcommand {
		case "build":
			err = db.Put(context.Background(), idx)
		case "update":
			err = db.Update(context.Background(), idx)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error modifying index:", err)
			return 1
		}
	case "tidy":
		if err := db.Tidy(); err != nil {
			fmt.Fprintln(os.Stderr, "Error while tidying:", err)
			return 1
		}
	default:
		fmt.Fprintln(os.Stderr, "Unrecognized index subcommands: ", iFlags.Subcommand)
		return 2
	}

	return 0
}

func init() {
}
