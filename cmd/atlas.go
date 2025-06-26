package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/adrg/xdg"
	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/index"
	"github.com/jpappel/atlas/pkg/query"
	"github.com/jpappel/atlas/pkg/shell"
)

const ExitCommand = 2           // exit because of a command parsing error
const dateFormat = time.RFC3339 // TODO: make a flag

type GlobalFlags struct {
	IndexRoot  string
	DBPath     string
	LogLevel   string
	LogJson    bool
	NumWorkers uint
}

func addGlobalFlagUsage(fs *flag.FlagSet) func() {
	return func() {
		f := fs.Output()
		fmt.Fprintln(f, "Usage of", fs.Name())
		fs.PrintDefaults()
		fmt.Fprintln(f, "\nGlobal Flags:")
		flag.PrintDefaults()
	}
}

func printHelp() {
	fmt.Println("atlas is a note indexing and querying tool")
	fmt.Printf("\nUsage:\n  %s [global-flags] <command>\n\n", os.Args[0])
	fmt.Println("Commands:")
	fmt.Println("  index - build, update, or modify the index")
	fmt.Println("  query - search against the index")
	fmt.Println("  shell - start a debug query shell")
	fmt.Println("  help  - print this help then exit")
}

func main() {
	home, _ := os.UserHomeDir()
	dataHome := xdg.DataHome
	if dataHome == "" {
		dataHome = strings.Join([]string{home, ".local", "share"}, string(os.PathSeparator))
	}
	dataHome += string(os.PathSeparator) + "atlas"
	if err := os.Mkdir(dataHome, 0755); errors.Is(err, fs.ErrExist) {
	} else if err != nil {
		panic(err)
	}

	globalFlags := GlobalFlags{}
	flag.StringVar(&globalFlags.IndexRoot, "root", xdg.UserDirs.Documents, "root `directory` for indexing")
	flag.StringVar(&globalFlags.DBPath, "db", dataHome+string(os.PathSeparator)+"default.db", "`path` to document database")
	flag.StringVar(&globalFlags.LogLevel, "logLevel", "error", "set log `level` (debug, info, warn, error)")
	flag.BoolVar(&globalFlags.LogJson, "logJson", false, "log to json")
	flag.UintVar(&globalFlags.NumWorkers, "numWorkers", uint(runtime.NumCPU()), "number of worker threads to use (defaults to core count)")

	indexFs := flag.NewFlagSet("index flags", flag.ExitOnError)
	queryFs := flag.NewFlagSet("query flags", flag.ExitOnError)
	shellFs := flag.NewFlagSet("debug shell flags", flag.ExitOnError)

	indexFs.Usage = addGlobalFlagUsage(indexFs)
	queryFs.Usage = addGlobalFlagUsage(queryFs)
	shellFs.Usage = addGlobalFlagUsage(shellFs)

	flag.Parse()
	args := flag.Args()

	queryFlags := struct {
		Output       query.Outputer
		CustomFormat string
	}{}
	indexFlags := struct {
		Filters []index.DocFilter
		index.ParseOpts
	}{}

	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "No Command provided")
		printHelp()
		fmt.Fprintln(flag.CommandLine.Output(), "\nGlobal Flags:")
		flag.PrintDefaults()
		os.Exit(ExitCommand)
	}
	command := args[0]

	switch command {
	case "query":
		// NOTE: providing `-outFormat` before `-outCustomFormat` might ignore user specified format
		queryFs.Func("outFormat", "output `format` for queries (default, json, custom)",
			func(arg string) error {
				switch arg {
				case "default":
					queryFlags.Output = query.DefaultOutput{}
					return nil
				case "json":
					queryFlags.Output = query.JsonOutput{}
					return nil
				case "custom":
					var err error
					queryFlags.Output, err = query.NewCustomOutput(queryFlags.CustomFormat, dateFormat)
					return err
				}
				return fmt.Errorf("Unrecognized output format: %s", arg)
			})
		queryFs.StringVar(&queryFlags.CustomFormat, "outCustomFormat", query.DefaultOutputFormat, "format string for --outFormat custom, see EXAMPLES for more details")

		queryFs.Parse(args[1:])
	case "index":
		indexFs.BoolVar(&indexFlags.IgnoreDateError, "ignoreBadDates", false, "ignore malformed dates while indexing")
		indexFs.BoolVar(&indexFlags.IgnoreMetaError, "ignoreMetaError", false, "ignore errors while parsing general YAML header info")
		indexFs.BoolVar(&indexFlags.ParseMeta, "parseMeta", true, "parse YAML header values other title, authors, date, tags")

		customFilters := false
		indexFlags.Filters = index.DefaultFilters()
		indexFs.Func("filter",
			"accept or reject files from indexing, applied in supplied order"+
				"\n(default Ext_.md, MaxSize_204800, YAMLHeader, ExcludeParent_templates)\n"+
				index.FilterHelp,
			func(s string) error {
				if !customFilters {
					indexFlags.Filters = indexFlags.Filters[:0]
				}

				filter, err := index.ParseFilter(s)
				if err != nil {
					return err
				}
				indexFlags.Filters = append(indexFlags.Filters, filter)

				return nil
			})

		indexFs.Parse(args[1:])
	case "help":
		printHelp()
		flag.PrintDefaults()
		os.Exit(0)
	case "shell":
		shellFs.Parse(args[1:])
	default:
		fmt.Fprintln(os.Stderr, "Unrecognized command: ", command)
		printHelp()
		os.Exit(ExitCommand)
	}

	slogLevel := &slog.LevelVar{}
	switch globalFlags.LogLevel {
	case "debug":
		slogLevel.Set(slog.LevelDebug)
	case "info":
		slogLevel.Set(slog.LevelInfo)
	case "warn":
		slogLevel.Set(slog.LevelWarn)
	case "error":
		slogLevel.Set(slog.LevelError)
	default:
		fmt.Fprintln(os.Stderr, "Unrecognized log level:", globalFlags.LogLevel)
		os.Exit(ExitCommand)
	}
	loggerOpts := &slog.HandlerOptions{Level: slogLevel}
	var logHandler slog.Handler
	if globalFlags.LogJson {
		logHandler = slog.NewJSONHandler(os.Stderr, loggerOpts)
	} else {
		logHandler = slog.NewTextHandler(os.Stderr, loggerOpts)
	}
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	querier := data.NewQuery(globalFlags.DBPath)
	defer querier.Close()

	// command specific
	switch command {
	case "query":
		// TODO: evaluate query
		s, err := queryFlags.Output.Output(nil)
		if err != nil {
			slog.Error("Error while outputing query results", slog.String("err", err.Error()))
			return
		}
		fmt.Print(s)
	case "index":
		idx := index.Index{Root: globalFlags.IndexRoot, Filters: indexFlags.Filters}
		if logger.Enabled(context.Background(), slog.LevelDebug) {
			filterNames := make([]string, 0, len(indexFlags.Filters))
			for _, filter := range indexFlags.Filters {
				filterNames = append(filterNames, filter.Name)
			}
			logger.Debug("index",
				slog.String("indexRoot", globalFlags.IndexRoot),
				slog.String("filters", strings.Join(filterNames, ", ")),
			)
		}

		traversedFiles := idx.Traverse(globalFlags.NumWorkers)
		fmt.Print("Crawled ", len(traversedFiles))

		filteredFiles := idx.Filter(traversedFiles, globalFlags.NumWorkers)
		fmt.Print(", Filtered ", len(filteredFiles))

		idx.Documents = index.ParseDocs(filteredFiles, globalFlags.NumWorkers, indexFlags.ParseOpts)
		fmt.Print(", Parsed ", len(idx.Documents), "\n")

		if err := querier.Put(idx); err != nil {
			panic(err)
		}
	case "shell":
		state := make(shell.State)
		env := make(map[string]string)

		env["workers"] = fmt.Sprint(globalFlags.NumWorkers)
		env["db_path"] = globalFlags.DBPath
		env["index_root"] = globalFlags.IndexRoot
		env["version"] = "0.0.1"

		interpreter := shell.NewInterpreter(state, env, globalFlags.NumWorkers)
		if err := interpreter.Run(); err != nil && err != io.EOF {
			slog.Error("Fatal error occured", slog.String("err", err.Error()))
			os.Exit(1)
		}
	}

}
