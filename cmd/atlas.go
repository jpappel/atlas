package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/index"
)

const ExitCommand = 2 // exit because of a command parsing error

var commands = []string{"query", "index", "version", "help"}

func main() {
	// global opts
	indexRoot := flag.String("root", "/home/goose/src/atlas/test", "root directory for indexing")
	docDB := flag.String("db", "/home/goose/src/atlas/test.db", "path to document database")
	logLevel := flag.String("logLevel", "error", "set log level (debug, info, warn, error)")

	// command specific opts
	// TODO: parse a list of fitlers
	docFilters := index.DefaultFilters()

	flag.Parse()

	slogLevel := &slog.LevelVar{}
	if *logLevel == "debug" {
		slogLevel.Set(slog.LevelDebug)
	} else if *logLevel == "info" {
		slogLevel.Set(slog.LevelInfo)
	} else if *logLevel == "warn" {
		slogLevel.Set(slog.LevelWarn)
	} else if *logLevel == "error" {
		slogLevel.Set(slog.LevelError)
	} else {
		fmt.Fprintln(os.Stderr, "Unrecognized log level:", *logLevel)
		os.Exit(ExitCommand)
	}
	loggerOpts := &slog.HandlerOptions{Level: slogLevel}
	logger := slog.New(slog.NewTextHandler(os.Stderr, loggerOpts))
	slog.SetDefault(logger)

	query := data.NewQuery(*docDB)
	defer query.Close()

	idx := index.Index{Root: *indexRoot, Filters: docFilters}
	fmt.Println("index:", idx)

	traversedFiles := idx.Traverse(4)
	fmt.Println("traversed files:", traversedFiles)

	filteredFiles := idx.Filter(traversedFiles, 4)
	fmt.Println("filtered files:", filteredFiles)

	fmt.Println("Putting index")
	if err := query.Put(idx); err != nil {
		panic(err)
	}
}
