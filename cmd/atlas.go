package main

import (
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
	"github.com/jpappel/atlas/pkg/query"
	"github.com/jpappel/atlas/pkg/shell"
)

const VERSION = "0.0.1"
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

	queryFlags := QueryFlags{Outputer: query.DefaultOutput{}}
	indexFlags := IndexFlags{}

	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "No Command provided")
		printHelp()
		fmt.Fprintln(flag.CommandLine.Output(), "\nGlobal Flags:")
		flag.PrintDefaults()
		os.Exit(ExitCommand)
	}
	command := args[0]

	switch command {
	case "query", "q":
		setupQueryFlags(args, queryFs, &queryFlags)
	case "index":
		setupIndexFlags(args, indexFs, &indexFlags)
	case "help":
		printHelp()
		flag.PrintDefaults()
		return
	case "shell":
		shellFs.Parse(args[1:])
	default:
		fmt.Fprintln(os.Stderr, "Unrecognized command: ", command)
		printHelp()
		os.Exit(ExitCommand)
	}

	slogLevel := &slog.LevelVar{}
	loggerOpts := &slog.HandlerOptions{Level: slogLevel}
	switch globalFlags.LogLevel {
	case "debug":
		slogLevel.Set(slog.LevelDebug)
		loggerOpts.AddSource = true
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
	var logHandler slog.Handler
	if globalFlags.LogJson {
		logHandler = slog.NewJSONHandler(os.Stderr, loggerOpts)
	} else {
		// strip time
		loggerOpts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				return slog.Attr{}
			}
			return a
		}
		logHandler = slog.NewTextHandler(os.Stderr, loggerOpts)
	}
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	querier := data.NewQuery(globalFlags.DBPath)

	// command specific
	var exitCode int
	switch command {
	case "query", "q":
		searchQuery := strings.Join(queryFs.Args(), " ")
		exitCode = int(runQuery(globalFlags, queryFlags, querier, searchQuery))
	case "index":
		exitCode = int(runIndex(globalFlags, indexFlags, querier))
	case "shell":
		state := make(shell.State)
		env := make(map[string]string)

		env["workers"] = fmt.Sprint(globalFlags.NumWorkers)
		env["db_path"] = globalFlags.DBPath
		env["index_root"] = globalFlags.IndexRoot
		env["version"] = VERSION

		interpreter := shell.NewInterpreter(state, env, globalFlags.NumWorkers, querier)
		if err := interpreter.Run(); err != nil && err != io.EOF {
			slog.Error("Fatal error occured", slog.String("err", err.Error()))
			exitCode = 1
		}
	}

	querier.Close()
	os.Exit(exitCode)
}
