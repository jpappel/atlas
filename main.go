package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/jpappel/atlas/cmd"
	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/query"
)

const VERSION = "0.5.1"
const ExitCommand = 2 // exit because of a command parsing error

func addGlobalFlagUsage(fs *flag.FlagSet) func() {
	return func() {
		f := fs.Output()
		fmt.Fprintln(f, "Usage of", fs.Name())
		fs.PrintDefaults()
		fmt.Fprintln(f, "\nGlobal Flags:")
		flag.PrintDefaults()
	}
}

func main() {
	exitCode := 0
	defer func() { os.Exit(exitCode) }()

	// global flags
	globalFlags := cmd.GlobalFlags{}
	cmd.SetupGlobalFlags(flag.CommandLine, &globalFlags)
	flag.Parse()
	args := flag.Args()

	queryFlags := cmd.QueryFlags{Outputer: query.DefaultOutput{}}
	indexFlags := cmd.IndexFlags{}
	serverFlags := cmd.ServerFlags{Port: 8080}

	indexFs := cmd.NewIndexFlagSet(&indexFlags)
	queryFs := cmd.NewQueryFlagSet(&queryFlags, globalFlags.DateFormat)
	shellFs := flag.NewFlagSet("debug", flag.ExitOnError)
	serverFs := cmd.NewServerFlagSet(&serverFlags)
	completionsFs := flag.NewFlagSet("completions", flag.ContinueOnError)

	// set default usage for flagsets without subcommands
	shellFs.Usage = addGlobalFlagUsage(shellFs)
	serverFs.Usage = addGlobalFlagUsage(serverFs)

	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "No Command provided")
		cmd.PrintHelp(os.Stderr)
		cmd.PrintGlobalFlags(os.Stderr)
		os.Exit(ExitCommand)
	}
	command := args[0]

	switch command {
	case "query", "q":
		queryFs.Parse(args[1:])
	case "index", "i":
		indexFs.Parse(args[1:])
		remainingArgs := indexFs.Args()
		if len(remainingArgs) == 0 {
			indexFlags.Subcommand = "build"
		} else if len(remainingArgs) == 1 {
			indexFlags.Subcommand = remainingArgs[0]
		}
	case "server":
		serverFs.Parse(args[1:])
	case "completions":
		completionsFs.Parse(args[1:])
	case "help":
		if len(args) > 1 {
			cmd.Help(strings.Join(args[1:], " "), os.Stdout)
		} else {
			cmd.Help("", os.Stdout)
		}
		return
	case "shell":
		shellFs.Parse(args[1:])
	default:
		cmd.Help(command, os.Stderr)
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

	var logFile *os.File
	var err error
	switch globalFlags.LogFile {
	case "":
		logFile = os.Stderr
	case "-":
		logFile = os.Stdout
	default:
		logFile, err = os.Create(globalFlags.LogFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Cannot use log file `%s`: %s", globalFlags.LogFile, err)
			os.Exit(1)
		}
		defer logFile.Close()
	}

	var logHandler slog.Handler
	if globalFlags.LogJson {
		logHandler = slog.NewJSONHandler(logFile, loggerOpts)
	} else {
		// strip time
		loggerOpts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				return slog.Attr{}
			}
			return a
		}
		logHandler = slog.NewTextHandler(logFile, loggerOpts)
	}
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	// TODO: remove later querier instances
	querier := data.NewQuery(globalFlags.DBPath, VERSION)
	defer querier.Close()

	// command specific
	switch command {
	case "query", "q":
		searchQuery := strings.Join(queryFs.Args(), " ")
		exitCode = int(cmd.RunQuery(globalFlags, queryFlags, querier, searchQuery))
	case "index", "i":
		exitCode = int(cmd.RunIndex(globalFlags, indexFlags, querier))
	case "server":
		exitCode = int(cmd.RunServer(globalFlags, serverFlags, querier))
	case "completions":
		lang := completionsFs.Arg(0)
		fmt.Fprintln(os.Stderr, "General Flags")
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(os.Stderr, "%s - %s\n", f.Name, f.Usage)
		})
		fmt.Fprintln(os.Stderr, "Index Flags")
		indexFs.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(os.Stderr, "%s - %s\n", f.Name, f.Usage)
		})

		switch lang {
		case "zsh":
			cmd.ZshCompletions()
		default:
			fmt.Fprintf(os.Stderr, "Unrecognized completion language `%s`\n", lang)
			fmt.Fprintf(os.Stderr, "Usage %s completions <language>\n", os.Args[0])
			fmt.Fprintln(os.Stderr, "Supported languages: zsh")
			exitCode = 2
		}
	case "shell":
		exitCode = int(cmd.RunShell(globalFlags, querier, VERSION))
	}
}
