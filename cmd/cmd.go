package cmd

import (
	"errors"
	"flag"
	"io/fs"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/adrg/xdg"
)

type GlobalFlags struct {
	IndexRoot  string
	DBPath     string
	LogLevel   string
	LogJson    bool
	NumWorkers uint
	DateFormat string
	LogFile    string
}

func SetupGlobalFlags(fs_ *flag.FlagSet, flags *GlobalFlags) {
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

	flag.StringVar(&flags.IndexRoot, "root", xdg.UserDirs.Documents, "root `directory` for indexing")
	flag.StringVar(&flags.DBPath, "db", dataHome+string(os.PathSeparator)+"default.db", "`path` to document database")
	flag.StringVar(&flags.LogLevel, "logLevel", "error", "set log `level` (debug, info, warn, error)")
	flag.BoolVar(&flags.LogJson, "logJson", false, "log to json")
	flag.UintVar(&flags.NumWorkers, "numWorkers", uint(runtime.NumCPU()), "number of worker threads to use (defaults to core count)")
	flag.StringVar(&flags.DateFormat, "dateFormat", time.RFC3339, "`format` for dates (see https://pkg.go.dev/time#Layout for more details)")
	flag.StringVar(&flags.LogFile, "logFile", "", "`file` to log errors to, use '-' for stdout and empty for stderr")
}
