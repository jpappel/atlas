package cmd

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/shell"
)

func RunShell(gFlags GlobalFlags, db *data.Query, version string) byte {
	state := make(shell.State)
	env := make(map[string]string)

	env["workers"] = fmt.Sprint(gFlags.NumWorkers)
	env["db_path"] = gFlags.DBPath
	env["index_root"] = gFlags.IndexRoot
	env["version"] = version

	interpreter := shell.NewInterpreter(state, env, gFlags.NumWorkers, db)
	if err := interpreter.Run(); err != nil && err != io.EOF {
		slog.Error("Fatal error occured", slog.String("err", err.Error()))
		return 1
	}

	return 0
}
