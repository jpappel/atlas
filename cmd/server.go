package cmd

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/server"
)

type ServerFlags struct {
	Address string
	Port    int
}

func SetupServerFlags(args []string, fs *flag.FlagSet, flags *ServerFlags) {
	fs.StringVar(&flags.Address, "address", "", "the address to listen on, prefix with 'unix:' to create a unixsocket")
	fs.IntVar(&flags.Port, "port", 8080, "the port to bind to")

	fs.Parse(args)
}

func RunServer(sFlags ServerFlags, db *data.Query) byte {

	var addr string
	var s server.Server
	if after, ok := strings.CutPrefix(sFlags.Address, "unix:"); ok {
		slog.Debug("Preparing unix domain socket")
		addr = after
		s = &server.UnixServer{Addr: addr, Db: db}
	} else {
		slog.Debug("Preparing http server")
		addr = fmt.Sprintf("%s:%d", sFlags.Address, sFlags.Port)
		s = &http.Server{Addr: addr, Handler: server.NewMux(db)}
	}

	serverErrors := make(chan error, 1)
	exit := make(chan os.Signal, 1)

	signal.Notify(exit, syscall.SIGTERM, os.Interrupt)

	slog.Info("Starting server on", slog.String("addr", addr))
	go func(serverErrors chan<- error) {
		if err := s.ListenAndServe(); err != nil {
			serverErrors <- err
		}
		close(serverErrors)
	}(serverErrors)

	select {
	case <-exit:
		slog.Info("Recieved signal to shutdown")
	case err := <-serverErrors:
		if err != nil {
			slog.Error("Server error", slog.String("err", err.Error()))
		}
	}

	slog.Info("Shutting down server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		slog.Error("Error shutting down server",
			slog.String("err", err.Error()))
		return 1
	}

	return 0
}
