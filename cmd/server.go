package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/server"
)

type ServerFlags struct {
	Address string
	Port    int
}

func setupServerFlags(args []string, fs *flag.FlagSet, flags *ServerFlags) {
	fs.StringVar(&flags.Address, "address", "", "the address to listen on")
	fs.IntVar(&flags.Port, "port", 8080, "the port to bind to")

	fs.Parse(args)
}

func runServer(gFlags GlobalFlags, sFlags ServerFlags, db *data.Query) byte {
	addr := fmt.Sprintf("%s:%d", sFlags.Address, sFlags.Port)

	s := http.Server{Addr: addr, Handler: server.New(db)}

	serverErrors := make(chan error, 1)
	exit := make(chan os.Signal, 1)

	signal.Notify(exit, syscall.SIGTERM, os.Interrupt)

	slog.Info("Starting server on", slog.String("addr", addr))
	go func(serverErrors chan<- error) {
		if err := s.ListenAndServe(); err != nil {
			serverErrors <- err
		}
	}(serverErrors)

	select {
	case <-exit:
		slog.Info("Recieved signal to shutdown")
	case err := <-serverErrors:
		slog.Error("Server error", err)
	}

	slog.Info("Shutting down server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		slog.Error("Error shutting down server", err)
		return 1
	}

	return 0
}
