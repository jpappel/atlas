package server

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/query"
)

// datagram based unix server
type UnixServer struct {
	Addr string
	Db   *data.Query
	conn *net.UnixConn
}

func (s *UnixServer) ListenAndServe() error {
	serverAddr := s.Addr + "_server.sock"
	clientAddr := s.Addr + "_client.sock"

	var err error
	s.conn, err = net.ListenUnixgram(
		"unixgram",
		&net.UnixAddr{Name: serverAddr, Net: "Unix"},
	)
	if err != nil {
		return err
	}
	defer os.RemoveAll(s.Addr)
	slog.Info("Listening on", slog.String("addr", s.Addr))

	var remote *net.UnixAddr
	remote, err = net.ResolveUnixAddr("unixgram", clientAddr)
	if err != nil {
		panic(err)
	}
	// FIXME: limits queries to 1kb, might have some data overflow into next msg
	buf := make([]byte, 1024)
	for {
		n, _, err := s.conn.ReadFromUnix(buf)
		if err != nil {
			return err
		}
		buf = buf[:n]
		queryTxt := string(buf)
		slog.Debug("New message",
			slog.String("msg", queryTxt),
			slog.String("local", s.conn.LocalAddr().String()),
			slog.String("remote", remote.String()),
		)

		// TODO: set reasonable numWorkers
		// TODO: rwrite error to remote
		artifact, err := query.Compile(queryTxt, 0, 2)
		if err != nil {
			slog.Error("Failed to compile query", slog.String("err", err.Error()))
			return err
		}

		// TODO: write error to remote
		docs, err := s.Db.Execute(artifact)
		if err != nil {
			slog.Error("Failed to execute query",
				slog.String("err", err.Error()))
			return err
		}

		buf := &bytes.Buffer{}
		o := query.DefaultOutput{}
		for _, doc := range docs {
			n, err = o.WriteDoc(buf, doc)
			if err != nil {
				return err
			}

			b := buf.Bytes()
			remaining := len(b)
			offset := 0
			for remaining > 0 {
				n, err := s.conn.WriteToUnix(b[offset:remaining], remote)
				if err != nil {
					return err
				}
				remaining -= n
				offset += n
			}
			buf.Reset()
		}
		// EOF
		s.conn.WriteToUnix([]byte{4}, remote)
	}
}

func (s *UnixServer) Shutdown(ctx context.Context) error {
	return s.conn.Close()
}
