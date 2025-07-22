package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"

	"github.com/jpappel/atlas/pkg/data"
)

// datagram based unix server
type UnixServer struct {
	Addr        string
	Db          *data.Query
	shouldClose chan struct{}
	bufPool     *sync.Pool
}

func (s *UnixServer) ListenAndServe() error {
	slog.Info("Listening on", slog.String("addr", s.Addr))
	conn, err := net.ListenUnixgram(
		"unixgram",
		&net.UnixAddr{Name: s.Addr, Net: "Unix"},
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer os.RemoveAll(s.Addr)
	slog.Debug("Accepted connection")

	// slog.Info("New Connection",
	// 	slog.String("addr", conn.RemoteAddr().String()),
	// )

	s.bufPool = &sync.Pool{}
	s.bufPool.New = func() any {
		return &bytes.Buffer{}
	}
	s.handleConn(conn)

	return nil
}

func (s UnixServer) handleConn(conn *net.UnixConn) {
	// buf, ok := s.bufPool.Get().(*bytes.Buffer)
	// if !ok {
	// 	panic("Expected *bytes.Buffer in pool")
	// }
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		panic(err)
	}
	buf = buf[:n]

	fmt.Println(string(buf))

	// if _, err := io.Copy(buf, conn); err != nil {
	// 	panic(err)
	// }
	// defer buf.Reset()
	//
	// io.Copy(os.Stdout, buf)

	// artifact, err := query.Compile(buf.String(), 0, 1)
	// if err != nil {
	// 	panic(err)
	// }
	//
	// docs, err := s.Db.Execute(artifact)
	// if err != nil {
	// 	panic(err)
	// }
	//
	// _, err = query.DefaultOutput{}.OutputTo(conn, slices.Collect(maps.Values(docs)))
	// if err != nil {
	// 	panic(err)
	// }
}

func (s *UnixServer) Shutdown(ctx context.Context) error {
	return nil
}
