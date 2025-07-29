package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/index"
	"github.com/jpappel/atlas/pkg/query"
)

const (
	START_HEADER byte = 1
	START_BODY   byte = 2
	END_BODY     byte = 3
	END_MSG      byte = 4
	END_QUERY    byte = 5
)

type UnixServer struct {
	Addr           string
	Db             *data.Query
	WorkersPerConn uint
	ln             *net.UnixListener
	conns          map[uint64]*net.UnixConn
	lock           sync.RWMutex
	bufPool        sync.Pool
}

func (s *UnixServer) ListenAndServe() error {
	addr, err := net.ResolveUnixAddr("unix", s.Addr)
	if err != nil {
		return err
	}

	s.ln, err = net.ListenUnix("unix", addr)
	if err != nil {
		return err
	}

	s.conns = make(map[uint64]*net.UnixConn)
	s.bufPool.New = func() any {
		return make([]byte, 1024)
	}

	var connId uint64
	for {
		conn, err := s.ln.AcceptUnix()
		if err != nil {
			break
		}
		connId++
		slog.Info("New connection", slog.Uint64("connId", connId))

		s.lock.Lock()
		s.conns[connId] = conn
		s.lock.Unlock()

		go s.handleConn(conn, connId)
	}

	return nil
}

func (s *UnixServer) writeError(conn *net.UnixConn, msg string) {
	conn.Write(fmt.Append([]byte{START_HEADER}, "Error handling query"))
	conn.Write([]byte{START_BODY, END_BODY})
	conn.Write([]byte(msg))
	conn.Write([]byte{END_MSG})
}

func (s *UnixServer) writeResults(conn *net.UnixConn, docs map[string]*index.Document) {
	defer conn.Write([]byte{END_MSG})
	conn.Write(fmt.Appendf([]byte{START_HEADER}, "Num Docs: %d", len(docs)))
	conn.Write([]byte{START_BODY})
	defer conn.Write([]byte{END_BODY})

	o := query.DefaultOutput{}
	for _, doc := range docs {
		if _, err := o.WriteDoc(conn, doc); err != nil {
			slog.Error("Failed to write doc",
				slog.String("err", err.Error()),
			)
			break
		}
	}
}

func (s *UnixServer) handleConn(conn *net.UnixConn, id uint64) {
	defer func(id uint64) {
		s.lock.Lock()
		delete(s.conns, id)
		s.lock.Unlock()
	}(id)

	buf := s.bufPool.Get().([]byte)
	defer s.bufPool.Put(buf)
	defer slog.Info("Closing connection",
		slog.String("local", conn.LocalAddr().String()),
	)

	for {
		slog.Debug("Waiting for query")
		n, err := conn.Read(buf)
		if n == 0 || err != nil {
			break
		}
		buf = buf[:n]
		if buf[len(buf)-1] != 5 {
			slog.Info("Missing ENQ at end of message")
			break
		}

		queryTxt := string(buf[:len(buf)-1])
		slog.Debug("Recieved query",
			slog.String("query", queryTxt),
		)

		// TODO: cache compilation artifacts
		artifact, err := query.Compile(queryTxt, 0, s.WorkersPerConn)
		if err != nil {
			slog.Warn("Failed to compile query",
				slog.String("err", err.Error()))
			s.writeError(conn, "query compilation error")
			break
		}

		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		docs, err := s.Db.Execute(ctx, artifact)
		if err != nil {
			slog.Warn("Failed to execute query",
				slog.String("query", queryTxt),
				slog.String("err", err.Error()),
			)
			s.writeError(conn, "query execution error")
			cancel()
			break
		}
		cancel()

		slog.Debug("Sending results")
		s.writeResults(conn, docs)
	}
}

func (s *UnixServer) Shutdown(ctx context.Context) error {
	s.ln.Close()
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, conn := range s.conns {
		conn.Write([]byte("Closing Server"))
		conn.Close()
	}

	return nil
}
