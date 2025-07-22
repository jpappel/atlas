package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jpappel/atlas/pkg/data"
	"github.com/jpappel/atlas/pkg/index"
	"github.com/jpappel/atlas/pkg/query"
)

type Server interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

func info(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(`
	<h1>Atlas Server</h1>
	<p>This is the experimental atlas server!
	Try POSTing a query to <pre>/search</pre></p>
	`))
}

func NewMux(db *data.Query) *http.ServeMux {
	mux := http.NewServeMux()

	outputBufPool := &sync.Pool{}
	outputBufPool.New = func() any {
		return &bytes.Buffer{}
	}

	mux.HandleFunc("/", info)
	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		b := &strings.Builder{}
		if _, err := io.Copy(b, r.Body); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error processing request"))
			slog.Error("Error reading request body", slog.String("err", err.Error()))
			return
		}
		artifact, err := query.Compile(b.String(), 0, 1)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			slog.Error("Error compiling query", slog.String("err", err.Error()))
			return
		}

		pathDocs, err := db.Execute(artifact)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error executing query"))
			slog.Error("Error executing query", slog.String("err", err.Error()))
			return
		}

		docs := make([]*index.Document, 0, len(pathDocs))
		var maxFileTime time.Time
		for _, doc := range pathDocs {
			docs = append(docs, doc)
			if doc.FileTime.After(maxFileTime) {
				maxFileTime = doc.FileTime
			}
		}

		if !maxFileTime.IsZero() {
			w.Header().Add("Last-Modified", maxFileTime.UTC().Format(http.TimeFormat))
		}

		buf, ok := outputBufPool.Get().(*bytes.Buffer)
		if !ok {
			panic("Expected *bytes.Buffer in pool")
		}
		_, err = query.JsonOutput{}.OutputTo(buf, docs)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error while writing output"))
			slog.Error("Error writing json output", slog.String("err", err.Error()))
		}

		http.ServeContent(w, r, "result.json", maxFileTime, bytes.NewReader(buf.Bytes()))
	})

	return mux
}
