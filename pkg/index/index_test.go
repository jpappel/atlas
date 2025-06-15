package index_test

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jpappel/atlas/pkg/index"
)

var indexCases map[string]func(t *testing.T) index.Index

func init() {
	indexCases = make(map[string]func(t *testing.T) index.Index)

	indexCases["single file"] = func(t *testing.T) index.Index {
		root := t.TempDir()
		index := index.Index{Root: root, Filters: []index.DocFilter{index.NewExtensionFilter(".md")}}

		f, err := os.Create(root + "/a_file.md")
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString("some file contents\n")

		return index
	}

	indexCases["large file"] = func(t *testing.T) index.Index {
		root := t.TempDir()
		index := index.Index{Root: root}

		return index
	}

	indexCases["worker saturation"] = func(t *testing.T) index.Index {
		root := t.TempDir()
		index := index.Index{Root: root}

		permission := os.FileMode(0o777)
		for _, dirName := range []string{"a", "b", "c", "d", "e", "f"} {
			dir := root + "/" + dirName
			if err := os.Mkdir(dir, permission); err != nil {
				t.Fatal(err)
			}
			for i := range 8 {
				fName := fmt.Sprint(dirName, i)
				f, err := os.Create(dir + "/" + fName)
				if err != nil {
					t.Fatal(err)
				}
				f.WriteString(fName)
			}
		}

		return index
	}
}

func TestIndex_Traverse(t *testing.T) {
	tests := []struct {
		name       string
		indexCase  func(t *testing.T) index.Index
		numWorkers uint
		want       []string
	}{
		{name: "single file", indexCase: indexCases["single file"], numWorkers: 2, want: []string{"a_file.md"}},
		{name: "saturation test", indexCase: indexCases["worker saturation"], numWorkers: 2, want: []string{
			"a/a0", "a/a1", "a/a2", "a/a3", "a/a4", "a/a5", "a/a6", "a/a7",
			"b/b0", "b/b1", "b/b2", "b/b3", "b/b4", "b/b5", "b/b6", "b/b7",
			"c/c0", "c/c1", "c/c2", "c/c3", "c/c4", "c/c5", "c/c6", "c/c7",
			"d/d0", "d/d1", "d/d2", "d/d3", "d/d4", "d/d5", "d/d6", "d/d7",
			"e/e0", "e/e1", "e/e2", "e/e3", "e/e4", "e/e5", "e/e6", "e/e7",
			"f/f0", "f/f1", "f/f2", "f/f3", "f/f4", "f/f5", "f/f6", "f/f7",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := tt.indexCase(t)
			got := idx.Traverse(tt.numWorkers)

			slices.Sort(got)
			slices.Sort(tt.want)

			n := min(len(got), len(tt.want))
			if len(got) != len(tt.want) {
				t.Errorf("Wanted %v got %v paths", len(tt.want), len(got))
				t.Logf("Checking up to %d values", n)
			}

			for i := range n {
				gotPath := got[i]
				wantPath := idx.Root + "/" + tt.want[i]
				if gotPath != wantPath {
					t.Errorf("At %d wanted %v, got %v", i, wantPath, gotPath)
				}
			}
		})
	}
}

func TestIndex_Filter(t *testing.T) {
	tests := []struct {
		name       string
		paths      []string
		indexCase  func(t *testing.T) index.Index
		numWorkers uint
		want       []string
	}{
		{"single file", []string{"a_file.md"}, indexCases["single file"], 2, []string{"a_file.md"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := tt.indexCase(t)
			for i, path := range tt.paths {
				tt.paths[i] = idx.Root + "/" + path
			}

			got := idx.Filter(tt.paths, tt.numWorkers)

			slices.Sort(got)
			slices.Sort(tt.want)

			n := min(len(got), len(tt.want))
			if len(got) != len(tt.want) {
				t.Errorf("Wanted %v got %v paths", len(tt.want), len(got))
				t.Logf("Checking up to %d values", n)
			}

			for i := range n {
				gotPath := got[i]
				wantPath := idx.Root + "/" + tt.want[i]
				if gotPath != wantPath {
					t.Errorf("At %d wanted %v, got %v", i, wantPath, gotPath)
				}
			}
		})
	}
}

func newTestFile(t *testing.T, name string) (*os.File, string) {
	dir := t.TempDir()
	path := dir + "/" + name
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}

	return f, path
}

func TestIndex_ParseOne(t *testing.T) {
	tests := []struct {
		name      string
		pathMaker func(t *testing.T) string
		parseOpts index.ParseOpts
		want      *index.Document
		wantErr   error
	}{
		{
			"title only header",
			func(t *testing.T) string {
				f, path := newTestFile(t, "title")
				defer f.Close()

				f.WriteString("---\ntitle: A title\n---\n")
				return path
			},
			index.ParseOpts{},
			&index.Document{Title: "A title"},
			nil,
		},
		{
			"tags",
			func(t *testing.T) string {
				f, path := newTestFile(t, "tags")
				defer f.Close()

				f.WriteString("---\n")
				f.WriteString("tags:\n")
				f.WriteString("- a\n")
				f.WriteString("- b\n")
				f.WriteString("- c\n")
				f.WriteString("---\n")

				return path
			},
			index.ParseOpts{},
			&index.Document{Tags: []string{"a", "b", "c"}},
			nil,
		},
		{
			"date",
			func(t *testing.T) string {
				f, path := newTestFile(t, "date")
				defer f.Close()

				f.WriteString("---\ndate: May 1, 2025\n---\n")

				return path
			},
			index.ParseOpts{},
			&index.Document{Date: time.Date(2025, time.May, 1, 0, 0, 0, 0, time.UTC)},
			nil,
		},
		{
			"single author",
			func(t *testing.T) string {
				f, path := newTestFile(t, "author")
				defer f.Close()

				f.WriteString("---\nauthor: Rob Pike\n---\n")

				return path
			},
			index.ParseOpts{},
			&index.Document{Authors: []string{"Rob Pike"}},
			nil,
		},
		{
			"multi author",
			func(t *testing.T) string {
				f, path := newTestFile(t, "author")
				defer f.Close()

				f.WriteString("---\nauthor:\n- Robert Griesemer\n- Rob Pike\n- Ken Thompson\n---\n")

				return path
			},
			index.ParseOpts{},
			&index.Document{Authors: []string{"Robert Griesemer", "Rob Pike", "Ken Thompson"}},
			nil,
		},
		{
			"meta",
			func(t *testing.T) string {
				f, path := newTestFile(t, "metadata")
				defer f.Close()

				f.WriteString("---\n")
				f.WriteString("unknownKey: value\n")
				f.WriteString("---\n")

				return path
			},
			index.ParseOpts{ParseMeta: true},
			&index.Document{OtherMeta: "unknownKey: value\n"},
			nil,
		},
		{
			"bad tags",
			func(t *testing.T) string {
				f, path := newTestFile(t, "badtags")
				defer f.Close()

				f.WriteString("---\n")
				f.WriteString("tags:\n- good tag\n-bad tag\n")
				f.WriteString("---\n")

				return path
			},
			index.ParseOpts{},
			&index.Document{},
			index.ErrHeaderParse,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.pathMaker(t)
			tt.want.Path = path

			got, gotErr := index.ParseDoc(path, tt.parseOpts)

			if !errors.Is(gotErr, tt.wantErr) {
				t.Errorf("Recieved unexpected error: want %v got %v", tt.wantErr, gotErr)
				return
			} else if gotErr != nil {
				return
			}

			if !got.Equal(*tt.want) {
				t.Error("Recieved document is not equal")
				t.Logf("Got  = %+v", got)
				t.Logf("Want = %+v", tt.want)
			}
		})
	}
}
