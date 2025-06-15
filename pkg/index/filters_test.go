package index_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/jpappel/atlas/pkg/index"
)

func noYamlHeader() io.ReadSeeker {
	buf := []byte("just some text")
	return bytes.NewReader(buf)
}

func incompleteYamlHeader() io.ReadSeeker {
	buf := []byte("---\nfoo:bar\ntitle:bizbaz\nauthor:\n-JP Appel\n---")
	return bytes.NewReader(buf)
}

func completeYamlHeader() io.ReadSeeker {
	buf := []byte("---\nfoo:bar\ntitle:bizbaz\nauthor:\n-JP Appel\n---\n")
	return bytes.NewReader(buf)
}

func trailingYamlHeader() io.ReadSeeker {
	buf := []byte("---\nfoo:bar\ntitle:bizbaz\nauthor:\n-JP Appel\n---\nhere are some content\nanother line of text")
	return bytes.NewReader(buf)
}

func extensionless(t *testing.T) index.InfoPath {
	root := t.TempDir()
	path := root + "/" + "afile"
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if _, err := f.WriteString("this is a file"); err != nil {
		t.Fatal(err)
	}

	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	return index.InfoPath{path, info}
}

func markdownExtension(t *testing.T) index.InfoPath {
	root := t.TempDir()
	path := root + "/" + "a.md"
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	return index.InfoPath{path, info}
}

func parentDirectory(t *testing.T) index.InfoPath {
	root := t.TempDir()
	dir := root + "/parent"
	path := dir + "/a"

	return index.InfoPath{Path: path}
}

func grandparentDirectory(t *testing.T) index.InfoPath {
	root := t.TempDir()
	dir := root + "/grandparent/parent"
	path := dir + "/a"

	return index.InfoPath{Path: path}
}

func TestYamlHeaderFilter(t *testing.T) {
	tests := []struct {
		name string
		r    io.ReadSeeker
		want bool
	}{
		{"completeYamlHeader", completeYamlHeader(), true},
		{"trailingYamlHeader", trailingYamlHeader(), true},
		{"noYamlHeader", noYamlHeader(), false},
		{"incompleteYamlHeader", incompleteYamlHeader(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := index.YamlHeaderPos(tt.r) > 0
			if got != tt.want {
				t.Errorf("YamlHeaderFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtensionFilter(t *testing.T) {
	tests := []struct {
		name    string
		infoGen func(*testing.T) index.InfoPath
		ext     string
		want    bool
	}{
		{"no extension, accept .md", extensionless, ".md", false},
		{"no extension, accept all", extensionless, "", true},
		{"markdown, accept .md", markdownExtension, ".md", true},
		{"makdown, accept .png", markdownExtension, ".png", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docFilter := index.NewExtensionFilter(tt.ext)
			ip := tt.infoGen(t)
			got := docFilter.Filter(ip, nil)

			if got != tt.want {
				t.Errorf("ExtensionFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExcludeParentFilter(t *testing.T) {
	tests := []struct {
		name    string
		infoGen func(*testing.T) index.InfoPath
		parent  string
		want    bool
	}{
		{
			"no matching parent",
			parentDirectory,
			"foobar", true,
		},
		{
			"direct parent",
			parentDirectory,
			"parent", false,
		},
		{
			"nested parent",
			grandparentDirectory,
			"grandparent", false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docFilter := index.NewExcludeParentFilter(tt.parent)
			ip := tt.infoGen(t)
			got := docFilter.Filter(ip, nil)

			if got != tt.want {
				t.Errorf("ExcludeParentFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}
