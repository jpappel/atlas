package index

import (
	"fmt"
	"io"
	"path/filepath"
)

// NOTE: in the future it would be interesting lua filters

type DocFilter struct {
	Name   string
	Filter func(infoPath, io.ReadSeeker) bool
}

func NewExtensionFilter(ext string) DocFilter {
	return DocFilter{
		ext + " Filter",
		func(ip infoPath, _ io.ReadSeeker) bool {
			return filepath.Ext(ip.path) == ext
		},
	}
}

func NewMaxFilesizeFilter(size int64) DocFilter {
	return DocFilter{
		fmt.Sprintf("Max Size Filter %d", size),
		func(ip infoPath, _ io.ReadSeeker) bool {
			return ip.info.Size() <= size
		},
	}
}

func NewExcludeFilenameFilter(excluded []string) DocFilter {
	excludedSet := make(map[string]bool, len(excluded))
	for _, filename := range excluded {
		excludedSet[filename] = true
	}
	return DocFilter{
		"Excluded Filename filter",
		func(ip infoPath, _ io.ReadSeeker) bool {
			_, ok := excludedSet[filepath.Base(ip.path)]
			return !ok
		},
	}
}

func NewIncludeFilenameFilter(included []string) DocFilter {
	includedSet := make(map[string]bool, len(included))
	for _, filename := range included {
		includedSet[filename] = true
	}
	return DocFilter{
		"Included Filename filter",
		func(ip infoPath, _ io.ReadSeeker) bool {
			_, ok := includedSet[filepath.Base(ip.path)]
			return ok
		},
	}
}

var YamlHeaderFilter = DocFilter{
	"YAML Header Filter",
	yamlHeaderFilterFunc,
}

func yamlHeaderFilterFunc(_ infoPath, r io.ReadSeeker) bool {
	return yamlHeaderPos(r) > 0
}

// Position of the end of a yaml header, negative
func yamlHeaderPos(r io.ReadSeeker) int64 {
	const bufSize = 4096
	buf := make([]byte, bufSize)

	carry := make([]byte, 4)
	cmp := make([]byte, 4)
	n, err := r.Read(carry)
	if err != nil || n < 4 || string(carry) != "---\n" {
		return -1
	}

	pos := int64(3)
	headerFound := false
	readMore := true
	for readMore {
		buf = buf[:bufSize]
		n, err := r.Read(buf)
		if err == io.EOF {
			readMore = false
		} else if err != nil {
			return -1
		}
		buf = buf[:n]

		// PERF: the carry doesn't need to be checked on the first loop iteration
		for i := range min(4, n) {
			pos++
			b := carry[i]
			for j := range 4 {
				if i+j < 4 {
					cmp[j] = carry[i+j]
				} else {
					cmp[j] = buf[(i+j)%4]
				}
			}
			if b == '\n' && string(cmp) == "\n---\n" {
				headerFound = true
				readMore = false
				break
			}
		}
		for i := range n - 4 {
			pos++
			b := buf[i]
			if b == '\n' && string(buf[i:i+5]) == "\n---\n" {
				headerFound = true
				readMore = false
				break
			}
		}

		if readMore {
			for i := range 4 {
				carry[i] = buf[n-4+i]
			}
		}
	}

	if headerFound {
		return pos
	} else {
		return -1
	}
}

func DefaultFilters() []DocFilter {
	return []DocFilter{NewExtensionFilter(".md"), NewMaxFilesizeFilter(200 * 1024), YamlHeaderFilter}
}
