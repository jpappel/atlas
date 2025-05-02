package index

import (
	"io"
	"path/filepath"
)

// NOTE: in the future it would be interesting lua filters
// TODO: create excluded path filter factory

type DocFilter func(infoPath, io.ReadSeeker) bool

func NewExtensionFilter(ext string) DocFilter {
	return func(ip infoPath, _ io.ReadSeeker) bool {
		return filepath.Ext(ip.path) == ext
	}
}

func NewMaxFilesizeFilter(size int64) DocFilter {
	return func(ip infoPath, _ io.ReadSeeker) bool {
		return ip.info.Size() <= size
	}
}

func NewFilenameFilter(excluded []string) DocFilter {
	excludedSet := make(map[string]bool, len(excluded))
	for _, filename := range excluded {
		excludedSet[filename] = true
	}
	return func(ip infoPath, _ io.ReadSeeker) bool {
		_, ok := excludedSet[filepath.Base(ip.path)]
		return ok
	}
}

func YamlHeaderFilter(_ infoPath, r io.ReadSeeker) bool {
	const bufSize = 4096
	buf := make([]byte, bufSize)

	carry := make([]byte, 4)
	cmp := make([]byte, 4)
	n, err := r.Read(carry)
	if err != nil || n < 4 || string(carry) != "---\n" {
		return false
	}

	headerFound := false
	readMore := true
	for readMore {
		buf = buf[:bufSize]
		n, err := r.Read(buf)
		if err == io.EOF {
			readMore = false
		} else if err != nil {
			return false
		}
		buf = buf[:n]

		// PERF: the carry doesn't need to be checked on the first loop iteration
		for i := range min(4, n) {
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

	return headerFound
}

func DefaultFilters() []DocFilter {
	return []DocFilter{NewExtensionFilter(".md"), NewMaxFilesizeFilter(200 * 1024), YamlHeaderFilter}
}
