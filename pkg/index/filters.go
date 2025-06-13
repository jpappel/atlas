package index

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// NOTE: in the future it would be interesting lua filters

type DocFilter struct {
	Name   string
	Filter func(infoPath, io.ReadSeeker) bool
}

const FilterHelp string = `
YAMLHeader                                      - reject files without YAML header
Ext,Extension_<ext>                             - accept files ending with <ext>
MaxSize,MaxFilesize_<size>                      - accept files of at most <size> bytes
ExcludeName,ExcludeFilename_<name1>,...,<nameN> - reject files with names in list
IncludeName,IncludeFilename_<name1>,...,<nameN> - accept files with names in list
ExcludeParent_<dir>                             - reject files if <dir> is a parent directory
IncludeRegex_<pattern>                          - accept files whose path matches <pattern>
ExcludeRegex_<pattern>                          - reject files whose path matches <pattern>`

func ParseFilter(s string) (DocFilter, error) {
	name, param, found := strings.Cut(s, "_")

	// paramless filters
	if name == "YAMLHeader" {
		return YamlHeaderFilter, nil
	}

	if !found {
		return DocFilter{}, fmt.Errorf("Expected parameter with filter %s", name)
	}

	switch name {
	case "Ext", "Extension":
		return NewExtensionFilter(param), nil
	case "MaxSize", "MaxFilesize":
		size, err := strconv.ParseInt(param, 10, 64)
		if err != nil {
			return DocFilter{}, err
		}
		return NewMaxFilesizeFilter(size), nil
	case "ExcludeName", "ExcludeFilename":
		// FIXME: support escaped commas
		return NewExcludeFilenameFilter(strings.Split(param, ",")), nil
	case "IncludeName", "IncludeFilename":
		// FIXME: support escaped commas
		return NewIncludeFilenameFilter(strings.Split(param, ",")), nil
	case "ExcludeParent":
		return NewExcludeParentFilter(param), nil
	case "IncludeRegex":
		filter, err := NewIncludeRegexFilter(param)
		if err != nil {
			return DocFilter{}, err
		}
		return filter, nil
	case "ExcludeRegex":
		filter, err := NewIncludeRegexFilter(param)
		if err != nil {
			return DocFilter{}, err
		}
		return filter, nil
	default:
		return DocFilter{}, fmt.Errorf("Unrecognized filter %s, see FILTERS", s)
	}
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
	return DocFilter{
		"Excluded Filename filter",
		func(ip infoPath, _ io.ReadSeeker) bool {
			filename := filepath.Base(ip.path)
			return !slices.Contains(excluded, filename)
		},
	}
}

func NewIncludeFilenameFilter(included []string) DocFilter {
	return DocFilter{
		"Included Filename filter",
		func(ip infoPath, _ io.ReadSeeker) bool {
			filename := filepath.Base(ip.path)
			return slices.Contains(included, filename)
		},
	}
}

// exclude files if it has a parent directory badParent
func NewExcludeParentFilter(badParent string) DocFilter {
	return DocFilter{
		"Excluded Parent Directory filter: " + badParent,
		func(ip infoPath, _ io.ReadSeeker) bool {
			return !slices.Contains(strings.Split(ip.path, string(os.PathSeparator)), badParent)
		},
	}
}

func NewIncludeRegexFilter(pattern string) (DocFilter, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return DocFilter{}, fmt.Errorf("Cannot compile regex: %v", err)
	}

	return DocFilter{
		"Included Regex Filter: " + pattern,
		func(ip infoPath, _ io.ReadSeeker) bool {
			return re.MatchString(ip.path)
		},
	}, nil
}
func NewExcludeRegexFilter(pattern string) (DocFilter, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return DocFilter{}, fmt.Errorf("Cannot compile regex: %v", err)
	}

	return DocFilter{
		"Excluded Regex Filter: " + pattern,
		func(ip infoPath, _ io.ReadSeeker) bool {
			return !re.MatchString(ip.path)
		},
	}, nil
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
	return []DocFilter{NewExtensionFilter(".md"), NewMaxFilesizeFilter(200 * 1024), NewExcludeParentFilter("templates"), YamlHeaderFilter}
}
