package index

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/jpappel/atlas/pkg/util"
)

var ErrHeaderParse error = errors.New("Unable to parse YAML header")
var linkRegex *regexp.Regexp

type Document struct {
	Path      string    `yaml:"-" json:"path"`
	Title     string    `yaml:"title" json:"title"`
	Date      time.Time `yaml:"-" json:"date"`
	FileTime  time.Time `yaml:"-" json:"filetime"`
	Authors   []string  `yaml:"-" json:"authors"`
	Tags      []string  `yaml:"tags,omitempty" json:"tags"`
	Links     []string  `yaml:"-" json:"links"`
	OtherMeta string    `yaml:"-" json:"meta"`
	parseOpts ParseOpts
}

type ParseOpts struct {
	ParseMeta       bool
	ParseLinks      bool
	IgnoreDateError bool
	IgnoreMetaError bool
	IgnoreHidden    bool
}

type InfoPath struct {
	Path string
	Info os.FileInfo
}

type Index struct {
	Root      string // root directory for searching
	Documents map[string]*Document
	Filters   []DocFilter
}

func (idx Index) String() string {
	b := strings.Builder{}
	fmt.Fprintf(&b, "%s Documents[%d]\n", idx.Root, len(idx.Documents))
	fmt.Fprintf(&b, "Filters[%d]: ", len(idx.Filters))

	for i, docFilter := range idx.Filters {
		b.WriteString(docFilter.Name)
		if i != len(idx.Filters) {
			b.WriteByte(',')
		}
	}

	return b.String()
}

var _ yaml.NodeUnmarshaler = (*Document)(nil)

func (doc *Document) UnmarshalYAML(node ast.Node) error {
	// parse top level fields
	type alias Document
	var temp alias
	if err := yaml.NodeToValue(node, &temp); err != nil {
		return err
	}
	doc.Title = temp.Title
	doc.Tags = temp.Tags

	mapnode, ok := node.(*ast.MappingNode)
	if !ok {
		return ErrHeaderParse
	}

	ignored_keyPaths := map[string]bool{
		"$.title": true,
		"$.tags":  true,
	}

	buf := strings.Builder{}
	for _, kv := range mapnode.Values {
		k, v := kv.Key, kv.Value
		keyPath := k.GetPath()

		if ignored_keyPaths[keyPath] {
			continue
		}

		if keyPath == "$.date" {
			if err := doc.parseDateNode(v); err != nil && !doc.parseOpts.IgnoreDateError {
				return err
			}
		} else if keyPath == "$.author" {
			if err := doc.parseAuthor(v); err != nil {
				return err
			}
		} else if doc.parseOpts.ParseMeta {
			field, err := kv.MarshalYAML()
			if err != nil {
				if doc.parseOpts.IgnoreMetaError {
					continue
				} else {
					return err
				}
			}
			buf.Write(field)
			buf.WriteByte('\n')
		}
	}

	doc.OtherMeta = buf.String()

	return nil
}

func (doc *Document) parseDateNode(node ast.Node) error {
	dateNode, ok := node.(*ast.StringNode)
	if !ok {
		return ErrHeaderParse
	}
	dateStr := dateNode.Value

	if dateStr == "" {
		return nil
	}

	if date, err := util.ParseDateTime(dateStr); err != nil {
		return fmt.Errorf("Unable to parse date: %s", dateNode.Value)
	} else {
		doc.Date = date
	}

	return nil
}

func (doc *Document) parseAuthor(node ast.Node) error {
	authorsNode, ok := node.(*ast.SequenceNode)
	if ok {
		doc.Authors = make([]string, 0, len(authorsNode.Values))
		for _, authorNode := range authorsNode.Values {
			authorStrNode, ok := authorNode.(*ast.StringNode)
			if !ok {
				return ErrHeaderParse
			}
			doc.Authors = append(doc.Authors, authorStrNode.Value)
		}
	} else {
		authorNode, ok := node.(*ast.StringNode)
		if ok {
			doc.Authors = []string{authorNode.Value}
		} else {
			return ErrHeaderParse
		}
	}

	return nil
}

func (doc Document) Equal(other Document) bool {
	if len(doc.Authors) != len(other.Authors) || len(doc.Tags) != len(other.Tags) || len(doc.Links) != len(other.Links) || doc.Path != other.Path || doc.Title != other.Title || doc.OtherMeta != other.OtherMeta || !doc.Date.Equal(other.Date) {
		return false
	}

	if !slices.Equal(doc.Authors, other.Authors) {
		return false
	}

	slices.Sort(doc.Tags)
	slices.Sort(other.Tags)
	for i := range doc.Tags {
		if doc.Tags[i] != other.Tags[i] {
			return false
		}
	}

	slices.Sort(doc.Links)
	slices.Sort(other.Links)
	for i := range doc.Links {
		if doc.Links[i] != other.Links[i] {
			return false
		}
	}

	return true
}

func visit(file InfoPath, visitQueue chan<- InfoPath, filterQueue chan<- InfoPath, ignoreHidden bool, wg *sync.WaitGroup) {
	// TODO: check if symlink, and handle appropriately
	// TODO: extract error out of function

	if ignoreHidden && path.Base(file.Path)[0] == '.' {
		wg.Done()
		return
	}

	if file.Info.IsDir() {
		entries, err := os.ReadDir(file.Path)
		if err != nil {
			panic(err)
		}
		wg.Add(len(entries))
		for _, entry := range entries {
			entryInfo, err := entry.Info()
			if err != nil {
				panic(err)
			}
			// PERF: prevents deadlock but introduces an additional goroutine overhead per file
			go func(path string) {
				visitQueue <- InfoPath{Path: path, Info: entryInfo}
			}(file.Path + "/" + entry.Name())
		}
	} else if file.Info.Mode().IsRegular() {
		filterQueue <- file
	}

	wg.Done()
}

func workerTraverse(wg *sync.WaitGroup, ignoreHidden bool, visitQueue chan InfoPath, filterQueue chan<- InfoPath) {
	for work := range visitQueue {
		visit(work, visitQueue, filterQueue, ignoreHidden, wg)
	}
}

func (idx Index) Traverse(numWorkers uint, ignoreHidden bool) []string {
	if numWorkers <= 1 {
		panic(fmt.Sprint("Invalid number of workers: ", numWorkers))
	}
	docs := make([]string, 0)

	rootInfo, err := os.Stat(idx.Root)
	if err != nil {
		panic(err)
	}

	jobs := make(chan InfoPath, numWorkers)
	filterQueue := make(chan InfoPath, numWorkers)

	activeJobs := &sync.WaitGroup{}

	// start workers
	for range numWorkers {
		go workerTraverse(activeJobs, ignoreHidden, jobs, filterQueue)
	}

	// init send
	activeJobs.Add(1)
	jobs <- InfoPath{Path: idx.Root, Info: rootInfo}

	// close jobs queue
	go func() {
		activeJobs.Wait()
		close(jobs)
		close(filterQueue)
	}()

	// gather
	for doc := range filterQueue {
		docs = append(docs, doc.Path)
	}

	return docs
}

func (idx Index) FilterOne(path string) bool {
	info, err := os.Stat(string(path))
	if err != nil {
		return false
	}

	f, err := os.Open(string(path))
	if err != nil {
		return false
	}
	defer f.Close()

	for _, docFilter := range idx.Filters {
		if !docFilter.Filter(InfoPath{string(path), info}, f) {
			return false
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return false
		}
	}
	return true
}

func (idx Index) Filter(paths []string, numWorkers uint) []string {
	fPaths := make([]string, 0, len(paths))
	jobs := make(chan string, numWorkers)
	accepted := make(chan string, numWorkers)
	wg := &sync.WaitGroup{}

	wg.Add(int(numWorkers))
	for range numWorkers {
		go func(jobs <-chan string, accepted chan<- string, wg *sync.WaitGroup) {
			for path := range jobs {
				if idx.FilterOne(path) {
					accepted <- path
				}
			}
			wg.Done()
		}(jobs, accepted, wg)
	}

	go func(jobs chan<- string) {
		for _, path := range paths {
			jobs <- path
		}
		close(jobs)
	}(jobs)

	go func() {
		wg.Wait()
		close(accepted)
	}()

	for path := range accepted {
		fPaths = append(fPaths, path)
	}

	return fPaths
}

// Create a comparison function for documents by field.
// Allowed fields: path,title,date,filetime,meta
func NewDocCmp(field string, reverse bool) (func(*Document, *Document) int, bool) {
	descMod := 1
	if reverse {
		descMod = -1
	}
	switch field {
	case "path":
		return func(a, b *Document) int {
			return descMod * strings.Compare(a.Path, b.Path)
		}, true
	case "title":
		return func(a, b *Document) int {
			return descMod * strings.Compare(a.Title, b.Title)
		}, true
	case "date":
		return func(a, b *Document) int {
			return descMod * a.Date.Compare(b.Date)
		}, true
	case "filetime":
		return func(a, b *Document) int {
			return descMod * a.FileTime.Compare(b.FileTime)
		}, true
	case "meta":
		return func(a, b *Document) int {
			return descMod * strings.Compare(a.OtherMeta, b.OtherMeta)
		}, true
	}

	return nil, false
}

func ParseDoc(path string, opts ParseOpts) (*Document, error) {
	doc := &Document{Path: path, parseOpts: opts}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	doc.FileTime = info.ModTime()

	pos := YamlHeaderPos(f)
	f.Seek(0, io.SeekStart)
	if pos < 0 {
		return nil, fmt.Errorf("Can't find YAML header in %s", path)
	}
	header := io.NewSectionReader(f, 0, pos)

	if err := yaml.NewDecoder(header).Decode(doc); err != nil {
		return nil, errors.Join(ErrHeaderParse, err)
	}

	if opts.ParseLinks {
		var buf bytes.Buffer
		f.Seek(pos, io.SeekStart)
		if _, err := io.Copy(&buf, f); err != nil {
			return nil, err
		}

		matches := linkRegex.FindAllSubmatch(buf.Bytes(), -1)
		for _, match := range matches {
			if len(match) != 2 {
				panic("Link parsing regex returned unexpected number of matches")
			}
			link := string(match[1])
			if len(link) > 0 {
				doc.Links = append(doc.Links, link)
			}
		}
	}

	return doc, nil
}

func ParseDocs(paths []string, numWorkers uint, opts ParseOpts) (map[string]*Document, uint64) {
	jobs := make(chan string, numWorkers)
	results := make(chan *Document, numWorkers)
	docs := make(map[string]*Document, len(paths))
	wg := &sync.WaitGroup{}

	errCnt := &atomic.Uint64{}
	wg.Add(int(numWorkers))
	for range numWorkers {
		go func(jobs <-chan string, results chan<- *Document, wg *sync.WaitGroup) {
			for path := range jobs {
				doc, err := ParseDoc(path, opts)
				if err != nil {
					slog.Warn("Error occured while parsing file",
						slog.String("path", path), slog.String("err", err.Error()),
					)
					errCnt.Add(1)
					continue
				}

				results <- doc
			}
			wg.Done()
		}(jobs, results, wg)
	}

	go func(jobs chan<- string, paths []string) {
		for _, path := range paths {
			jobs <- path
		}
		close(jobs)
	}(jobs, paths)

	go func(results chan *Document, wg *sync.WaitGroup) {
		wg.Wait()
		close(results)
	}(results, wg)

	for doc := range results {
		docs[doc.Path] = doc
	}

	return docs, errCnt.Load()
}

func init() {
	linkRegex = regexp.MustCompile(`\[.*\]\(\s*([^\)]+)\s*\)`)
}
