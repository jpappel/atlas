package index

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/jpappel/atlas/pkg/util"
)

var ErrHeaderParse error = errors.New("Unable to parse YAML header")

type Document struct {
	Path      string    `yaml:"-" json:"path"`
	Title     string    `yaml:"title" json:"title"`
	Date      time.Time `yaml:"-" json:"date"`
	FileTime  time.Time `yaml:"-" json:"filetime"`
	Authors   []string  `yaml:"-" json:"authors"`
	Tags      []string  `yaml:"tags,omitempty" json:"tags"`
	Links     []string  `yaml:"-" json:"links"`
	OtherMeta string    `yaml:"-" json:"meta"`
}

type infoPath struct {
	path string
	info os.FileInfo
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
			if err := doc.parseDateNode(v); err != nil {
				return err
			}
		} else if keyPath == "$.author" {
			if err := doc.parseAuthor(v); err != nil {
				return err
			}
		} else {
			field, err := kv.MarshalYAML()
			if err != nil {
				return err
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

	var err error
	if doc.Date, err = util.ParseDateTime(dateStr); err != nil {
		return fmt.Errorf("Unable to parse date: %s", dateNode.Value)
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

func visit(file infoPath, visitQueue chan<- infoPath, filterQueue chan<- infoPath, wg *sync.WaitGroup) {
	// TODO: check if symlink, and handle appropriately
	// TODO: extract error out of function

	if file.info.IsDir() {
		entries, err := os.ReadDir(file.path)
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
				visitQueue <- infoPath{path: path, info: entryInfo}
			}(file.path + "/" + entry.Name())
		}
	} else if file.info.Mode().IsRegular() {
		filterQueue <- file
	}

	wg.Done()
}

func workerTraverse(wg *sync.WaitGroup, visitQueue chan infoPath, filterQueue chan<- infoPath) {
	for work := range visitQueue {
		visit(work, visitQueue, filterQueue, wg)
	}
}

func (idx Index) Traverse(numWorkers uint) []string {
	if numWorkers <= 1 {
		panic(fmt.Sprint("Invalid number of workers: ", numWorkers))
	}
	docs := make([]string, 0)

	rootInfo, err := os.Stat(idx.Root)
	if err != nil {
		panic(err)
	}

	jobs := make(chan infoPath, numWorkers)
	filterQueue := make(chan infoPath, numWorkers)

	activeJobs := &sync.WaitGroup{}

	// start workers
	for range numWorkers {
		go workerTraverse(activeJobs, jobs, filterQueue)
	}

	// init send
	activeJobs.Add(1)
	jobs <- infoPath{path: idx.Root, info: rootInfo}

	// close jobs queue
	go func() {
		activeJobs.Wait()
		close(jobs)
		close(filterQueue)
	}()

	// gather
	for doc := range filterQueue {
		docs = append(docs, doc.path)
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
		if !docFilter.Filter(infoPath{string(path), info}, f) {
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

func ParseDoc(path string) (*Document, error) {
	doc := &Document{}
	doc.Path = path

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

	pos := yamlHeaderPos(f)
	f.Seek(0, io.SeekStart)
	if pos < 0 {
		return nil, fmt.Errorf("Can't find YAML header in %s", path)
	}

	// FIXME: decoder reads past yaml header into document
	if err := yaml.NewDecoder(io.LimitReader(f, pos)).Decode(doc); err != nil {
		return nil, errors.Join(ErrHeaderParse, err)
	}

	// TODO: read the rest of the file to find links
	return doc, nil
}

func ParseDocs(paths []string, numWorkers uint) map[string]*Document {
	jobs := make(chan string, numWorkers)
	results := make(chan Document, numWorkers)
	docs := make(map[string]*Document, len(paths))
	wg := &sync.WaitGroup{}

	wg.Add(int(numWorkers))
	for range numWorkers {
		go func(jobs <-chan string, results chan<- Document, wg *sync.WaitGroup) {
			for path := range jobs {
				doc, err := ParseDoc(path)
				if err != nil {
					// TODO: propagate error
					slog.Error("Error occured while parsing file",
						slog.String("path", path), slog.String("err", err.Error()),
					)
					continue
				}

				results <- *doc
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

	go func(results chan Document, wg *sync.WaitGroup) {
		wg.Wait()
		close(results)
	}(results, wg)

	for doc := range results {
		docs[doc.Path] = &doc
	}

	return docs
}
