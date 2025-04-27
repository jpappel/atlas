package index

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/goccy/go-yaml"
)

type Document struct {
	Path      string
	Title     string    `yaml:"title"`
	Date      time.Time `yaml:"date"`
	FileTime  time.Time
	Authors   []string `yaml:"authors"`
	Tags      []string `yaml:"tags"`
	Links     []string
	OtherMeta string // unsure about how to handle this
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
	// TODO: print info about active filters
	return fmt.Sprintf("%s Documents[%d] Filters[%d]", idx.Root, len(idx.Documents), len(idx.Filters))
}

func (doc Document) Equal(other Document) bool {
	if len(doc.Authors) != len(other.Authors) || len(doc.Tags) != len(other.Tags) || len(doc.Links) != len(other.Links) || doc.Path != other.Path || doc.Title != other.Title || doc.OtherMeta != other.OtherMeta || !doc.Date.Equal(other.Date) || !doc.FileTime.Equal(other.FileTime) {
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

	// TODO: close jobs queue
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

	for _, filter := range idx.Filters {
		if !filter(infoPath{string(path), info}, f) {
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

func (idx Index) ParseOne(path string) (*Document, error) {
	doc := &Document{}
	doc.Path = path

	f, err := os.Open(string(path))
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	doc.FileTime = info.ModTime()

	buf := make([]byte, 4, 1024)
	n, err := f.Read(buf)
	if err != nil {
		return nil, err
	} else if n != 4 {
		return nil, errors.New("Short read")
	}

	// TODO: implement custom unmarshaller, for singular `Author`
	dec := yaml.NewDecoder(f)
	// TODO: handle no yaml header error
	if err := dec.Decode(&doc); err != nil {
		panic(err)
	}

	// TODO: body parsing

	return doc, nil
}

func (idx Index) Parse(paths []string, numWorkers uint) {
	jobs := make(chan string, numWorkers)
	results := make(chan Document, numWorkers)
	wg := &sync.WaitGroup{}

	wg.Add(int(numWorkers))
	for range numWorkers {
		go func(jobs <-chan string, results chan<- Document, wg *sync.WaitGroup) {
			for path := range jobs {
				doc, err := idx.ParseOne(path)
				if err != nil {
					// TODO: propagate error
					panic(err)
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
		idx.Documents[doc.Path] = &doc
	}
}
