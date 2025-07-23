# Atlas

A tool for querying markdown files with YAML metadata.

## Build

```bash
make
```

### Install

Default installation path is `$HOME/.local/bin`

```bash
make install
```

## Usage

```
atlas is a note indexing and querying tool

Usage:
  atlas [global-flags] <command>

Commands:
  index <subcommand>    - build, update, or modify an index
  query <subcommand>    - search against an index
  shell                 - start a debug shell
  server                - start an http query server (EXPERIMENTAL)
  help  <help-topic>    - print help info

Global Flags:
  -dateFormat format
    	format for dates (see https://pkg.go.dev/time#Layout for more details) (default "2006-01-02T15:04:05Z07:00")
  -db path
    	path to document database (default "/home/goose/.local/share/atlas/default.db")
  -logFile file
    	file to log errors to, use '-' for stdout and empty for stderr
  -logJson
    	log to json
  -logLevel level
    	set log level (debug, info, warn, error) (default "error")
  -numWorkers uint
    	number of worker threads to use (defaults to core count) (default 4)
  -root directory
    	root directory for indexing (default "/home/goose/doc")
```
