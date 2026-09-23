#compdef atlas

_atlas() {
	local -a commands

	commands=(
		'query:Query stuff'
		'index:Index stuff'
		'server:Launch server'
		'shell:Launch interactive shell'
		'completions:Generate shell completions'
		'help:View help'
	)

    _arguments -C \
        '-root[root directory for indexing]:directory:_directories' \
        '-db[path to document database]:database:_files' \
        '-logLevel[set log level]:level:(debug info warn error)' \
        '-logJson[output log information as json]' \
        '-numWorkers[number of worker threads to use (defaults to logical core count)]:count:' \
        '-dateFormat[format for dates (see https://pkg.go.dev/time#Layout for more details)]' \
        '-logFile[file to log errors to, use - for stdout and do not set a value for stderr]:logfile:_files' \
        '1:command:->command' \
        '*::arguments:->arguments'

    case $state in
        command)
            _describe 'command' commands
            ;;
        arguments)
            case $words[1] in
                query) _atlas_query ;;
                index) _atlas_index ;;
                server) _atlas_server ;;
                shell) _atlas_shell ;;
                completions) _atlas_completions ;;
                help) _atlas_help ;;
            esac
    esac
}

_atlas_query() {

    _arguments \
        '-outFormat[output format for queries]:format:(default json yaml pathonly custom)' \
        '-sortBy[category to sort by]:sortCriteria:(path title date filetime meta)' \
        '-sortDesc[sort in descending order]' \
        '-customFormat[format string for -outFormat custom, see `atlas help query` for more detials]:format:' \
        '-optLevel[optimization level for queries, 0 is automatic, set to < 0 to disable optimizations]:level:' \
        '-docSeparator[separator for custom output format]:separator:' \
        '-listSeparator[separator for list fields]:separator:'

    # TODO: encode langauge grammar for completions
}

_atlas_index() {
    local -a commands filters
    commands=(
        'build:Create or replace document index'
        'update:Update existing document index'
        'tidy:Cleanup document index, removing stale files'
    )
    filters=(
        'YAMLHeader'
        'Ext_'
        'Extension_'
        'MaxSize_'
        'MaxFilesize_'
        'ExcludeName_'
        'ExcludeFilename_'
        'IncludeName_'
        'IncludeFilename_'
        'ExcludeParent_'
        'IncludeRegex_'
        'ExcludeRegex_'
    )

    if (( CURRENT == 3)); then
        _describe 'subcommand' commands
        return
    fi

    # TODO: input validation for filters
    _arguments \
        '-ignoreBadDates[ignore malformed dates while indexing]' \
        '-ignoreMetaError[ignore errors while parsing general YAML header info]' \
        '-ignoreMeta[only parse title,authors,date, and tags from YAML headers]' \
        '-ignoreHeadings[avoid parsing file contents for section headings]' \
        '-ignoreHidden[ignore hidden files while crawling]' \
        '-filter[accept or reject files from indexing, applied in supplied order (default Ex_.md, MaxSize_204800, YAMLHeader, ExcludeParent_templates)]:filter:'
}

_atlas_server() {
    _arguments \
        '-address[listening address, prefix with unix: to create a unix socket]:host:' \
        '-port[port to bind to]:port:'
}

_atlas_shell() {}

_atlas_completions() {
    (( CURRENT > 2)) && return

    local -a shells
    shells=(
        'zsh'
    )
    _describe 'shell' shells
}

_atlas_help() {
    (( CURRENT > 2 )) && return
    local -a topics
    topics=(
        'index'
        'index build'
        'index update'
        'index tidy'
        'query'
        'shell'
        'server'
        'completions'
    )

    _describe 'help topic' topics
}
