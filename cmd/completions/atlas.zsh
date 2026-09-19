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

	_describe 'command' commands
}

_atlas "$@"
