REBAR ?= ./rebar3

docs:
	${REBAR} doc skip_deps=true

xref: compile
	${REBAR} xref
