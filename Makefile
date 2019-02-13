PACKAGE         ?= partisan
VERSION         ?= $(shell git describe --tags)
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= rebar3
MAKE             = make
CONCURRENCY     ?= 4
LATENCY         ?= 0
SIZE            ?= 1024

.PHONY: rel deps test plots

all: compile

##
## Compilation targets
##

compile:
	$(REBAR) compile

clean: packageclean
	$(REBAR) clean

packageclean:
	rm -fr *.deb
	rm -fr *.tar.gz

##
## Test targets
##

perf:
	pkill -9 beam.smp; pkill -9 epmd; SIZE=${SIZE} LATENCY=${LATENCY} CONCURRENCY=${CONCURRENCY} ${REBAR} ct --readable=false -v --suite=partisan_SUITE --case=performance_test --group=with_disterl
	pkill -9 beam.smp; pkill -9 epmd; SIZE=${SIZE} LATENCY=${LATENCY} CONCURRENCY=${CONCURRENCY} ${REBAR} ct --readable=false -v --suite=partisan_SUITE --case=performance_test --group=default
	pkill -9 beam.smp; pkill -9 epmd; SIZE=${SIZE} LATENCY=${LATENCY} CONCURRENCY=${CONCURRENCY} PARALLELISM=${CONCURRENCY} ${REBAR} ct --readable=false -v --suite=partisan_SUITE --case=performance_test --group=with_parallelism

kill: 
	pkill -9 beam.smp; pkill -9 epmd; exit 0

check: kill test xref dialyzer

test: ct eunit

lint:
	${REBAR} as lint lint

eunit:
	${REBAR} as test eunit

ct:
	openssl rand -out test/partisan_SUITE_data/RAND 4096
	${REBAR} ct
	${REBAR} cover

shell:
	${REBAR} shell --apps partisan

tail-logs:
	tail -F priv/lager/*/log/*.log

logs:
	cat priv/lager/*/log/*.log

##
## Release targets
##

rel:
	${REBAR} release

stage:
	${REBAR} release -d

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto

include tools.mk
