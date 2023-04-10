BASE_DIR         = $(shell pwd)
CONCURRENCY 	 ?= 4
DEP_DIR         ?= "deps"
EBIN_DIR        ?= "ebin"
ERLANG_BIN       = $(shell dirname $(shell which erl))
LATENCY 		 ?= 0
MAKE			 = make
PACKAGE         ?= partisan
PROJECT         ?= $(shell basename `find src -name "*.app.src"` .app.src)
REBAR           ?= rebar3
REVISION        ?= $(shell git rev-parse --short HEAD)
SIZE 			?= 1024
VERSION         ?= $(shell git describe --tags)
CODESPELL 		= $(shell which codespell)
SPELLCHECK 	    = $(CODESPELL) -S _build -S doc -S .git -L applys,nd,accout,mattern,pres,fo
SPELLFIX      	= $(SPELLCHECK) -i 3 -w
OTPVSN 			= $(shell erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().' -noshell)

.PHONY: compile-no-deps alt-test core-test otp-test test docs xref dialyzer-run dialyzer-quick dialyzer eqwalizer \
		cleanplt upload-docs rel deps test plots spellcheck spellfix certs node1 node2 node3 node

all: compile

##
## Compilation targets
##

certs:
	cd config && ./make_certs

compile-no-deps:
	${REBAR} compile skip_deps=true

docs:
	${REBAR} ex_doc skip_deps=true

xref: compile
	${REBAR} xref skip_deps=true

dialyzer: compile
	${REBAR} dialyzer

eqwalizer: src/*.erl
ifeq ($(shell expr $(OTPVSN) \> 24),1)
	for file in $(shell ls $^ | sed 's|.*/\(.*\)\.erl|\1|'); do elp eqwalize $${file}; done
else
	$(info OTPVSN is not higher than 24)
	$(eval override mytarget=echo "Eqwalizer requires OTP25 or higher, skipping eqwalizer")
endif


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

proper:
	pkill -9 beam.smp; rm -rf priv/lager; ./rebar3 proper -m prop_partisan -p prop_sequential --noshrink -n 10

perf:
	pkill -9 beam.smp; pkill -9 epmd; SIZE=${SIZE} LATENCY=${LATENCY} CONCURRENCY=${CONCURRENCY} ${REBAR} ct --readable=false -v --suite=partisan_SUITE --case=performance_test --group=with_disterl
	pkill -9 beam.smp; pkill -9 epmd; SIZE=${SIZE} LATENCY=${LATENCY} CONCURRENCY=${CONCURRENCY} ${REBAR} ct --readable=false -v --suite=partisan_SUITE --case=performance_test --group=default
	pkill -9 beam.smp; pkill -9 epmd; SIZE=${SIZE} LATENCY=${LATENCY} CONCURRENCY=${CONCURRENCY} PARALLELISM=${CONCURRENCY} ${REBAR} ct --readable=false -v --suite=partisan_SUITE --case=performance_test --group=with_parallelism

kill:
	pkill -9 beam.smp; pkill -9 epmd; exit 0

check: kill xref dialyzer eqwalizer

spellcheck:
	$(if $(CODESPELL), $(SPELLCHECK), $(error "Aborting, command codespell not found in PATH"))


spellfix:
	$(if $(CODESPELL), $(SPELLFIX), $(error "Aborting, command codespell not found in PATH"))


test: eunit core-test otp-test cover

core-test: setup-tls
	${REBAR} as test ct -v --readable=false --suite=partisan_SUITE

otp-test: setup-tls
	${REBAR} as test ct --suite=partisan_gen_server_SUITE,partisan_gen_event_SUITE,partisan_gen_statem_SUITE

alt-test: setup-tls
	mkdir -p test/partisan_alt_SUITE_data/
	openssl rand -out test/partisan_alt_SUITE_data/RAND 4096
	${REBAR} as test ct -v --readable=false --suite=partisan_alt_SUITE

setup-tls:
	mkdir -p test/partisan_SUITE_data/
	openssl rand -out test/partisan_SUITE_data/RAND 4096

lint:
	${REBAR} as lint lint

eunit:
	${REBAR} as test eunit


cover:
	${REBAR} cover

shell:
	${REBAR} shell --apps partisan

tail-logs:
	tail ---disable-inotify -F priv/lager/*/log/*.log

unsorted-logs:
	cat priv/lager/*/log/*.log

logs:
	cat priv/lager/*/log/*.log | sort -k2M # -k3n -k4


##
## Release targets
##



node1: export ERL_NODE_NAME=node1@127.0.0.1 export PARTISAN_PEER_PORT=10100
node1: noderun

node2: export ERL_NODE_NAME=node2@127.0.0.1 export PARTISAN_PEER_PORT=10200
node2: noderun

node3: export ERL_NODE_NAME=node3@127.0.0.1 export PARTISAN_PEER_PORT=10300
node3: noderun

# ERL_NODE_NAME=node4@127.0.0.1 make node
node: noderun
	ifndef ERL_NODE_NAME
		$(error ERL_NODE_NAME is undefined)
	endif
	ifndef PARTISAN_PEER_PORT
		$(error PARTISAN_PEER_PORT is undefined)
	endif
	${REBAR} as node release
	RELX_REPLACE_OS_VARS=true _build/node/rel/partisan/bin/partisan console


noderun:
	${REBAR} as node release
	RELX_REPLACE_OS_VARS=true _build/node/rel/partisan/bin/partisan console

rel:
	${REBAR} as test release

stage:
	${REBAR} as test release -d

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto


##
## Container targets
##

containerize-deps:
	docker build -f partisan-base.Dockerfile -t cmeiklejohn/partisan-base .

containerize-tests: containerize-deps
	docker build --no-cache -f partisan-test-suite.Dockerfile -t cmeiklejohn/partisan-test-suite .

containerize: containerize-deps
	docker build --no-cache -f Dockerfile -t cmeiklejohn/partisan .

compose: containerize
	docker-compose down; docker-compose rm; docker-compose up

##
## CI targets
##

verify-lampson-2pc: kill bin-perms compile
	make lampson-2pc | grep "Passed: 7, Failed: 1"

verify-bernstein-ctp: kill bin-perms compile
	make bernstein-ctp | grep "Passed: 11, Failed: 1"

verify-skeen-3pc: kill bin-perms compile
	make skeen-3pc | grep "Passed: 25, Failed: 1"

##
## Testing targets
##

make bin-perms:
	chmod 755 bin/*.sh
	chmod 755 bin/*.escript

demers-anti-entropy: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_reliable_broadcast RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=demers_anti_entropy SUBLIST=0 bin/check-model.sh

demers-rumor-mongering: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_reliable_broadcast RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=demers_rumor_mongering SUBLIST=0 bin/check-model.sh

demers-direct-mail-acked: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_reliable_broadcast RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=demers_direct_mail_acked SUBLIST=0 bin/check-model.sh

demers-direct-mail: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_reliable_broadcast RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=demers_direct_mail SUBLIST=0 bin/check-model.sh

lampson-2pc: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_reliable_broadcast RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=lampson_2pc SUBLIST=0 bin/check-model.sh

bernstein-ctp: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_reliable_broadcast RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=bernstein_ctp SUBLIST=0 bin/check-model.sh

skeen-3pc: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_reliable_broadcast RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=skeen_3pc SUBLIST=0 bin/check-model.sh

lampson-2pc-noise: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_reliable_broadcast EXIT_ON_COUNTEREXAMPLE=true NOISE=true RECURSIVE=false PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=lampson_2pc SUBLIST=0 bin/check-model.sh

paxoid: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_paxoid RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=paxoid SUBLIST=0 bin/check-paxoid.sh

lashup: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_lashup RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=lashup SUBLIST=0 bin/check-lashup.sh

zraft: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_zraft RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=zraft SUBLIST=0 bin/check-zraft.sh

hbbft: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_hbbft RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=hbbft SUBLIST=0 bin/check-hbbft.sh

alsberg-day: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_primary_backup RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=alsberg_day SUBLIST=0 bin/filibuster.sh

alsberg-day-acked: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_primary_backup RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=alsberg_day_acked SUBLIST=0 bin/filibuster.sh

alsberg-day-acked-membership: kill bin-perms compile
	SYSTEM_MODEL=prop_partisan_primary_backup RECURSIVE=true PRELOAD_SCHEDULES=false IMPLEMENTATION_MODULE=alsberg_day_acked_membership SUBLIST=0 bin/filibuster.sh
