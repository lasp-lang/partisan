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

.PHONY: compile-no-deps alt-test core-test test ci-light ci-heavy security-test docs xref dialyzer-run dialyzer-quick dialyzer\
		cleanplt upload-docs rel deps test plots spellcheck spellfix certs node1 node2 node3 node checkssl

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

## Runs one property against one system model. `prop_sequential' reads the model
## and its implementation from the environment and fails without them, so both
## carry defaults here; override either on the command line:
##   make proper SYSTEM_MODEL=... IMPLEMENTATION_MODULE=...
## The exhaustive model checking lives in the demers-*, lampson-* and skeen-*
## targets.
proper:
	pkill -9 beam.smp; rm -rf priv/lager; \
	SYSTEM_MODEL=$${SYSTEM_MODEL:-prop_partisan_reliable_broadcast} \
	IMPLEMENTATION_MODULE=$${IMPLEMENTATION_MODULE:-demers_direct_mail} \
	RESTART_NODES=false USE_STARTED_NODES=false \
	${REBAR} proper -m prop_partisan -p prop_sequential --noshrink -n 10

perf:
	pkill -9 beam.smp; pkill -9 epmd; SIZE=${SIZE} LATENCY=${LATENCY} CONCURRENCY=${CONCURRENCY} ${REBAR} ct --readable=false -v --suite=partisan_SUITE --case=performance_test --group=with_disterl
	pkill -9 beam.smp; pkill -9 epmd; SIZE=${SIZE} LATENCY=${LATENCY} CONCURRENCY=${CONCURRENCY} ${REBAR} ct --readable=false -v --suite=partisan_SUITE --case=performance_test --group=default
	pkill -9 beam.smp; pkill -9 epmd; SIZE=${SIZE} LATENCY=${LATENCY} CONCURRENCY=${CONCURRENCY} PARALLELISM=${CONCURRENCY} ${REBAR} ct --readable=false -v --suite=partisan_SUITE --case=performance_test --group=with_parallelism

kill:
	pkill -9 beam.smp; pkill -9 epmd; exit 0

check: kill xref dialyzer

spellcheck:
	$(if $(CODESPELL), $(SPELLCHECK), $(error "Aborting, command codespell not found in PATH"))


spellfix:
	$(if $(CODESPELL), $(SPELLFIX), $(error "Aborting, command codespell not found in PATH"))


test: eunit core-test otp-compat-test cover

# CI split (see test/fly/): light suites run on GitHub runners; heavy multi-node
# suites run on a large Fly.io Machine. `ci-heavy` is what test/fly/run.sh runs.
ci-light: eunit otp-compat-test security-test

ci-heavy: core-test alt-test proper

security-test:
	${REBAR} as test ct -v --readable=false --suite=partisan_security_SUITE

core-test: setup-tls
	${REBAR} as test ct -v --readable=false --suite=partisan_SUITE

alt-test: setup-tls
	mkdir -p test/partisan_alt_SUITE_data/
	openssl rand -out test/partisan_alt_SUITE_data/RAND 4096
	${REBAR} as test ct -v --readable=false --suite=partisan_alt_SUITE

## Run OTP compatibility test suites.
## These are adapted versions of OTP's own gen_server_SUITE, supervisor_SUITE, etc.
## that validate the generated partisan modules behave identically to OTP.
## The -eval expressions MUST each stay on one physical line: GNU Make >= 3.82
## passes backslash-newlines through to the shell, which keeps them literal
## inside single quotes, and erl_parse rejects the resulting '\' tokens.
## (GNU Make 3.81, as shipped by macOS, strips them — which is why a multi-line
## eval works locally but breaks on CI runners.)
otp-compat-test:
	${REBAR} as test compile
	erl -noshell -pa _build/test/lib/*/ebin \
		-eval 'case partisan_otp_test_gen:generate_all_suites("_build/test/lib/partisan/test") of ok -> halt(0); _ -> halt(1) end.'
	erl -noshell -sname partisan_ct_runner \
		-pa _build/test/lib/*/ebin \
		-pa _build/test/lib/partisan/test \
		-pa _build/test/lib/partisan/test/otp \
		-partisan connect_disterl true \
		-eval 'case partisan_otp_test_gen:run_all_suites("_build/test/lib/partisan/test", "_build/test/logs") of ok -> halt(0); _ -> halt(1) end.'


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


node1:
	${REBAR} as node1 release
	ERL_DIST_PORT=37781 _build/node1/rel/partisan/bin/partisan console

node2:
	${REBAR} as node2 release
	ERL_DIST_PORT=37782 _build/node2/rel/partisan/bin/partisan console

node3:
	${REBAR} as node3 release
	ERL_DIST_PORT=37783 _build/node3/rel/partisan/bin/partisan console

node4:
	${REBAR} as node4 release
	ERL_DIST_PORT=37784 _build/node4/rel/partisan/bin/partisan console


checkssl:
	openssl s_client -connect localhost:10100 \
	-cert config/_ssl/client/cert.pem \
	-key config/_ssl/client/keycert.pem \
	-CAfile config/_ssl/client/cacerts.pem
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

bin-perms:
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
