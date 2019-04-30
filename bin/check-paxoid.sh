#!/usr/bin/env bash

export MODULE=paxoid
export SUBLIST=0
export PRELOAD_SCHEDULES=false
export RECURSIVE=true
export EXIT_ON_COUNTEREXAMPLE=true
export PRUNING=false
export SYSTEM_MODEL=prop_partisan_paxoid

# echo "Performing static analaysis [checkout] ..."
# IMPLEMENTATION_MODULE=${MODULE} bin/partisan-analysis.escript _checkouts/paxoid/src/paxoid.erl

# echo "Performing static analaysis [build] ..."
# IMPLEMENTATION_MODULE=${MODULE} bin/partisan-analysis.escript _build/default/lib/paxoid/src/paxoid.erl

# echo "Running example suite to identify minimal successful example..."
# rm -rf priv/lager; pkill -9 beam.smp; DISABLE_RANDOM=true RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

# echo "Running single run model checker..."
# pkill -9 beam.smp; DISABLE_RANDOM=true TRACE_FILE=/tmp/partisan-latest.trace REPLAY_TRACE_FILE=/tmp/partisan-replay.trace PRELOAD_OMISSIONS_FILE=/tmp/partisan-preload.trace SHRINKING=true REPLAY=true SYSTEM_MODEL=${SYSTEM_MODEL} IMPLEMENTATION_MODULE=${MODULE} RESTART_NODES=false ./rebar3 ct --readable=false -v --suite=filibuster_SUITE

echo "Running multi run fault-injector..."
rm -rf priv/lager; pkill -9 beam.smp; DISABLE_RANDOM=true RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=10 SCHEDULER=finite_fault FAULT_INJECTION=true bin/counterexample-find.sh