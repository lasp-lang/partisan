#!/usr/bin/env bash

export USE_STARTED_NODES=false
export RESTART_NODES=false
# export EXIT_ON_COUNTEREXAMPLE=true

if [ -z ${SYSTEM_MODEL} ]; then 
    echo "SYSTEM_MODEL is unset"
    exit 1;
fi

if [ -z ${IMPLEMENTATION_MODULE} ]; then 
    echo "IMPLEMENTATION_MODULE is unset"
    exit 1;
fi

echo "Performing static analaysis..."
IMPLEMENTATION_MODULE=${IMPLEMENTATION_MODULE} bin/partisan-analysis.escript protocols/$IMPLEMENTATION_MODULE

echo "Running example suite to identify minimal successful example..."
rm -rf priv/lager; pkill -9 beam.smp; SYSTEM_MODEL=${SYSTEM_MODEL} RESTART_NODES=${RESTART_NODES} NOISE=${NOISE} IMPLEMENTATION_MODULE=${IMPLEMENTATION_MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

echo "Attempting to validate annotations..."
pkill -9 beam.smp; TRACE_FILE=/tmp/partisan-latest.trace SYSTEM_MODEL=prop_partisan_reliable_broadcast IMPLEMENTATION_MODULE=${IMPLEMENTATION_MODULE} RESTART_NODES=false ./rebar3 ct --readable=false -v --suite=filibuster_SUITE --case=annotations_test

echo "Running single run model checker..."
pkill -9 beam.smp; TRACE_FILE=/tmp/partisan-latest.trace REPLAY_TRACE_FILE=/tmp/partisan-replay.trace PRELOAD_OMISSIONS_FILE=/tmp/partisan-preload.trace RECURSIVE=true SHRINKING=true REPLAY=true SYSTEM_MODEL=prop_partisan_reliable_broadcast IMPLEMENTATION_MODULE=${IMPLEMENTATION_MODULE} RESTART_NODES=false PRELOAD_SCHEDULES=false SUBLIST=0 ./rebar3 ct --readable=false -v --suite=filibuster_SUITE --case=model_checker_test