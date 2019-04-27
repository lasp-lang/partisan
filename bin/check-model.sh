#!/usr/bin/env bash

export USE_STARTED_NODES=false
export RESTART_NODES=true

echo "Performing static analaysis..."
IMPLEMENTATION_MODULE=${MODULE} bin/partisan-analysis.escript protocols/$MODULE

echo "Running example suite to identify minimal successful example..."
rm -rf priv/lager; pkill -9 beam.smp; SYSTEM_MODEL=${SYSTEM_MODEL} RESTART_NODES=${RESTART_NODES} NOISE=${NOISE} IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

echo "Checking for counterexamples..."
pkill -9 beam.smp; TRACE_FILE=/tmp/partisan-latest.trace REPLAY_TRACE_FILE=/tmp/partisan-replay.trace PRELOAD_OMISSIONS_FILE=/tmp/partisan-preload.trace SHRINKING=true REPLAY=true SYSTEM_MODEL=prop_partisan_reliable_broadcast IMPLEMENTATION_MODULE=${MODULE} RESTART_NODES=false ./rebar3 ct --readable=false -v --suite=filibuster_SUITE