#!/usr/bin/env bash

export MODULE=paxoid
export SUBLIST=0
export PRELOAD_SCHEDULES=false
export RECURSIVE=true
export EXIT_ON_COUNTEREXAMPLE=true
export PRUNING=false
export SYSTEM_MODEL=prop_partisan_paxoid

echo "Running example suite to identify minimal successful example..."
rm -rf priv/lager; pkill -9 beam.smp; DISABLE_RANDOM=true RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

echo "Attemping single run execution..."
pkill -9 beam.smp; TRACE_FILE=/tmp/partisan-latest.trace REPLAY_TRACE_FILE=/tmp/partisan-replay.trace PRELOAD_OMISSIONS_FILE=/tmp/partisan-preload.trace SHRINKING=true REPLAY=true SYSTEM_MODEL=${SYSTEM_MODEL} IMPLEMENTATION_MODULE=${MODULE} RESTART_NODES=false ./rebar3 ct --readable=false -v --suite=filibuster_SUITE

# echo "Attempting single replay."
# IMPLEMENTATION_MODULE=${MODULE} bin/counterexample-replay.sh

# echo "Beginning reduction to find support for successful example..."
# PRUNING=${PRUNING} PRELOAD_SCHEDULES=${PRELOAD_SCHEDULES} RECURSIVE=${RECURSIVE} SUBLIST=${SUBLIST} EXIT_ON_COUNTEREXAMPLE=${EXIT_ON_COUNTEREXAMPLE} IMPLEMENTATION_MODULE=${MODULE} bin/counterexample-model-checker.sh