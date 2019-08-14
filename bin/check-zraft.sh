#!/usr/bin/env bash

export MODULE=zraft
export SUBLIST=0
export PRELOAD_SCHEDULES=false
export RECURSIVE=true
# export EXIT_ON_COUNTEREXAMPLE=true
export PRUNING=false
export SYSTEM_MODEL=prop_partisan_zraft
export RESTART_NODES=true 

# echo "Running example suite to identify minimal successful example..."
# rm -rf priv/lager; pkill -9 beam.smp; DISABLE_RANDOM=true RESTART_NODES=${RESTART_NODES} IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

# echo "Replaying existing example..."
# rm -rf priv/lager; pkill -9 beam.smp; REPLAY=true DISABLE_RANDOM=true RESTART_NODES=${RESTART_NODES} IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-replay.sh

# echo "Running single run model checker..."
# rm -rf priv/lager; pkill -9 beam.smp; TRACE_FILE=/tmp/partisan-latest.trace REPLAY_TRACE_FILE=/tmp/partisan-replay.trace PRELOAD_OMISSIONS_FILE=/tmp/partisan-preload.trace SHRINKING=true REPLAY=true SYSTEM_MODEL=${SYSTEM_MODEL} IMPLEMENTATION_MODULE=${MODULE} RESTART_NODES=${RESTART_NODES} ./rebar3 ct --readable=false -v --suite=filibuster_SUITE --case=model_checker_test

echo "Running multi run fault-injector..."
rm -rf priv/lager; pkill -9 beam.smp; DISABLE_RANDOM=true RESTART_NODES=${RESTART_NODES} IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=10 SCHEDULER=finite_fault FAULT_INJECTION=true bin/counterexample-find.sh