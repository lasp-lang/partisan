#!/usr/bin/env bash

export USE_STARTED_NODES=false
export RESTART_NODES=false
export EXIT_ON_COUNTEREXAMPLE=false

# Verify multi-run sequential execution.

# echo "Running multi run fault-injector [without fault-injection]..."
# rm -rf priv/lager; pkill -9 beam.smp; SYSTEM_MODEL=${SYSTEM_MODEL} RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=${NUM_TESTS} SCHEDULER=finite_fault bin/counterexample-find.sh

# Identify minimal example and perform fault-injection dynamic analysis...

# echo "Performing static analaysis..."
# IMPLEMENTATION_MODULE=${MODULE} bin/partisan-analysis.escript protocols/$MODULE

# echo "Identifying minimal passing example..."
# rm -rf priv/lager; pkill -9 beam.smp; SYSTEM_MODEL=${SYSTEM_MODEL} RESTART_NODES=${RESTART_NODES} IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

# echo "Attempting to validate annotations..."
# pkill -9 beam.smp; TRACE_FILE=/tmp/partisan-latest.trace SYSTEM_MODEL=${SYSTEM_MODEL} IMPLEMENTATION_MODULE=${MODULE} RESTART_NODES=false ./rebar3 ct --readable=false -v --suite=filibuster_SUITE --case=annotations_test

# echo "Performing minimal example dynamic analysis..."
# pkill -9 beam.smp; TRACE_FILE=/tmp/partisan-latest.trace REPLAY_TRACE_FILE=/tmp/partisan-replay.trace PRELOAD_OMISSIONS_FILE=/tmp/partisan-preload.trace SHRINKING=true REPLAY=true SYSTEM_MODEL=${SYSTEM_MODEL} IMPLEMENTATION_MODULE=${MODULE} RESTART_NODES=false ./rebar3 ct --readable=false -v --suite=filibuster_SUITE --case=model_checker_test

# Multi-run fault-injection.

# echo "Running multi run fault-injector [without fault-injection]..."
# rm -rf priv/lager; pkill -9 beam.smp; SYSTEM_MODEL=${SYSTEM_MODEL} RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=${NUM_TESTS} SCHEDULER=finite_fault bin/counterexample-find.sh

# echo "Running multi run fault-injector [with fault-injection f = 1]..."
# rm -rf priv/lager; pkill -9 beam.smp; FAULT_INJECTION=true FAULT_TOLERANCE=1 SYSTEM_MODEL=${SYSTEM_MODEL} RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=${NUM_TESTS} SCHEDULER=finite_fault bin/counterexample-find.sh