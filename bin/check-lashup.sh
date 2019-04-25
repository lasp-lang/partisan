#!/usr/bin/env bash

export USE_STARTED_NODES=false
export RESTART_NODES=true

# echo "Performing static analaysis..."
# IMPLEMENTATION_MODULE=${MODULE} bin/partisan-analysis.escript protocols/$MODULE

# echo "Running example suite to identify minimal successful example..."
# rm -rf priv/lager; pkill -9 beam.smp; SYSTEM_MODEL=${SYSTEM_MODEL} RESTART_NODES=${RESTART_NODES} NOISE=${NOISE} IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

# echo "Beginning reduction to find support for successful example..."
# ERL_LIBS=_build/default/lib SYSTEM_MODEL=${SYSTEM_MODEL} USE_STARTED_NODES=${USE_STARTED_NODES} RESTART_NODES=${RESTART_NODES} NOISE=${NOISE} PRELOAD_SCHEDULES=${PRELOAD_SCHEDULES} RECURSIVE=${RECURSIVE} SUBLIST=${SUBLIST} EXIT_ON_COUNTEREXAMPLE=${EXIT_ON_COUNTEREXAMPLE} IMPLEMENTATION_MODULE=${MODULE} bin/counterexample-model-checker.sh

echo "Running multi-run execution..."
rm -rf priv/lager; pkill -9 beam.smp; SYSTEM_MODEL=${SYSTEM_MODEL} RESTART_NODES=${RESTART_NODES} NOISE=${NOISE} IMPLEMENTATION_MODULE=${MODULE} FAULT_INJECTION=true SCHEDULER=finite_fault bin/counterexample-find.sh