#!/usr/bin/env bash

export MODULE=paxoid
export SUBLIST=0
export PRELOAD_SCHEDULES=false
export RECURSIVE=true
export EXIT_ON_COUNTEREXAMPLE=true

echo "Running example suite to identify minimal successful example..."
rm -rf priv/lager; pkill -9 beam.smp; IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

# echo "Attempting single replay."
# IMPLEMENTATION_MODULE=${MODULE} bin/counterexample-replay.sh

echo "Beginning reduction to find support for successful example..."
PRELOAD_SCHEDULES=${PRELOAD_SCHEDULES} RECURSIVE=${RECURSIVE} SUBLIST=${SUBLIST} EXIT_ON_COUNTEREXAMPLE=${EXIT_ON_COUNTEREXAMPLE} IMPLEMENTATION_MODULE=${MODULE} bin/counterexample-identify-support.sh