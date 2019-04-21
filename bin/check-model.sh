#!/usr/bin/env bash

echo "Performing static analaysis..."
IMPLEMENTATION_MODULE=${MODULE} bin/partisan-analysis.escript protocols/$MODULE

echo "Running example suite to identify minimal successful example..."
rm -rf priv/lager; pkill -9 beam.smp; RESTART_NODES=false NOISE=${NOISE} IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

echo "Beginning reduction to find support for successful example..."
NOISE=${NOISE} PRELOAD_SCHEDULES=${PRELOAD_SCHEDULES} RECURSIVE=${RECURSIVE} SUBLIST=${SUBLIST} EXIT_ON_COUNTEREXAMPLE=${EXIT_ON_COUNTEREXAMPLE} IMPLEMENTATION_MODULE=${MODULE} bin/counterexample-identify-support.sh
