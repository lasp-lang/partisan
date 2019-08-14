#!/usr/bin/env bash

echo "Running example suite to identify minimal successful example..."
rm -rf priv/lager; pkill -9 beam.smp; IMPLEMENTATION_MODULE=riak_ensemble NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

echo "Beginning reduction to find support for successful example..."
PRELOAD_SCHEDULES=${PRELOAD_SCHEDULES} RECURSIVE=${RECURSIVE} SUBLIST=${SUBLIST} EXIT_ON_COUNTEREXAMPLE=${EXIT_ON_COUNTEREXAMPLE} IMPLEMENTATION_MODULE=riak_ensemble bin/counterexample-model-checker.sh