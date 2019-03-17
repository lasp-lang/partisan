#!/usr/bin/env bash

echo "Running example suite to identify minimal successful example..."
rm -rf priv/lager; pkill -9 beam.smp; IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

echo "Performing static analaysis..."
IMPLEMENTATION_MODULE=${MODULE} bin/partisan-analysis.escript protocols/$MODULE

echo ""
read -p "Please ensure the annotations file has been updated prior to running the test suite."

echo "Beginning reduction to find support for successful example..."
RECURSIVE=${RECURSIVE} SUBLIST=${SUBLIST} EXIT_ON_COUNTEREXAMPLE=${EXIT_ON_COUNTEREXAMPLE} IMPLEMENTATION_MODULE=${MODULE} bin/counterexample-identify-support.sh
