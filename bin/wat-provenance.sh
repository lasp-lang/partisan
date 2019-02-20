#!/usr/bin/env bash

echo "Running example suite to identify minimal successful example..."
rm -rf priv/lager; pkill -9 beam.smp; NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

echo "Beginning reduction to find support for successful example..."
bin/counterexample-identify-support.sh