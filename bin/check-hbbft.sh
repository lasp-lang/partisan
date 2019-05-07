#!/usr/bin/env bash

export MODULE=hbbft
export SUBLIST=0
export PRELOAD_SCHEDULES=false
export RECURSIVE=true
export EXIT_ON_COUNTEREXAMPLE=true
export PRUNING=false
export SYSTEM_MODEL=prop_partisan_hbbft

echo "Running example suite to identify minimal successful example..."
rm -rf priv/lager; pkill -9 beam.smp; RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

# echo "Running multi run fault-injector [without fault-injection]..."
# rm -rf priv/lager; pkill -9 beam.smp; RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=10 SCHEDULER=finite_fault bin/counterexample-find.sh

# echo "Running multi run fault-injector [with fault-injection]..."
# rm -rf priv/lager; pkill -9 beam.smp; RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=10 SCHEDULER=finite_fault FAULT_INJECTION=true bin/counterexample-find.sh