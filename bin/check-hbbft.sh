#!/usr/bin/env bash

export MODULE=hbbft
export SUBLIST=0
export PRELOAD_SCHEDULES=false
export RECURSIVE=true
export EXIT_ON_COUNTEREXAMPLE=true
export PRUNING=false
export SYSTEM_MODEL=prop_partisan_hbbft
export PARTISAN=true
export NUM_TESTS=10
# export FAULT_MODEL=prop_partisan_arbitrary_fault_model
export FAULT_MODEL=prop_partisan_crash_fault_model

# echo "Running example suite to identify minimal successful example [with bootstrap]..."
# rm -rf priv/lager; pkill -9 beam.smp; BOOTSTRAP=true RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

# echo "Running example suite to identify minimal successful example [without bootstrap]..."
# rm -rf priv/lager; pkill -9 beam.smp; RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=3 SCHEDULER=single_success bin/counterexample-find.sh

echo "Running multi run fault-injector [without fault-injection]..."
rm -rf priv/lager; pkill -9 beam.smp; RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=${NUM_TESTS} SCHEDULER=finite_fault bin/counterexample-find.sh

# echo "Running multi run fault-injector [with fault-injection, F = 1]..."
# rm -rf priv/lager; pkill -9 beam.smp; RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=${NUM_TESTS} SCHEDULER=finite_fault FAULT_INJECTION=true FAULT_TOLERANCE=1 bin/counterexample-find.sh

# echo "Running multi run fault-injector [with fault-injection, F = 2]..."
# rm -rf priv/lager; pkill -9 beam.smp; RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=${NUM_TESTS} SCHEDULER=finite_fault FAULT_INJECTION=true FAULT_TOLERANCE=2 bin/counterexample-find.sh