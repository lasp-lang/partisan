#!/usr/bin/env bash

export MODULE=zraft
export SUBLIST=0
export PRELOAD_SCHEDULES=false
export RECURSIVE=true
export EXIT_ON_COUNTEREXAMPLE=true
export PRUNING=false
export SYSTEM_MODEL=prop_partisan_zraft

echo "Running multi run fault-injector..."
rm -rf priv/lager; pkill -9 beam.smp; DISABLE_RANDOM=true RESTART_NODES=false IMPLEMENTATION_MODULE=${MODULE} NUM_TESTS=10 SCHEDULER=finite_fault FAULT_INJECTION=true bin/counterexample-find.sh