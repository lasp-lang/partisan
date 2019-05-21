#!/bin/sh

export PARTISAN=true
export FAULT_TOLERANCE=1

PARTISAN=${PARTISAN} FAULT_TOLERANCE=${FAULT_TOLERANCE} FAULT_INJECTION=true SYSTEM_MODEL=prop_partisan_hbbft FAULT_MODEL=prop_partisan_arbitrary_fault_model IMPLEMENTATION_MODULE=hbbft ./rebar3 proper --retry