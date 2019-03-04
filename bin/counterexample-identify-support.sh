#!/usr/bin/env sh

TRACE_FILE=/tmp/partisan-latest.trace
REPLAY_TRACE_FILE=/tmp/partisan-replay.trace
SAVED_TRACE_FILE=/tmp/partisan-saved.trace
COUNTEREXAMPLE_CONSULT_FILE=/tmp/partisan-counterexample.consult
REBAR_COUNTEREXAMPLE_CONSULT_FILE=_build/test/rebar3_proper-counterexamples.consult
PRELOAD_OMISSION_FILE=/tmp/partisan-preload.trace

if [ ! -f ${COUNTEREXAMPLE_CONSULT_FILE} ]; then
    echo "No counterexample consult file!"
    exit 1
fi

if [ ! -f ${TRACE_FILE} ]; then
    echo "No trace file!"
    exit 1
fi

# Save the original trace file.
cp ${TRACE_FILE} ${SAVED_TRACE_FILE}

# Shrink counterexample, which should copy files in place.
echo "Deriving schedules to test for counterexample support..."
bin/counterexample-identify-support.escript ${TRACE_FILE} ${REPLAY_TRACE_FILE} ${COUNTEREXAMPLE_CONSULT_FILE} ${REBAR_COUNTEREXAMPLE_CONSULT_FILE} ${PRELOAD_OMISSION_FILE}