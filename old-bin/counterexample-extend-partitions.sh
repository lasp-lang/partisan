#!/usr/bin/env sh

TRACE_FILE=/tmp/partisan-latest.trace
REPLAY_TRACE_FILE=/tmp/partisan-replay.trace
COUNTEREXAMPLE_CONSULT_FILE=/tmp/partisan-counterexample.consult
REBAR_COUNTEREXAMPLE_CONSULT_FILE=_build/test/rebar3_proper-counterexamples.consult

if [ ! -f ${COUNTEREXAMPLE_CONSULT_FILE} ]; then
    echo "No counterexample consult file!"
    exit 1
fi

if [ ! -f ${TRACE_FILE} ]; then
    echo "No trace file!"
    exit 1
fi

# Shrink counterexample, which should copy files in place.
echo "Staging shrunk counterexample..."
bin/counterexample-extend-partitions.escript ${TRACE_FILE} ${REPLAY_TRACE_FILE} ${COUNTEREXAMPLE_CONSULT_FILE} ${REBAR_COUNTEREXAMPLE_CONSULT_FILE}

# Replay counterexample.
echo "Replaying counterexample..."
make kill; pkill -9 beam.smp; rm -rf priv/lager; SHRINKING=true REPLAY=true REPLAY_TRACE_FILE=${REPLAY_TRACE_FILE} TRACE_FILE=${TRACE_FILE} ./rebar3 proper --retry

RETVAL=$?

if [ $RETVAL -ne 0 ]; then
    echo "Copying the shrunk rebar3 counterexample file..."
    cp ${REBAR_COUNTEREXAMPLE_CONSULT_FILE} ${COUNTEREXAMPLE_CONSULT_FILE}
else
    echo "Minimal counterexample found..."
    cp ${REPLAY_TRACE_FILE} ${TRACE_FILE}
fi