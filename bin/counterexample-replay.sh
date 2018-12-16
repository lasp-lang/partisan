#!/usr/bin/env sh

TRACE_FILE=/tmp/partisan-latest.trace
REPLAY_TRACE_FILE=/tmp/partisan-replay.trace
COUNTEREXAMPLE_FILE=/tmp/partisan.counterexample

if [ ! -f ${COUNTEREXAMPLE_FILE} ]; then
    echo "No counterexample!"
    exit 1
fi

if [ ! -f ${TRACE_FILE} ]; then
    echo "No trace file!"
    exit 1
fi

# Replay counterexample.
echo "Replaying counterexample..."
mv ${TRACE_FILE} ${REPLAY_TRACE_FILE}
pkill -9 beam.smp; rm -rf priv/lager; REPLAY=true REPLAY_TRACE_FILE=${REPLAY_TRACE_FILE} TRACE_FILE=${TRACE_FILE} ./rebar3 proper --retry

RETVAL=$?

if [ $RETVAL -ne 0 ]; then
    echo "Counterexample held and replayed..."
fi