#!/usr/bin/env sh

NUM_TESTS=100
TRACE_FILE=/tmp/partisan-latest.trace
COUNTEREXAMPLE_FILE=/tmp/partisan.counterexample

echo "Removing existing trace files..."
rm -rf ${TRACE_FILE}
rm -rf ${COUNTEREXAMPLE_FILE}

# Generate counterexample.
echo "Generating counterexample..."
pkill -9 beam.smp; rm -rf priv/lager; TRACE_FILE=${TRACE_FILE} ./rebar3 proper -m prop_partisan -p prop_sequential --noshrink -n ${NUM_TESTS}

RETVAL=$?

if [ $RETVAL -ne 0 ]; then
    echo "Counterexample generated..."
    touch ${COUNTEREXAMPLE_FILE}
fi