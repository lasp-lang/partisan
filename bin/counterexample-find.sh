#!/usr/bin/env sh

NUM_TESTS=10
TRACE_FILE=/tmp/partisan-latest.trace
COUNTEREXAMPLE_CONSULT_FILE=/tmp/partisan-counterexample.consult
REBAR_COUNTEREXAMPLE_CONSULT_FILE=_build/test/rebar3_proper-counterexamples.consult

echo "Removing existing trace and counterexample files..."
rm -rf ${TRACE_FILE}
rm -rf ${COUNTEREXAMPLE_CONSULT_FILE}

# Generate counterexample.
echo "Generating counterexample..."
make kill; rm -rf priv/lager; TRACE_FILE=${TRACE_FILE} ./rebar3 proper -m prop_partisan -p prop_sequential --noshrink -n ${NUM_TESTS}

RETVAL=$?

if [ $RETVAL -ne 0 ]; then
    echo "Copying the rebar3 counterexample file..."
    cp ${REBAR_COUNTEREXAMPLE_CONSULT_FILE} ${COUNTEREXAMPLE_CONSULT_FILE}
fi