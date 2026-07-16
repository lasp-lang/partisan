#!/usr/bin/env bash
# Fly.io test entrypoint: start epmd, then run the requested make target.
set -euo pipefail
cd /opt/partisan

# The CT harness and the OTP-compat suites use Erlang distribution to spawn and
# control the cluster's peer nodes, which needs epmd running.
epmd -daemon || true

OTP=$(erl -noshell -eval 'io:format("~s",[erlang:system_info(otp_release)]),halt()')
echo "==> OTP ${OTP} — running: make ${TEST_TARGET:-test}"
exec make "${TEST_TARGET:-test}"
