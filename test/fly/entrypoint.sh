#!/usr/bin/env bash
# Fly.io test entrypoint: start epmd, then run the requested make target.
set -uo pipefail
cd /opt/partisan

# The CT harness and the OTP-compat suites use Erlang distribution to spawn and
# control the cluster's peer nodes, which needs epmd running.
epmd -daemon || true

OTP=$(erl -noshell -eval 'io:format("~s",[erlang:system_info(otp_release)]),halt()')
echo "==> OTP ${OTP} — running: make ${TEST_TARGET:-test}"

# Run without `exec', and with `set -e' off, so that this script outlives a
# failing suite and reaches the diagnosis below. The machine is ephemeral and
# its Common Test logs are destroyed with it, so whatever is not written to
# standard output here cannot be recovered afterwards.
make "${TEST_TARGET:-test}"
RC=$?

if [ "$RC" -ne 0 ]; then
  echo ""
  echo "########## SUITE FAILED (exit ${RC}) — CT DIAGNOSIS ##########"

  echo "----- per-suite totals -----"
  grep -rah "TEST COMPLETE" _build/test/logs 2>/dev/null | sort -u | tail -20

  echo "----- failed cases and reasons -----"
  grep -rah -E "failed on line|^=result +failed|\*\*\* FAILED test case" \
    _build/test/logs 2>/dev/null | sort -u | tail -40

  echo "----- tail of most recent suite.log -----"
  LATEST=$(ls -t _build/test/logs/*/*/*/suite.log 2>/dev/null | head -1)
  [ -n "$LATEST" ] && tail -80 "$LATEST"

  echo "########## END DIAGNOSIS ##########"
fi

exit "$RC"
