#!/usr/bin/env bash
#
# Run Partisan's heavy test suite on a large ephemeral Fly.io machine.
#
#   test/fly/run.sh [TARGET] [OTP_VERSION]
#
# TARGET is a Makefile target and defaults to "ci-heavy" (core-test +
# monitor-test + alt-test + proper). Others: ci-light | core-test | alt-test |
# proper | test | eunit.
#
# OTP_VERSION picks the erlang base image tag (e.g. 27.3, 28.3, 29.0). It
# defaults to the Dockerfile's own default. The OTP test sources the compat
# suites read are fetched on demand by partisan_otp_test_gen:otp_src_dir/0 for
# whichever major is running, so no cache needs seeding per version.
#
# Uses `fly deploy` with the repo-root fly.toml. NOTE: `fly deploy` is required
# rather than `fly machine run` — the latter cannot initialise a fresh (pending)
# app's container registry, so the first run of a new app fails on image push.
# `fly deploy` builds on Fly's remote builder (repo as the Docker context),
# pushes, and creates a machine that runs `make $TARGET` then stops.
#
# Private Fly org is read from test/fly/.env (git-ignored).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

if [ -f "$SCRIPT_DIR/.env" ]; then
  set -a; . "$SCRIPT_DIR/.env"; set +a
fi

APP="${FLY_APP:-partisan-ci}"
ORG="${FLY_ORG:-personal}"
TARGET="${1:-${TEST_TARGET:-ci-heavy}}"
OTP="${2:-${OTP_VERSION:-}}"

command -v fly >/dev/null 2>&1 || { echo "fly CLI not found: https://fly.io/docs/flyctl/install/"; exit 1; }
fly auth whoami >/dev/null 2>&1 || { echo "Not logged in — run: fly auth login"; exit 1; }

# Ensure the app exists (idempotent; created under your private org from .env).
fly apps list 2>/dev/null | grep -qw "$APP" || fly apps create "$APP" --org "$ORG"

# Destroy any machine left from an earlier run before deploying.
#
# The machine runs one suite and stops, since its restart policy is "never".
# Deploying over a stopped machine updates its configuration and leaves it
# stopped, which reports success without running anything. Only creating a
# machine starts it, so any existing one is removed first.
STALE="$(fly machines list -a "$APP" -q 2>/dev/null || true)"
if [ -n "$STALE" ]; then
  echo "==> destroying previous machine(s): $(echo "$STALE" | tr '\n' ' ')"
  for m in $STALE; do
    fly machine destroy -f "$m" -a "$APP" >/dev/null 2>&1 || true
  done
fi

echo "==> fly deploy $APP  (make ${TARGET}${OTP:+ on OTP ${OTP}})"
# --remote-only builds on Fly; --env overrides the suite target from fly.toml.
# --build-arg overrides the Dockerfile's OTP_VERSION, which is declared before
# FROM so it selects the erlang base image tag.
if [ -n "$OTP" ]; then
  fly deploy --remote-only --ha=false \
    --env "TEST_TARGET=${TARGET}" --build-arg "OTP_VERSION=${OTP}"
else
  fly deploy --remote-only --ha=false --env "TEST_TARGET=${TARGET}"
fi

# `fly machines list -q' pads the id with spaces and a trailing blank line. The
# id is taken from the first non-blank line with all whitespace removed, since
# every later status query needs it verbatim.
MACHINE="$(fly machines list -a "$APP" -q 2>/dev/null \
  | awk 'NF { gsub(/[[:space:]]/, ""); print; exit }')"
[ -n "$MACHINE" ] || { echo "no machine after deploy"; exit 1; }

# Record the whole run locally. `fly logs --no-tail' replays only a short recent
# window, too little to identify a failing case, and the machine's own Common
# Test logs are destroyed with the machine. Following the stream from the start
# keeps the only durable copy.
LOGFILE="fly-${TARGET}${OTP:+-otp${OTP}}-$(date -u +%Y%m%dT%H%M%SZ).log"
echo "==> streaming logs to ${LOGFILE}  (machine ${MACHINE})"
fly logs -a "$APP" > "$LOGFILE" 2>&1 &
LOGPID=$!
cleanup() { kill "$LOGPID" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

machine_state() { fly machine status "$MACHINE" -a "$APP" 2>/dev/null | sed -n 's/^State: *//p' | head -1; }

# Wait for it to actually start (deploy returns as soon as the machine is created).
for _ in $(seq 1 60); do
  [ "$(machine_state)" = "started" ] && break
  sleep 5
done
echo "==> running (make ${TARGET}); this takes ~25-40 min for ci-heavy"
while [ "$(machine_state)" = "started" ]; do sleep 30; done

sleep 5            # let the last log lines flush
cleanup; trap - EXIT INT TERM

# The machine's exit status is the suite's exit status.
EXIT_INFO="$(fly machine status "$MACHINE" -a "$APP" 2>/dev/null | grep -m1 -o 'exit_code=[0-9]*' || true)"
RC="${EXIT_INFO#exit_code=}"; RC="${RC:-1}"

echo ""
echo "===================== SUITE RESULT (exit ${RC}) ====================="
# Select this machine's lines. `fly logs' replays recent history for the whole
# application, so an unfiltered search reports an earlier run's results as this
# run's.
grep -a "$MACHINE" "$LOGFILE" \
  | grep -aE "TEST COMPLETE|Total:|OK: Passed|properties passed|\*\*\* FAILED|failed on line|make.*Error|CT DIAGNOSIS|END DIAGNOSIS" \
  | tail -40
echo "===================================================================="
echo "Full log: ${LOGFILE}"
exit "$RC"
