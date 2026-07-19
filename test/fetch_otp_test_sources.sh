#!/usr/bin/env bash
#
# Fetch the OTP stdlib TEST sources that partisan_otp_test_gen adapts into the
# partisan OTP-compatibility suites, for one OTP version, into a cache dir that
# mirrors the historical `otp_src/otp_<vsn>/test/' layout.
#
# An installed OTP release ships module sources but not its own test suites, so
# the compatibility suites cannot read them from the installed tree the way the
# partisan modules are generated from the installed beams at compile time. The
# suites are fetched here for the OTP version in use and cached under otp_src/,
# which version control ignores. Fetching rather than committing a copy per OTP
# version keeps a new OTP release from requiring a new snapshot; the tags are
# immutable, so a cached copy reproduces exactly.
#
#   test/fetch_otp_test_sources.sh <full-otp-version> <dest-dir>
#     e.g. test/fetch_otp_test_sources.sh 29.0.3 otp_src/otp_29.0.3
#
# Idempotent: if every file is already present it does nothing (offline-safe
# once cached). Exits non-zero and removes any partial file on failure.
set -uo pipefail

VSN="${1:?usage: fetch_otp_test_sources.sh <full-otp-version> <dest-dir>}"
DEST="${2:?usage: fetch_otp_test_sources.sh <full-otp-version> <dest-dir>}"
TAG="OTP-${VSN}"
BASE="https://raw.githubusercontent.com/erlang/otp/${TAG}/lib/stdlib/test"
TESTDIR="${DEST}/test"

# The exact set partisan_otp_test_gen consumes (generate_all_suites/1,
# setup_data_dirs/2, compile_standalone_helpers/2). Keep in sync with those.
FILES=(
  # Suites
  gen_server_SUITE.erl supervisor_SUITE.erl gen_statem_SUITE.erl
  gen_event_SUITE.erl proc_lib_SUITE.erl sys_SUITE.erl
  # Standalone helpers
  dummy_h.erl dummy_via.erl dummy1_h.erl error_logger_forwarder.erl
  naughty_child.erl supervisor_1.erl supervisor_2.erl supervisor_3.erl
  supervisor_4.erl supervisor_deadlock.erl sys_sp1.erl sys_sp2.erl
  # data_dir helpers
  gen_event_SUITE_data/oc_event.erl
  gen_server_SUITE_data/format_status_server.erl
  gen_server_SUITE_data/oc_server.erl
  gen_statem_SUITE_data/format_status_statem.erl
  gen_statem_SUITE_data/oc_statem.erl
  supervisor_SUITE_data/app_faulty/ebin/app_faulty.app
  supervisor_SUITE_data/app_faulty/src/app_faulty.erl
  supervisor_SUITE_data/app_faulty/src/app_faulty_server.erl
  supervisor_SUITE_data/app_faulty/src/app_faulty_sup.erl
)

# Already fully cached? Do nothing.
have_all=1
for f in "${FILES[@]}"; do
  [ -s "${TESTDIR}/${f}" ] || { have_all=0; break; }
done
if [ "$have_all" = 1 ]; then
  echo "otp test sources for ${TAG} already cached in ${TESTDIR}"
  exit 0
fi

command -v curl >/dev/null 2>&1 || { echo "fetch_otp_test_sources: curl not found" >&2; exit 1; }

echo "fetching OTP ${VSN} test sources -> ${TESTDIR}"
fail=0
for f in "${FILES[@]}"; do
  out="${TESTDIR}/${f}"
  [ -s "$out" ] && continue
  mkdir -p "$(dirname "$out")"
  code="$(curl -fsSL -w '%{http_code}' --retry 2 --max-time 60 -o "$out" "${BASE}/${f}" || true)"
  if [ "$code" != "200" ] || [ ! -s "$out" ]; then
    echo "  FAILED (${code:-error}): ${f}" >&2
    rm -f "$out"
    fail=$((fail + 1))
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "fetch_otp_test_sources: ${fail} file(s) failed for ${TAG}" >&2
  echo "  (does the git tag ${TAG} exist? see https://github.com/erlang/otp/tags)" >&2
  echo "  For an offline/hermetic build, pre-seed ${TESTDIR} with a COMPLETE" >&2
  echo "  copy of these files; a complete cache makes this script a no-op." >&2
  exit 1
fi
echo "fetched $(printf '%s\n' "${FILES[@]}" | wc -l | tr -d ' ') files for ${TAG}"
