#!/usr/bin/env bash
#
# Run Partisan's heavy test suite on a large ephemeral Fly.io machine.
#
#   test/fly/run.sh [TARGET]
#
# TARGET is a Makefile target and defaults to "ci-heavy" (core-test + alt-test +
# proper). Others: core-test | alt-test | proper | test | eunit.
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

command -v fly >/dev/null 2>&1 || { echo "fly CLI not found: https://fly.io/docs/flyctl/install/"; exit 1; }
fly auth whoami >/dev/null 2>&1 || { echo "Not logged in — run: fly auth login"; exit 1; }

# Ensure the app exists (idempotent; created under your private org from .env).
fly apps list 2>/dev/null | grep -qw "$APP" || fly apps create "$APP" --org "$ORG"

echo "==> fly deploy $APP  (make ${TARGET})"
# --remote-only builds on Fly; -e overrides the suite target from fly.toml.
# Watch the run with:  fly logs -a $APP
exec fly deploy --remote-only --ha=false --env "TEST_TARGET=${TARGET}"
