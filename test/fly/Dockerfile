# syntax=docker/dockerfile:1
#
# Heavy CT suite runner for Fly.io.
#
# Partisan's `partisan_SUITE` (and the OTP-compat suites) spin up MANY BEAM
# nodes on a SINGLE host — they are OS processes, not separate machines — so the
# suite doesn't need a cluster of runners, just one host with enough RAM/CPU.
# A GitHub free runner (~2 CPU / 7 GB) OOM-kills it; this image runs the whole
# suite inside one large Fly Machine instead. See test/fly/README.md.

ARG OTP_VERSION=27.3
FROM erlang:${OTP_VERSION}

# Test prerequisites: make, openssl (TLS certs used by the suites), git, ps.
# (erl + epmd ship with OTP; rebar3 is pinned below to match CI.)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential git openssl ca-certificates curl procps \
 && rm -rf /var/lib/apt/lists/*

# Pin rebar3 to the version CI uses, for reproducibility.
ARG REBAR3_VERSION=3.27.0
RUN curl -fsSL -o /usr/local/bin/rebar3 \
      "https://github.com/erlang/rebar3/releases/download/${REBAR3_VERSION}/rebar3" \
 && chmod +x /usr/local/bin/rebar3

WORKDIR /opt/partisan
COPY . .

# Warm the build: fetch deps + compile the test profile. This also runs the
# pre-compile hook that generates the partisan OTP modules (partisan_gen_*),
# so the machine run spends its time on tests, not on compiling.
RUN rebar3 as test compile

# Which make target to run:
#   test (full) | core-test | otp-compat-test | proper | alt-test | eunit
ENV TEST_TARGET=test

ENTRYPOINT ["test/fly/entrypoint.sh"]
