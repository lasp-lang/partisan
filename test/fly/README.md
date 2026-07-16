# Running the heavy test suite on Fly.io

`partisan_SUITE` (and the OTP-compat / PropEr suites) spin up **many BEAM nodes
on a single host** — they are OS processes, not separate machines. So the suite
doesn't need a cluster of runners, just one host with enough RAM/CPU. A GitHub
free runner (~2 CPU / 7 GB) OOM-kills it; this runs the whole suite inside one
large **ephemeral Fly.io Machine** and destroys it afterwards.

## One-time setup

1. Install and log in to flyctl: <https://fly.io/docs/flyctl/install/>, then
   `fly auth login`.
2. Create your private config from the template (this file is **git-ignored**):
   ```sh
   cp test/fly/.env.example test/fly/.env
   # edit test/fly/.env — set FLY_ORG (see: fly orgs list) and, if you like,
   # FLY_APP / FLY_REGION / size. Your account/org lives ONLY in this file.
   ```

## Run

From the repo root:

```sh
./test/fly/run.sh              # heavy suites  (make ci-heavy = core-test + alt-test + proper)
./test/fly/run.sh core-test    # just partisan_SUITE
./test/fly/run.sh proper       # just the PropEr fault-injection suites
./test/fly/run.sh test         # everything (light + heavy) on the big machine
```

`run.sh` sources `test/fly/.env`, ensures the app exists (under your private
org), then runs `fly deploy` — which builds the image on Fly's **remote builder**
(no local Docker needed; the repository is the build context via the root
`fly.toml`), pushes it, and creates one big Machine that runs `make $TARGET` and
stops. Watch it with `fly logs -a partisan-ci`; destroy the finished Machine with
`fly machine destroy <id> -a partisan-ci` (or `fly apps destroy partisan-ci`).

> **Why `fly deploy`, not `fly machine run`?** `fly machine run` cannot
> initialise a fresh (pending) app's container registry, so the first run of a
> new app fails on the image push. `fly deploy` initialises the registry and
> builds with the repository as the Docker context.

## Sizing & cost

Defaults (override in `.env`): `performance` / 8 CPU / 16 GB. Runs are ephemeral
— a ~40-minute `ci-heavy` run costs cents. The Machine stops when the suite
exits; destroy it afterwards (see Run). Size is set in the root `fly.toml`
(`performance-8x` / 16 GB by default; Fly performance Machines go up to
16 CPU / 128 GB).

## OTP version

The image defaults to OTP 27.3 (matches `otp_src/otp_27.3.4`, which
`otp-compat-test` reads). For OTP 28:

Set the build arg in the root `fly.toml`:

```toml
[build]
  dockerfile = "test/fly/Dockerfile"
  [build.args]
    OTP_VERSION = "28.3"
```

Keep the container's OTP major in sync with a directory under `otp_src/`.

## CI (GitHub Actions)

`.github/workflows/fly-test.yml` runs this on demand (and can be scheduled). It
needs two repo secrets — `FLY_API_TOKEN` (a deploy token:
`fly tokens create deploy -a partisan-ci`) and `FLY_ORG`. The light suites
(`compile`, `eunit`, `otp-compat-test`) stay on normal GitHub runners in
`build_and_test.yml`; only the resource-heavy cluster runs are offloaded to Fly.

## Troubleshooting

- **`image push: app repository not found`.** That's `fly machine run` on a
  fresh app — use `fly deploy` (what `run.sh` does); it initialises the registry.
- **Dockerfile "not found" / wrong build context on deploy.** Keep `fly.toml` at
  the repository root so `fly deploy` uses the repo as the Docker build context
  (the Dockerfile does `COPY . .`).
