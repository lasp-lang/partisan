# Installation

This tutorial walks through adding Partisan as a dependency to a project and
explains the build-time machinery that produces the Partisan-flavoured OTP
modules (`partisan_gen_server`, `partisan_gen_statem`, …) on every compile.

## Requirements

- **OTP 27 or newer.** The build aborts on older releases (see
  `rebar.config.script`).
- **rebar3** (for Erlang projects) or **Mix** (for Elixir projects).
- A working C toolchain, only because Partisan transitively pulls
  `eredis` / `quickrand` which compile native code.

## Adding Partisan as a dependency

### Erlang — rebar3

From [hex.pm](https://hex.pm/packages/partisan):

```erlang
%% rebar.config
{deps, [
    {partisan, "6.0.0"}
]}.
```

From a git tag (useful for tracking unreleased fixes):

```erlang
%% rebar.config
{deps, [
    {partisan,
        {git, "https://github.com/lasp-lang/partisan.git", {tag, "6.0.0"}}
    }
]}.
```

To track `master`:

```erlang
{deps, [
    {partisan,
        {git, "https://github.com/lasp-lang/partisan.git", {branch, "master"}}
    }
]}.
```

Then build:

```bash
rebar3 compile
```

### Elixir — Mix

From hex.pm:

```elixir
# mix.exs
defp deps do
  [
    {:partisan, "~> 6.0"}
  ]
end
```

From a git tag:

```elixir
defp deps do
  [
    {:partisan,
      git: "https://github.com/lasp-lang/partisan.git",
      tag: "6.0.0"}
  ]
end
```

Then build:

```bash
mix deps.get
mix compile
```

Mix's Erlang compiler honours rebar3 hooks for rebar projects, so the OTP
modules generator (described below) runs the same way it would under
`rebar3 compile`.

## How the Partisan OTP modules generator is triggered

When you compile a project that depends on Partisan you will see one extra
line on top of the usual rebar3 output:

```
Generated 7 partisan OTP module sources into src/
===> Compiling partisan
```

That first line is the **OTP modules generator** running. It is wired up
automatically — consumers do not need to add anything to their `rebar.config`
or `mix.exs`.

### What it produces

The generator takes the source of OTP's `gen`, `proc_lib`, `sys`,
`gen_server`, `gen_event`, `gen_statem`, and `supervisor` modules and
writes Partisan-flavoured `.erl` source files into Partisan's own `src/`
directory:

| OTP module    | Generated source / module      |
|---------------|--------------------------------|
| `gen`         | `partisan_gen`                 |
| `proc_lib`    | `partisan_proc_lib`            |
| `sys`         | `partisan_sys`                 |
| `gen_server`  | `partisan_gen_server`          |
| `gen_event`   | `partisan_gen_event`           |
| `gen_statem`  | `partisan_gen_statem`          |
| `supervisor`  | `partisan_gen_supervisor`      |

In each generated module every reference to disterl —
`erlang:monitor/2,3`, `erlang:demonitor/1,2`, `Pid ! Msg` to a remote
target, `gen_server:call({Name, Node}, …)`, calls to `rpc`, etc. — is
rewritten to go through Partisan's transport. rebar3 then compiles those
`.erl` files alongside Partisan's other source, so the resulting `.beam`
files land in Partisan's own `ebin/`
(`_build/<profile>/lib/partisan/ebin/`) and are picked up automatically
by the auto-discovered modules list in `partisan.app`.

### How it gets triggered

The trigger is a rebar3 `pre_hooks` registration that lives directly in
Partisan's `rebar.config`:

```erlang
%% rebar.config
{pre_hooks, [
    {compile, "escript priv/generate_otp_sources.escript"}
]}.
```

It is a **pre-compile** (not post-compile) hook on purpose. When
Partisan and another Partisan-using dep (e.g. plum_db) are both direct
deps of the same top-level project, rebar3 may start the second
dep's compile before Partisan's post-compile hook can fire — and the
second dep then fails with `behaviour partisan_gen_supervisor
undefined`. Generating the sources in a pre-compile hook avoids that
race entirely: the `partisan_gen_*.beam` files are produced as part of
Partisan's own normal compile, so they always exist before any
downstream dep starts compiling.

The escript at `priv/generate_otp_sources.escript`:

1. Compiles the three support modules (`partisan_otp_rewrite`,
   `partisan_otp_patches`, `partisan_gen_transform`) in-memory directly
   from `src/*.erl`.
2. For each target OTP module, finds its installed beam via
   `code:which/1` and extracts the abstract syntax tree with
   `beam_lib:chunks/2`.
3. Applies the AST rewrite (module renames, BIF rewrites, behaviour
   attribute rewrites, atom-in-data rewrites — see
   `partisan_otp_rewrite:rename_map/0`) plus any version-specific patches
   under `priv/otp/<otp-version>/`.
4. Pretty-prints the rewritten forms with `erl_pp:form/1` and writes
   them as `partisan_gen_server.erl`, `partisan_gen_supervisor.erl`,
   etc. into Partisan's `src/` directory.
5. Returns. rebar3 then compiles those `.erl` files alongside the rest
   of Partisan's source.

The generated `.erl` files are listed in `.gitignore` so they never
end up in version control. The generator is **stateless and
deterministic** — the inputs are the OTP source on the build host and
the rename maps in `partisan_otp_rewrite`; the output is fully
determined by them.

### When the generator becomes visible

In day-to-day use, never. The cases where it does:

- **You upgraded OTP.** Re-running `rebar3 compile` regenerates the modules
  against the new OTP source. If the new OTP release added a BIF or atom
  that needs rewriting (e.g. an auto-imported `monitor/2` in OTP 26+),
  add it to `partisan_otp_rewrite` first.
- **You see “module gen\_server is not loaded” at runtime in a release.**
  Confirm `_build/<profile>/lib/partisan/ebin/partisan.app` lists the
  generated modules. A partial build can leave the app file stale; a
  clean `rebar3 clean -a && rebar3 compile` fixes it.
- **You want to add a new OTP behaviour to Partisan’s set.** Add the
  module to `partisan_gen_transform:modules/0` and the rename maps in
  `partisan_otp_rewrite`; the build does the rest.

## Minimal configuration

Once Partisan is installed, point your `sys.config` at the right interface
and ports:

```erlang
[
    {partisan, [
        {peer_ip, {127,0,0,1}},
        {peer_port, 10200},
        {channels, #{
            data       => #{parallelism => 1},
            membership => #{parallelism => 1}
        }}
    ]}
].
```

See `partisan_config` for the full set of options.

## Verifying the install

After `rebar3 compile`, drop into a shell:

```erlang
1> application:ensure_all_started(partisan).
{ok, [...,partisan]}

2> partisan:node().
'nonode@nohost'

3> partisan_gen_server:module_info(module).
partisan_gen_server
```

If step (3) returns `partisan_gen_server` the OTP modules generator ran and
the generated module loaded successfully — you are ready to write
`partisan_gen_server` callbacks the same way you would write `gen_server`
ones.
