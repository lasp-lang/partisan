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
    {partisan, "5.0.3"}
]}.
```

From a git tag (useful for tracking unreleased fixes):

```erlang
%% rebar.config
{deps, [
    {partisan,
        {git, "https://github.com/lasp-lang/partisan.git", {tag, "5.0.3"}}
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
    {:partisan, "~> 5.0"}
  ]
end
```

From a git tag:

```elixir
defp deps do
  [
    {:partisan,
      git: "https://github.com/lasp-lang/partisan.git",
      tag: "5.0.3"}
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

When you compile a project that depends on Partisan you will see two extra
lines on top of the usual rebar3 output:

```
===> Compiling partisan
Generating partisan OTP modules into _build/default/lib/partisan/ebin
Generated 7 partisan OTP modules: [gen,proc_lib,sys,gen_server,gen_event,
                                   gen_statem,supervisor]
```

That last line is the **OTP modules generator** running. It is wired up
automatically — consumers do not need to add anything to their `rebar.config`
or `mix.exs`.

### What it produces

The generator takes the source of OTP's `gen`, `proc_lib`, `sys`,
`gen_server`, `gen_event`, `gen_statem`, and `supervisor` modules and emits
Partisan-flavoured copies:

| OTP module    | Generated module               |
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
rewritten to go through Partisan's transport. The resulting beam files land
in Partisan's own `ebin/` (i.e. `_build/<profile>/lib/partisan/ebin/`) and
the generated module names are added to `partisan.app` so they are loadable
in releases.

### How it gets triggered

The trigger is a rebar3 `post_hooks` registration that lives directly in
Partisan's `rebar.config`:

```erlang
%% rebar.config
{post_hooks, [
    {compile, "escript priv/generate_otp_modules.escript"}
]}.
```

The hook lives in `rebar.config` (not `rebar.config.script`) on purpose:
some rebar3 versions do not evaluate a dependency's `rebar.config.script`
the first time the dep is fetched, which silently left consumer projects
without the generated modules. Putting the hook in `rebar.config` makes it
unambiguous — rebar3 honours it on every `rebar3 compile`, both inside the
Partisan repository and inside any project that depends on Partisan.

The escript at `priv/generate_otp_modules.escript` then:

1. Loads the just-compiled support modules (`partisan_gen_transform`,
   `partisan_otp_rewrite`, `partisan_otp_patches`) from Partisan's own
   `ebin/`.
2. For each target OTP module, finds its installed beam via `code:which/1`
   and extracts the abstract syntax tree with
   `beam_lib:chunks/2`.
3. Applies the AST rewrite (module renames, BIF rewrites, behaviour
   attribute rewrites, atom-in-data rewrites — see
   `partisan_otp_rewrite:rename_map/0`) plus any version-specific patches
   under `priv/otp/<otp-version>/`.
4. Compiles the rewritten forms with `compile:forms/2` and writes the
   resulting beam.
5. Updates `partisan.app` so the generated module names appear in the
   `modules` key. This step matters in release mode (embedded), where only
   modules listed in `.app` are loadable.

The generator is **stateless and deterministic** — the inputs are the
OTP source on the build host and the rename maps in
`partisan_otp_rewrite`; the output is fully determined by them.

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
