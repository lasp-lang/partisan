%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(partisan_otp_modules).

-moduledoc """
The OTP-to-Partisan module correspondence: the single source of truth for which
OTP modules Partisan substitutes, and how.

Every consumer derives its view from `substitutions/0` rather than restating the
table:

- `partisan_otp_rewrite` renames atoms, behaviour attributes and remote calls
  (`atom_renames/0`, `call_renames/0`).
- `partisan_gen_transform` generates one beam per OTP module (`otp_modules/0`,
  `partisan_module/1`).
- `partisan_app` and `partisan_otp_test_gen` load the generated set
  (`partisan_modules/0`).
- `priv/generate_otp_sources.escript` writes the generated set into the
  `modules` key of `partisan.app.src`.

## Origins

A row's origin says how its Partisan module comes to exist, and that is exactly
what decides which syntactic positions the rewrite renames:

- `generated` — `partisan_gen_transform` derives the module mechanically from
  the OTP module's abstract code. Every such name is also a *behaviour* name, so
  it occurs as a bare atom in data (child specs, `{via, Mod, Name}` tuples,
  logger report labels) as well as in calls and `-behaviour` attributes. All
  positions are renamed.
- `handwritten` — Partisan ships its own module with a compatible API. Only
  remote calls are redirected. The atom keeps its OTP meaning in data positions,
  where renaming it would corrupt ordinary terms.

The two rename maps are therefore no longer independent tables that must be kept
consistent by hand: `call_renames/0` is by construction a superset of
`atom_renames/0`.
""".

-export([substitutions/0]).
-export([partisan_module/1]).
-export([otp_modules/0]).
-export([partisan_modules/0]).
-export([atom_renames/0]).
-export([call_renames/0]).

-doc "How a Partisan module comes to exist; see the module documentation.".
-type origin() :: generated | handwritten.

-doc "One OTP-to-Partisan substitution.".
-type substitution() :: {OtpModule :: module(), Partisan :: module(), origin()}.

-export_type([origin/0, substitution/0]).

%% =============================================================================
%% API
%% =============================================================================

-doc """
Returns the substitution table, in generation order.

Order is significant: `partisan_gen_transform` generates the `generated` rows top
to bottom, and `gen` and `proc_lib` must exist before `gen_server` is generated.
""".
-spec substitutions() -> [substitution()].

substitutions() ->
    [
        {gen, partisan_gen, generated},
        {proc_lib, partisan_proc_lib, generated},
        {sys, partisan_sys, generated},
        {gen_server, partisan_gen_server, generated},
        {gen_event, partisan_gen_event, generated},
        {gen_statem, partisan_gen_statem, generated},
        {supervisor, partisan_gen_supervisor, generated},
        %% NOTE on `erpc': deliberately absent. This table drives the rewrite of
        %% OTP-derived code only — the generated `partisan_gen_*' behaviours and
        %% the generated OTP test suites. It is not applied to user code: the
        %% user-facing parse transform is `partisan_transform', which rewrites
        %% `!' into `partisan:forward_message/2' and renames no module calls at
        %% all. So an `erpc' row would not help user code, and it would break the
        %% generated suites: `gen_server_SUITE' uses `erpc:call/4' as scaffolding
        %% to drive peer nodes (see `multicall_remote_test'), and those peers
        %% never start the partisan application, so a rewritten call would have
        %% no `partisan_rpc_backend' to reach. Code wanting Partisan-transported
        %% erpc calls `partisan_erpc' directly, exactly as it calls
        %% `partisan_rpc' directly.
        {rpc, partisan_rpc, handwritten}
    ].

-doc """
Returns the Partisan module that substitutes for `OtpModule'.

Errors with `{badkey, OtpModule}' when the module has no substitution.
""".
-spec partisan_module(module()) -> module().

partisan_module(OtpModule) ->
    maps:get(OtpModule, call_renames()).

-doc """
Returns the OTP modules `partisan_gen_transform' generates from, in generation
order.
""".
-spec otp_modules() -> [module()].

otp_modules() ->
    [OtpModule || {OtpModule, _, generated} <- substitutions()].

-doc """
Returns the generated Partisan modules, in generation order.

This is the set `partisan_app' requires to be loadable at application start.
""".
-spec partisan_modules() -> [module()].

partisan_modules() ->
    [Partisan || {_, Partisan, generated} <- substitutions()].

-doc """
Returns the renames applied to atoms in data positions, to `-behaviour'
attributes and to the `-module' attribute.

Restricted to `generated' rows, whose names double as behaviour names.
""".
-spec atom_renames() -> #{module() => module()}.

atom_renames() ->
    maps:from_list([
        {OtpModule, Partisan}
     || {OtpModule, Partisan, generated} <- substitutions()
    ]).

-doc """
Returns the renames applied to remote calls and remote fun references.

Covers every row: a handwritten substitute is reached by call even though its
name must not be renamed as data.
""".
-spec call_renames() -> #{module() => module()}.

call_renames() ->
    maps:from_list([
        {OtpModule, Partisan}
     || {OtpModule, Partisan, _Origin} <- substitutions()
    ]).
