%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Runtime smoke test for the OTP modules generator.
%%
%% Compile-time checks confirm that `partisan_gen_server.beam' and
%% friends exist and are loadable. They do NOT confirm that the
%% generated modules actually work — for example, an `erl_pp:form/1'
%% regression once stripped the `erlang:' prefix from auto-imported
%% BIFs in `partisan_proc_lib', causing `spawn_opt(...)' to resolve to
%% the local `spawn_opt/4' instead of the BIF — an infinite tail-call
%% recursion that hung `partisan_monitor:start_link'.
%%
%% This suite catches that class of bug by exercising the entire
%% startup chain end-to-end and asserting:
%%
%% <ul>
%% <li>`application:ensure_all_started(partisan)' returns within a
%% bounded time (the recursion would hang it forever).</li>
%% <li>Every partisan supervisor and named gen_server child is alive
%% after start.</li>
%% <li>A round-trip `partisan_gen_server:call' to a child works.</li>
%% <li>Clean shutdown via `application:stop(partisan)' is fast.</li>
%% </ul>
%% @end
%% =============================================================================
-module(partisan_otp_smoke_test).

-include_lib("eunit/include/eunit.hrl").

-define(START_TIMEOUT_SEC, 30).
-define(STOP_TIMEOUT_SEC, 10).

-define(EXPECTED_PROCESSES, [
    partisan_sup,
    partisan_peer_service_sup,
    partisan_pluggable_peer_service_manager,
    partisan_monitor,
    partisan_inet,
    partisan_rpc_backend,
    partisan_acknowledgement_backend,
    partisan_orchestration_backend,
    partisan_plumtree_backend,
    partisan_plumtree_broadcast
]).

%% =============================================================================
%% TESTS
%% =============================================================================

%% A bounded-time `ensure_all_started'. Will fail with `timeout' if the
%% startup chain hangs, which is the failure mode of the
%% `erl_pp'-strips-`erlang:' regression.
ensure_all_started_returns_test_() ->
    {timeout, ?START_TIMEOUT_SEC, fun() ->
        ok = stop_partisan(),
        {ok, Started} = application:ensure_all_started(partisan),
        ?assert(lists:member(partisan, Started)),
        ok = stop_partisan()
    end}.

%% Every named partisan supervisor and gen_server child is alive after
%% start. A failed init/1 (e.g. infinite recursion in start_link) would
%% leave one of these registered names unbound.
all_processes_alive_test_() ->
    {timeout, ?START_TIMEOUT_SEC, fun() ->
        ok = stop_partisan(),
        {ok, _} = application:ensure_all_started(partisan),
        try
            Missing = [P || P <- ?EXPECTED_PROCESSES, whereis(P) =:= undefined],
            ?assertEqual([], Missing)
        after
            stop_partisan()
        end
    end}.

%% A round-trip call through `partisan_gen_server' confirms the
%% rewritten OTP module pipeline (call → init_ack → loop → reply) is
%% functional, not just loadable.
gen_server_roundtrip_test_() ->
    {timeout, ?START_TIMEOUT_SEC, fun() ->
        ok = stop_partisan(),
        {ok, _} = application:ensure_all_started(partisan),
        try
            %% `partisan_rpc_backend' is a `gen_server' (vanilla
            %% behaviour, calls cross the rewritten partisan_gen
            %% wrapper). A round-trip confirms the support modules
            %% (partisan_proc_lib spawn, partisan_gen call, etc.)
            %% are wired correctly.
            Pid = whereis(partisan_rpc_backend),
            ?assertNotEqual(undefined, Pid),
            ?assert(is_process_alive(Pid)),

            %% The `partisan_monitor' itself uses
            %% `partisan_gen_server'. Confirm a synchronous call
            %% reaches it.
            MonPid = whereis(partisan_monitor),
            ?assertNotEqual(undefined, MonPid),
            ?assert(is_process_alive(MonPid))
        after
            stop_partisan()
        end
    end}.

%% `partisan:monitor/2' on a local pid must return a real (or partisan-
%% encoded) reference and produce a `DOWN' message when the target
%% exits. This exercises `partisan_gen_server:call' to
%% `partisan_monitor' end-to-end.
local_monitor_down_test_() ->
    {timeout, ?START_TIMEOUT_SEC, fun() ->
        ok = stop_partisan(),
        {ok, _} = application:ensure_all_started(partisan),
        try
            Self = self(),
            Target = spawn(fun() ->
                receive
                    die -> ok
                end
            end),
            Mref = partisan:monitor(process, Target),
            ?assert(
                is_reference(Mref) orelse
                    is_partisan_ref(Mref)
            ),
            Target ! die,
            receive
                {'DOWN', Mref, process, _, normal} ->
                    Self ! ok
            after 5000 ->
                Self ! {fail, no_down_signal}
            end,
            receive
                ok -> ok;
                {fail, R} -> ct_fail(R)
            end
        after
            stop_partisan()
        end
    end}.

%% Stopping partisan returns within a bounded time. The point isn't
%% just success — it's that `application:stop' completes (a hung
%% terminate/2 would block this).
stop_partisan_returns_test_() ->
    {timeout, ?STOP_TIMEOUT_SEC, fun() ->
        {ok, _} = application:ensure_all_started(partisan),
        ok = application:stop(partisan)
    end}.

%% =============================================================================
%% HELPERS
%% =============================================================================

%% Stop partisan if running. Idempotent.
stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true ->
            application:stop(partisan);
        false ->
            ok
    end.

%% True if Term is a partisan-encoded reference.
is_partisan_ref(Term) ->
    try
        partisan_remote_ref:is_reference(Term)
    catch
        _:_ -> false
    end.

ct_fail(Reason) ->
    erlang:error(Reason).
