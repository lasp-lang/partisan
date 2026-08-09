%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests for the outstanding-message (acknowledgement) store.
%%
%% `store/2' and `ack/1' are single ETS operations executed in the calling
%% process against a `public' table with write concurrency, so recording an
%% acknowledged message costs no cross-process round trip.
%%
%% The owning process must still exist and stay registered: it owns the table's
%% lifetime, and `partisan_otp_smoke_test' asserts it is alive after boot.
%% @end
%% =============================================================================
-module(partisan_acknowledgement_backend_test).

-include_lib("eunit/include/eunit.hrl").

-define(BACKEND, partisan_acknowledgement_backend).

ack_backend_test_() ->
    {timeout, 60,
        {foreach, fun setup/0, fun cleanup/1, [
            fun stores_and_lists_outstanding/0,
            fun ack_removes_the_entry/0,
            fun outstanding_is_empty_when_nothing_stored/0,
            fun writes_from_many_processes_all_land/0,
            fun owner_process_is_registered/0
        ]}}.

setup() ->
    stop_partisan(),
    {ok, _} = application:ensure_all_started(partisan),
    ok.

cleanup(_) ->
    stop_partisan().

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.

stores_and_lists_outstanding() ->
    ok = ?BACKEND:store(clock_a, msg_a),
    ok = ?BACKEND:store(clock_b, msg_b),

    {ok, Outstanding} = ?BACKEND:outstanding(),

    ?assertEqual(
        [{clock_a, msg_a}, {clock_b, msg_b}], lists:sort(Outstanding)
    ).

ack_removes_the_entry() ->
    ok = ?BACKEND:store(clock_a, msg_a),
    ok = ?BACKEND:store(clock_b, msg_b),

    ok = ?BACKEND:ack(clock_a),

    {ok, Outstanding} = ?BACKEND:outstanding(),
    ?assertEqual([{clock_b, msg_b}], Outstanding),

    %% Acking an absent clock is a no-op, not an error — a duplicate or late
    %% acknowledgement must not crash the caller.
    ?assertEqual(ok, ?BACKEND:ack(clock_a)).

outstanding_is_empty_when_nothing_stored() ->
    ?assertEqual({ok, []}, ?BACKEND:outstanding()).

%% The point of the change: writes come from arbitrary caller processes, not
%% from the owning server. This fails against a `protected' table.
writes_from_many_processes_all_land() ->
    N = 50,
    Self = self(),

    Pids = [
        spawn(fun() ->
            ok = ?BACKEND:store({clock, I}, {msg, I}),
            Self ! {stored, I}
        end)
     || I <- lists:seq(1, N)
    ],

    ok = collect(N),

    {ok, Outstanding} = ?BACKEND:outstanding(),
    ?assertEqual(N, length(Outstanding)),

    _ = [exit(P, kill) || P <- Pids, is_process_alive(P)],
    ok.

owner_process_is_registered() ->
    ?assert(is_pid(whereis(?BACKEND))).

collect(0) ->
    ok;
collect(N) ->
    receive
        {stored, _} -> collect(N - 1)
    after 5000 ->
        {error, timeout}
    end.
