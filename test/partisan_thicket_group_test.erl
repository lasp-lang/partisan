%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Smoke test for the Thicket engine wired behind a broadcast group
%% (PDDR-000004 seam). A group is started with `engine => partisan_thicket_engine',
%% which the shell drives in RAW dispatch mode: it hands whole wire messages to the
%% engine and executes the engine's `deliver'/`fetch' actions against the group's
%% single handler. This guards the wiring end-to-end on one node — config selection
%% -> raw init -> local broadcast -> `deliver' action -> handler apply. The
%% multi-node protocol itself is validated by the engine's PropEr model.
%% @end
%% =============================================================================
-module(partisan_thicket_group_test).

-include_lib("eunit/include/eunit.hrl").

-define(HANDLER, partisan_bcast_test_handler).

thicket_group_test_() ->
    {timeout, 60,
        {foreach, fun setup/0, fun cleanup/1, [
            fun runs_as_a_raw_engine_group/0,
            fun local_broadcast_delivers_via_handler/0,
            fun sustained_broadcast_all_delivered/0
        ]}}.

setup() ->
    stop_partisan(),
    {ok, _} = application:ensure_all_started(partisan),
    {ok, _} = ?HANDLER:start_link(),
    {ok, _} = partisan_broadcast:start_group(#{
        mods => [?HANDLER],
        engine => partisan_thicket_engine
    }),
    ok.

cleanup(_) ->
    catch partisan_broadcast:stop_group(?HANDLER),
    catch ?HANDLER:stop(),
    stop_partisan().

%% The group is up under the handler's derived name and driven by the Thicket
%% engine (raw dispatch) — proving `engine => thicket' config selection works.
runs_as_a_raw_engine_group() ->
    Name = partisan_broadcast:group_name(?HANDLER),
    ?assert(is_pid(whereis(Name))),
    ?assert(lists:member(Name, partisan_broadcast:groups())),
    ?assertEqual(raw, partisan_thicket_engine:dispatch_mode()).

%% A locally originated broadcast is delivered to the handler: the engine's
%% `deliver' action is executed by the shell against the group's single handler.
local_broadcast_delivers_via_handler() ->
    ?HANDLER:reset(),
    ?HANDLER:set_notify(self()),
    ?HANDLER:set_delay(0),
    Id = {thicket, erlang:unique_integer()},
    ok = partisan_broadcast:broadcast(Id, ?HANDLER),
    ?assertEqual(ok, wait_applied(Id, 5000)),
    ?assert(?HANDLER:applied(Id)).

%% Sustained traffic (Thicket's design point): every originated message is
%% delivered exactly once.
sustained_broadcast_all_delivered() ->
    ?HANDLER:reset(),
    ?HANDLER:set_notify(self()),
    ?HANDLER:set_delay(0),
    Ids = [{thicket, N, erlang:unique_integer()} || N <- lists:seq(1, 8)],
    [ok = partisan_broadcast:broadcast(Id, ?HANDLER) || Id <- Ids],
    [?assertEqual({Id, ok}, {Id, wait_applied(Id, 5000)}) || Id <- Ids],
    [?assert(?HANDLER:applied(Id)) || Id <- Ids],
    ?assertEqual(length(Ids), ?HANDLER:applied_count()).

%% =============================================================================
%% HELPERS
%% =============================================================================

wait_applied(Id, Timeout) ->
    receive
        {applied, Id} -> ok
    after Timeout ->
        {error, timeout}
    end.

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.
