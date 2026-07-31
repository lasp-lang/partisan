%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Guards the per-group broadcast architecture (ADR-000001, Phase 2c): a
%% handler runs in its own supervised group (own process/tree/table), broadcasts
%% route to it, groups start/stop at runtime, and one group's slow apply does not
%% block another group (here, the default control-plane group).
%% @end
%% =============================================================================
-module(partisan_broadcast_group_test).

-include_lib("eunit/include/eunit.hrl").

-define(HANDLER, partisan_bcast_test_handler).
-define(DEFAULT_GROUP, partisan_plumtree_broadcast).

group_test_() ->
    {timeout, 60,
        {foreach, fun setup/0, fun cleanup/1, [
            fun starts_a_dedicated_group/0,
            fun receive_path_delivers_via_group/0,
            fun start_stop_lifecycle/0,
            fun app_group_does_not_block_default_group/0
        ]}}.

setup() ->
    stop_partisan(),
    {ok, _} = application:ensure_all_started(partisan),
    {ok, _} = ?HANDLER:start_link(),
    {ok, _} = partisan_broadcast:start_group(?HANDLER),
    ok.

cleanup(_) ->
    catch partisan_broadcast:stop_group(?HANDLER),
    catch ?HANDLER:stop(),
    stop_partisan().

%% The handler gets its own group process, distinct from the default group.
starts_a_dedicated_group() ->
    Name = partisan_broadcast:group_name(?HANDLER),
    ?assert(is_pid(whereis(Name))),
    ?assertNotEqual(whereis(Name), whereis(?DEFAULT_GROUP)),
    ?assert(lists:member(Name, partisan_broadcast:groups())),
    ?assert(lists:member(?DEFAULT_GROUP, partisan_broadcast:groups())).

%% A received broadcast addressed to the handler's group is delivered.
receive_path_delivers_via_group() ->
    ?HANDLER:reset(),
    ?HANDLER:set_notify(self()),
    ?HANDLER:set_delay(0),
    Id = {grp, erlang:unique_integer()},
    cast_to_group(Id),
    ?assertEqual(ok, wait_applied(Id, 5000)),
    ?assert(?HANDLER:applied(Id)).

%% Groups can be retired and recreated at runtime.
start_stop_lifecycle() ->
    Name = partisan_broadcast:group_name(?HANDLER),
    ?assert(lists:member(Name, partisan_broadcast:groups())),
    ok = partisan_broadcast:stop_group(?HANDLER),
    ?assertNot(lists:member(Name, partisan_broadcast:groups())),
    ?assertEqual(undefined, whereis(Name)),
    {ok, _} = partisan_broadcast:start_group(?HANDLER),
    ?assert(lists:member(Name, partisan_broadcast:groups())).

%% While the app group's apply sleeps, the default control-plane group stays
%% responsive — the isolation the whole design is for.
app_group_does_not_block_default_group() ->
    ?HANDLER:reset(),
    ?HANDLER:set_notify(self()),
    ?HANDLER:set_delay(1500),
    Id = {slow, erlang:unique_integer()},
    T0 = erlang:monotonic_time(millisecond),
    cast_to_group(Id),
    _ = partisan_plumtree_broadcast:broadcast_members(5000),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed < 500),
    ?assertEqual(ok, wait_applied(Id, 5000)).

%% =============================================================================
%% HELPERS
%% =============================================================================

cast_to_group(Id) ->
    Node = partisan:node(),
    gen_server:cast(
        partisan_broadcast:group_name(?HANDLER),
        {broadcast, Id, Id, ?HANDLER, 1, Node, Node}
    ).

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
