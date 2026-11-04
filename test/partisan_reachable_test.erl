%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Exercises which peers the pluggable manager's distance `ping' and
%% `pong' are sent to: `is_reachable/1' must agree with the connection lookup
%% the dispatch makes, including the fallback to the default channel.
%%
%% Each case runs on a peer node of its own, in a process that owns a fresh
%% `partisan_peer_connections' table seeded with connection records, as in
%% `partisan_stale_spec_test'. `is_reachable/1' is private and reachable here
%% through the export_all in partisan.hrl.
%%
%% Not covered here: that a filtered-out peer is measured on a later round.
%% @end
%% =============================================================================
-module(partisan_reachable_test).

-include_lib("eunit/include/eunit.hrl").

-define(PEER, 'reachable_peer@127.0.0.1').
-define(MANAGER, partisan_pluggable_peer_service_manager).
-define(CASES, [
    local_node_is_reachable,
    disconnected_peer_is_not_reachable,
    peer_on_membership_channel_is_reachable,
    peer_on_default_channel_only_is_reachable,
    peer_on_other_channel_only_is_not_reachable,
    peer_on_default_channel_without_fallback_is_not_reachable
]).

-export([local_node_is_reachable/0]).
-export([disconnected_peer_is_not_reachable/0]).
-export([peer_on_membership_channel_is_reachable/0]).
-export([peer_on_default_channel_only_is_reachable/0]).
-export([peer_on_other_channel_only_is_not_reachable/0]).
-export([peer_on_default_channel_without_fallback_is_not_reachable/0]).

%% =============================================================================
%% TEST DESCRIPTORS
%% =============================================================================

reachable_test_() ->
    [
        {atom_to_list(Case),
            {setup, fun start_peer/0, fun peer:stop/1, fun(Peer) ->
                ?_test(peer:call(Peer, ?MODULE, Case, [], 30000))
            end}}
     || Case <- ?CASES
    ].

start_peer() ->
    Paths = lists:append([["-pa", Dir] || Dir <- code:get_path()]),
    Opts = #{connection => standard_io, args => Paths},
    {ok, Peer, _} = peer:start_link(Opts),
    ok = peer:call(Peer, partisan_config, init, []),
    Peer.

%% =============================================================================
%% CASES
%% =============================================================================

%% A send to ourselves needs no connection.
local_node_is_reachable() ->
    Me = #{name => partisan:node(), listen_addrs => []},
    ?assert(reachable(Me, [])).

disconnected_peer_is_not_reachable() ->
    ?assertNot(reachable(spec(), [])).

peer_on_membership_channel_is_reachable() ->
    ?assert(reachable(spec(), [partisan_membership])).

%% The dispatch falls back to the default channel (`channel_fallback',
%% default `true'), so the send succeeds.
peer_on_default_channel_only_is_reachable() ->
    ?assert(reachable(spec(), [undefined])).

%% Neither the membership channel nor the default one: the dispatch fails.
peer_on_other_channel_only_is_not_reachable() ->
    ?assertNot(reachable(spec(), [data])).

peer_on_default_channel_without_fallback_is_not_reachable() ->
    ok = partisan_config:set(channel_fallback, false),
    ?assertNot(reachable(spec(), [undefined])).

%% =============================================================================
%% UTILS
%% =============================================================================

spec() ->
    #{name => ?PEER, listen_addrs => [addr()]}.

addr() ->
    #{ip => {127, 0, 0, 1}, port => 9}.

%% Seeds a fresh connections table with one connection to `Spec' per channel
%% in `Channels' and returns `is_reachable(Spec)'.
reachable(Spec, Channels) ->
    Parent = self(),
    {Pid, Ref} = spawn_monitor(fun() ->
        ok = partisan_peer_connections:init(),
        ok = lists:foreach(
            fun(Channel) ->
                Conn = spawn(fun() ->
                    receive
                        stop -> ok
                    end
                end),
                ok = partisan_peer_connections:store(
                    Spec, Conn, Channel, addr()
                )
            end,
            Channels
        ),
        Parent ! {self(), ?MANAGER:is_reachable(Spec)}
    end),
    receive
        {Pid, Result} ->
            Result;
        {'DOWN', Ref, process, Pid, Reason} ->
            error({table_owner_crashed, Reason})
    after 10000 ->
        error(timeout)
    end.
