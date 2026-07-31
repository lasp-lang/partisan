%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests for broadcast fan-out batching.
%%
%% The tree engine emits one `{send, Peer, Msg, Mod}' action per peer, so a
%% fan-out reaches the shell as several actions sharing a payload. Those are
%% grouped and the wire encoding is done once for the group
%% (`partisan_peer_connections:dispatch_many/4') instead of once per peer.
%%
%% Batching splits the peer list in two — peers routed by the batch path, and
%% peers deferred back to the ordinary per-peer path — which makes it easy for
%% telemetry to drift. `instrument_transmission/2' must fire exactly once per
%% peer regardless of how the list splits, so that is asserted directly here.
%% @end
%% =============================================================================
-module(partisan_broadcast_fanout_test).

-include_lib("eunit/include/eunit.hrl").

%% Used as the broadcast handler module by the tests below.
-export([extract_log_type_and_payload/1]).
%% Target of the `transmission_logging_mfa' hook.
-export([count_transmission/2]).

-define(TAB, partisan_broadcast_fanout_test_counter).

fanout_test_() ->
    {timeout, 60,
        {foreach, fun setup/0, fun cleanup/1, [
            fun instruments_once_per_peer_when_all_deferred/0,
            fun instruments_once_for_a_single_peer/0,
            fun instruments_nothing_for_an_empty_peer_list/0
        ]}}.

setup() ->
    stop_partisan(),
    {ok, _} = application:ensure_all_started(partisan),
    _ = (catch ets:delete(?TAB)),
    ?TAB = ets:new(?TAB, [named_table, public, set]),
    true = ets:insert(?TAB, {count, 0}),
    ok = partisan_config:set(
        transmission_logging_mfa, {?MODULE, count_transmission, []}
    ),
    ok.

cleanup(_) ->
    ok = partisan_config:set(transmission_logging_mfa, undefined),
    _ = (catch ets:delete(?TAB)),
    stop_partisan().

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.

%% None of these peers is connected, so `dispatch_many/4' defers all of them
%% and every send falls back to the per-peer path. The count must still be one
%% per peer — this is the case that regressed when the deferred peers were
%% routed through a non-instrumenting helper.
instruments_once_per_peer_when_all_deferred() ->
    Peers = ['p1@nowhere', 'p2@nowhere', 'p3@nowhere'],

    ok = partisan_plumtree_broadcast:send(a_message, ?MODULE, Peers),

    ?assertEqual(length(Peers), count()).

%% The single-peer list short-circuits to the per-peer clause; still exactly
%% one transmission.
instruments_once_for_a_single_peer() ->
    ok = partisan_plumtree_broadcast:send(a_message, ?MODULE, ['solo@nowhere']),
    ?assertEqual(1, count()).

instruments_nothing_for_an_empty_peer_list() ->
    ok = partisan_plumtree_broadcast:send(a_message, ?MODULE, []),
    ?assertEqual(0, count()).

%% =============================================================================
%% HANDLER / HOOK CALLBACKS
%% =============================================================================

%% `instrument_transmission/2' calls this on the handler module and emits one
%% hook call per returned element. One element => one call per transmission.
extract_log_type_and_payload(_Message) ->
    [{test_type, test_payload}].

count_transmission(_Type, _Payload) ->
    ets:update_counter(?TAB, count, 1).

count() ->
    [{count, N}] = ets:lookup(?TAB, count),
    N.
