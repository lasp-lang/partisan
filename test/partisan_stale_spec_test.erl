%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Exercises which node specifications
%% `partisan_peer_service_manager:connect(Spec, #{prune => true})' reports as
%% stale.
%%
%% Each case runs on a peer node of its own, in a process that owns a fresh
%% `partisan_peer_connections' table, seeds it with connection records and
%% then calls `connect/2'. The peer keeps the cases independent of the
%% connections table other eunit modules leave in the shared test process.
%% Every connection attempt is real and fails, as nothing listens on the
%% addresses used, so each case drives the `{error, _} when Count == 0' path
%% into the stale check.
%%
%% Not covered here: membership removal and gossip of a pruned spec, and a
%% connection attempt that succeeds. `partisan_SUITE' covers a peer that
%% restarts on a new listen address.
%% @end
%% =============================================================================
-module(partisan_stale_spec_test).

-include_lib("eunit/include/eunit.hrl").

-define(NODE, 'stale_spec_peer@127.0.0.1').
-define(CASES, [
    spec_on_old_address_is_pruned,
    spec_on_old_address_is_pruned_when_info_holds_it,
    live_spec_is_kept,
    live_spec_with_dead_address_is_kept,
    spec_differing_only_in_channels_is_kept,
    spec_of_disconnected_node_is_kept,
    stale_check_on_disconnected_node_keeps_spec
]).

-export([spec_on_old_address_is_pruned/0]).
-export([spec_on_old_address_is_pruned_when_info_holds_it/0]).
-export([live_spec_is_kept/0]).
-export([live_spec_with_dead_address_is_kept/0]).
-export([spec_differing_only_in_channels_is_kept/0]).
-export([spec_of_disconnected_node_is_kept/0]).
-export([stale_check_on_disconnected_node_keeps_spec/0]).

%% =============================================================================
%% TEST DESCRIPTORS
%% =============================================================================

stale_spec_test_() ->
    [
        {atom_to_list(Case),
            {setup, fun start_peer/0, fun peer:stop/1, fun(Peer) ->
                ?_test(peer:call(Peer, ?MODULE, Case, [], 60000))
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

%% The node is connected on its new address; the spec with the old address
%% is stale.
spec_on_old_address_is_pruned() ->
    [Live, Dead] = addrs(2),
    Stale = spec([Dead]),
    ?assertEqual(
        [Stale],
        stale_specs(Stale, [{spec([Live]), undefined, Live}])
    ).

%% As above, but the info record still holds the stale spec while the
%% connection uses the new address. The decision must come from the
%% connections, not from the info record.
spec_on_old_address_is_pruned_when_info_holds_it() ->
    [Live, Dead] = addrs(2),
    Stale = spec([Dead]),
    ?assertEqual(
        [Stale],
        stale_specs(Stale, [{Stale, undefined, Live}])
    ).

%% The node is connected with this spec on the default channel only;
%% opening the `foo' channel fails.
live_spec_is_kept() ->
    [Live] = addrs(1),
    Spec = spec([Live]),
    ?assertEqual([], stale_specs(Spec, [{Spec, undefined, Live}])).

%% A multi-address spec where one address is dead and the node is
%% connected on the other one.
live_spec_with_dead_address_is_kept() ->
    [Live, Dead] = addrs(2),
    Spec = spec([Live, Dead]),
    ?assertEqual([], stale_specs(Spec, [{Spec, undefined, Live}])).

%% Same listen addresses as the connected spec, different channels.
spec_differing_only_in_channels_is_kept() ->
    [Live] = addrs(1),
    Connected = spec([Live]),
    Other = Connected#{
        channels => maps:put(
            bar, #{parallelism => 1, monotonic => false}, channels()
        )
    },
    ?assertEqual(
        [], stale_specs(Other, [{Connected, undefined, Live}])
    ).

%% A node with no connections at all is down, not stale.
spec_of_disconnected_node_is_kept() ->
    [Dead] = addrs(1),
    ?assertEqual([], stale_specs(spec([Dead]), [])).

%% The node lost its last connection between connect/2's connection count
%% check and the stale check. maybe_stale/6 is private and reachable here
%% through the export_all in partisan.hrl.
stale_check_on_disconnected_node_keeps_spec() ->
    [Dead] = addrs(1),
    Spec = spec([Dead]),
    ?assertEqual(
        [],
        in_table_owner([], fun() ->
            partisan_peer_service_manager:maybe_stale(
                Spec, undefined, Dead, [], 0, normal
            )
        end)
    ).

%% =============================================================================
%% UTILS
%% =============================================================================

channels() ->
    #{
        undefined => #{parallelism => 1, monotonic => false},
        foo => #{parallelism => 1, monotonic => false}
    }.

spec(ListenAddrs) ->
    #{name => ?NODE, listen_addrs => ListenAddrs, channels => channels()}.

%% Returns N distinct loopback addresses on which nothing listens.
addrs(N) ->
    Sockets = [
        begin
            {ok, S} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}]),
            S
        end
     || _ <- lists:seq(1, N)
    ],
    Addrs = [
        begin
            {ok, Port} = inet:port(S),
            #{ip => {127, 0, 0, 1}, port => Port}
        end
     || S <- Sockets
    ],
    ok = lists:foreach(fun gen_tcp:close/1, Sockets),
    Addrs.

%% Seeds a fresh connections table with `Conns' ({Spec, Channel, ListenAddr})
%% and returns the distinct specs `connect/2' reports as stale for `Spec'.
stale_specs(Spec, Conns) ->
    in_table_owner(Conns, fun() ->
        {ok, Stale} = partisan_peer_service_manager:connect(
            Spec, #{prune => true}
        ),
        lists:usort(Stale)
    end).

%% Runs `Fun' in a fresh process that owns a connections table seeded with
%% `Conns', and returns its result.
in_table_owner(Conns, Fun) ->
    Parent = self(),
    {Pid, Ref} = spawn_monitor(fun() ->
        ok = partisan_peer_connections:init(),
        ok = lists:foreach(
            fun({S, Channel, Addr}) ->
                Conn = spawn(fun() ->
                    receive
                        stop -> ok
                    end
                end),
                ok = partisan_peer_connections:store(S, Conn, Channel, Addr)
            end,
            Conns
        ),
        Parent ! {self(), Fun()}
    end),
    receive
        {Pid, Result} ->
            Result;
        {'DOWN', Ref, process, Pid, Reason} ->
            error({table_owner_crashed, Reason})
    after 30000 ->
        error(timeout)
    end.
