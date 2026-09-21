%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%% @doc Guards the contract of `partisan_peer_discovery_agent': a discovery
%% answer is a hint for joining, never a membership authority. A member that a
%% lookup does not mention stays a member; a peer a lookup does mention is
%% joined.
%%
%% Cloud discovery backends are readiness-gated and eventually consistent: a
%% DNS answer omits a pod that is booting or briefly unhealthy. Treating the
%% omission as a departure removes the member and gossips the removal, and
%% the victim then evicts itself ("membership doesn't contain us"). Under a
%% full-membership strategy every node that is slow to boot is thrown out of
%% the cluster once per poll until it is ready — which is what the agent did
%% while it called `update_members/1'.
%%
%% The static manager is used for the same reason as in
%% `partisan_peer_service_manager_boot_test': its membership can be seeded
%% without a real peer, and its `update_members/1' removes absent members
%% exactly like the pluggable one. The two-node reproduction with real gossip
%% and a real self-eviction is `partisan_SUITE:discovery_never_evicts_test/1'.
%% @end
%% =============================================================================
-module(partisan_peer_discovery_agent_test).

-include_lib("eunit/include/eunit.hrl").
-include("partisan.hrl").

-define(MANAGER, partisan_static_peer_service_manager).
-define(POLL, 50).
-define(DISCOVERY(Addresses), #{
    enabled => true,
    type => partisan_peer_discovery_list,
    config => #{addresses => Addresses},
    initial_delay => 0,
    polling_interval => ?POLL
}).

%% =============================================================================
%% TEST DESCRIPTOR
%% =============================================================================

discovery_agent_test_() ->
    {timeout, 60,
        {setup, fun start/0, fun cleanup/1, fun(_) ->
            [
                ?_test(a_member_absent_from_every_lookup_stays_a_member()),
                ?_test(a_peer_a_lookup_mentions_is_joined())
            ]
        end}}.

%% =============================================================================
%% CASES
%% =============================================================================

%% The backend answers with no addresses, so every poll names the local node
%% alone. Three polls later the seeded peer must still be a member and the
%% agent must still be running. Before `add_members/1' the first poll after
%% the seed removed the peer.
a_member_absent_from_every_lookup_stays_a_member() ->
    Peer = seed_membership('absent@127.0.0.1'),
    ?assert(is_a_member(Peer)),
    ok = await_polls(3),
    ?assert(is_a_member(Peer)),
    ?assertEqual(enabled, partisan_peer_discovery_agent:status()).

%% The join half must survive the change: a peer the backend does mention is
%% put on the join path, so the manager's `connected' signal for it makes it
%% a member. The peer seeded above is not mentioned and must stay.
a_peer_a_lookup_mentions_is_joined() ->
    Absent = 'absent@127.0.0.1',
    Address = "present@127.0.0.1:12345",
    ok = restart_agent(?DISCOVERY([Address])),
    ok = await_polls(2),

    %% The spec the backend builds is what the manager holds as pending, and
    %% the `connected' signal only moves an exact match into membership.
    {ok, S} = partisan_peer_discovery_list:init(#{addresses => [Address]}),
    {ok, [Spec], _} = partisan_peer_discovery_list:lookup(S, 1000),
    signal_connected(Spec),

    ?assert(is_a_member('present@127.0.0.1')),
    ?assert(is_a_member(Absent)).

%% =============================================================================
%% UTILS
%% =============================================================================

start() ->
    stop_partisan(),
    ok = application:set_env(partisan, peer_service_manager, ?MANAGER),
    ok = application:set_env(partisan, peer_discovery, ?DISCOVERY([])),
    {ok, _} = application:ensure_all_started(partisan),
    ok.

cleanup(_) ->
    stop_partisan(),
    %% Every test module shares the VM, so the overrides must not leak.
    application:unset_env(partisan, peer_service_manager),
    application:unset_env(partisan, peer_discovery).

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.

%% The agent reads its options when it starts, so a new backend configuration
%% takes effect through a supervisor restart of the agent alone.
restart_agent(Opts) ->
    ok = partisan_config:set(peer_discovery, Opts),
    Sup = partisan_peer_service_sup,
    ok = supervisor:terminate_child(Sup, partisan_peer_discovery_agent),
    {ok, _} = supervisor:restart_child(Sup, partisan_peer_discovery_agent),
    ok.

%% Polls are timer-driven; the trailing `lookup/0' call is served by the same
%% state machine, so once it answers every poll due in the window has run.
await_polls(N) ->
    timer:sleep(?POLL * N + ?POLL),
    {ok, _} = partisan_peer_discovery_agent:lookup(),
    ok.

seed_membership(Name) ->
    Spec = #{name => Name, listen_addrs => [], channels => #{}},
    ok = partisan_peer_service:join(Spec),
    signal_connected(Spec),
    Name.

%% The manager reacts to the `connected' signal the connection process sends
%% once its handshake completes; driving it directly avoids needing a peer.
signal_connected(Spec) ->
    Mgr = whereis(?MANAGER),
    Mgr ! {connected, Spec, ?DEFAULT_CHANNEL, undefined, undefined},
    _ = sys:get_state(Mgr),
    ok.

is_a_member(Name) ->
    {ok, Members} = partisan_peer_service:members(),
    lists:member(Name, Members).
