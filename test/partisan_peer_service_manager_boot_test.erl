%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Guards the boot path of every shipped `partisan_peer_service_manager'
%% and the dispatch of the behaviour's optional callbacks.
%%
%% `on_up/3' and `on_down/3' are optional callbacks, so a manager is entitled
%% not to export them. `partisan_monitor:init/1' nevertheless subscribes to
%% channel status through `partisan_peer_service:on_up/3' on every boot where
%% `connect_disterl' is disabled — the default — so dispatching to the manager
%% unconditionally turns a missing optional callback into an `undef' that takes
%% down `partisan_peer_service_sup', and with it the whole application.
%%
%% Both halves are asserted here: the API answers `{error, not_implemented}'
%% for a manager that does not export the callback, and each shipped manager
%% boots with the default configuration.
%% @end
%% =============================================================================
-module(partisan_peer_service_manager_boot_test).

-include_lib("eunit/include/eunit.hrl").
-include("partisan.hrl").

-define(MANAGERS, [
    partisan_pluggable_peer_service_manager,
    partisan_hyparview_peer_service_manager,
    partisan_client_server_peer_service_manager,
    partisan_static_peer_service_manager
]).

%% =============================================================================
%% TEST DESCRIPTORS
%% =============================================================================

boot_test_() ->
    {timeout, 120, [
        {
            atom_to_list(Mod),
            {setup, fun() -> start_with(Mod) end, fun cleanup/1, fun(_) ->
                [
                    ?_test(manager_is_the_configured_one(Mod)),
                    ?_test(monitor_is_running())
                ]
            end}
        }
     || Mod <- ?MANAGERS
    ]}.

optional_callback_test_() ->
    {timeout, 60,
        {setup,
            fun() -> start_with(partisan_pluggable_peer_service_manager) end,
            fun cleanup/1, fun(_) ->
                [
                    ?_test(on_up_3_is_optional()),
                    ?_test(on_down_3_is_optional())
                ]
            end}}.

pluggable_channel_event_test_() ->
    {timeout, 60,
        {setup,
            fun() -> start_with(partisan_pluggable_peer_service_manager) end,
            fun cleanup/1, fun(_) ->
                [
                    ?_test(wildcard_channel_subscription_fires()),
                    ?_test(channel_subscription_is_scoped()),
                    ?_test(channel_up_fires_once_per_channel())
                ]
            end}}.

static_forward_message_test_() ->
    {timeout, 60,
        {setup, fun() -> start_with(partisan_static_peer_service_manager) end,
            fun cleanup/1, fun(_) ->
                [
                    ?_test(forwards_to_a_pid()),
                    ?_test(forwards_to_a_registered_name()),
                    ?_test(forwards_to_a_name_and_node()),
                    ?_test(forwards_to_a_name_on_a_node()),
                    ?_test(unreachable_peer_is_an_error_not_a_crash()),
                    ?_test(leaving_a_peer_removes_it()),
                    ?_test(leaving_ourselves_keeps_only_us()),
                    ?_test(update_members_joins_and_drops()),
                    ?_test(on_up_fires_once_per_node()),
                    ?_test(on_up_can_be_scoped_to_a_channel()),
                    ?_test(monitoring_is_supported())
                ]
            end}}.

%% =============================================================================
%% BOOT
%% =============================================================================

manager_is_the_configured_one(Mod) ->
    ?assertEqual(Mod, partisan_peer_service:manager()),
    ?assert(is_pid(whereis(Mod))).

%% `partisan_monitor' is the child whose `init/1' subscribes to node and
%% channel status, so it is the one that fails when the manager does not
%% implement an optional callback.
monitor_is_running() ->
    ?assert(is_pid(whereis(partisan_monitor))).

%% =============================================================================
%% OPTIONAL CALLBACKS
%% =============================================================================

%% This module stands in for a manager implementing only the mandatory
%% callbacks: it exports neither `on_up/3' nor `on_down/3'.
on_up_3_is_optional() ->
    ?assertEqual(
        {error, not_implemented},
        with_manager(?MODULE, fun() ->
            partisan_peer_service:on_up('_', fun(_, _) -> ok end, #{
                channel => '_'
            })
        end)
    ).

on_down_3_is_optional() ->
    ?assertEqual(
        {error, not_implemented},
        with_manager(?MODULE, fun() ->
            partisan_peer_service:on_down('_', fun(_, _) -> ok end, #{
                channel => '_'
            })
        end)
    ).

%% =============================================================================
%% STATIC MANAGER SERVER REFS
%% =============================================================================
%%
%% The static manager has to accept the same `server_ref()' forms as the rest,
%% and resolve the local ones without going through a peer connection — it
%% holds none to itself.

forwards_to_a_pid() ->
    ok = partisan:forward_message(self(), {ping, pid}),
    ?assertEqual(ok, await({ping, pid})).

forwards_to_a_registered_name() ->
    ok = register_self(),
    ok = partisan:forward_message(?MODULE, {ping, name}),
    ?assertEqual(ok, await({ping, name})).

forwards_to_a_name_and_node() ->
    ok = register_self(),
    ok = partisan:forward_message({?MODULE, partisan:node()}, {ping, nn}),
    ?assertEqual(ok, await({ping, nn})).

forwards_to_a_name_on_a_node() ->
    ok = register_self(),
    ok = partisan:forward_message(
        partisan:node(), ?MODULE, {ping, node_name}, #{}
    ),
    ?assertEqual(ok, await({ping, node_name})).

%% A peer we never joined has no connection, which is an error return and not
%% a `badarg'.
unreachable_peer_is_an_error_not_a_crash() ->
    ?assertMatch(
        {error, _},
        partisan:forward_message({?MODULE, 'ghost@127.0.0.1'}, {ping, ghost})
    ).

%% =============================================================================
%% STATIC MANAGER MEMBERSHIP
%% =============================================================================
%%
%% Membership here is explicit, so `leave' has to actually remove the peer,
%% not just answer without changing anything.

leaving_a_peer_removes_it() ->
    Peer = seed_membership('peer1@127.0.0.1'),
    ?assert(is_a_member(Peer)),
    ?assertEqual(ok, partisan_peer_service:leave(#{name => Peer})),
    ?assertNot(is_a_member(Peer)),
    %% We are still here.
    ?assert(is_a_member(partisan:node())).

leaving_ourselves_keeps_only_us() ->
    _ = seed_membership('peer2@127.0.0.1'),
    ?assertEqual(ok, partisan_peer_service:leave()),
    ?assertEqual([partisan:node()], sorted_members()).

%% `join' only reaches membership once the connection is up, which needs a
%% real peer. The `connected' signal the manager acts on is the same one the
%% connection process sends, so it is used directly here.
seed_membership(Name) ->
    ok = partisan_peer_service:join(spec(Name)),
    signal_connected(spec(Name), ?DEFAULT_CHANNEL),
    Name.

spec(Name) ->
    #{name => Name, listen_addrs => [], channels => #{}}.

%% The manager reacts to the `connected' signal the connection process sends
%% once its handshake completes; driving it directly avoids needing a peer.
signal_connected(Spec, Channel) ->
    Mgr = whereis(partisan_static_peer_service_manager),
    Mgr ! {connected, Spec, Channel, undefined, undefined},
    sync(Mgr).

await_event(Tag) ->
    receive
        Event when element(1, Event) == Tag -> Event
    after 1000 -> timeout
    end.

is_a_member(Name) ->
    lists:member(Name, sorted_members()).

sorted_members() ->
    {ok, Members} = partisan_peer_service:members(),
    lists:sort(Members).

%% A `sys:get_state' round trip guarantees the async `connected' message above
%% has been processed before the assertions read the membership.
sync(Pid) ->
    _ = sys:get_state(Pid),
    ok.

%% =============================================================================
%% STATIC MANAGER CONNECTION EVENTS
%% =============================================================================
%%
%% Node monitoring is built on these: `partisan_monitor' registers wildcard
%% `on_up'/`on_down' callbacks at boot, so a manager that does not fire them
%% cannot support `partisan:monitor_node/2'.

monitoring_is_supported() ->
    ?assert(
        partisan_peer_service_manager:supports_capability(
            partisan_static_peer_service_manager, monitoring
        )
    ).

%% Node-level events are edge-triggered: a second channel to the same peer is
%% not a second `nodeup'.
on_up_fires_once_per_node() ->
    Me = self(),
    ok = partisan_peer_service:on_up('_', fun(N) -> Me ! {up, N} end),
    Peer = 'ev1@127.0.0.1',
    Spec = spec(Peer),
    signal_connected(Spec, ?DEFAULT_CHANNEL),
    ?assertEqual({up, Peer}, await_event(up)),
    %% A second channel to the same peer must not fire again.
    signal_connected(Spec, other_channel),
    ?assertEqual(timeout, await_event(up)).

%% `on_down' needs a real connection to prune: the connection table is
%% `protected', so only the manager process can populate it. That path is
%% covered by the two-node check in `examples/' rather than here.

%% A `channel' option scopes the subscription and passes the channel to the
%% callback as a second argument.
on_up_can_be_scoped_to_a_channel() ->
    Me = self(),
    ok = partisan_peer_service:on_up(
        '_', fun(N, C) -> Me ! {chan, N, C} end, #{channel => ?DEFAULT_CHANNEL}
    ),
    Peer = 'ev3@127.0.0.1',
    signal_connected(spec(Peer), ?DEFAULT_CHANNEL),
    ?assertEqual({chan, Peer, ?DEFAULT_CHANNEL}, await_event(chan)).

update_members_joins_and_drops() ->
    Keep = seed_membership('keep@127.0.0.1'),
    Drop = seed_membership('drop@127.0.0.1'),
    ?assert(is_a_member(Keep) andalso is_a_member(Drop)),

    New = 'new@127.0.0.1',
    ok = partisan_peer_service:update_members([spec(Keep), spec(New)]),

    %% `Drop' is gone, `Keep' stays. `New' is only pending until it connects,
    %% which is what membership means here.
    ?assertNot(is_a_member(Drop)),
    ?assert(is_a_member(Keep)),
    signal_connected(spec(New), ?DEFAULT_CHANNEL),
    ?assert(is_a_member(New)).

%% =============================================================================
%% PLUGGABLE CHANNEL EVENTS
%% =============================================================================
%%
%% Channel subscriptions are keyed by `{Node, Channel}', so they have to be
%% looked up by that tuple: a lookup by node name alone can never match one,
%% and no channel callback would run — including the ones `partisan_monitor'
%% registers.

wildcard_channel_subscription_fires() ->
    Me = self(),
    ok = partisan_peer_service:on_up(
        '_', fun(N, C) -> Me ! {chan, N, C} end, #{channel => '_'}
    ),
    Peer = 'pc1@127.0.0.1',
    pluggable_connected(Peer, ?DEFAULT_CHANNEL),
    ?assertEqual({chan, Peer, ?DEFAULT_CHANNEL}, await_event(chan)).

%% A subscription naming a channel must not fire for a different one.
channel_subscription_is_scoped() ->
    Me = self(),
    ok = partisan_peer_service:on_up(
        '_', fun(N, C) -> Me ! {scoped, N, C} end, #{channel => wanted}
    ),
    Peer = 'pc2@127.0.0.1',
    pluggable_connected(Peer, unwanted),
    ?assertEqual(timeout, await_event(scoped)),
    pluggable_connected(Peer, wanted),
    ?assertEqual({scoped, Peer, wanted}, await_event(scoped)).

%% Channel parallelism means several connections carry the same channel;
%% only the first of them is an event.
channel_up_fires_once_per_channel() ->
    Me = self(),
    ok = partisan_peer_service:on_up(
        '_', fun(N, C) -> Me ! {once, N, C} end, #{channel => '_'}
    ),
    Peer = 'pc3@127.0.0.1',
    pluggable_connected(Peer, ?DEFAULT_CHANNEL),
    ?assertEqual({once, Peer, ?DEFAULT_CHANNEL}, await_event(once)),
    pluggable_connected(Peer, ?DEFAULT_CHANNEL),
    ?assertEqual(timeout, await_event(once)).

%% The peer is deliberately not pending, so the membership-strategy half of
%% the handler is skipped and only the channel event is exercised.
pluggable_connected(Name, Channel) ->
    Mgr = whereis(partisan_pluggable_peer_service_manager),
    Mgr ! {connected, spec(Name), Channel, undefined, undefined},
    sync(Mgr).

%% =============================================================================
%% UTILS
%% =============================================================================

start_with(Mod) ->
    stop_partisan(),
    ok = application:set_env(partisan, peer_service_manager, Mod),
    {ok, _} = application:ensure_all_started(partisan),
    ok.

cleanup(_) ->
    _ = unregister_self(),
    stop_partisan(),
    %% Every test module shares the VM, so the override must not leak.
    application:unset_env(partisan, peer_service_manager).

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.

%% Runs `Fun' with `Mod' configured as the peer service manager. The manager
%% is read from the config on each call, so this needs no restart.
with_manager(Mod, Fun) ->
    Prev = partisan_config:get(peer_service_manager),
    ok = partisan_config:set(peer_service_manager, Mod),
    try
        Fun()
    after
        partisan_config:set(peer_service_manager, Prev)
    end.

register_self() ->
    _ = unregister_self(),
    true = register(?MODULE, self()),
    ok.

unregister_self() ->
    catch unregister(?MODULE),
    ok.

await(Message) ->
    receive
        Message -> ok
    after 5000 -> {error, timeout}
    end.
