%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc A conformance checklist run against **every** shipped
%% `partisan_peer_service_manager'.
%%
%% The managers other than the default had no test coverage at all, which is
%% how a manager that could not even boot shipped in a release. The point of
%% this module is that adding a manager, or changing one, means satisfying the
%% same checklist as the rest. What is asserted here:
%%
%% <ol>
%% <li>**Lifecycle** — the application starts with the manager configured and
%% `partisan_monitor' survives its own `init/1'.</li>
%% <li>**Membership reads** — `members/0', `members_for_orchestration/0' and
%% `get_local_state/0' answer, this node is a member, and the lock-free
%% `partisan_membership' snapshot is seeded.</li>
%% <li>**The optional-callback contract** — every callback listed in
%% `-optional_callbacks' either works or returns exactly
%% `{error, not_implemented}'. Never `undef', never a bare `error'. This is
%% the invariant that was violated: `on_up/3' was simply absent, so
%% `partisan_peer_service:on_up/3' raised `undef' during `partisan_monitor'
%% init and took the whole application down.</li>
%% <li>**Messaging surface** — `forward_message/2,3' accepts every documented
%% `server_ref()' form without raising.</li>
%% <li>**Capability honesty** — a manager claiming `monitoring' must actually
%% accept `on_up'/`on_down' subscriptions, since node monitoring is built on
%% them.</li>
%% <li>**Event liveness** — a manager that accepts a channel-scoped
%% subscription must actually fire it. A subscription that is stored and never
%% delivered passes every other check here.</li>
%% </ol>
%%
%% Per-manager expectations are data (`support/1'), so declining a callback is
%% a legitimate, *declared* answer — but declining it silently, or crashing
%% instead, is not.
%% @end
%% =============================================================================
-module(partisan_manager_conformance_test).

-include_lib("eunit/include/eunit.hrl").
-include("partisan.hrl").

-define(PLUGGABLE, partisan_pluggable_peer_service_manager).
-define(HYPARVIEW, partisan_hyparview_peer_service_manager).
-define(CLIENT_SERVER, partisan_client_server_peer_service_manager).
-define(STATIC, partisan_static_peer_service_manager).

-define(MANAGERS, [?PLUGGABLE, ?HYPARVIEW, ?CLIENT_SERVER, ?STATIC]).

%% The checklist each manager is run through.
-define(CHECKS, [
    boots_and_supervises,
    reports_itself_as_a_member,
    seeds_the_membership_snapshot,
    optional_callbacks_decline_cleanly,
    forward_message_accepts_every_server_ref,
    capability_claim_is_honest,
    channel_subscriptions_actually_fire
]).

%% =============================================================================
%% WHAT EACH MANAGER CLAIMS
%% =============================================================================

%% `true'  — the callback must work.
%% `false' — the callback must decline with `{error, not_implemented}'.
%% Keeping this as data means a manager losing a capability shows up as a diff
%% here rather than passing unnoticed.
support(?PLUGGABLE) ->
    #{
        sync_join => true,
        update_members => true,
        on_up_3 => true,
        on_down_3 => true,
        monitoring => true,
        partitions => false,
        global_delivery => true
    };
support(?HYPARVIEW) ->
    #{
        sync_join => false,
        update_members => true,
        on_up_3 => false,
        on_down_3 => false,
        monitoring => false,
        partitions => true,
        %% Documented as unsupported: the manager logs and drops.
        global_delivery => false
    };
support(?CLIENT_SERVER) ->
    #{
        sync_join => false,
        update_members => false,
        on_up_3 => false,
        on_down_3 => false,
        monitoring => false,
        partitions => false,
        global_delivery => true
    };
support(?STATIC) ->
    #{
        sync_join => true,
        update_members => true,
        on_up_3 => true,
        on_down_3 => true,
        monitoring => true,
        partitions => false,
        global_delivery => true
    }.

%% =============================================================================
%% TEST DESCRIPTOR
%% =============================================================================

conformance_test_() ->
    {timeout, 300, [
        {
            atom_to_list(Mgr) ++ " / " ++ atom_to_list(Check),
            {setup, fun() -> start_with(Mgr) end, fun cleanup/1, fun(_) ->
                [?_test(?MODULE:Check(Mgr))]
            end}
        }
     || Mgr <- ?MANAGERS, Check <- ?CHECKS
    ]}.

%% =============================================================================
%% THE CHECKS
%% =============================================================================

%% The manager is running and so is `partisan_monitor', whose `init/1'
%% subscribes to node and channel status through the manager. A manager that
%% does not answer those subscriptions takes the supervision tree down here.
boots_and_supervises(Mgr) ->
    ?assertEqual(Mgr, partisan_peer_service:manager()),
    ?assert(is_pid(whereis(Mgr))),
    ?assert(is_pid(whereis(partisan_monitor))),
    ?assert(is_pid(whereis(partisan_peer_service_sup))).

reports_itself_as_a_member(_Mgr) ->
    ?assertMatch({ok, [_ | _]}, partisan_peer_service:members()),
    {ok, Members} = partisan_peer_service:members(),
    ?assert(lists:member(partisan:node(), Members)),
    ?assertMatch({ok, _}, partisan_peer_service:members_for_orchestration()),
    ?assertMatch({ok, _}, partisan_peer_service:get_local_state()).

%% Readers go through the snapshot rather than calling the manager, so every
%% manager has to seed it — a manager that does not leaves broadcast with an
%% empty membership.
seeds_the_membership_snapshot(_Mgr) ->
    ?assert(partisan_membership:version() >= 1),
    ?assert(
        ordsets:is_element(partisan:node(), partisan_membership:node_names())
    ).

%% The heart of the checklist. Each optional callback is invoked through the
%% public API and must answer, one way or the other.
optional_callbacks_decline_cleanly(Mgr) ->
    Claims = support(Mgr),
    Fun = fun(_) -> ok end,
    Fun2 = fun(_, _) -> ok end,

    %% Called on the manager rather than through `partisan_peer_service',
    %% which answers `{error, self_join}' for the local spec without ever
    %% reaching the callback — and the callback is what is under test here.
    %% The local spec is also the only argument with a bounded answer:
    %% `sync_join/1' blocks until the peer connects, so a fabricated remote
    %% peer would block for as long as it stays unreachable, which is forever.
    check(Mgr, sync_join, Claims, fun() ->
        Mgr:sync_join(partisan:node_spec())
    end),
    check(Mgr, update_members, Claims, fun() ->
        partisan_peer_service:update_members([partisan:node_spec()])
    end),
    check(Mgr, on_up_3, Claims, fun() ->
        partisan_peer_service:on_up('_', Fun2, #{channel => '_'})
    end),
    check(Mgr, on_down_3, Claims, fun() ->
        partisan_peer_service:on_down('_', Fun2, #{channel => '_'})
    end),
    check(Mgr, partitions, Claims, fun() ->
        partisan_peer_service:partitions()
    end),

    %% `on_up/2' and `on_down/2' are mandatory, so they must answer for every
    %% manager — either `ok' or a declared `{error, not_implemented}'.
    ?assert(answers(fun() -> partisan_peer_service:on_up('_', Fun) end)),
    ?assert(answers(fun() -> partisan_peer_service:on_down('_', Fun) end)),

    %% `leave/1' must never reply with a bare `error' the caller cannot
    %% interpret.
    Left = partisan_peer_service:leave(#{name => 'nosuch@127.0.0.1'}),
    ?assert(
        Left == ok orelse Left == {error, not_implemented},
        lists:flatten(io_lib:format("~p returned ~p from leave/1", [Mgr, Left]))
    ).

%% Every documented `server_ref()' shape has to be routable, `{Name, Node}'
%% included — a manager that raises `badarg' on one of them is unusable for
%% the callers that pass it.
forward_message_accepts_every_server_ref(Mgr) ->
    Name = ?MODULE,
    true = register(Name, self()),
    yes = global:register_name(Name, self()),
    Node = partisan:node(),

    try
        [
            ?assert(delivers(Ref, Msg), atom_to_list(element(2, Msg)))
         || {Ref, Msg} <- [
                {self(), {ref, pid}},
                {Name, {ref, name}},
                {{Name, Node}, {ref, name_and_node}}
            ]
        ],

        %% `{global, Name}' has the same shape as `{Name, Node}', so a
        %% manager matching the latter first sends this to a node called
        %% `Name' instead of resolving it globally. Not every manager
        %% supports global delivery, but none may misroute it.
        Global = partisan:forward_message({global, Name}, {ref, global}),
        ?assertEqual(ok, Global),
        case maps:get(global_delivery, support(Mgr)) of
            true -> ?assertEqual({ref, global}, await({ref, global}));
            false -> ?assertEqual(timeout, await({ref, global}))
        end,
        %% `/4' addresses a registered name on a node directly.
        ?assertEqual(
            ok, partisan:forward_message(Node, Name, {ref, arity4}, #{})
        ),
        ?assertEqual({ref, arity4}, await({ref, arity4}))
    after
        catch global:unregister_name(Name),
        catch unregister(Name)
    end.

%% Node monitoring is implemented on top of the connection callbacks, so a
%% manager cannot claim the capability while declining them.
capability_claim_is_honest(Mgr) ->
    Claims = support(Mgr),
    Claimed = partisan_peer_service_manager:supports_capability(
        Mgr, monitoring
    ),
    ?assertEqual(maps:get(monitoring, Claims), Claimed),

    Claimed andalso
        begin
            Fun = fun(_) -> ok end,
            ?assertEqual(ok, partisan_peer_service:on_up('_', Fun)),
            ?assertEqual(ok, partisan_peer_service:on_down('_', Fun))
        end.

%% Accepting a subscription and never calling it back passes every other
%% check in this module. This is the one that catches dead wiring.
channel_subscriptions_actually_fire(Mgr) ->
    case maps:get(on_up_3, support(Mgr)) of
        false ->
            ok;
        true ->
            Me = self(),
            ok = partisan_peer_service:on_up(
                '_', fun(N, C) -> Me ! {fired, N, C} end, #{channel => '_'}
            ),
            Peer = 'conformance_peer@127.0.0.1',
            signal_connected(Mgr, Peer, ?DEFAULT_CHANNEL),
            ?assertEqual(
                {fired, Peer, ?DEFAULT_CHANNEL},
                await_tagged(fired)
            )
    end.

%% =============================================================================
%% UTILS
%% =============================================================================

%% A declared capability must not answer `not_implemented'; a declined one
%% must answer exactly that. Either way it must not raise.
check(Mgr, Feature, Claims, Fun) ->
    Result = (catch Fun()),
    Ctx = lists:flatten(
        io_lib:format("~p / ~p returned ~p", [Mgr, Feature, Result])
    ),
    case maps:get(Feature, Claims) of
        true ->
            ?assertNotEqual({error, not_implemented}, Result, Ctx),
            ?assert(not is_exception(Result), Ctx);
        false ->
            ?assertEqual({error, not_implemented}, Result, Ctx)
    end.

answers(Fun) ->
    not is_exception(catch Fun()).

is_exception({'EXIT', _}) -> true;
is_exception(_) -> false.

delivers(Ref, Msg) ->
    ok == partisan:forward_message(Ref, Msg) andalso Msg == await(Msg).

%% The manager learns a peer is up from the `connected' signal its connection
%% process sends after the handshake; driving it directly avoids a real peer.
signal_connected(Mgr, Name, Channel) ->
    Pid = whereis(Mgr),
    Spec = #{name => Name, listen_addrs => [], channels => #{}},
    Pid ! {connected, Spec, Channel, undefined, undefined},
    _ = sys:get_state(Pid),
    ok.

await(Msg) ->
    receive
        Msg -> Msg
    after 2000 -> timeout
    end.

await_tagged(Tag) ->
    receive
        Event when element(1, Event) == Tag -> Event
    after 2000 -> timeout
    end.

start_with(Mgr) ->
    stop_partisan(),
    ok = application:set_env(partisan, peer_service_manager, Mgr),
    {ok, _} = application:ensure_all_started(partisan),
    ok.

cleanup(_) ->
    stop_partisan(),
    %% Every test module shares the VM, so the override must not leak.
    application:unset_env(partisan, peer_service_manager).

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.
