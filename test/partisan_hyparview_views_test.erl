%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Exercises the HyParView manager's active-view bounds, its reserved
%% slots, its membership-snapshot publication and its X-BOT round.
%%
%% Each case runs on a peer node of its own. The case starts
%% `partisan_hyparview_peer_service_manager' in-process, stores stand-in
%% connection processes from inside the manager, which owns the protected
%% `partisan_peer_connections' table, and then delivers protocol messages with
%% `receive_message/3'. A stand-in answers every `{send_message, Msg}' call
%% and records `Msg', so a case can assert what the manager sent. Periodic
%% shuffles, promotions and X-BOT rounds are disabled, so only the delivered
%% messages change the views.
%%
%% `active_max_size' counts the manager's own entry, which the active view
%% always holds.
%%
%% Not covered here: real connections, the paper's random walks across several
%% nodes, and the X-BOT messages other than the round itself.
%% @end
%% =============================================================================
-module(partisan_hyparview_views_test).

-include_lib("eunit/include/eunit.hrl").

-define(M, partisan_hyparview_peer_service_manager).
-define(CASES, [
    reserved_slot_admits_its_tag_on_neighbor,
    reserved_slot_admits_its_tag_on_join_without_eviction,
    join_never_exceeds_active_max_size,
    refused_join_is_still_forwarded,
    refused_high_priority_neighbor_request_is_rejected,
    reservation_is_emptied_when_its_peer_exits,
    snapshot_published_once_when_neighbor_adds,
    snapshot_not_published_when_nothing_changes,
    snapshot_published_once_when_exit_removes,
    snapshot_published_once_when_disconnect_removes,
    xbot_round_leaves_manager_responsive
]).

-export([reserved_slot_admits_its_tag_on_neighbor/0]).
-export([reserved_slot_admits_its_tag_on_join_without_eviction/0]).
-export([join_never_exceeds_active_max_size/0]).
-export([refused_join_is_still_forwarded/0]).
-export([refused_high_priority_neighbor_request_is_rejected/0]).
-export([reservation_is_emptied_when_its_peer_exits/0]).
-export([snapshot_published_once_when_neighbor_adds/0]).
-export([snapshot_not_published_when_nothing_changes/0]).
-export([snapshot_published_once_when_exit_removes/0]).
-export([snapshot_published_once_when_disconnect_removes/0]).
-export([xbot_round_leaves_manager_responsive/0]).

%% =============================================================================
%% TEST DESCRIPTORS
%% =============================================================================

views_test_() ->
    [
        {atom_to_list(Case),
            {setup, fun() -> start_peer(Case) end, fun peer:stop/1,
                fun(Peer) ->
                    {timeout, 60,
                        ?_test(peer:call(Peer, ?MODULE, Case, [], 60000))}
                end}}
     || Case <- ?CASES
    ].

%% The X-BOT case needs Erlang distribution, because the oracle pings over it.
start_peer(xbot_round_leaves_manager_responsive) ->
    start_peer(#{name => peer:random_name(?MODULE), host => "127.0.0.1",
        longnames => true});
start_peer(Case) when is_atom(Case) ->
    start_peer(#{});
start_peer(Opts0) ->
    Paths = lists:append([["-pa", Dir] || Dir <- code:get_path()]),
    Opts = Opts0#{connection => standard_io, args => Paths},
    {ok, Peer, _} = peer:start_link(Opts),
    {ok, _} = peer:call(Peer, application, ensure_all_started, [telemetry]),
    ok = peer:call(Peer, partisan_config, init, []),
    Peer.

%% =============================================================================
%% CASES: RESERVED SLOTS AND THE SIZE BOUND
%% =============================================================================

%% The open slot reserved for `server' is S's own slot, so it must not count
%% against S.
reserved_slot_admits_its_tag_on_neighbor() ->
    start(3, [server]),
    A = peer(a),
    S = peer(s),
    neighbor(A, undefined),
    neighbor(S, server),
    ?assertEqual([a, s], active()),
    ?assertEqual(#{server => s}, reserved()).

%% As above on the `join' path, which may evict: S's slot is open, so A stays.
reserved_slot_admits_its_tag_on_join_without_eviction() ->
    start(3, [server]),
    A = peer(a),
    S = peer(s),
    neighbor(A, undefined),
    join(S, server),
    ?assertEqual([a, s], active()),
    ?assertEqual(#{server => s}, reserved()).

%% Every other active peer holds a reserved slot, so there is no one to drop.
join_never_exceeds_active_max_size() ->
    start(3, [s1, s2]),
    join(peer(s1), s1),
    join(peer(s2), s2),
    join(peer(b), undefined),
    ?assert(length(active()) + 1 =< 3),
    ?assertEqual([s1, s2], active()).

%% A join this node cannot hold still walks on, so the joiner lands elsewhere
%% (Leitao et al., DSN'07, §4.2).
refused_join_is_still_forwarded() ->
    start(3, [s1, s2]),
    S1 = peer(s1),
    S2 = peer(s2),
    join(S1, s1),
    join(S2, s2),
    join(peer(b), undefined),
    Sent = sent(S1) ++ sent(S2),
    ?assert(lists:any(fun(M) -> is_forward_join_of(b, M) end, Sent)).

%% A high-priority request is accepted unless no one can be dropped; then the
%% requester is told so and tries another passive peer (DSN'07, §4.3).
refused_high_priority_neighbor_request_is_rejected() ->
    start(3, [s1, s2]),
    join(peer(s1), s1),
    join(peer(s2), s2),
    B = peer(b),
    send(B, {neighbor_request, B, high, undefined, undefined, []}),
    ?assertEqual([s1, s2], active()),
    ?assert(lists:any(fun is_neighbor_rejected/1, sent(B))).

%% When a reserved peer's only connection exits, its slot opens again, and the
%% next peer with that tag fills it.
reservation_is_emptied_when_its_peer_exits() ->
    start(3, [s1]),
    S1 = peer(s1),
    join(S1, s1),
    ?assertEqual(#{s1 => s1}, reserved()),
    exit_connection(S1),
    ?assertEqual([], active()),
    ?assertEqual(#{s1 => undefined}, reserved()),
    join(peer(t), s1),
    ?assertEqual(#{s1 => t}, reserved()).

%% =============================================================================
%% CASES: MEMBERSHIP SNAPSHOT
%% =============================================================================

snapshot_published_once_when_neighbor_adds() ->
    start(5, []),
    A = peer(a),
    ?assertEqual({true, 1}, publications(fun() -> neighbor(A, undefined) end)).

snapshot_not_published_when_nothing_changes() ->
    start(5, []),
    A = peer(a),
    neighbor(A, undefined),
    ForwardJoin = {forward_join, A, undefined, 1, 0, A},
    ?assertEqual({false, 0}, publications(fun() -> send(A, ForwardJoin) end)).

snapshot_published_once_when_exit_removes() ->
    start(5, []),
    A = peer(a),
    neighbor(A, undefined),
    ?assertEqual({true, 1}, publications(fun() -> exit_connection(A) end)).

snapshot_published_once_when_disconnect_removes() ->
    start(5, []),
    B = peer(b),
    neighbor(B, undefined),
    Disconnect = {disconnect, B, {1, 1}},
    ?assertEqual({true, 1}, publications(fun() -> send(B, Disconnect) end)).

%% =============================================================================
%% CASES: X-BOT
%% =============================================================================

%% A full active view and a passive candidate make the round consult the
%% oracle. The candidate's address does not route, so a ping to it cannot
%% answer soon. The manager must survive the round and keep answering calls.
xbot_round_leaves_manager_responsive() ->
    start(3, [], #{xbot_enabled => true}),
    neighbor(peer(a), undefined),
    neighbor(peer(b), undefined),
    Candidate = #{
        name => 'c@10.255.255.1',
        listen_addrs => [#{ip => {10, 255, 255, 1}, port => 9}],
        channels => #{}
    },
    ok = gen_server:call(?M, {update_members, [Candidate]}),
    Pid = whereis(?M),
    unlink(Pid),
    Pid ! xbot_execution,
    Reply =
        try gen_server:call(?M, active, 1000) of
            Result -> Result
        catch
            exit:Reason -> {exit, Reason}
        end,
    ?assertMatch({ok, _}, Reply),
    ?assertEqual(Pid, whereis(?M)).

%% =============================================================================
%% HELPERS
%% =============================================================================

start(Max, Reservations) ->
    start(Max, Reservations, #{}).

start(Max, Reservations, Extra) ->
    Quiet = #{
        active_max_size => Max,
        random_promotion => false,
        random_promotion_interval => 3600000,
        shuffle_interval => 3600000,
        xbot_interval => 3600000
    },
    HyParView = partisan_config:get(hyparview),
    ok = partisan_config:set(hyparview, maps:merge(HyParView, maps:merge(Quiet, Extra))),
    ok = partisan_config:set(reservations, Reservations),
    _ = ets:new(partisan_membership, [public, named_table, set]),
    {ok, _} = ?M:start_link(),
    ok.

%% A node spec for `Short' with one stand-in connection on the default
%% channel, stored by the manager because it owns the connections table.
peer(Short) ->
    Port = 30000 + erlang:phash2(Short, 10000),
    Addr = #{ip => {127, 0, 0, 1}, port => Port},
    Spec = #{name => name(Short), listen_addrs => [Addr], channels => #{}},
    Conn = spawn(fun() -> stand_in([]) end),
    Store = fun(S) ->
        ok = partisan_peer_connections:store(Spec, Conn, undefined, Addr),
        S
    end,
    _ = sys:replace_state(?M, Store),
    put({conn, Short}, Conn),
    Spec.

name(Short) ->
    list_to_atom(atom_to_list(Short) ++ "@127.0.0.1").

short(#{name := Name}) ->
    [Short, _] = string:split(atom_to_list(Name), "@"),
    list_to_atom(Short).

stand_in(Sent) ->
    receive
        {'$gen_call', From, {send_message, Msg}} ->
            gen_server:reply(From, ok),
            stand_in([Msg | Sent]);
        {'$gen_call', From, sent} ->
            gen_server:reply(From, lists:reverse(Sent)),
            stand_in(Sent);
        {'$gen_call', From, _} ->
            gen_server:reply(From, ok),
            stand_in(Sent);
        {system, _, _} ->
            exit(normal);
        _ ->
            stand_in(Sent)
    end.

sent(Spec) ->
    gen_server:call(get({conn, short(Spec)}), sent).

send(Spec, Msg) ->
    ok = ?M:receive_message(Spec, undefined, Msg),
    sync().

neighbor(Spec, Tag) ->
    send(Spec, {neighbor, Spec, Tag, undefined, partisan:node_spec()}).

join(Spec, Tag) ->
    send(Spec, {join, Spec, Tag, 1}).

exit_connection(Spec) ->
    whereis(?M) ! {'EXIT', get({conn, short(Spec)}), normal},
    sync().

sync() ->
    _ = sys:get_state(?M),
    ok.

%% Peers in the active view, by short name, without the manager's own entry.
active() ->
    {ok, Active} = ?M:active(),
    Myself = partisan:node(),
    lists:sort([short(S) || #{name := N} = S <- sets:to_list(Active), N =/= Myself]).

reserved() ->
    State = sys:get_state(?M),
    Reserved = field(reserved, State),
    maps:map(
        fun(_, undefined) -> undefined; (_, Spec) -> short(Spec) end,
        Reserved
    ).

%% Whether `Fun' changed the active view, and how many times it published.
publications(Fun) ->
    Active0 = active(),
    Version0 = partisan_membership:version(),
    Fun(),
    {active() =/= Active0, partisan_membership:version() - Version0}.

is_forward_join_of(Short, Msg) when element(1, Msg) =:= forward_join ->
    short(element(2, Msg)) =:= Short;
is_forward_join_of(_, _) ->
    false.

is_neighbor_rejected(Msg) ->
    element(1, Msg) =:= neighbor_rejected.

%% Reads a field of the manager's state record by name.
field(Name, State) ->
    {ok, {_, [{abstract_code, {_, Forms}}]}} =
        beam_lib:chunks(code:which(?M), [abstract_code]),
    [Fields] = [F || {attribute, _, record, {state, F}} <- Forms],
    Names = [record_field_name(F) || F <- Fields],
    Index = length(lists:takewhile(fun(N) -> N =/= Name end, Names)) + 2,
    element(Index, State).

record_field_name({typed_record_field, F, _}) -> record_field_name(F);
record_field_name({record_field, _, {atom, _, N}}) -> N;
record_field_name({record_field, _, {atom, _, N}, _}) -> N.
