%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Deterministic simulation of the pure Thicket engine (PDDR-000004 §4).
%% Instantiates N engine states over a fully-connected overlay, routes their
%% `{send, ...}' actions between nodes, advances repair/summary ticks, and asserts
%% the two load-bearing invariants: full COVERAGE (every node delivers every
%% broadcast) and the INTERIOR-LOAD BOUND (no node interior in more than
%% max_load trees). This is the primary validator; an off-by-default engine is
%% invisible to Common Test.
%% @end
%% =============================================================================
-module(partisan_thicket_engine_test).

-include_lib("eunit/include/eunit.hrl").

-define(ENG, partisan_thicket_engine).

%% Basic dissemination with a generous cap behaves like a single tree: every
%% node must receive the message.
basic_coverage_test() ->
    {Eng, Ids} = run(6, #{max_load => 10, fanout => 3}, [{n1, m1}], 8),
    assert_coverage(Eng, Ids).

%% new/3 accepts the paper's recommended parameters and defaults, rejects
%% structurally-invalid values, and (deliberately) still permits the documented
%% not-recommended max_load=1 multi-tree config so the load bound can be exercised.
config_validation_test() ->
    Members = [n1, n2, n3],
    %% valid / recommended
    ?assertMatch(
        _, ?ENG:new(n1, Members, #{max_load => 7, fanout => 5, trees => 5})
    ),
    ?assertMatch(_, ?ENG:new(n1, Members, #{})),
    %% not recommended but permitted (load bound still holds; see load_bound_sweep)
    ?assertMatch(
        _, ?ENG:new(n1, Members, #{max_load => 1, fanout => 2, trees => 3})
    ),
    %% structurally invalid -> rejected
    ?assertError(
        {badarg, {max_load, 0}}, ?ENG:new(n1, Members, #{max_load => 0})
    ),
    ?assertError({badarg, {fanout, 1}}, ?ENG:new(n1, Members, #{fanout => 1})),
    ?assertError({badarg, {trees, 0}}, ?ENG:new(n1, Members, #{trees => 0})).

%% COVERAGE sweep (deterministic counterpart of prop_coverage_holds): with
%% max_load >= T over up to FOUR divergent trees — including cases where T > f, so
%% the trees cannot be interior-node-disjoint and §4.4 link-reassignment carries
%% coverage — every node delivers every message and the interior-load bound is
%% preserved, under the deterministic tick/delivery ordering.
coverage_sweep_test_() ->
    Configs = [
        %% {N, MaxLoad, Fanout, Sources}  (max_load >= T, T =< 4)
        {6, 1, 2, [n1]},
        {8, 2, 2, [n1, n5]},
        {10, 2, 2, [n1, n6]},
        {12, 3, 2, [n1, n5, n9]},
        {15, 3, 3, [n1, n6, n11]},
        %% T = 4 with f = 3 and f = 2 (T > f): interior-disjoint trees do not fit,
        %% so coverage relies on link-reassignment + load-capped branching.
        {15, 4, 3, [n1, n5, n9, n13]},
        {13, 4, 2, [n1, n4, n7, n10]}
    ],
    [sweep_case(N, ML, F, Srcs, coverage) || {N, ML, F, Srcs} <- Configs].

%% LOAD-BOUND sweep (deterministic counterpart of prop_load_bound_holds): under
%% genuinely BINDING caps (max_load < T) — outside the regime where coverage is
%% guaranteed — the interior-load bound is still never exceeded. The load-critical
%% safety invariant holds unconditionally, for every cap.
load_bound_sweep_test_() ->
    Configs = [
        {10, 1, 2, [n1, n6]},
        {12, 1, 3, [n1, n5, n9]},
        {12, 2, 2, [n1, n4, n7, n10]},
        {15, 3, 3, [n1, n5, n9, n13]},
        {13, 2, 2, [n1, n4, n7, n10, n12]}
    ],
    [sweep_case(N, ML, F, Srcs, load_bound) || {N, ML, F, Srcs} <- Configs].

sweep_case(N, ML, F, Srcs, Check) ->
    Label = lists:flatten(
        io_lib:format("N=~p max_load=~p f=~p T=~p", [N, ML, F, length(Srcs)])
    ),
    {Label, fun() ->
        {Eng, Ids} = run(
            N, #{max_load => ML, fanout => F}, [{S, ignore} || S <- Srcs], 0
        ),
        case Check of
            coverage -> assert_coverage(Eng, Ids);
            load_bound -> ok
        end,
        assert_load_bound(Eng, ML)
    end}.

%% Failure recovery: after a broadcast has covered the cluster, fail one of the
%% source's tree children, then broadcast a new message from the same source. The
%% repair mechanism (periodic summaries + grafts) must still deliver the new
%% message to every survivor — including nodes the initial flood had trimmed off
%% the tree — proving coverage is preserved across a mid-operation node failure.
neighbor_down_repair_test() ->
    N = 10,
    Opts = #{max_load => 3, fanout => 2},
    {Eng0, _} = run(N, Opts, [{n1, m1}], 20),
    Nodes = [node_name(I) || I <- lists:seq(1, N)],
    %% fail a node the source actively forwards to (a tree child of n1)
    Victim =
        case active_peers_of(n1, n1, Eng0) of
            [V | _] -> V;
            [] -> node_name(2)
        end,
    Survivors = Nodes -- [Victim],
    Eng1 = maps:map(
        fun
            (Nd, St) when Nd =/= Victim ->
                partisan_thicket_engine:neighbor_down(Victim, St);
            (_Nd, St) ->
                St
        end,
        Eng0
    ),
    %% new traffic on the same tree after the failure
    {ES, Actions} = partisan_thicket_engine:broadcast(
        m2, {payload, m2}, maps:get(n1, Eng1)
    ),
    Eng1b = Eng1#{n1 => ES},
    {Q, Store0} = resolve(n1, Actions, Eng1b, #{}),
    {Eng2, Store1} = drain(Q, Eng1b, Store0),
    {Eng3, _StoreF} = run_to_quiescence(Survivors, Eng2, Store1, 40 * N, 0),
    %% every survivor must have delivered the post-failure message
    maps:foreach(
        fun
            (Nd, St) when Nd =/= Victim ->
                Delivered = partisan_thicket_engine:delivered(St),
                ?assert(lists:member(m2, Delivered));
            (_Nd, _St) ->
                ok
        end,
        Eng3
    ).

%% Age-based GC must bound the announcement set to a sliding window: emitting many
%% more rounds from the sources does not grow it without limit. Pre-GC the set grew
%% roughly linearly with the round count (an announcement was retained for all time
%% once recorded), so a long run would accumulate far past this flat bound. We check
%% both that a long run stays under an absolute bound and that going from a short to
%% a long run does not increase the peak — i.e. the growth is genuinely flat in the
%% number of messages.
announcements_bounded_test() ->
    %% A genuinely BINDING cap (max_load < trees) keeps some nodes unable to attach
    %% to every tree, so they keep receiving summaries for ids they never deliver —
    %% the case where announcements actually accumulate and the pre-GC set grew with
    %% the round count.
    N = 12,
    Opts = #{max_load => 1, fanout => 2, trees => 3},
    Srcs = [n1, n5, n9],
    Short = max_announcements(N, Opts, Srcs, 8),
    Long = max_announcements(N, Opts, Srcs, 40),
    %% Flat in the number of messages: 5x the rounds does NOT grow the peak (a
    %% small slack absorbs ordering noise). Pre-GC, retaining every announcement
    %% for all time, the long run would be several times the short one and this
    %% would fail loudly.
    ?assert(Long =< Short + 4),
    %% ...and bounded by a fixed window, independent of the 40 rounds emitted.
    ?assert(Long =< 20).

%% Emit `Rounds' messages per source with settling, then report the largest
%% live-announcement count across all nodes.
max_announcements(N, Opts0, Srcs, Rounds) ->
    Nodes = [node_name(I) || I <- lists:seq(1, N)],
    Opts = Opts0#{trees => maps:get(trees, Opts0, length(Srcs))},
    Eng0 = maps:from_list([{Nd, ?ENG:new(Nd, Nodes, Opts)} || Nd <- Nodes]),
    Stream = [{S, {msg, S, R}} || R <- lists:seq(1, Rounds), S <- Srcs],
    {EngF, _Store} = lists:foldl(
        fun({Src, MsgId}, {Eng, Store}) ->
            {ES, Actions} = ?ENG:broadcast(
                MsgId, {payload, MsgId}, maps:get(Src, Eng)
            ),
            EngA = Eng#{Src => ES},
            {Q, StoreA} = resolve(Src, Actions, EngA, Store),
            {EngB, StoreB} = drain(Q, EngA, StoreA),
            run_to_quiescence(Nodes, EngB, StoreB, 8, 0)
        end,
        {Eng0, #{}},
        Stream
    ),
    lists:max([?ENG:announcement_count(St) || St <- maps:values(EngF)]).

%% =============================================================================
%% ASSERTIONS
%% =============================================================================

assert_coverage(Eng, Ids) ->
    maps:foreach(
        fun(Node, State) ->
            Delivered = ?ENG:delivered(State),
            Missing = [Id || Id <- Ids, not lists:member(Id, Delivered)],
            ?assertEqual({Node, []}, {Node, Missing})
        end,
        Eng
    ).

assert_load_bound(Eng, MaxLoad) ->
    maps:foreach(
        fun(Node, State) ->
            Load = ?ENG:interior_load(State),
            ?assert({Node, Load} =< {Node, MaxLoad} orelse Load =< MaxLoad)
        end,
        Eng
    ).

%% =============================================================================
%% SIMULATION
%% =============================================================================

%% Messages emitted per source and quiescence definition — Thicket is designed
%% for sustained broadcast, so a warm stream of traffic gives repair repeated
%% chances to attach every node to every tree (see prop_partisan_thicket_engine).
-define(ROUNDS, 3).
-define(QUIET_ROUNDS, 6).

%% Emit ?ROUNDS messages per source (sustained traffic) interleaved with repair
%% ticks, then run to quiescence. Deterministic tick ordering (no shuffle) — the
%% randomized-ordering counterpart lives in the PropEr model. `Sources' is a list
%% of {Node, _} pairs (the second element is ignored); returns {Engines, Ids}.
run(N, Opts0, Sources, _Ticks) ->
    Nodes = [node_name(I) || I <- lists:seq(1, N)],
    Srcs = lists:usort([Src || {Src, _} <- Sources]),
    Opts = Opts0#{trees => maps:get(trees, Opts0, length(Srcs))},
    Eng0 = maps:from_list([{Nd, ?ENG:new(Nd, Nodes, Opts)} || Nd <- Nodes]),
    Stream = [{S, {msg, S, R}} || R <- lists:seq(1, ?ROUNDS), S <- Srcs],
    {Eng1, Store1} = lists:foldl(
        fun({Src, MsgId}, {Eng, Store}) ->
            {ES, Actions} = ?ENG:broadcast(
                MsgId, {payload, MsgId}, maps:get(Src, Eng)
            ),
            EngA = Eng#{Src => ES},
            {Q, StoreA} = resolve(Src, Actions, EngA, Store),
            {EngB, StoreB} = drain(Q, EngA, StoreA),
            run_to_quiescence(Nodes, EngB, StoreB, 8, 0)
        end,
        {Eng0, #{}},
        Stream
    ),
    {EngF, _StoreF} = run_to_quiescence(Nodes, Eng1, Store1, 40 * N, 0),
    {EngF, [Id || {_Src, Id} <- Stream]}.

%% Tick until ?QUIET_ROUNDS consecutive rounds emit nothing, or fuel runs out.
run_to_quiescence(_Nodes, Eng, Store, 0, _Quiet) ->
    {Eng, Store};
run_to_quiescence(_Nodes, Eng, Store, _Fuel, ?QUIET_ROUNDS) ->
    {Eng, Store};
run_to_quiescence(Nodes, Eng0, Store0, Fuel, Quiet) ->
    {Eng1, Store1, Sent} = tick_round(Nodes, Eng0, Store0),
    NextQuiet =
        case Sent of
            0 -> Quiet + 1;
            _ -> 0
        end,
    run_to_quiescence(Nodes, Eng1, Store1, Fuel - 1, NextQuiet).

tick_round(Nodes, Eng0, Store0) ->
    lists:foldl(
        fun(Nd, {Eng, Store, Count}) ->
            {ES, Actions} = ?ENG:tick(maps:get(Nd, Eng)),
            EngA = Eng#{Nd => ES},
            {Q, StoreA} = resolve(Nd, Actions, EngA, Store),
            {EngB, StoreB} = drain(Q, EngA, StoreA),
            {EngB, StoreB, Count + length(Q)}
        end,
        {Eng0, Store0, 0},
        Nodes
    ).

%% Process a queue of {To, Msg} to quiescence, threading the mock handler store.
drain([], Eng, Store) ->
    {Eng, Store};
drain([{To, Msg} | Rest], Eng, Store) ->
    {ES, Actions} = ?ENG:handle(Msg, maps:get(To, Eng)),
    EngA = Eng#{To => ES},
    {Q, Store1} = resolve(To, Actions, EngA, Store),
    drain(Rest ++ Q, EngA, Store1).

%% Execute `From''s returned actions against the mock world: `send' enqueues a
%% message; `deliver' stores the payload in the delivering node's handler (a
%% per-node #{id => payload} map — the engine holds no payloads); a `fetch' models
%% the shell asking that handler for the payload (graft/1) and forwarding it as
%% data. Returns the produced send queue (in order) and the updated store.
resolve(From, Actions, _Eng, Store) ->
    {RevQ, Store1} = lists:foldl(
        fun
            ({send, To, Msg}, {Q, St}) ->
                {[{To, Msg} | Q], St};
            ({deliver, Id, Payload}, {Q, St}) ->
                Mine = maps:get(From, St, #{}),
                {Q, St#{From => Mine#{Id => Payload}}};
            ({fetch, To, Id, Tree, Load}, {Q, St}) ->
                case maps:get(From, St, #{}) of
                    #{Id := Payload} ->
                        {[{To, {data, Id, Payload, Tree, Load, From}} | Q], St};
                    _ ->
                        {Q, St}
                end
        end,
        {[], Store},
        Actions
    ),
    {lists:reverse(RevQ), Store1}.

node_name(I) ->
    list_to_atom("n" ++ integer_to_list(I)).

%% The active (tree) peers Node forwards to in Tree, sorted for determinism.
active_peers_of(Tree, Node, Eng) ->
    ordsets:to_list(
        partisan_thicket_engine:active_peers(Tree, maps:get(Node, Eng))
    ).
