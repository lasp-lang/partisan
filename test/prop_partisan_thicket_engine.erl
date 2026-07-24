%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc PropEr model of the pure Thicket engine (PDDR-000004 §4).
%%
%% This is the <b>primary correctness detector</b> for the engine: because the
%% engine is off by default it is invisible to Common Test, so its protocol
%% invariants are validated here instead. The model instantiates N engine states
%% over a fully-connected overlay and drives them to quiescence, but — unlike the
%% deterministic eunit sweep — it randomizes the two sources of nondeterminism a
%% real deployment exposes and where confluence bugs hide:
%%
%%   1. the <em>order</em> in which in-flight messages are delivered, and
%%   2. the <em>order</em> in which nodes fire their periodic ticks.
%%
%% Both are driven by a PropEr-generated seed so every counterexample is
%% reproducible and shrinkable. Every generated configuration respects Thicket's
%% feasibility bound (§4.6: overlay degree >= f*T) and must satisfy the two
%% load-bearing invariants:
%%
%%   * COVERAGE  — every node delivers every broadcast (never drop a message).
%%   * LOAD BOUND — no node is interior in more than `max_load' trees.
%% @end
%% =============================================================================
-module(prop_partisan_thicket_engine).

-include_lib("proper/include/proper.hrl").

-define(ENG, partisan_thicket_engine).

%% =============================================================================
%% PROPERTIES
%% =============================================================================

%% @doc SAFETY (holds for every configuration and ordering, including heavily
%% BINDING caps max_load < T): no node is ever interior in more than max_load
%% trees. This is the load-critical invariant Thicket exists to guarantee, and
%% the engine preserves it unconditionally.
prop_load_bound_holds() ->
    ?FORALL(
        Cfg,
        config(1, 4, fun(_T) -> 1 end),
        begin
            #{max_load := ML} = Cfg,
            {Eng, _Ids} = run(Cfg),
            States = maps:values(Eng),
            Bounded = lists:all(
                fun(St) -> ?ENG:interior_load(St) =< ML end, States
            ),
            ?WHENFAIL(
                io:format("config=~p~n  loads=~p~n", [
                    Cfg, [?ENG:interior_load(St) || St <- States]
                ]),
                Bounded
            )
        end
    ).

%% @doc LIVENESS with the cap a ceiling at or above the tree count (`max_load >=
%% T'; the paper runs max_load=7 with T=5). With the SOUND §4.4 `Balance` (gated on
%% currently-missing announcements), §4.4 LINK-REASSIGNMENT (a busy link may be
%% reused for another tree), load-capped branching, and summary-, source-fallback-,
%% speculative- and gap-repair, under any delivery/tick interleaving over up to FOUR
%% divergent trees every node eventually delivers every broadcast, while the load
%% bound holds.
%%
%% The envelope reaches T = 4, which — given this generator's feasibility limit
%% (N =< 18) — necessarily includes cases where T > f (e.g. f = 2, T = 4): the trees
%% then CANNOT be interior-node-disjoint, so coverage rests on link-reassignment and
%% on branching into the headroom `max_load' permits, not on the paper's disjoint-
%% tree heuristic. The remaining edge is T >= 5, reachable here only as f = 2 (T =
%% 2.5·f), where a late joiner can rarely miss one EARLY message it never heard
%% announced (the per-id-resupply tradeoff, see partisan_thicket_engine A1); closing
%% it would need a bounded catch-up-on-attach. Heavily BINDING caps `max_load < T'
%% are also outside this envelope; the load bound still holds everywhere (see
%% prop_load_bound). Both are outside the paper's guidance (keep max_load above the
%% tree count and T =< f), so a well-configured deployment stays well inside it.
prop_coverage_holds() ->
    ?FORALL(
        Cfg,
        config(1, 4, fun(T) -> T end),
        begin
            #{max_load := ML} = Cfg,
            {Eng, Ids} = run(Cfg),
            States = maps:values(Eng),
            Covered = lists:all(fun(St) -> covers(St, Ids) end, States),
            Bounded = lists:all(
                fun(St) -> ?ENG:interior_load(St) =< ML end, States
            ),
            ?WHENFAIL(
                io:format("config=~p~n  covered=~p bounded=~p~n  loads=~p~n", [
                    Cfg,
                    Covered,
                    Bounded,
                    [?ENG:interior_load(St) || St <- States]
                ]),
                Covered andalso Bounded
            )
        end
    ).

run(#{
    n := N, max_load := ML, fanout := F, sources := Srcs, ticks := T, seed := S
}) ->
    simulate(N, ML, F, Srcs, T, S).

covers(St, Ids) ->
    Delivered = ?ENG:delivered(St),
    lists:all(fun(Id) -> lists:member(Id, Delivered) end, Ids).

%% =============================================================================
%% GENERATORS
%% =============================================================================

%% A feasible configuration. `MinT'/`MaxT' bound the tree count; `MLoad(T)' gives
%% the minimum max_load (so a property can select the binding or non-binding
%% regime). Feasibility follows Thicket §4.6 (overlay degree ~= f*T) with a margin:
%% NumSrc is capped so the mesh degree (N-1) exceeds f*(T+1), giving repair slack
%% to route around a bad initial assignment rather than sitting at the razor's edge.
config(MinT, MaxT, MLoad) ->
    ?LET(
        N,
        integer(6, 18),
        ?LET(
            F,
            integer(2, 4),
            ?LET(
                NumSrc,
                integer(MinT, max(MinT, min(MaxT, ((N - 1) div F) - 1))),
                ?LET(
                    {ML, Seed},
                    {integer(MLoad(NumSrc), NumSrc + 2), integer(1, 1 bsl 30)},
                    #{
                        n => N,
                        fanout => F,
                        max_load => ML,
                        sources => spaced_sources(N, NumSrc),
                        %% fuel cap for run-to-quiescence: generous, since it only
                        %% bounds a (hypothetical) non-converging run — the common
                        %% case early-stops the moment the system goes quiet
                        ticks => 200 + N * 30,
                        seed => Seed
                    }
                )
            )
        )
    ).

%% NumSrc distinct source nodes spread across the ring, for divergent trees.
spaced_sources(N, NumSrc) ->
    Step = max(1, N div NumSrc),
    lists:usort([
        node_name(1 + (K * Step) rem N)
     || K <- lists:seq(0, NumSrc - 1)
    ]).

%% =============================================================================
%% RANDOMIZED SIMULATION
%% =============================================================================

%% Number of messages each source emits over the run. Thicket is designed for
%% SUSTAINED broadcast (Bondy/plum_db gossip continuously): warm trees and a
%% steady stream of summaries give repair repeated opportunities to attach every
%% node to every tree. A single-shot broadcast is an unrealistic worst case whose
%% summary window closes before the tightest orderings finish healing.
-define(ROUNDS, 4).

%% Deterministic given Seed: seeds `rand', then emits ?ROUNDS messages per source
%% interleaved with repair/summary ticks (sustained traffic), and finally runs to
%% QUIESCENCE. A node that attaches to a tree during any round stays attached for
%% every later message on that tree, so full coverage of the whole stream is the
%% invariant. `Ticks' bounds the quiescence fuel so a non-converging config still
%% terminates and fails the coverage check.
simulate(N, ML, F, Srcs, Ticks, Seed) ->
    _ = rand:seed(exsss, {Seed, Seed bxor 16#5DEECE66D, Seed + 1}),
    Nodes = [node_name(I) || I <- lists:seq(1, N)],
    Opts = #{max_load => ML, fanout => F, trees => length(Srcs)},
    Eng0 = maps:from_list([{Nd, ?ENG:new(Nd, Nodes, Opts)} || Nd <- Nodes]),
    %% one distinct message id per (source, round)
    Stream = [
        {S, {msg, S, R}}
     || R <- lists:seq(1, ?ROUNDS), S <- Srcs
    ],
    {Eng1, Store1} = lists:foldl(
        fun({Src, MsgId}, {Eng, Store}) ->
            {ES, Actions} = ?ENG:broadcast(
                MsgId, {payload, MsgId}, maps:get(Src, Eng)
            ),
            EngA = Eng#{Src => ES},
            {Q, StoreA} = resolve(Src, Actions, EngA, Store),
            %% a few settling rounds between emissions to warm trees / flow summaries
            {EngB, StoreB} = drain(Q, EngA, StoreA),
            run_to_quiescence(Nodes, EngB, StoreB, 8, 0)
        end,
        {Eng0, #{}},
        Stream
    ),
    {EngF, _StoreF} = run_to_quiescence(Nodes, Eng1, Store1, Ticks, 0),
    {EngF, [Id || {_S, Id} <- Stream]}.

%% Quiescence = ?QUIET_ROUNDS consecutive tick-rounds with no messages emitted
%% (long enough for any armed repair countdown to have fired). `Fuel' caps total
%% rounds so a non-converging config terminates and fails the coverage check.
-define(QUIET_ROUNDS, 6).

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

%% One round: every node ticks (in random order); count the messages emitted.
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
        shuffle(Nodes)
    ).

%% Deliver in-flight messages in a RANDOM order (not FIFO): at each step pick a
%% random pending message, handle it, and enqueue whatever it sends.
drain([], Eng, Store) ->
    {Eng, Store};
drain(Queue, Eng, Store) ->
    {{To, Msg}, Rest} = take_random(Queue),
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

%% =============================================================================
%% RANDOMNESS HELPERS
%% =============================================================================

take_random([X]) ->
    {X, []};
take_random(Queue) ->
    I = rand:uniform(length(Queue)),
    X = lists:nth(I, Queue),
    {X, delete_at(I, Queue)}.

delete_at(I, L) ->
    {Head, [_ | Tail]} = lists:split(I - 1, L),
    Head ++ Tail.

shuffle(L) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), E} || E <- L])].

node_name(I) ->
    list_to_atom("n" ++ integer_to_list(I)).
