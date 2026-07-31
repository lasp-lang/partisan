%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Benchmark scenarios. Reports numbers; asserts almost nothing.
%%
%% This is a measurement suite, not a test suite. It is not part of any gate and
%% is not run by `make test' — a machine-dependent number cannot be a pass/fail
%% condition, and wiring one into CI produces a suite that fails for reasons
%% nobody can act on. Run it deliberately with `make bench' and read the output.
%%
%% The one thing it *does* assert is the property that decides whether a run is
%% evidence at all: that a scenario's repetitions agree with each other
%% (`partisan_bench:meets_variance_gate/2'). A scenario whose own repetitions
%% disagree by more than the gate cannot resolve a change smaller than that
%% disagreement, so the failure is meaningful and actionable — rerun on a quiet
%% machine, or raise the iteration count.
%%
%% == What is being measured, and where ==
%%
%% Every scenario runs the driver **on a peer node**, not on the CT node. The
%% number of interest is what a sending application process experiences, and the
%% CT node is not a member of the Partisan cluster. `rpc:call/4' into the peer is
%% harness scaffolding over disterl; everything inside the measured function is
%% Partisan.
%%
%% == Reading the numbers ==
%%
%% Round-trip scenarios (`p2p', `acked', `rpc') measure an echo, so each sample
%% includes both directions plus the receiver's turnaround. They bound
%% end-to-end latency; they do not isolate send-side cost.
%%
%% The fan-out scenario measures from "broadcast issued" to "every peer has it".
%% It has to: `partisan_broadcast:broadcast/2' hands the message to the group
%% server and returns, so timing the call itself reports ~0 us and measures an
%% enqueue. The first version of this scenario did exactly that, and reported
%% 1.2M ops/s at a p50 of 0 us — a number that is both true and completely
%% uninformative about fan-out. Peers now acknowledge via the handler, so the
%% sample covers the work the batching actually changes.
%% @end
%% =============================================================================
-module(partisan_bench_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("partisan.hrl").
-include("partisan_test.hrl").

-compile([export_all, nowarn_export_all]).

%% `?SUPPORT' and `?DEFAULT_PEER_SERVICE_MANAGER' come from `partisan_test.hrl'.
-define(BENCH, partisan_bench).

%% A benchmark whose repetitions disagree by more than this cannot resolve a
%% change smaller than the disagreement.
-define(MAX_SPREAD_PCT, 25.0).

%% Deliberately modest. These run on whatever machine the developer has, in
%% ~minutes, and a scenario nobody runs measures nothing.
-define(ITERATIONS, 2000).
-define(WARMUP, 200).
-define(REPETITIONS, 3).

%% =============================================================================
%% CT CALLBACKS
%% =============================================================================

all() ->
    [
        fanout_diagnostic,
        p2p_roundtrip,
        p2p_roundtrip_large_payload,
        p2p_roundtrip_bounded,
        acked_roundtrip,
        rpc_throughput,
        rpc_under_a_slow_call,
        broadcast_fanout,
        broadcast_fanout_concurrent
    ].

suite() ->
    [{timetrap, {minutes, 30}}].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(Case, Config) ->
    ct:pal("Beginning benchmark: ~p", [Case]),
    [{hash, erlang:phash2({Case, Config})} | Config].

end_per_testcase(Case, Config) ->
    ?SUPPORT:stop(?TAKE_NODES(Case)),
    Config.

%% =============================================================================
%% T-0.2 — POINT-TO-POINT FAST PATH
%% =============================================================================

p2p_roundtrip(Config) ->
    run_p2p(p2p_roundtrip, Config, 100).

%% 100 KB, where encoding cost dominates and the A-1 change (encode in the
%% caller rather than the connection process) is expected to show.
p2p_roundtrip_large_payload(Config) ->
    run_p2p(p2p_roundtrip_large_payload, Config, 100 * 1024).

%% The same send path with the connection high-water mark **enabled**, which is
%% what makes `partisan_peer_connections:admit/2' call `process_info/2' on every
%% send. Compared against `p2p_roundtrip', this is the price of backpressure.
%%
%% It has to be measured rather than argued about: the default is `infinity',
%% which short-circuits before the BIF, so the default path is free by
%% construction — but that says nothing about the cost to anyone who turns the
%% bound on, which is the only configuration where it does anything.
%%
%% The mark is set far above anything this scenario can reach, so it measures the
%% *check*, not the refusal.
p2p_roundtrip_bounded(Config) ->
    {Node1, Node2} = two_nodes(p2p_roundtrip_bounded, Config),

    lists:foreach(
        fun(Node) ->
            ok = rpc:call(Node, partisan_config, set, [
                connection_high_watermark, 100000
            ])
        end,
        [Node1, Node2]
    ),

    Report = measure_on(Node1, #{
        name => p2p_roundtrip_bounded,
        setup => echo_setup(Node2, 100, #{}),
        teardown => fun echo_teardown/1,
        work => fun ?MODULE:echo_work/2,
        iterations => ?ITERATIONS,
        warmup => ?WARMUP,
        repetitions => ?REPETITIONS,
        concurrency => 1
    }),

    report(Report).

run_p2p(Case, Config, PayloadBytes) ->
    {Node1, Node2} = two_nodes(Case, Config),

    Report = measure_on(Node1, #{
        name => Case,
        setup => echo_setup(Node2, PayloadBytes, #{}),
        teardown => fun echo_teardown/1,
        work => fun ?MODULE:echo_work/2,
        iterations => ?ITERATIONS,
        warmup => ?WARMUP,
        repetitions => ?REPETITIONS,
        concurrency => 1
    }),

    report(Report).

%% =============================================================================
%% T-0.3 — ACKED PATH (the BATCH B baseline)
%% =============================================================================

acked_roundtrip(Config) ->
    {Node1, Node2} = two_nodes(acked_roundtrip, Config),

    Report = measure_on(Node1, #{
        name => acked_roundtrip,
        setup => echo_setup(Node2, 100, #{ack => true}),
        teardown => fun echo_teardown/1,
        work => fun ?MODULE:echo_work/2,
        iterations => ?ITERATIONS,
        warmup => ?WARMUP,
        repetitions => ?REPETITIONS,
        concurrency => 1
    }),

    report(Report).

%% =============================================================================
%% T-0.5 — RPC (the BATCH C baseline)
%% =============================================================================

%% Concurrent callers against one target. Before the re-platform every inbound
%% RPC was applied inline in one `gen_server' callback, so this number was
%% bounded by a single process regardless of how many callers there were.
rpc_throughput(Config) ->
    {Node1, Node2} = two_nodes(rpc_throughput, Config),

    lists:foreach(
        fun(Concurrency) ->
            Report = measure_on(Node1, #{
                name => lists:flatten(
                    io_lib:format("rpc_throughput_c~p", [Concurrency])
                ),
                setup => fun() -> Node2 end,
                work => fun ?MODULE:rpc_work/2,
                iterations => ?ITERATIONS,
                warmup => ?WARMUP,
                repetitions => ?REPETITIONS,
                concurrency => Concurrency
            }),
            report(Report)
        end,
        [1, 8]
    ).

%% **The head-of-line demonstration.** One caller holds a deliberately slow RPC
%% open on the target for the whole run while others issue fast ones.
%%
%% Against the pre-6.0.0 backend, which applied `M:F(A)' inline in its
%% `handle_info/2', every fast call queued behind the slow one and the tail was
%% the slow call's duration. With a worker per request the slow call delays only
%% itself, so p99 should be indistinguishable from `rpc_throughput'. That
%% comparison — this scenario's p99 against the plain one's — is the single most
%% informative pair of numbers in this suite.
rpc_under_a_slow_call(Config) ->
    {Node1, Node2} = two_nodes(rpc_under_a_slow_call, Config),

    Report = measure_on(Node1, #{
        name => rpc_under_a_slow_call,
        setup => fun ?MODULE:slow_rpc_setup/0,
        teardown => fun ?MODULE:slow_rpc_teardown/1,
        work => fun ?MODULE:slow_rpc_work/2,
        iterations => ?ITERATIONS,
        warmup => ?WARMUP,
        repetitions => ?REPETITIONS,
        concurrency => 4,
        %% Carried into the context by `slow_rpc_setup/0'.
        peer => Node2
    }),

    report(Report).

%% =============================================================================
%% T-0.4 — BROADCAST FAN-OUT (the A-2 baseline)
%% =============================================================================

%% Sender-side cost of one `broadcast/2', which is what the batching work
%% targets: the tree engine emits one action per peer, and each used to be
%% encoded separately.
%%
%% **On node count.** The plan asks for N in {5, 25, 100}. Five is what this runs
%% by default, because 25 and 100 Erlang nodes on one developer machine measure
%% that machine's scheduler contention rather than Partisan's fan-out cost.
%% Override with `BENCH_NODES` on a machine that can host them; the number is
%% recorded in the report either way, so no reading of this scenario can be
%% detached from the size it was taken at.
broadcast_fanout(Config) ->
    NumNodes = env_int("BENCH_NODES", 5),

    Servers = ?SUPPORT:node_list(1, "server", Config),
    Clients = ?SUPPORT:node_list(NumNodes - 1, "client", Config),

    Nodes = ?SUPPORT:start(
        broadcast_fanout,
        Config,
        [
            {peer_service_manager, ?DEFAULT_PEER_SERVICE_MANAGER},
            {servers, Servers},
            {clients, Clients}
        ]
    ),

    ?PUT_NODES(Nodes),
    ?PAUSE_FOR_CLUSTERING,

    %% Every node needs the handler and its broadcast group: the sender to emit,
    %% the peers to receive without erroring. Started at runtime rather than via
    %% `broadcast_mods', which is only read when the application starts.
    lists:foreach(
        fun({_, Node}) ->
            ok = rpc:call(Node, partisan_bench_handler, start, []),
            {ok, _} = rpc:call(Node, partisan_broadcast, start_group, [
                partisan_bench_handler
            ])
        end,
        Nodes
    ),

    [{_, Node1} | _] = Nodes,

    ok = rpc:call(Node1, persistent_term, put, [
        {?MODULE, fanout_peers}, NumNodes - 1
    ]),

    Report = measure_on(Node1, #{
        name => lists:flatten(
            io_lib:format("broadcast_fanout_n~p", [NumNodes])
        ),
        setup => fun ?MODULE:fanout_setup/0,
        teardown => fun ?MODULE:fanout_teardown/1,
        work => fun ?MODULE:broadcast_work/2,
        %% Fan-out is far more expensive per operation than a point-to-point
        %% send, and every operation costs every peer, so this runs fewer.
        iterations => 500,
        warmup => 50,
        repetitions => ?REPETITIONS,
        concurrency => 1
    }),

    report(Report).

%% Not a benchmark: a guard that the fan-out plumbing works at all, run before
%% anything tries to time it.
%%
%% It exists because two separate defects made the fan-out scenario report
%% confident, stable, meaningless numbers. First the payload did not match the
%% handler, so every broadcast raised inside the callback and was cancelled — the
%% scenario measured the cost of failing. Then the handler exported `claim/2'
%% without registering a process under its own name, so the group's off-path
%% apply cast went nowhere and `handle_broadcast/2' was never called — peers
%% recorded everything and acknowledged nothing.
%%
%% Neither showed up as a benchmark failure. Both show up here immediately.
fanout_diagnostic(Config) ->
    NumNodes = 3,
    Servers = ?SUPPORT:node_list(1, "server", Config),
    Clients = ?SUPPORT:node_list(NumNodes - 1, "client", Config),

    Nodes = ?SUPPORT:start(
        fanout_diagnostic,
        Config,
        [
            {peer_service_manager, ?DEFAULT_PEER_SERVICE_MANAGER},
            {servers, Servers},
            {clients, Clients}
        ]
    ),

    ?PUT_NODES(Nodes),
    ?PAUSE_FOR_CLUSTERING,

    lists:foreach(
        fun({_, Node}) ->
            ok = rpc:call(Node, partisan_bench_handler, start, []),
            R = rpc:call(Node, partisan_broadcast, start_group, [
                partisan_bench_handler
            ]),
            ct:pal("start_group on ~p -> ~p", [Node, R])
        end,
        Nodes
    ),

    [{_, Node1} | _] = Nodes,

    ct:pal("groups on ~p: ~p", [
        Node1, rpc:call(Node1, partisan_broadcast, groups, [])
    ]),
    ct:pal("members on ~p: ~p", [
        Node1, rpc:call(Node1, partisan_membership, members, [])
    ]),

    ok = rpc:call(Node1, partisan_broadcast, broadcast, [
        {1, <<"diag">>}, partisan_bench_handler
    ]),

    timer:sleep(3000),

    %% Every peer must have recorded the broadcast. The origin does not claim
    %% its own, so it is expected to hold nothing.
    lists:foreach(
        fun({_, Node}) ->
            Size = rpc:call(Node, ets, info, [
                partisan_bench_handler_seen, size
            ]),
            case Node of
                Node1 -> ?assertEqual(0, Size);
                _ -> ?assertEqual(1, Size)
            end
        end,
        Nodes
    ),

    %% And the notify payload the benchmark actually uses must come back from
    %% every peer. This is the assertion the second defect would have failed.
    Expected = NumNodes - 1,
    Got = rpc:call(Node1, ?MODULE, diag_notify, [Expected], 30000),
    ct:pal("notifications received by origin: ~p (expected ~p)", [
        Got, Expected
    ]),
    ?assertEqual(Expected, Got),

    ok.

%% Runs on the origin: broadcasts a notify payload and counts acknowledgements.
diag_notify(Expected) ->
    Id = erlang:unique_integer([monotonic, positive]),
    Self = partisan:self(),
    Node = partisan:node(),
    ok = partisan_broadcast:broadcast(
        {Id, {notify, Self, Node, <<"diag">>}}, partisan_bench_handler
    ),
    diag_collect(Id, Expected, 0).

diag_collect(_Id, Expected, N) when N >= Expected ->
    N;
diag_collect(Id, Expected, N) ->
    receive
        {fanout_seen, Id} -> diag_collect(Id, Expected, N + 1)
    after 5000 ->
        N
    end.

%% Does one broadcast group scale with the number of processes broadcasting into
%% it, or is its `gen_server' the ceiling?
%%
%% This is the measurement E-4 rests on. Every `broadcast/2', every inbound
%% gossip message and every lazy tick for a group funnels through one process, so
%% the *claim* is that a single hot group is a bottleneck that neither channels
%% nor tree engines can relieve — the proposed remedy being to shard a group
%% across several trees. That remedy is expensive and irreversible enough to be
%% worth evidence first.
%%
%% Read it against `broadcast_fanout' (concurrency 1). If throughput is flat as
%% concurrency rises, the group server is saturated and E-4 has a case. If it
%% rises, the limit is elsewhere — the network, the peers, or the handler — and
%% sharding would move work without removing a constraint.
broadcast_fanout_concurrent(Config) ->
    NumNodes = env_int("BENCH_NODES", 5),

    Servers = ?SUPPORT:node_list(1, "server", Config),
    Clients = ?SUPPORT:node_list(NumNodes - 1, "client", Config),

    Nodes = ?SUPPORT:start(
        broadcast_fanout_concurrent,
        Config,
        [
            {peer_service_manager, ?DEFAULT_PEER_SERVICE_MANAGER},
            {servers, Servers},
            {clients, Clients}
        ]
    ),

    ?PUT_NODES(Nodes),
    ?PAUSE_FOR_CLUSTERING,

    lists:foreach(
        fun({_, Node}) ->
            ok = rpc:call(Node, partisan_bench_handler, start, []),
            {ok, _} = rpc:call(Node, partisan_broadcast, start_group, [
                partisan_bench_handler
            ])
        end,
        Nodes
    ),

    [{_, Node1} | _] = Nodes,

    ok = rpc:call(Node1, persistent_term, put, [
        {?MODULE, fanout_peers}, NumNodes - 1
    ]),
    ok = rpc:call(Node1, persistent_term, put, [
        {?MODULE, fanout_peer_nodes}, [N || {_, N} <- Nodes, N =/= Node1]
    ]),

    lists:foreach(
        fun(Concurrency) ->
            Report = measure_on(Node1, #{
                name => lists:flatten(
                    io_lib:format(
                        "broadcast_fanout_n~p_c~p", [NumNodes, Concurrency]
                    )
                ),
                setup => fun ?MODULE:fanout_setup/0,
                teardown => fun ?MODULE:fanout_teardown/1,
                work => fun ?MODULE:broadcast_work/2,
                %% Higher than `broadcast_fanout' deliberately. At concurrency 8
                %% a 500-iteration repetition is ~60 operations per worker and
                %% lasts tens of milliseconds, which is short enough for startup
                %% effects to dominate — the first attempt at this scenario ran
                %% at 500 and reported a 59% spread, i.e. no result at all.
                iterations => 4000,
                warmup => 400,
                repetitions => ?REPETITIONS,
                concurrency => Concurrency
            }),
            report_with_backlog(Node1, Report)
        end,
        [1, 4, 8]
    ).

%% =============================================================================
%% WORK FUNCTIONS — these run on the sending peer node
%% =============================================================================
%%
%% They are exported and referenced as `fun ?MODULE:f/N' rather than written
%% inline as closures. A closure carries its defining module's version, so a
%% recompile between the CT node and the peer makes it undefined there; a named
%% external fun only needs the module loaded, which it is.

%% Builds a context of `{Peer, ReceiverRef, Payload, Opts}' with an echo process
%% registered on the peer.
echo_setup(Peer, PayloadBytes, Opts) ->
    fun() ->
        Payload = crypto:strong_rand_bytes(PayloadBytes),
        Ref = rpc:call(Peer, ?MODULE, start_echo, []),
        {Peer, Ref, Payload, Opts}
    end.

echo_teardown({Peer, _Ref, _Payload, _Opts}) ->
    _ = rpc:call(Peer, ?MODULE, stop_echo, []),
    ok.

%% One operation: send to the peer's echo process and wait for the reply. The
%% sample therefore covers both directions plus the peer's turnaround.
echo_work({Peer, _Ref, Payload, Opts}, _Seq) ->
    Self = partisan:self(),
    ok = partisan:forward_message(
        Peer,
        partisan_bench_echo,
        {echo, Self, Payload},
        Opts#{channel => partisan:default_channel()}
    ),
    receive
        {echo_reply, _} -> ok
    after 30000 ->
        error(echo_timeout)
    end.

%% Context is `{PeersToAwait, Payload}'.
fanout_setup() ->
    {persistent_term:get({?MODULE, fanout_peers}), <<"bench">>}.

%% Samples the broadcast group server's mailbox depth once the repetition's
%% measured operations are done.
%%
%% This is what lets the scenario **attribute** a stall instead of merely
%% reporting one. Every `broadcast/2' is a cast into that one process, which then
%% encodes and fans out; if it cannot keep up, its mailbox grows without bound and
%% latency degrades with the length of the run. A deep queue here is direct
%% evidence for the premise E-4 rests on — that one group's `gen_server' is the
%% ceiling — and a queue at zero rules it out and sends the search elsewhere.
%%
%% Sampled in teardown rather than during the run so it costs the measurement
%% nothing.
fanout_teardown(_Ctx) ->
    Group = partisan_broadcast:group_name(partisan_bench_handler),
    Local = mailbox_depth(Group),

    %% And the *handler* process on each peer. Every applied broadcast funnels
    %% through one `partisan_bench_handler' per node before it acknowledges, so
    %% if the group server is keeping up but latency still degrades with run
    %% length, this is where to look next. Sampled over disterl, which is harness
    %% scaffolding and outside the measured region.
    Peers = persistent_term:get({?MODULE, fanout_peer_nodes}, []),
    Remote = [
        {N, rpc:call(N, ?MODULE, mailbox_depth, [partisan_bench_handler])}
     || N <- Peers
    ],

    Seen = persistent_term:get({?MODULE, group_backlog}, []),
    ok = persistent_term:put(
        {?MODULE, group_backlog}, [{Local, Remote} | Seen]
    ),
    ok.

mailbox_depth(Name) ->
    case erlang:whereis(Name) of
        undefined ->
            undefined;
        Pid ->
            {message_queue_len, N} = erlang:process_info(Pid, message_queue_len),
            N
    end.

rpc_work(Peer, _Seq) ->
    ok = partisan_erpc:call(Peer, ?MODULE, noop, [], 30000).

%% Holds one slow RPC open on the target for the duration, then measures fast
%% ones alongside it.
slow_rpc_setup() ->
    Peer = persistent_term:get({?MODULE, peer}),
    Holder = spawn(fun() -> hold_slow_rpc(Peer) end),
    {Peer, Holder}.

slow_rpc_teardown({_Peer, Holder}) ->
    exit(Holder, kill),
    ok.

slow_rpc_work({Peer, _Holder}, _Seq) ->
    ok = partisan_erpc:call(Peer, ?MODULE, noop, [], 30000).

hold_slow_rpc(Peer) ->
    %% Long enough to span a repetition; the caller is killed in teardown.
    catch partisan_erpc:call(Peer, timer, sleep, [60000], 120000),
    hold_slow_rpc(Peer).

%% One operation: broadcast a novel message and wait until **every peer** has
%% received it.
%%
%% The wait is the point. `partisan_broadcast:broadcast/2' hands the message to
%% the group server and returns immediately, so timing the call alone reports
%% ~0 us and measures an enqueue. What the fan-out work changes is the cost of
%% reaching the peer set, and that is only observable once the peers say so.
%%
%% A unique id per operation keeps every broadcast novel; a reused id would be
%% discarded as stale by the peers and turn this into a measurement of the dedup
%% path.
broadcast_work({Expected, Payload}, _Seq) ->
    Id = erlang:unique_integer([monotonic, positive]),
    Self = partisan:self(),
    Node = partisan:node(),

    ok = partisan_broadcast:broadcast(
        {Id, {notify, Self, Node, Payload}}, partisan_bench_handler
    ),

    await_fanout(Id, Expected).

%% Two clauses on purpose.
%%
%% A single clause matching the bound `Id' is a selective receive that scans past
%% anything not matching, so a straggling acknowledgement from an earlier
%% operation would make every subsequent wait rescan it. A worker only ever has
%% one broadcast outstanding, so in the healthy case nothing non-matching is
%% there and the cost is the same either way — but the second clause makes that
%% *guaranteed* rather than incidental, and turns a straggler into a loud error
%% instead of a silent miscount against the current operation.
await_fanout(_Id, 0) ->
    ok;
await_fanout(Id, N) ->
    receive
        {fanout_seen, Id} ->
            await_fanout(Id, N - 1);
        {fanout_seen, Other} ->
            error({stray_acknowledgement, [{expected, Id}, {got, Other}]})
    after 30000 ->
        error({fanout_timeout, Id, N})
    end.

%% =============================================================================
%% APPLIED ON THE TARGET NODE
%% =============================================================================

noop() ->
    ok.

start_echo() ->
    Pid = spawn(fun echo_loop/0),
    true = erlang:register(partisan_bench_echo, Pid),
    Pid.

stop_echo() ->
    case erlang:whereis(partisan_bench_echo) of
        undefined -> ok;
        Pid -> exit(Pid, kill)
    end,
    ok.

echo_loop() ->
    receive
        {echo, From, _Payload} ->
            _ = partisan:forward_message(From, {echo_reply, ok}, #{
                channel => partisan:default_channel()
            }),
            echo_loop();
        _ ->
            echo_loop()
    end.

%% =============================================================================
%% PRIVATE
%% =============================================================================

two_nodes(Case, Config) ->
    Servers = ?SUPPORT:node_list(1, "server", Config),
    Clients = ?SUPPORT:node_list(1, "client", Config),

    Nodes = ?SUPPORT:start(
        Case,
        Config,
        [
            {peer_service_manager, ?DEFAULT_PEER_SERVICE_MANAGER},
            {servers, Servers},
            {clients, Clients}
        ]
    ),

    ?PUT_NODES(Nodes),
    ?PAUSE_FOR_CLUSTERING,

    [{_, Node1}, {_, Node2}] = Nodes,
    {Node1, Node2}.

%% Runs the driver on `Node'. `peer' in the spec, if present, is stashed in a
%% `persistent_term' there so setup functions can reach it without the spec
%% having to carry a closure.
measure_on(Node, Spec0) ->
    Spec =
        case maps:take(peer, Spec0) of
            {Peer, Rest} ->
                ok = rpc:call(Node, persistent_term, put, [
                    {?MODULE, peer}, Peer
                ]),
                Rest;
            error ->
                Spec0
        end,

    case rpc:call(Node, ?BENCH, run, [Spec], infinity) of
        {badrpc, Reason} ->
            ct:fail({benchmark_failed, Reason});
        Report ->
            Report
    end.

%% Prints the result and enforces the only assertion this suite makes: that the
%% run's repetitions agree well enough for the numbers to mean anything.
%% Reports the scenario, then the group server's mailbox depth sampled at the end
%% of each repetition — the number that says whether the group server is the
%% bottleneck or merely downstream of one.
report_with_backlog(Node, Report) ->
    Backlog = rpc:call(Node, persistent_term, get, [
        {?MODULE, group_backlog}, []
    ]),
    ok = rpc:call(Node, persistent_term, put, [{?MODULE, group_backlog}, []]),

    ct:pal(
        "    mailbox depths at end of each repetition, "
        "{group server, [{peer, handler}]}:~n      ~p~n"
        "    (a deep or growing queue names the bottleneck; all zero means the "
        "limit is neither the group server nor the handlers)",
        [lists:reverse(Backlog)]
    ),

    report(Report).

report(Report) ->
    ct:pal("~s", [?BENCH:format_report(Report)]),

    Spread = maps:get(spread_pct, Report),

    case ?BENCH:meets_variance_gate(Report, ?MAX_SPREAD_PCT) of
        true ->
            ok;
        false ->
            ct:fail(
                {unstable_benchmark, [
                    {name, maps:get(name, Report)},
                    {spread_pct, Spread},
                    {max_spread_pct, ?MAX_SPREAD_PCT},
                    {throughput_ops, maps:get(throughput_ops, Report)},
                    {note,
                        "Repetitions disagree by more than the gate; this "
                        "result cannot resolve a change smaller than that. "
                        "Rerun on an otherwise idle machine, or raise the "
                        "iteration count."}
                ]}
            )
    end,

    Report.

env_int(Name, Default) ->
    case os:getenv(Name) of
        false -> Default;
        "" -> Default;
        V -> list_to_integer(V)
    end.
