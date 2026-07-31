%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests for `partisan_erpc' over the Partisan transport.
%%
%% Upstream `erpc' reaches the peer with `erlang:spawn_request/5' and receives
%% the result as the exit reason of a distributed monitor — both disterl
%% mechanisms. This module now issues an explicit correlated request to
%% `partisan_rpc_backend' and receives an explicit reply; the monitor is kept
%% only to detect that the target became unreachable.
%%
%% These run on a single node. `partisan_erpc:call/5' only takes its local
%% short-circuit when the timeout is `infinity', so a self-directed call with a
%% *finite* timeout exercises the whole transport path — forward_message,
%% backend, worker, correlated reply — without needing a cluster.
%% @end
%% =============================================================================
-module(partisan_erpc_test).

-include_lib("eunit/include/eunit.hrl").

%% Applied by the tests.
-export([echo/1]).
-export([slow_echo/2]).
-export([boom/0]).
-export([thrower/0]).
-export([exiter/0]).
-export([notify/2]).

erpc_test_() ->
    {timeout, 120,
        {foreach, fun setup/0, fun cleanup/1, [
            fun call_over_transport_returns_value/0,
            fun call_with_infinity_uses_local_shortcircuit/0,
            fun remote_error_is_reported_as_exception/0,
            fun remote_throw_propagates/0,
            fun remote_exit_propagates/0,
            fun timeout_is_reported/0,
            fun concurrent_requests_do_not_cross_match/0,
            fun send_request_and_receive_response/0,
            fun check_response_matches_only_its_own_request/0,
            fun cast_runs_on_the_target/0,
            fun slow_call_does_not_block_other_calls/0,
            fun reqids_collection_roundtrip/0,
            fun receive_response_from_collection/0,
            fun wait_response_from_collection_is_non_blocking/0,
            fun requests_over_the_concurrency_cap_are_rejected/0,
            fun cap_slots_are_released_when_workers_finish/0,
            fun late_reply_after_timeout_is_dropped/0,
            fun late_reply_after_abandoned_collection_is_dropped/0,
            fun send_request_accepts_per_call_opts/0,
            fun send_request_accepts_a_proplist/0,
            fun send_request_rejects_malformed_opts/0,
            fun collection_send_request_accepts_per_call_opts/0,
            fun cast_accepts_per_call_opts/0,
            fun multicast_accepts_per_call_opts/0,
            fun multicall_reads_timeout_out_of_the_opts_map/0,
            fun multicall_with_opts_still_reports_bad_nodes/0,
            fun multicall_rejects_a_bad_timeout_in_the_opts_map/0,
            fun transport_opts_never_carry_the_timeout/0
        ]}}.

%% C-2b.2b — the vendored module was an OTP 23/24 snapshot and had silently
%% drifted eight functions behind upstream. Make the next drift a test failure
%% rather than an `undef' in a user's fan-out code.
export_parity_test() ->
    Upstream = lists:sort(erpc:module_info(exports)),
    Ours = lists:sort(partisan_erpc:module_info(exports)),
    Missing = [FA || FA <- Upstream, not lists:member(FA, Ours)],
    ?assertEqual([], Missing).

setup() ->
    stop_partisan(),
    {ok, _} = application:ensure_all_started(partisan),
    ok.

cleanup(_) ->
    stop_partisan().

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.

node_() ->
    partisan:node().

%% Finite timeout => transport path, not the local short-circuit.
call_over_transport_returns_value() ->
    ?assertEqual(
        hello, partisan_erpc:call(node_(), ?MODULE, echo, [hello], 5000)
    ).

call_with_infinity_uses_local_shortcircuit() ->
    ?assertEqual(
        hello, partisan_erpc:call(node_(), ?MODULE, echo, [hello], infinity)
    ).

%% erpc reports a remote error as `error:{exception, Reason, Stack}'.
remote_error_is_reported_as_exception() ->
    ?assertError(
        {exception, deliberate, _Stack},
        partisan_erpc:call(node_(), ?MODULE, boom, [], 5000)
    ).

remote_throw_propagates() ->
    ?assertThrow(
        tossed, partisan_erpc:call(node_(), ?MODULE, thrower, [], 5000)
    ).

remote_exit_propagates() ->
    ?assertExit(
        {exception, bailed},
        partisan_erpc:call(node_(), ?MODULE, exiter, [], 5000)
    ).

timeout_is_reported() ->
    ?assertError(
        {partisan_erpc, timeout},
        partisan_erpc:call(node_(), ?MODULE, slow_echo, [x, 3000], 300)
    ).

%% THE regression test for reply correlation.
%%
%% Two requests are outstanding in the SAME process at the same time, and the
%% second one completes first. Each reply carries the correlation reference of
%% its own request, so each `receive_response' returns its own value.
%%
%% `partisan_rpc' cannot express this: it replies with a bare
%% `{rpc_response, Result}' and receives with a bare `receive {rpc_response, R}',
%% so whichever reply arrives first is returned to whichever call receives
%% first.
concurrent_requests_do_not_cross_match() ->
    Node = node_(),

    Slow = partisan_erpc:send_request(Node, ?MODULE, slow_echo, [slow, 1500]),
    Fast = partisan_erpc:send_request(Node, ?MODULE, slow_echo, [fast, 0]),

    %% The fast reply is already in the mailbox ahead of the slow one; asking
    %% for the slow request must still yield the slow request's value.
    ?assertEqual(slow, partisan_erpc:receive_response(Slow, 10000)),
    ?assertEqual(fast, partisan_erpc:receive_response(Fast, 10000)).

send_request_and_receive_response() ->
    ReqId = partisan_erpc:send_request(node_(), ?MODULE, echo, [async]),
    ?assertEqual(async, partisan_erpc:receive_response(ReqId, 5000)).

%% `check_response/2' must ignore a message belonging to a different request.
check_response_matches_only_its_own_request() ->
    Node = node_(),

    A = partisan_erpc:send_request(Node, ?MODULE, echo, [a]),
    B = partisan_erpc:send_request(Node, ?MODULE, echo, [b]),

    %% Drain both replies, checking each against BOTH request ids. Every
    %% message must match exactly one of them.
    Results = collect_via_check([A, B], [], 2),

    ?assertEqual([a, b], lists:sort(Results)).

collect_via_check(_ReqIds, Acc, 0) ->
    Acc;
collect_via_check(ReqIds, Acc, N) ->
    receive
        Msg ->
            case
                [
                    R
                 || Id <- ReqIds,
                    {response, R} <- [partisan_erpc:check_response(Msg, Id)]
                ]
            of
                [] ->
                    %% Not one of ours; keep waiting.
                    collect_via_check(ReqIds, Acc, N);
                [One] ->
                    collect_via_check(ReqIds, [One | Acc], N - 1)
            end
    after 5000 ->
        Acc
    end.

cast_runs_on_the_target() ->
    Self = partisan:self(),
    Ref = make_ref(),

    ok = partisan_erpc:cast(node_(), ?MODULE, notify, [Self, Ref]),

    receive
        {done, Ref} -> ok
    after 5000 ->
        ?assert(false)
    end.

%% As for `partisan_rpc': one slow request must not head-of-line block others.
slow_call_does_not_block_other_calls() ->
    Node = node_(),
    Self = self(),

    Slow = spawn(fun() ->
        R = partisan_erpc:call(Node, ?MODULE, slow_echo, [s, 3000], 10000),
        Self ! {slow_done, R}
    end),

    timer:sleep(300),

    T0 = erlang:monotonic_time(millisecond),
    ?assertEqual(quick, partisan_erpc:call(Node, ?MODULE, echo, [quick], 1000)),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed < 1000),

    receive
        {slow_done, R} -> ?assertEqual(s, R)
    after 10000 ->
        exit(Slow, kill),
        ?assert(false)
    end.

%% The OTP 25+ collection API: build a collection, inspect it, round-trip it.
reqids_collection_roundtrip() ->
    Node = node_(),

    C0 = partisan_erpc:reqids_new(),
    ?assertEqual(0, partisan_erpc:reqids_size(C0)),
    ?assertEqual([], partisan_erpc:reqids_to_list(C0)),

    C1 = partisan_erpc:send_request(Node, ?MODULE, echo, [one], lbl_one, C0),
    C2 = partisan_erpc:send_request(Node, ?MODULE, echo, [two], lbl_two, C1),

    ?assertEqual(2, partisan_erpc:reqids_size(C2)),

    Labels = [L || {_ReqId, L} <- partisan_erpc:reqids_to_list(C2)],
    ?assertEqual([lbl_one, lbl_two], lists:sort(Labels)),

    %% `reqids_add/3' of an id already present is an error.
    [{ReqId, _} | _] = partisan_erpc:reqids_to_list(C2),
    ?assertError(
        {partisan_erpc, badarg},
        partisan_erpc:reqids_add(ReqId, dup, C2)
    ),

    %% Drain so the mailbox does not leak into the next case.
    _ = drain_collection(C2, 2),
    ok.

%% Each response comes back with its own label, and deleting removes it from
%% the returned collection.
receive_response_from_collection() ->
    Node = node_(),

    C0 = partisan_erpc:reqids_new(),
    C1 = partisan_erpc:send_request(Node, ?MODULE, echo, [a], label_a, C0),
    C2 = partisan_erpc:send_request(Node, ?MODULE, echo, [b], label_b, C1),

    {R1, L1, C3} = partisan_erpc:receive_response(C2, 5000, true),
    ?assertEqual(1, partisan_erpc:reqids_size(C3)),

    {R2, L2, C4} = partisan_erpc:receive_response(C3, 5000, true),
    ?assertEqual(0, partisan_erpc:reqids_size(C4)),

    %% Whatever order they arrive in, value and label must travel together.
    ?assertEqual([{a, label_a}, {b, label_b}], lists:sort([{R1, L1}, {R2, L2}])).

%% `wait_response/3' with a zero wait returns `no_response' rather than
%% blocking when nothing has arrived yet.
wait_response_from_collection_is_non_blocking() ->
    Node = node_(),

    C0 = partisan_erpc:reqids_new(),
    C1 = partisan_erpc:send_request(
        Node, ?MODULE, slow_echo, [later, 2000], slow_label, C0
    ),

    ?assertEqual(no_response, partisan_erpc:wait_response(C1, 0, true)),

    {{response, Val}, Label, C2} =
        partisan_erpc:wait_response(C1, 5000, true),
    ?assertEqual(later, Val),
    ?assertEqual(slow_label, Label),
    ?assertEqual(0, partisan_erpc:reqids_size(C2)).

%% C-3 — each request runs in its own process, so a peer could otherwise spawn
%% without limit. Over the cap the request is rejected outright rather than
%% queued: a queue with no credit scheme is an unbounded mailbox with extra
%% steps.
requests_over_the_concurrency_cap_are_rejected() ->
    Node = node_(),
    Old = partisan_config:get(rpc_max_concurrency, 10000),
    ok = partisan_config:set(rpc_max_concurrency, 1),

    try
        %% Occupy the single slot.
        Busy = partisan_erpc:send_request(
            Node, ?MODULE, slow_echo, [held, 2000]
        ),
        timer:sleep(300),

        %% The next request has nowhere to run.
        ?assertError(
            {partisan_erpc, overloaded},
            partisan_erpc:call(Node, ?MODULE, echo, [rejected], 5000)
        ),

        %% The in-flight one still completes normally.
        ?assertEqual(held, partisan_erpc:receive_response(Busy, 10000))
    after
        partisan_config:set(rpc_max_concurrency, Old)
    end.

%% A rejected request must not leak a slot, and a finished worker must return
%% its slot — otherwise the node wedges after `Max' total requests.
cap_slots_are_released_when_workers_finish() ->
    Node = node_(),
    Old = partisan_config:get(rpc_max_concurrency, 10000),
    ok = partisan_config:set(rpc_max_concurrency, 2),

    try
        %% Far more sequential requests than the cap. Each must succeed,
        %% proving slots are released on worker exit.
        Results = [
            partisan_erpc:call(Node, ?MODULE, echo, [I], 5000)
         || I <- lists:seq(1, 20)
        ],
        ?assertEqual(lists:seq(1, 20), Results)
    after
        partisan_config:set(rpc_max_concurrency, Old)
    end.

%% A reply that arrives after its caller gave up must not land in the mailbox.
%%
%% The correlation reference is a process alias (`erlang:alias/1'); abandoning
%% the request deactivates it, so the runtime discards the late reply. Upstream
%% `erpc' gets the same guarantee from `demonitor(_, [flush])', because there
%% the result travels as a monitor DOWN.
late_reply_after_timeout_is_dropped() ->
    Node = node_(),
    [] = drain_mailbox(),

    ?assertError(
        {partisan_erpc, timeout},
        partisan_erpc:call(Node, ?MODULE, slow_echo, [late, 1200], 200)
    ),

    %% Well past the point at which the reply would have arrived.
    timer:sleep(2000),
    ?assertEqual([], drain_mailbox()).

%% Same guarantee for the collection API: `collection_result(timeout, ...)'
%% abandons every outstanding request in the collection.
late_reply_after_abandoned_collection_is_dropped() ->
    Node = node_(),
    [] = drain_mailbox(),

    C0 = partisan_erpc:reqids_new(),
    C1 = partisan_erpc:send_request(
        Node, ?MODULE, slow_echo, [a, 1200], la, C0
    ),
    C2 = partisan_erpc:send_request(
        Node, ?MODULE, slow_echo, [b, 1200], lb, C1
    ),

    ?assertError(
        {partisan_erpc, timeout},
        partisan_erpc:receive_response(C2, 200, true)
    ),

    timer:sleep(2000),
    ?assertEqual([], drain_mailbox()).

%% =============================================================================
%% PER-CALL TRANSPORT OPTIONS
%% =============================================================================
%%
%% Upstream `erpc' has no notion of channels, so on its API every request rides
%% the globally configured `forward_options'. `call/5' was widened to accept
%% `forward_opts()' in the timeout position; the asynchronous, cast and fan-out
%% surfaces have no free argument to widen, so each gained an extra arity.
%%
%% **What these cases can and cannot prove.** They run on one node, and
%% `partisan_pluggable_peer_service_manager:forward_message/4' delivers straight
%% to the local process when the target node is this node — *discarding the
%% options*. So a single node cannot observe which channel a request travelled
%% on; what it can observe is that every new arity resolves its options and
%% completes end to end, that a malformed option map is rejected, and that
%% `timeout' is honoured where the shape allows it. The cross-node proof that the
%% requested channel actually reaches the transport is
%% `partisan_SUITE:erpc_test'.

%% The asynchronous surface: a request issued with per-call options must still
%% correlate and return its value.
send_request_accepts_per_call_opts() ->
    Opts = #{channel => partisan:default_channel()},
    ReqId = partisan_erpc:send_request(
        node_(), ?MODULE, echo, [with_opts], Opts
    ),
    ?assertEqual(with_opts, partisan_erpc:receive_response(ReqId, 5000)).

%% `forward_opts()' is `map() | proplist()' everywhere else in Partisan —
%% `partisan_gen' passes proplists — so these arities accept both.
send_request_accepts_a_proplist() ->
    ReqId = partisan_erpc:send_request(
        node_(), ?MODULE, echo, [prop], [{channel, partisan:default_channel()}]
    ),
    ?assertEqual(prop, partisan_erpc:receive_response(ReqId, 5000)).

%% Anything that is neither a map nor a list is a `badarg', not a silently
%% ignored argument — otherwise a typo'd option quietly reverts to the global
%% configuration.
send_request_rejects_malformed_opts() ->
    ?assertError(
        {partisan_erpc, badarg},
        partisan_erpc:send_request(node_(), ?MODULE, echo, [x], not_options)
    ),
    ?assertError(
        {partisan_erpc, badarg},
        partisan_erpc:send_request(
            node_(), ?MODULE, echo, [x], lbl, partisan_erpc:reqids_new(), 42
        )
    ).

%% The collection form — the shape used for high fan-out, and so the one that
%% most wants a channel of its own.
collection_send_request_accepts_per_call_opts() ->
    Node = node_(),
    Opts = #{channel => partisan:default_channel()},

    C0 = partisan_erpc:reqids_new(),
    C1 = partisan_erpc:send_request(Node, ?MODULE, echo, [a], la, C0, Opts),
    C2 = partisan_erpc:send_request(Node, ?MODULE, echo, [b], lb, C1, Opts),

    ?assertEqual(2, partisan_erpc:reqids_size(C2)),

    {R1, L1, C3} = partisan_erpc:receive_response(C2, 5000, true),
    {R2, L2, C4} = partisan_erpc:receive_response(C3, 5000, true),

    ?assertEqual(0, partisan_erpc:reqids_size(C4)),
    ?assertEqual([{a, la}, {b, lb}], lists:sort([{R1, L1}, {R2, L2}])).

%% A cast is fire-and-forget, so the only observable is that the function ran on
%% the target.
cast_accepts_per_call_opts() ->
    Self = partisan:self(),
    Ref = make_ref(),

    ?assertEqual(
        ok,
        partisan_erpc:cast(node_(), ?MODULE, notify, [Self, Ref], #{
            channel => partisan:default_channel()
        })
    ),

    receive
        {done, Ref} -> ok
    after 5000 -> ?assert(false)
    end.

multicast_accepts_per_call_opts() ->
    Self = partisan:self(),
    Ref = make_ref(),

    ?assertEqual(
        ok,
        partisan_erpc:multicast([node_()], ?MODULE, notify, [Self, Ref], #{
            channel => partisan:default_channel()
        })
    ),

    receive
        {done, Ref} -> ok
    after 5000 -> ?assert(false)
    end.

%% `multicall/5' is overloaded the way `call/5' is: the fifth argument may be a
%% timeout or an option map. When it is a map the timeout comes out of it — this
%% is the one part of the options contract a single node *can* discriminate,
%% because a too-short timeout must still time out.
multicall_reads_timeout_out_of_the_opts_map() ->
    Node = node_(),
    Chan = partisan:default_channel(),

    %% A per-node timeout is reported *in* the result list, not raised —
    %% `multicall' promises one entry per node whatever happens to each.
    ?assertEqual(
        [{error, {partisan_erpc, timeout}}],
        partisan_erpc:multicall(
            [Node],
            ?MODULE,
            slow_echo,
            [slow, 2000],
            #{timeout => 200, channel => Chan}
        )
    ),

    ?assertEqual(
        [{ok, quick}],
        partisan_erpc:multicall(
            [Node], ?MODULE, echo, [quick], #{timeout => 5000, channel => Chan}
        )
    ).

%% The options overload must not change how per-node results are reported: one
%% result per node, in node order, with an unreachable node reported as such.
multicall_with_opts_still_reports_bad_nodes() ->
    Node = node_(),
    Bogus = 'nonexistent@nowhere',

    Results = partisan_erpc:multicall(
        [Node, Bogus], ?MODULE, echo, [m], #{timeout => 2000}
    ),

    ?assertMatch([{ok, m}, {error, {partisan_erpc, noconnection}}], Results).

multicall_rejects_a_bad_timeout_in_the_opts_map() ->
    ?assertError(
        {partisan_erpc, badarg},
        partisan_erpc:multicall(
            [node_()], ?MODULE, echo, [x], #{timeout => not_a_timeout}
        )
    ).

%% `timeout' is a caller-side concern, not a transport option. It must be
%% stripped before the options reach `partisan:forward_message/4', which would
%% otherwise carry a key it has no meaning for.
transport_opts_never_carry_the_timeout() ->
    Opts = partisan_rpc:forward_opts(#{
        timeout => 5000, channel => a_channel, partition_key => 3
    }),
    ?assertNot(maps:is_key(timeout, Opts)),
    ?assertEqual(a_channel, maps:get(channel, Opts)),
    ?assertEqual(3, maps:get(partition_key, Opts)).

drain_mailbox() ->
    receive
        M -> [M | drain_mailbox()]
    after 0 ->
        []
    end.

drain_collection(_C, 0) ->
    ok;
drain_collection(C, N) ->
    {_R, _L, C1} = partisan_erpc:receive_response(C, 5000, true),
    drain_collection(C1, N - 1).

%% =============================================================================
%% FUNCTIONS APPLIED BY THE TESTS
%% =============================================================================

echo(X) ->
    X.

slow_echo(X, Ms) ->
    timer:sleep(Ms),
    X.

boom() ->
    error(deliberate).

thrower() ->
    throw(tossed).

exiter() ->
    exit(bailed).

notify(Origin, Ref) ->
    partisan:forward_message(Origin, {done, Ref}, #{}).
