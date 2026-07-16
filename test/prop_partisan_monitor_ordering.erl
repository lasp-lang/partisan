%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Property-based validation of the disterl-style signal-ordering
%% guarantees that `partisan_monitor' provides for remote process
%% monitors.
%%
%% Properties:
%%
%% <ul>
%% <li>{@link prop_fifo_message_then_down/0} — for any N, if a remote
%% process sends N sequence-numbered messages on a partisan channel
%% and then exits, the local monitor receives those N messages strictly
%% in order before the `DOWN' signal that follows.</li>
%% <li>{@link prop_consecutive_downs_in_order/0} — for any K pairs of
%% remote processes (Leader, Follower) where Follower exits strictly
%% after Leader, the K corresponding `DOWN' signals on the local node
%% never invert pair order.</li>
%% <li>{@link prop_no_fallback_for_explicit_channel/0} — for any
%% channel name that is NOT configured in the cluster, monitor
%% establishment with an explicit `{channel, _}' option fires an
%% immediate `DOWN' with reason `noconnection' instead of silently
%% falling back to the default channel.</li>
%% </ul>
%%
%% Run with: `rebar3 proper -m prop_partisan_monitor_ordering'.
%% @end
%% =============================================================================
-module(prop_partisan_monitor_ordering).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Exported so partisan:spawn(Node, ?MODULE, _, _) works.
-export([fifo_sender_loop/0]).

-define(PEER_KEY, {?MODULE, peer_node}).
-define(RECV_TIMEOUT, 30000).

%% =============================================================================
%% PROPERTIES
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc FIFO ordering between user messages and the DOWN signal.
%%
%% This locks in the disterl guarantee that messages sent by a process
%% before its exit are delivered to its monitor before the corresponding
%% DOWN. Failure indicates that:
%% (a) the DOWN took a different connection from the user messages, OR
%% (b) the partisan_monitor introduced a mailbox hop / spawn that
%%     reordered the DOWN past in-flight messages, OR
%% (c) channel_fallback silently routed the DOWN over a different
%%     connection from the user traffic.
%% @end
%% -----------------------------------------------------------------------------
prop_fifo_message_then_down() ->
    ?SETUP(
        fun setup/0,
        ?FORALL(
            N,
            range(1, 2000),
            do_fifo_test(get_peer(), N) =:= ok
        )
    ).

%% -----------------------------------------------------------------------------
%% @doc Consecutive DOWN signals are delivered in the order the local
%% runtime produced them on the dying-process node.
%%
%% Strategy: spawn K pairs of remote processes. Each pair has a Leader
%% and a Follower. Both are told to exit, but the Follower sleeps
%% briefly before exiting so that on the remote node Leader's exit
%% strictly precedes Follower's exit. The corresponding DOWN signals
%% must reach this process in the same order.
%%
%% Failure indicates that either the partisan_monitor on the remote
%% node parallelised DOWN delivery (e.g. via `spawn') or that the
%% transport reordered.
%% @end
%% -----------------------------------------------------------------------------
prop_consecutive_downs_in_order() ->
    ?SETUP(
        fun setup/0,
        ?FORALL(
            K,
            range(1, 50),
            do_consecutive_downs_test(get_peer(), K) =:= ok
        )
    ).

%% -----------------------------------------------------------------------------
%% @doc An explicit non-default channel that is not connected MUST fire
%% an immediate `noconnection' DOWN, not silently fall back to the
%% default channel. This locks in the policy in
%% `partisan_monitor:monitor/2' where `channel_fallback' defaults to
%% `false' for explicit non-default channels.
%% @end
%% -----------------------------------------------------------------------------
prop_no_fallback_for_explicit_channel() ->
    ?SETUP(
        fun setup/0,
        ?FORALL(
            ChanSuffix,
            range(1, 1000),
            do_no_fallback_test(
                get_peer(), unconfigured_channel(ChanSuffix)
            ) =:= ok
        )
    ).

%% =============================================================================
%% TEST BODIES
%% =============================================================================

do_fifo_test(Peer, NMsgs) ->
    Self = partisan:self(),
    Sender = partisan:spawn(Peer, ?MODULE, fifo_sender_loop, []),

    %% Monitor before triggering — guarantees we see the DOWN.
    Mref = partisan:monitor(process, Sender),

    partisan:forward_message(Sender, {go, Self, NMsgs}),

    case collect_in_order(NMsgs, Mref, []) of
        {ok, Got} ->
            Expected = lists:seq(1, NMsgs),
            Actual = [I || {msg, I} <- Got],
            case Expected =:= Actual of
                true ->
                    ok;
                false ->
                    {fail,
                        {out_of_order, #{
                            first_diverged => first_diverge(Expected, Actual),
                            expected_len => length(Expected),
                            actual_len => length(Actual)
                        }}}
            end;
        {fail, Reason} ->
            {fail, Reason}
    end.

do_consecutive_downs_test(Peer, K) ->
    Self = partisan:self(),

    %% Build K pairs. For each, the Leader is spawned and asked to exit
    %% with delay 0; then the Follower is spawned and asked to exit
    %% with delay >0 so that it exits strictly after the Leader.
    Pairs = lists:map(
        fun(I) ->
            Leader = partisan:spawn(Peer, ?MODULE, fifo_sender_loop, []),
            Follower = partisan:spawn(Peer, ?MODULE, fifo_sender_loop, []),
            LRef = partisan:monitor(process, Leader),
            FRef = partisan:monitor(process, Follower),
            partisan:forward_message(Leader, {exit_after, Self, 0}),
            partisan:forward_message(Follower, {exit_after, Self, 5}),
            {I, LRef, FRef}
        end,
        lists:seq(1, K)
    ),

    %% Drain the {sent, _} acks before they exit. Order does not matter.
    case drain_acks(K * 2) of
        ok ->
            Downs = collect_downs(K * 2, []),
            Inversions = [
                I
             || {I, LRef, FRef} <- Pairs,
                position_of(LRef, Downs) >= position_of(FRef, Downs)
            ],
            case Inversions of
                [] -> ok;
                _ -> {fail, {inversions, Inversions}}
            end;
        Other ->
            {fail, {ack_drain_failed, Other}}
    end.

do_no_fallback_test(Peer, Chan) ->
    Sender = partisan:spawn(Peer, ?MODULE, fifo_sender_loop, []),

    Mref = partisan:monitor(
        process, Sender, [{channel, Chan}]
    ),

    Result =
        receive
            {'DOWN', Mref, process, _, noconnection} ->
                ok;
            {'DOWN', Mref, process, _, OtherReason} ->
                {fail, {wrong_reason, OtherReason}};
            Other ->
                {fail, {unexpected, Other}}
        after 2000 ->
            {fail, no_immediate_down}
        end,

    %% Cleanup the sender so we don't leak pids on the remote node.
    catch partisan:exit(Sender, kill),

    Result.

%% =============================================================================
%% SETUP / TEARDOWN
%% =============================================================================

setup() ->
    %% Bring up disterl on the runner so partisan_support_otp can spawn
    %% peer nodes via :peer.
    partisan_support:start_disterl(),
    erlang:is_alive() orelse error({runner_not_in_distribution_mode}),
    {ok, _} = application:ensure_all_started(partisan),

    {ok, Peer} = partisan_support_otp:start_node(monitor_order),
    partisan_support:cluster(Peer),
    %% Allow the cluster handshake to settle.
    timer:sleep(2000),

    persistent_term:put(?PEER_KEY, Peer),

    fun() ->
        %% Teardown
        partisan_support_otp:stop_node(Peer),
        persistent_term:erase(?PEER_KEY),
        application:stop(partisan),
        ok
    end.

get_peer() ->
    persistent_term:get(?PEER_KEY).

%% =============================================================================
%% REMOTE HELPER (runs on peer node)
%% =============================================================================

%% Two commands:
%%
%% - `{go, Dest, NMsgs}': send NMsgs sequence-numbered messages to
%%   `Dest', then exit normally.
%% - `{exit_after, Dest, Delay}': ack with `{sent, partisan:self()}'
%%   to `Dest', sleep `Delay' ms, then exit normally. The delay lets
%%   the test order two peers' deaths.
fifo_sender_loop() ->
    receive
        {go, Dest, NMsgs} ->
            send_seq(Dest, 1, NMsgs);
        {exit_after, Dest, Delay} ->
            partisan:forward_message(Dest, {sent, partisan:self()}),
            timer:sleep(Delay),
            exit(normal)
    after 60000 ->
        exit(timeout)
    end.

send_seq(_Dest, I, N) when I > N ->
    ok;
send_seq(Dest, I, N) ->
    partisan:forward_message(Dest, {msg, I}),
    send_seq(Dest, I + 1, N).

%% =============================================================================
%% LOCAL HELPERS
%% =============================================================================

%% Collect exactly N messages tagged `{msg, _}' followed by exactly one
%% DOWN with reason `normal' for the given Mref. Any other interleaving
%% returns `{fail, _}'. Both refs are partisan remote refs encoded for
%% the same (peer) node, so direct `=:=' is the correct comparison.
collect_in_order(0, Mref, Acc) ->
    receive
        {'DOWN', Mref, process, _, normal} ->
            {ok, lists:reverse(Acc)};
        {msg, _} = More ->
            {fail, {extra_message_after_all_collected, More}};
        Other ->
            {fail, {unexpected_after_all_collected, Other}}
    after ?RECV_TIMEOUT ->
        {fail, {no_down_after_all_collected, length(Acc)}}
    end;
collect_in_order(N, Mref, Acc) ->
    receive
        {msg, _} = M ->
            collect_in_order(N - 1, Mref, [M | Acc]);
        {'DOWN', Mref, process, _, _} = D ->
            {fail, #{
                premature_down => true,
                collected => length(Acc),
                still_expected => N,
                down => D
            }};
        {'DOWN', _OtherRef, _, _, _} ->
            %% DOWN for an unrelated ref; ignore and keep waiting.
            collect_in_order(N, Mref, Acc);
        Other ->
            {fail, {unexpected_message, Other, length(Acc)}}
    after ?RECV_TIMEOUT ->
        {fail, {timeout_at_msg, N, length(Acc)}}
    end.

%% Collect exactly N DOWN signals into a list ordered by arrival.
collect_downs(0, Acc) ->
    lists:reverse(Acc);
collect_downs(N, Acc) ->
    receive
        {'DOWN', M, process, _, _} ->
            collect_downs(N - 1, [M | Acc])
    after ?RECV_TIMEOUT ->
        error({timeout_collecting_downs, N, length(Acc)})
    end.

%% Drain N {sent, _} acks. Returns ok or {timeout, _}.
drain_acks(0) ->
    ok;
drain_acks(N) ->
    receive
        {sent, _} -> drain_acks(N - 1)
    after 5000 ->
        {timeout, N}
    end.

%% First arrival index of `Ref' in `Downs' (1-based). Both refs are
%% encoded partisan remote refs for the same node, so `=:=' suffices.
position_of(Ref, Downs) ->
    position_of(Ref, Downs, 1).

position_of(_Ref, [], _I) ->
    %% Sentinel "not found" — far past any legitimate index, so the
    %% strict-inequality inversion check fails as expected.
    1 bsl 30;
position_of(Ref, [Ref | _T], I) ->
    I;
position_of(Ref, [_ | T], I) ->
    position_of(Ref, T, I + 1).

%% Return the index where two lists first diverge, or `none' if the
%% prefix matches up to the shorter list's length.
first_diverge(Expected, Actual) ->
    first_diverge(Expected, Actual, 0).

first_diverge([], _, _) -> none;
first_diverge(_, [], _) -> none;
first_diverge([H | T1], [H | T2], I) -> first_diverge(T1, T2, I + 1);
first_diverge([_ | _], [_ | _], I) -> I.

%% A channel atom that is *not* configured in the cluster. We
%% deliberately avoid `binary_to_existing_atom' so callers cannot
%% accidentally create matching atoms — channels are configured at
%% startup and any unused atom suffices.
unconfigured_channel(Suffix) ->
    list_to_atom(
        "this_channel_is_never_configured_" ++ integer_to_list(Suffix)
    ).
