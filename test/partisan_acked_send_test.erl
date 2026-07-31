%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests for the acknowledged send path.
%%
%% An acknowledged message used to be forced through
%% `partisan_pluggable_peer_service_manager' as a `gen_server:call', for two
%% reasons: the message clock came from that server's state, and recording the
%% outstanding message was itself a `gen_server:call'. Both are gone — the clock
%% comes from a lock-free counter and the record is a direct ETS write — so an
%% acknowledged send now runs in the calling process.
%%
%% Two properties are asserted here:
%%
%% <ul>
%% <li>clocks are unique and monotonic even when drawn concurrently, since they
%% key the outstanding-message table and a collision would let one
%% acknowledgement clear another message;</li>
%% <li>when there is no usable connection the fast path has **no side effects**
%% — no clock consumed, nothing recorded — so the serialised fallback can handle
%% the message without the risk of it being recorded twice under two clocks.</li>
%% </ul>
%% @end
%% =============================================================================
-module(partisan_acked_send_test).

-include_lib("eunit/include/eunit.hrl").

-define(MGR, partisan_pluggable_peer_service_manager).
-define(ACKS, partisan_acknowledgement_backend).

acked_send_test_() ->
    {timeout, 60,
        {foreach, fun setup/0, fun cleanup/1, [
            fun clock_shape_is_wire_compatible/0,
            fun clocks_are_monotonic/0,
            fun clocks_are_unique_across_processes/0,
            fun no_connection_falls_back_without_side_effects/0,
            fun registering_an_interposition_fun_disables_the_fast_path/0
        ]}}.

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

%% Peers echo this term back verbatim in their acknowledgements, so the shape
%% must stay exactly what the vector-clock implementation produced.
clock_shape_is_wire_compatible() ->
    Node = partisan:node(),
    ?assertMatch(
        {undefined, [{Node, N}]} when is_integer(N),
        ?MGR:next_message_clock(Node)
    ).

clocks_are_monotonic() ->
    Node = partisan:node(),
    {undefined, [{Node, A}]} = ?MGR:next_message_clock(Node),
    {undefined, [{Node, B}]} = ?MGR:next_message_clock(Node),
    {undefined, [{Node, C}]} = ?MGR:next_message_clock(Node),
    ?assert(A < B),
    ?assert(B < C).

%% The clock keys the outstanding-message table. Two senders drawing the same
%% clock would mean one peer's acknowledgement clears the other's message, so
%% uniqueness under concurrency is a correctness property, not a nicety.
clocks_are_unique_across_processes() ->
    Node = partisan:node(),
    N = 200,
    Self = self(),

    _ = [
        spawn(fun() ->
            {undefined, [{_, Counter}]} = ?MGR:next_message_clock(Node),
            Self ! {clock, Counter}
        end)
     || _ <- lists:seq(1, N)
    ],

    Counters = collect(N, []),
    ?assertEqual(N, length(Counters)),
    ?assertEqual(N, length(lists:usort(Counters))).

collect(0, Acc) ->
    Acc;
collect(N, Acc) ->
    receive
        {clock, C} -> collect(N - 1, [C | Acc])
    after 5000 ->
        Acc
    end.

%% THE safety property. There is no connection to this peer, so the fast path
%% must decline *before* drawing a clock or recording anything — otherwise the
%% serialised fallback would send the message a second time under a second
%% clock, leaving an entry that is never acknowledged.
no_connection_falls_back_without_side_effects() ->
    {ok, Before} = ?ACKS:outstanding(),
    ?assertEqual([], Before),

    ClockBefore = counter_of(?MGR:next_message_clock(partisan:node())),

    Result = ?MGR:fast_forward_acked(
        'nonexistent@nowhere',
        undefined,
        some_server,
        a_message,
        undefined,
        #{ack => true, channel => partisan:default_channel()}
    ),

    %% Declines, so `forward_message/4' falls through to the serialised path.
    ?assertMatch({error, _}, Result),

    %% Nothing recorded as outstanding.
    ?assertEqual({ok, []}, ?ACKS:outstanding()),

    %% And no clock was consumed: the next one is exactly one past the last.
    ClockAfter = counter_of(?MGR:next_message_clock(partisan:node())),
    ?assertEqual(ClockBefore + 1, ClockAfter).

counter_of({undefined, [{_Node, Counter}]}) ->
    Counter.

%% An acknowledged send must NOT take the fast path while any interposition
%% function is registered, because the fast path does not fire them.
%%
%% `partisan_SUITE:ack_test' depends on this: it installs a fun that drops
%% messages to a peer, asserts the acknowledged message is *not* delivered, then
%% removes the fun and asserts retransmission delivers it. An acknowledged fast
%% path that skipped interposition delivered the message immediately and broke
%% that test — this asserts the gate that prevents it.
registering_an_interposition_fun_disables_the_fast_path() ->
    %% No funs registered at boot, so the gate is open.
    ?assertEqual(false, interposition_flag()),

    Fun = fun({_, _, M}) -> M end,
    ok = ?MGR:add_interposition_fun(test_fun, Fun),
    ?assertEqual(true, interposition_flag()),

    ok = ?MGR:remove_interposition_fun(test_fun),
    ?assertEqual(false, interposition_flag()),

    %% The pre/post registries gate it too — all three suppress the fast path.
    ok = ?MGR:add_pre_interposition_fun(test_pre, Fun),
    ?assertEqual(true, interposition_flag()),
    ok = ?MGR:remove_pre_interposition_fun(test_pre),
    ?assertEqual(false, interposition_flag()),

    ok = ?MGR:add_post_interposition_fun(test_post, fun(_, _, _, _) -> ok end),
    ?assertEqual(true, interposition_flag()),
    ok = ?MGR:remove_post_interposition_fun(test_post),
    ?assertEqual(false, interposition_flag()).

interposition_flag() ->
    persistent_term:get({?MGR, has_interposition_funs}, false).
