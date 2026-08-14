%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests for the connection high-water mark.
%%
%% `partisan_peer_connections:cast_encoded/3' is the single admission point for
%% outbound data. It exists because a bare `gen_server:cast/2' into an unbounded
%% mailbox cannot fail, cannot block and cannot tell the sender anything, so a
%% sender faster than its socket grows that mailbox until the node dies. These
%% cases pin the three things that decide whether admission control helps or
%% harms:
%%
%% <ul>
%% <li>past the mark it refuses, and **does not queue** — a bounded queue that
%% silently discards the newest message is a lossy channel with extra steps;</li>
%% <li>the default is `infinity', so an upgrade changes nothing for anyone who
%% has not opted in;</li>
%% <li>`monotonic' channels are exempt, because they already have a different and
%% deliberate overload strategy.</li>
%% </ul>
%%
%% The target of a send is any process with a mailbox, so these use an ordinary
%% inert process rather than a real connection: what is under test is the
%% admission decision, and a real socket would only add a way for the test to be
%% flaky.
%% @end
%% =============================================================================
-module(partisan_backpressure_test).

-include_lib("eunit/include/eunit.hrl").

-define(CONN, partisan_peer_connections).

backpressure_test_() ->
    {timeout, 60,
        {foreach, fun setup/0, fun cleanup/1, [
            fun default_is_unbounded/0,
            fun refuses_past_the_high_watermark/0,
            fun does_not_queue_what_it_refuses/0,
            fun admits_again_once_the_queue_drains/0,
            fun monotonic_channels_are_exempt/0,
            fun a_dead_connection_is_not_reported_as_overloaded/0
        ]}}.

setup() ->
    stop_partisan(),
    {ok, _} = application:ensure_all_started(partisan),
    ok.

cleanup(_) ->
    ok = partisan_config:set(connection_high_watermark, infinity),
    stop_partisan().

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.

%% A process that never reads its mailbox, so its queue length is exactly what
%% has been sent to it.
inert() ->
    spawn(fun() ->
        receive
            never -> ok
        end
    end).

queue_len(Pid) ->
    {message_queue_len, N} = erlang:process_info(Pid, message_queue_len),
    N.

%% =============================================================================

%% The historical behaviour. Turning an unbounded queue into a refusing one
%% changes what callers observe, so it must be opt-in — an upgrade must not start
%% returning errors to a system that has never had to handle them.
default_is_unbounded() ->
    ?assertEqual(
        infinity, partisan_config:get(connection_high_watermark, infinity)
    ),

    Pid = inert(),
    try
        _ = [
            ?assertEqual(
                ok, ?CONN:cast_encoded(Pid, <<"x">>, partisan:default_channel())
            )
         || _ <- lists:seq(1, 200)
        ],
        ?assert(queue_len(Pid) >= 200)
    after
        exit(Pid, kill)
    end.

refuses_past_the_high_watermark() ->
    ok = partisan_config:set(connection_high_watermark, 10),

    Pid = inert(),
    try
        Chan = partisan:default_channel(),

        %% Up to the mark, admitted.
        _ = [
            ?assertEqual(ok, ?CONN:cast_encoded(Pid, <<"x">>, Chan))
         || _ <- lists:seq(1, 10)
        ],

        %% At the mark, refused — and it stays refused.
        ?assertEqual(
            {error, overloaded}, ?CONN:cast_encoded(Pid, <<"x">>, Chan)
        ),
        ?assertEqual(
            {error, overloaded}, ?CONN:cast_encoded(Pid, <<"x">>, Chan)
        )
    after
        exit(Pid, kill)
    end.

%% THE property. Refusing is only useful if the refused message is genuinely not
%% queued: a bound that still enqueues is not a bound, and one that enqueues
%% *and* reports an error is worse than either.
does_not_queue_what_it_refuses() ->
    ok = partisan_config:set(connection_high_watermark, 5),

    Pid = inert(),
    try
        Chan = partisan:default_channel(),

        _ = [?CONN:cast_encoded(Pid, <<"x">>, Chan) || _ <- lists:seq(1, 5)],
        ?assertEqual(5, queue_len(Pid)),

        _ = [
            ?assertEqual(
                {error, overloaded}, ?CONN:cast_encoded(Pid, <<"x">>, Chan)
            )
         || _ <- lists:seq(1, 50)
        ],

        %% Fifty refusals later, the queue is exactly where it was.
        ?assertEqual(5, queue_len(Pid))
    after
        exit(Pid, kill)
    end.

%% The mark is a high-water mark, not a fuse: once the connection catches up,
%% sends must be admitted again without any explicit reset.
admits_again_once_the_queue_drains() ->
    ok = partisan_config:set(connection_high_watermark, 3),

    Chan = partisan:default_channel(),

    %% This one drains on demand.
    Self = self(),
    Pid = spawn(fun() -> drainer(Self) end),

    try
        _ = [?CONN:cast_encoded(Pid, <<"x">>, Chan) || _ <- lists:seq(1, 3)],
        ?assertEqual(
            {error, overloaded}, ?CONN:cast_encoded(Pid, <<"x">>, Chan)
        ),

        %% Let it consume everything queued.
        Pid ! {drain, self()},
        receive
            drained -> ok
        after 5000 -> ?assert(false)
        end,

        ?assertEqual(ok, ?CONN:cast_encoded(Pid, <<"x">>, Chan))
    after
        exit(Pid, kill)
    end.

%% Deliberately has no catch-all clause: anything other than `{drain, _}' stays
%% in the mailbox and counts towards the queue length. A catch-all would consume
%% the sends as fast as they arrived and the queue would never fill.
drainer(Parent) ->
    receive
        {drain, From} ->
            ok = drain_all(),
            From ! drained,
            drainer(Parent)
    end.

drain_all() ->
    receive
        _ -> drain_all()
    after 0 ->
        ok
    end.

%% A `monotonic' channel already has an overload strategy — drop the superseded
%% message inside `partisan_peer_socket:send/2' — and it is the right one for
%% traffic where only the freshest value matters. Applying the mark there would
%% turn deliberate, silent drops into errors those senders have never handled.
monotonic_channels_are_exempt() ->
    Old = partisan_config:get(channels, undefined),

    ok = partisan_config:set(channels, #{
        bulk => #{monotonic => false, parallelism => 1},
        sensors => #{monotonic => true, parallelism => 1}
    }),
    ok = partisan_config:set(connection_high_watermark, 5),

    Pid = inert(),
    try
        %% The ordinary channel is bounded.
        _ = [?CONN:cast_encoded(Pid, <<"x">>, bulk) || _ <- lists:seq(1, 5)],
        ?assertEqual(
            {error, overloaded}, ?CONN:cast_encoded(Pid, <<"x">>, bulk)
        ),

        %% The monotonic one is not, on the very same connection.
        _ = [
            ?assertEqual(ok, ?CONN:cast_encoded(Pid, <<"x">>, sensors))
         || _ <- lists:seq(1, 50)
        ]
    after
        exit(Pid, kill),
        restore_channels(Old)
    end.

restore_channels(undefined) ->
    ok;
restore_channels(Old) ->
    partisan_config:set(channels, Old).

%% `process_info/2' answers `undefined' for a dead process. That is not
%% overload, and reporting it as such would mislead the caller about why its
%% send failed — connection loss has its own handling.
a_dead_connection_is_not_reported_as_overloaded() ->
    ok = partisan_config:set(connection_high_watermark, 1),

    Pid = inert(),
    exit(Pid, kill),
    ok = wait_dead(Pid, 100),

    ?assertEqual(
        ok, ?CONN:cast_encoded(Pid, <<"x">>, partisan:default_channel())
    ).

wait_dead(_Pid, 0) ->
    error(still_alive);
wait_dead(Pid, N) ->
    case erlang:is_process_alive(Pid) of
        false ->
            ok;
        true ->
            timer:sleep(10),
            wait_dead(Pid, N - 1)
    end.
