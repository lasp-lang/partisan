%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests that `partisan_peer_service_manager:deliver/2' preserves the
%% priority of an OTP 28 <em>priority alias</em>.
%%
%% A priority alias (`erlang:alias/1' with the `priority' option) is the
%% receiver's opt-in to having messages inserted ahead of the ordinary part of
%% its message queue. The runtime honours that opt-in only when the sender
%% passes both the alias <em>and</em> the `priority' option to
%% `erlang:send/3'; a plain `Ref ! Msg' is delivered as an ordinary message.
%% Partisan's last hop used `!', so the receiver's opt-in was discarded --
%% silently, because `erlang:send/3' reports `ok' either way.
%%
%% <b>The ordering assertions are timing-free.</b> The receiver blocks in a
%% selective `receive' that matches only `{drain_now, Parent}', so it consumes
%% nothing else and every other message keeps the queue position the runtime
%% gave it. `drain_now' is sent last and is itself ordinary, so it lands at the
%% end; the drain that follows reports the queue exactly as it was built. No
%% sleeps, nothing to go flaky under load.
%%
%% What each case covers:
%% <ul>
%% <li>`priority_alias_overtakes_queued_ordinary_messages' -- the direct
%% `is_reference/1' clause of `do_deliver/2'.</li>
%% <li>`encoded_priority_alias_overtakes_queued_ordinary_messages' -- the
%% clause reached after `partisan_remote_ref:to_term/1' decodes an encoded
%% reference, which is the path an alias takes when it arrives from a peer.</li>
%% <li>`ordinary_alias_is_not_promoted' and
%% `deactivated_alias_is_dropped_without_crashing' -- guards. Partisan cannot
%% detect whether a reference is a priority alias (no BIF exposes it), so it
%% passes the `priority' option unconditionally. These two assert that doing so
%% neither promotes a message the receiver never opted in for, nor turns a
%% dropped message into a crash.</li>
%% </ul>
%%
%% Not covered: delivery from a genuinely remote node. These run on one node,
%% which exercises both `do_deliver/2' reference clauses but not the connection
%% process that calls them.
%% @end
%% =============================================================================
-module(partisan_priority_alias_test).

-include_lib("eunit/include/eunit.hrl").

-if(?OTP_RELEASE >= 28).

priority_alias_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        fun priority_alias_overtakes_queued_ordinary_messages/0,
        fun encoded_priority_alias_overtakes_queued_ordinary_messages/0,
        fun ordinary_alias_is_not_promoted/0,
        fun deactivated_alias_is_dropped_without_crashing/0
    ]}.


%% =============================================================================
%% CASES
%% =============================================================================


%% -----------------------------------------------------------------------------
%% Three ordinary messages are already queued. The message Partisan delivers to
%% the priority alias must be at the head of the queue, ahead of all of them.
%% -----------------------------------------------------------------------------
priority_alias_overtakes_queued_ordinary_messages() ->
    {Pid, Alias} = start_receiver(priority),
    ok = queue_ordinary(Pid, 3),

    ok = partisan_peer_service_manager:deliver(Alias, {priority_msg, first}),

    ?assertEqual(
        [
            {priority_msg, first},
            {ordinary, 1}, {ordinary, 2}, {ordinary, 3}
        ],
        drain_queue(Pid)
    ).


%% -----------------------------------------------------------------------------
%% The same, but addressed with an encoded reference, which is how an alias
%% reaches `do_deliver/2' when it has crossed the wire.
%% -----------------------------------------------------------------------------
encoded_priority_alias_overtakes_queued_ordinary_messages() ->
    {Pid, Alias} = start_receiver(priority),
    Encoded = partisan_remote_ref:from_term(Alias),
    ok = queue_ordinary(Pid, 3),

    ok = partisan_peer_service_manager:deliver(Encoded, {priority_msg, first}),

    ?assertEqual(
        [
            {priority_msg, first},
            {ordinary, 1}, {ordinary, 2}, {ordinary, 3}
        ],
        drain_queue(Pid)
    ).


%% -----------------------------------------------------------------------------
%% An ordinary alias carries no opt-in. Passing the `priority' option
%% unconditionally must leave it exactly where it was: last.
%% -----------------------------------------------------------------------------
ordinary_alias_is_not_promoted() ->
    {Pid, Alias} = start_receiver(ordinary),
    ok = queue_ordinary(Pid, 3),

    ok = partisan_peer_service_manager:deliver(Alias, {ordinary_msg, last}),

    ?assertEqual(
        [
            {ordinary, 1}, {ordinary, 2}, {ordinary, 3},
            {ordinary_msg, last}
        ],
        drain_queue(Pid)
    ).


%% -----------------------------------------------------------------------------
%% Deactivating an alias is what lets a caller abandon a request without a late
%% reply landing in its mailbox. The runtime drops the message; Partisan must
%% not turn that into an error.
%% -----------------------------------------------------------------------------
deactivated_alias_is_dropped_without_crashing() ->
    {Pid, Alias} = start_receiver(deactivated),
    ok = queue_ordinary(Pid, 2),

    ok = partisan_peer_service_manager:deliver(Alias, {should_not, arrive}),

    ?assertEqual([{ordinary, 1}, {ordinary, 2}], drain_queue(Pid)).


%% =============================================================================
%% HELPERS
%% =============================================================================


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


%% @private
%% @doc Spawns a process that creates an alias of the requested kind, hands it
%% back, and then blocks consuming nothing until told to drain.
%% -----------------------------------------------------------------------------
start_receiver(Kind) ->
    Parent = self(),

    Pid = spawn_link(fun() ->
        Alias = make_alias(Kind),
        Parent ! {alias, self(), Alias},
        receive
            {drain_now, Parent} ->
                Parent ! {drained, self(), drain_mailbox()}
        end
    end),

    receive
        {alias, Pid, Alias} ->
            {Pid, Alias}
    after
        5000 -> error(receiver_did_not_start)
    end.


%% @private
%% `unalias/1' only works for the owning process, so the deactivation happens
%% here, before the alias is handed back -- which also makes it ordered.
make_alias(priority) ->
    erlang:alias([priority]);

make_alias(ordinary) ->
    erlang:alias();

make_alias(deactivated) ->
    Alias = erlang:alias([priority]),
    true = erlang:unalias(Alias),
    Alias.


%% @private
queue_ordinary(Pid, N) ->
    _ = [Pid ! {ordinary, I} || I <- lists:seq(1, N)],
    ok.


%% @private
drain_queue(Pid) ->
    Pid ! {drain_now, self()},
    receive
        {drained, Pid, Msgs} -> Msgs
    after
        5000 -> error(receiver_did_not_drain)
    end.


%% @private
drain_mailbox() ->
    receive
        Msg -> [Msg | drain_mailbox()]
    after
        0 -> []
    end.

-endif.
