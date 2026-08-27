%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc A peer that connects and immediately disconnects must not take down
%% this node's listening socket.
%%
%% `acceptor.erl' escalated <em>per-connection</em> errors to the listener:
%% `failure/2' called `exit(LSock, Reason)', killing the listening socket, and
%% because a port is linked to its owner that also killed
%% `partisan_acceptor_socket'. `partisan_acceptor_socket_pool_sup' then
%% restarted it into the same condition until it hit
%% `reached_max_restart_intensity' and shut the subtree down, after which the
%% node could not accept at all.
%%
%% The trigger is ordinary: on darwin a client that closes right after
%% connecting makes the post-accept `inet:setopts/2' return `{error, einval}',
%% and a client that sends RST makes `gen_tcp:accept/2' itself return
%% `{error, einval}'. Partisan produces exactly this whenever it drops a
%% duplicate or rejected connection — and so does any TCP health check, load
%% balancer probe or port scan, which is why this is a production concern and
%% not only a test one.
%%
%% This test needs no TLS and no cluster. TLS only raised the <em>rate</em> of
%% connect-then-close enough to exhaust the restart intensity; it was never
%% part of the mechanism.
%%
%% What this does not cover: the escalation path for genuinely listener-fatal
%% conditions, which is deliberately left as it was.
%% @end
%% =============================================================================
-module(partisan_listener_resilience_test).

-include_lib("eunit/include/eunit.hrl").

-define(PROBES, 20).

listener_resilience_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        {timeout, 60, fun probes_do_not_kill_the_listener/0}
    ]}.


%% -----------------------------------------------------------------------------
%% Twenty connect-then-close probes, the shape a health check produces. The
%% listener must still be the same process afterwards and must still accept.
%% -----------------------------------------------------------------------------
probes_do_not_kill_the_listener() ->
    %% Take the address partisan actually bound. Assuming `127.0.0.1' is wrong:
    %% with the default configuration this host resolves to `::1', and probing
    %% the wrong family yields `econnrefused' whether or not the defect is
    %% present -- which is a false failure, not a reproduction.
    [#{ip := IP, port := Port} | _] = partisan_config:get(listen_addrs),

    Before = acceptor_socket_pid(),
    ?assert(is_pid(Before)),

    _ = [probe(IP, Port) || _ <- lists:seq(1, ?PROBES)],

    %% The listener dies almost immediately when the defect is present, but a
    %% negative assertion still needs a settle window; keep it generous.
    timer:sleep(1000),

    %% The socket owner must be the same process: when the defect is present it
    %% is killed and restarted until the supervisor gives up.
    ?assertEqual(Before, acceptor_socket_pid()),

    %% The requirement that actually matters: it still accepts.
    ?assertMatch({ok, _}, gen_tcp:connect(
        IP, Port, [binary, {active, false}], 2000
    )).


%% @private
%% Connect and drop immediately -- both flavours, since FIN and RST fail at
%% different points (post-accept setopts vs accept itself).
probe(IP, Port) ->
    Opts = case rand:uniform(2) of
        1 -> [binary, {active, false}];
        2 -> [binary, {active, false}, {linger, {true, 0}}]
    end,
    case gen_tcp:connect(IP, Port, Opts, 2000) of
        {ok, S} -> gen_tcp:close(S);
        _ -> ok
    end.


%% @private
acceptor_socket_pid() ->
    Children = supervisor:which_children(partisan_acceptor_socket_pool_sup),
    case [P || {Id, P, _, _} <- Children, is_acceptor_socket(Id)] of
        [Pid] -> Pid;
        Other -> Other
    end.


%% @private
is_acceptor_socket({partisan_acceptor_socket, _, _}) -> true;
is_acceptor_socket(_) -> false.


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
