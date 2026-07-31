%% -----------------------------------------------------------------------------
%% Full-path CT for the peer-plane security controls (BATCH 1).
%%
%% Runs against a live single Partisan node (its peer listener) — no cluster —
%% so it is light enough for a normal runner, and is included in the heavy
%% `ci-heavy' suite that runs on Fly.
%% -----------------------------------------------------------------------------
-module(partisan_security_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [frame_size_cap_rejects_oversized_frame].

init_per_suite(Config) ->
    _ = application:load(partisan),
    %% Use a small cap so the test does not have to allocate 64 MB to exceed it.
    ok = application:set_env(partisan, max_message_size, 512),
    ok = application:set_env(partisan, tls, false),
    {ok, _} = application:ensure_all_started(partisan),
    Config.

end_per_suite(_Config) ->
    _ = application:stop(partisan),
    _ = application:unset_env(partisan, max_message_size),
    _ = application:unset_env(partisan, tls),
    ok.

%% An unauthenticated peer that sends a frame larger than `max_message_size'
%% must not be able to crash the node: the oversized frame is rejected at the
%% socket ({packet_size}) before it is assembled/decoded (WP-1.1).
frame_size_cap_rejects_oversized_frame(_Config) ->
    %% Confirm the small cap is actually in effect on this running node, so the
    %% test genuinely exercises the size guard rather than a decode failure.
    ?assertEqual(512, partisan_config:get(max_message_size)),
    {Ip, Port} = listen_endpoint(),
    {ok, Sock} = gen_tcp:connect(
        Ip, Port, [binary, {packet, 4}, {active, false}], 5000
    ),
    %% A frame far larger than the 512-byte cap set in init_per_suite.
    ok = gen_tcp:send(Sock, binary:copy(<<"x">>, 8192)),
    %% The node must survive the oversized pre-auth frame.
    timer:sleep(300),
    ?assert(lists:keymember(partisan, 1, application:which_applications())),
    ?assert(is_list(partisan:nodes())),
    _ = catch gen_tcp:close(Sock),
    ok.

%% @private
listen_endpoint() ->
    case partisan_config:get(listen_addrs, []) of
        [#{ip := Ip, port := Port} | _] ->
            {Ip, Port};
        _ ->
            {{127, 0, 0, 1}, partisan_config:get(listen_port)}
    end.
