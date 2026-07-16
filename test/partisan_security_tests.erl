%% -----------------------------------------------------------------------------
%% Regression tests for the BATCH 1 peer-plane security hardening.
%%
%% These are lightweight, locally-runnable guards. The full end-to-end checks
%% (an oversized frame reaching a live peer listener, a stalled TLS handshake)
%% belong in the heavy CT suite that runs on Fly.
%% -----------------------------------------------------------------------------
-module(partisan_security_tests).

-include_lib("eunit/include/eunit.hrl").

%% -----------------------------------------------------------------------------
%% Config defaults — the security options must exist with safe defaults.
%% -----------------------------------------------------------------------------

max_message_size_default_test() ->
    ok = partisan_config:init(),
    %% 64 MB — bounds an inbound peer frame (WP-1.1).
    ?assertEqual(67108864, partisan_config:get(max_message_size)).

tls_handshake_timeout_default_test() ->
    ok = partisan_config:init(),
    %% 5000 ms — bounds a stalled server-side TLS handshake (WP-1.2).
    ?assertEqual(5000, partisan_config:get(tls_handshake_timeout)).

%% -----------------------------------------------------------------------------
%% Frame-size cap mechanism — a {packet, 4} socket with {packet_size, Max}
%% rejects an oversized frame before delivering its payload. This is the guard
%% partisan wires onto the connect and accept sockets (WP-1.1), so a peer cannot
%% force assembly/decode of an arbitrarily large frame.
%% -----------------------------------------------------------------------------

packet_size_rejects_oversized_frame_test() ->
    Max = 100,
    {LSock, CSock, ASock} = connected_pair(Max),
    try
        %% Frame body larger than Max; {packet, 4} prepends its length.
        ok = gen_tcp:send(CSock, binary:copy(<<"x">>, Max * 2)),
        %% The receiver must error (emsgsize), NOT deliver the oversized body.
        ?assertMatch({error, _}, gen_tcp:recv(ASock, 0, 1000))
    after
        close_all([CSock, ASock, LSock])
    end.

packet_size_allows_frame_within_limit_test() ->
    Max = 100,
    {LSock, CSock, ASock} = connected_pair(Max),
    try
        Payload = binary:copy(<<"y">>, 50),
        ok = gen_tcp:send(CSock, Payload),
        ?assertEqual({ok, Payload}, gen_tcp:recv(ASock, 0, 1000))
    after
        close_all([CSock, ASock, LSock])
    end.

%% -----------------------------------------------------------------------------
%% Helpers
%% -----------------------------------------------------------------------------

%% Listener uses the same framing partisan's accept socket uses, capped at Max.
connected_pair(Max) ->
    {ok, LSock} = gen_tcp:listen(0, [
        binary,
        {packet, 4},
        {packet_size, Max},
        {active, false},
        {reuseaddr, true}
    ]),
    {ok, Port} = inet:port(LSock),
    {ok, CSock} = gen_tcp:connect(
        {127, 0, 0, 1}, Port, [binary, {packet, 4}, {active, false}], 1000
    ),
    {ok, ASock} = gen_tcp:accept(LSock, 1000),
    {LSock, CSock, ASock}.

close_all(Socks) ->
    _ = [catch gen_tcp:close(S) || S <- Socks],
    ok.
