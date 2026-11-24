%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

%% Tests for the peer authentication phase (ADR-000005) on live sockets.
%%
%% One Partisan node runs in the test VM. A test plays the other end of a
%% connection in one of two ways:
%%
%% - as a raw socket against the node's peer listener, which exercises
%%   `partisan_peer_service_server';
%% - as a raw listener that a `partisan_peer_service_client' dials, which
%%   exercises the client.
%%
%% Every case runs over TCP and over TLS. The TLS group uses a self-signed
%% certificate and no peer verification: TLS is there to exercise the socket
%% paths, which differ from TCP for a frame over the limit.
%%
%% Not covered: two nodes with different trust material over sockets (one VM
%% has one configuration; `partisan_peer_auth_test' covers that without
%% sockets), a cluster of nodes, and a mixed-version cluster.
-module(partisan_peer_auth_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("partisan/include/partisan_peer_socket.hrl").

-compile([export_all, nowarn_export_all]).

-define(SECRET, <<"0123456789abcdef0123456789abcdef">>).
-define(FAKE, 'auth_suite_fake@host').
-define(RECV_TIMEOUT, 5000).

all() ->
    [{group, tcp}, {group, tls}, {group, enabled_at_runtime}].

groups() ->
    Cases = [
        unauthenticated_frame_creates_no_atom,
        phase_off_frame_creates_atom,
        oversized_frame_closes_connection,
        existing_protocol_refused,
        silent_peer_times_out,
        authenticated_peer_completes_handshake,
        authenticated_name_is_bound,
        client_and_server_authenticate,
        client_and_server_authenticate_cryptosign,
        client_raises_frame_limit,
        client_refuses_unauthenticated_frame,
        client_refuses_oversized_frame,
        client_refuses_server_without_phase,
        client_accepts_server_without_phase,
        client_holds_messages_until_handshake,
        client_fails_waiting_callers,
        frame_before_hello_closes_connection,
        frame_in_pieces_is_accepted,
        bytes_after_response_close_connection,
        oversized_length_closes_before_the_frame,
        pending_connections_are_limited,
        allowed_nodes,
        monotonic_channel_authenticates,
        no_term_decode_in_connection_processes,
        proofs_cover_channel_binding,
        relayed_exchange,
        earlier_tls_version_has_no_binding
    ],
    %% The node of this group starts with the phase off, and each case turns
    %% it on while the node runs. The listener is the one that started with
    %% the phase off.
    Runtime = [
        unauthenticated_frame_creates_no_atom,
        oversized_frame_closes_connection,
        existing_protocol_refused,
        authenticated_peer_completes_handshake,
        client_and_server_authenticate
    ],
    [{tcp, [], Cases}, {tls, [], Cases}, {enabled_at_runtime, [], Runtime}].

init_per_group(Group, Config) ->
    _ = application:load(partisan),
    TLS = Group =:= tls,
    Env =
        [
            {peer_ip, {127, 0, 0, 1}},
            {tls, TLS},
            {peer_auth_methods,
                case Group of
                    enabled_at_runtime -> [];
                    _ -> [hmac_secret]
                end},
            {peer_auth_secret, ?SECRET}
        ] ++ tls_env(TLS, Config),
    [ok = application:set_env(partisan, K, V) || {K, V} <- Env],
    {ok, _} = application:ensure_all_started(partisan),
    [{tls, TLS}, {env, Env} | Config].

end_per_group(_Group, Config) ->
    _ = application:stop(partisan),
    [application:unset_env(partisan, K) || {K, _} <- ?config(env, Config)],
    ok.

init_per_testcase(_Case, Config) ->
    process_flag(trap_exit, true),
    ok = partisan_config:set(peer_auth_methods, [hmac_secret]),
    ok = partisan_config:set(peer_auth_secret, ?SECRET),
    ok = partisan_config:set(peer_auth_cryptosign, undefined),
    ok = partisan_config:set(peer_auth_timeout, 5000),
    ok = partisan_config:set(peer_auth_allowed_nodes, all),
    ok = partisan_config:set(peer_auth_max_pending, 1024),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%% =============================================================================
%% THE ACCEPTING SIDE
%% =============================================================================

%% The reported attack, on a node that requires authentication: a frame that
%% names atoms the node does not hold. The node answers with a refusal,
%% closes, and holds none of the atoms.
unauthenticated_frame_creates_no_atom(Config) ->
    {Frame, Names} = atom_frame(100),
    ?assert(byte_size(Frame) =< partisan_peer_auth:max_frame_size()),
    Sock = dial(Config),
    ?assertMatch({auth_challenge, [<<"hmac_secret">>], _, _, _}, recv_frame(Sock)),
    ok = send(Sock, Frame),
    ?assertEqual({auth_result, error}, recv_frame(Sock)),
    ?assertEqual({error, closed}, recv(Sock)),
    ?assertEqual([], [N || N <- Names, atom_exists(N)]),
    ?assert(node_is_up()).

%% The control for the case above: with the phase off, the node decodes the
%% first frame as it always has, and an atom the frame names exists afterwards.
%% This is the reported behaviour. It shows that the case above fails on a
%% node that does not bound its decode.
phase_off_frame_creates_atom(Config) ->
    ok = partisan_config:set(peer_auth_methods, []),
    Name = unseen_name(),
    Sock = dial(Config),
    ?assertEqual({hello, partisan:node()}, recv_term(Sock)),
    %% `hello' with a node name the node has never seen, written by hand so
    %% that this VM does not create the atom itself.
    Channel = term_to_binary(partisan_config:default_channel()),
    <<131, ChannelExt/binary>> = Channel,
    Frame = <<
        131, 104, 3, 119, 5, "hello", 119, (byte_size(Name)), Name/binary,
        ChannelExt/binary
    >>,
    ?assertNot(atom_exists(Name)),
    ok = send(Sock, Frame),
    ?assertMatch({state, _, _}, recv_term(Sock)),
    ?assert(atom_exists(Name)),
    %% The node started with the phase on and now runs with it off: the
    %% connection has the frame limit of the existing protocol.
    Id = binary:copy(<<"x">>, 3 * partisan_peer_auth:max_frame_size()),
    From = binary_to_atom(Name),
    ok = send_term(Sock, #ping{from = From, id = Id, timestamp = 0}),
    ?assertMatch(#pong{id = Id}, recv_term(Sock)),
    close(Sock).

%% A frame over the limit of the phase: the node refuses it on its length
%% and does not assemble it. 100,000 atoms is the frame of the report.
oversized_frame_closes_connection(Config) ->
    {Frame, Names} = atom_frame(100000),
    Sock = dial(Config),
    ?assertMatch({auth_challenge, _, _, _, _}, recv_frame(Sock)),
    _ = send(Sock, Frame),
    %% The refusal, when the close has not overtaken it, and then the close.
    case recv(Sock) of
        {ok, Result} ->
            ?assertEqual(
                {auth_result, error}, partisan_peer_auth:decode(Result, true)
            ),
            ?assertMatch({error, _}, recv(Sock));
        {error, _} ->
            ok
    end,
    ?assertEqual([], [N || N <- Names, atom_exists(N)]),
    ?assert(node_is_up()).

%% A peer that speaks the existing protocol gets no membership from a node
%% that requires authentication.
existing_protocol_refused(Config) ->
    Sock = dial(Config),
    ?assertMatch({auth_challenge, _, _, _, _}, recv_frame(Sock)),
    ok = send_term(Sock, {hello, partisan:node(), partisan_config:default_channel()}),
    ?assertEqual({auth_result, error}, recv_frame(Sock)),
    ?assertEqual({error, closed}, recv(Sock)).

%% A peer that connects and sends nothing is closed after
%% `peer_auth_timeout'.
silent_peer_times_out(Config) ->
    ok = partisan_config:set(peer_auth_timeout, 300),
    Sock = dial(Config),
    ?assertMatch({auth_challenge, _, _, _, _}, recv_frame(Sock)),
    T0 = erlang:monotonic_time(millisecond),
    ?assertEqual({error, closed}, recv(Sock)),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed >= 250),
    ?assert(Elapsed < 3000).

%% A peer that authenticates gets the existing protocol, with its frame
%% limit: a frame larger than the limit of the phase passes in both
%% directions afterwards.
authenticated_peer_completes_handshake(Config) ->
    Node = partisan:node(),
    Sock = dial(Config),
    ?assertMatch(#{method := hmac_secret}, authenticate(Sock, Node, Node)),
    %% The node sends no `hello' after the phase. The peer sends its own.
    ok = send_term(Sock, {hello, Node, partisan_config:default_channel()}),
    ?assertMatch({state, _, _}, recv_term(Sock)),
    Id = binary:copy(<<"x">>, 3 * partisan_peer_auth:max_frame_size()),
    ok = send_term(Sock, #ping{from = Node, id = Id, timestamp = 0}),
    ?assertMatch(#pong{from = Node, id = Id}, recv_term(Sock)),
    close(Sock).

%% A peer that authenticates under one name cannot present another in
%% `hello'.
authenticated_name_is_bound(Config) ->
    Node = partisan:node(),
    Sock = dial(Config),
    ?assertMatch(#{method := hmac_secret}, authenticate(Sock, ?FAKE, Node)),
    ok = send_term(Sock, {hello, Node, partisan_config:default_channel()}),
    ?assertEqual({error, closed}, recv(Sock)).

%% =============================================================================
%% BOTH SIDES
%% =============================================================================

%% A `partisan_peer_service_client' dials the node's own listener. Both
%% processes run the phase and then the existing handshake.
client_and_server_authenticate(_Config) ->
    {ok, Pid} = start_client(partisan:node_spec(), listen_addr()),
    ?assertMatch({connected, _, _, _, _}, connected()),
    ?assert(is_process_alive(Pid)),
    stop_client(Pid).

client_and_server_authenticate_cryptosign(_Config) ->
    #{public_key := Public, private_key := Private} =
        partisan_peer_auth_cryptosign:generate_key(),
    ok = partisan_config:set(peer_auth_cryptosign, #{
        private_key => Private,
        trusted_peers => #{partisan:node() => Public}
    }),
    ok = partisan_config:set(peer_auth_methods, [cryptosign]),
    {ok, Pid} = start_client(partisan:node_spec(), listen_addr()),
    ?assertMatch({connected, _, _, _, _}, connected()),
    stop_client(Pid),
    %% The node does not trust the key of any other name.
    ok = partisan_config:set(peer_auth_cryptosign, #{
        private_key => Private,
        trusted_peers => #{?FAKE => Public}
    }),
    {ok, Pid2} = start_client(partisan:node_spec(), listen_addr()),
    ?assertEqual({shutdown, {auth_failed, rejected}}, exit_reason(Pid2)).

%% =============================================================================
%% THE CONNECTING SIDE
%% =============================================================================

%% After the phase a client accepts a frame larger than the limit of the
%% phase.
client_raises_frame_limit(Config) ->
    Big = binary:copy(<<"x">>, 3 * partisan_peer_auth:max_frame_size()),
    Server = raw_server(Config, fun(Sock) ->
        {Challenge, Neg} = partisan_peer_auth:server_init(
            [hmac_secret], ?FAKE, binding(Sock)
        ),
        ok = send(Sock, Challenge),
        {ok, Response} = recv(Sock),
        {ok, Result, _} = partisan_peer_auth:server_handle(Response, Neg),
        ok = send(Sock, Result),
        {hello, _, _} = recv_term(Sock),
        ok = send_term(Sock, {state, undefined, Big})
    end),
    {ok, Pid} = start_client(fake_spec(Server), Server),
    ?assertMatch({connected, _, _, undefined, Big}, connected()),
    stop_client(Pid).

%% A client that requires authentication dials a hostile server. The server
%% sends the frame of the report. The client closes and holds none of the
%% atoms.
client_refuses_unauthenticated_frame(Config) ->
    {Frame, Names} = atom_frame(100),
    ?assert(byte_size(Frame) =< partisan_peer_auth:max_frame_size()),
    Server = raw_server(Config, fun(Sock) -> send(Sock, Frame) end),
    {ok, Pid} = start_client(fake_spec(Server), Server),
    ?assertEqual(
        {shutdown, {auth_failed, authentication_required}}, exit_reason(Pid)
    ),
    ?assertEqual([], [N || N <- Names, atom_exists(N)]).

client_refuses_oversized_frame(Config) ->
    {Frame, Names} = atom_frame(100000),
    Server = raw_server(Config, fun(Sock) -> send(Sock, Frame) end),
    {ok, Pid} = start_client(fake_spec(Server), Server),
    %% The same on TCP and on TLS: the client takes the frames itself.
    ?assertEqual({shutdown, {auth_failed, frame_too_large}}, exit_reason(Pid)),
    ?assertEqual([], [N || N <- Names, atom_exists(N)]).

%% A client that requires authentication does not continue with a server that
%% runs no phase.
client_refuses_server_without_phase(Config) ->
    Server = raw_server(Config, fun(Sock) -> send_term(Sock, {hello, ?FAKE}) end),
    {ok, Pid} = start_client(fake_spec(Server), Server),
    ?assertEqual(
        {shutdown, {auth_failed, authentication_required}}, exit_reason(Pid)
    ).

%% A client that also accepts `anonymous' continues with the existing
%% protocol. This is the first step of a rollout.
client_accepts_server_without_phase(Config) ->
    ok = partisan_config:set(peer_auth_methods, [hmac_secret, anonymous]),
    Test = self(),
    Server = raw_server(Config, fun(Sock) ->
        ok = send_term(Sock, {hello, ?FAKE}),
        Test ! {server_received, recv_term(Sock)}
    end),
    {ok, Pid} = start_client(fake_spec(Server), Server),
    Node = partisan:node(),
    receive
        {server_received, Term} ->
            ?assertMatch({hello, Node, _}, Term)
    after ?RECV_TIMEOUT ->
        ct:fail(no_hello)
    end,
    stop_client(Pid).

%% A client gives no message to a server before the handshake completes, and
%% loses none. The server here sends its challenge and then waits. The test
%% casts and calls messages into the client, as a manager that holds it as a
%% connection does. The server receives the client's response and nothing
%% else. It then completes the phase. The client sends its `hello', and the
%% messages follow it in the order they came.
client_holds_messages_until_handshake(Config) ->
    Test = self(),
    Server = raw_server(Config, fun(Sock) ->
        {Challenge, Neg} = partisan_peer_auth:server_init(
            [hmac_secret], ?FAKE, binding(Sock)
        ),
        ok = send(Sock, Challenge),
        {ok, Response} = recv(Sock),
        Test ! {response_received, self()},
        receive messages_given -> ok end,
        Test ! {before_result, recv(Sock, 500)},
        {ok, Result, _} = partisan_peer_auth:server_handle(Response, Neg),
        ok = send(Sock, Result),
        Test ! {frames, [recv_term(Sock) || _ <- lists:seq(1, 4)]}
    end),
    {ok, Pid} = start_client(fake_spec(Server), Server),
    Wait = fun(Tag) ->
        receive
            {Tag, Value} -> Value
        after ?RECV_TIMEOUT -> ct:fail({no_message, Tag})
        end
    end,
    ServerPid = Wait(response_received),
    ok = gen_server:cast(Pid, {send_message, first}),
    ok = gen_server:cast(Pid, {send_encoded, term_to_binary(second)}),
    spawn_link(fun() ->
        Test ! {call_reply, gen_server:call(Pid, {send_message, third})}
    end),
    %% The client has the three messages before the server continues.
    timer:sleep(200),
    _ = sys:get_state(Pid),
    ServerPid ! messages_given,
    %% Nothing before the phase completes.
    ?assertEqual({error, timeout}, Wait(before_result)),
    Node = partisan:node(),
    ?assertMatch([{hello, Node, _}, first, second, third], Wait(frames)),
    ?assertEqual(ok, Wait(call_reply)),
    stop_client(Pid).

%% A caller that waits for a message to go out gets an error when the
%% handshake fails. It does not wait for its own timeout.
client_fails_waiting_callers(Config) ->
    ok = partisan_config:set(peer_auth_timeout, 500),
    Test = self(),
    Server = raw_server(Config, fun(Sock) ->
        {Challenge, _} = partisan_peer_auth:server_init(
            [hmac_secret], ?FAKE, binding(Sock)
        ),
        ok = send(Sock, Challenge),
        {ok, _Response} = recv(Sock),
        Test ! response_received
    end),
    {ok, Pid} = start_client(fake_spec(Server), Server),
    receive response_received -> ok after ?RECV_TIMEOUT -> ct:fail(no_response) end,
    spawn_link(fun() ->
        Test ! {call_reply, gen_server:call(Pid, {send_message, third}, 10000)}
    end),
    ?assertEqual({shutdown, {auth_failed, timeout}}, exit_reason(Pid)),
    receive
        {call_reply, Reply} -> ?assertEqual({error, closed}, Reply)
    after ?RECV_TIMEOUT -> ct:fail(no_reply)
    end.

%% A peer that authenticates and sends a frame before `hello' is closed. The
%% frame does not reach the manager, and the check of the authenticated name
%% cannot be skipped.
frame_before_hello_closes_connection(Config) ->
    Node = partisan:node(),
    Sock = dial(Config),
    ?assertMatch(#{method := hmac_secret}, authenticate(Sock, Node, Node)),
    ok = send_term(Sock, #ping{from = Node, id = make_ref(), timestamp = 0}),
    ?assertEqual({error, closed}, recv(Sock)),
    ?assert(node_is_up()).

%% The node takes the frames of the phase out of the bytes itself. A response
%% that arrives one byte at a time is the same response.
frame_in_pieces_is_accepted(Config) ->
    Node = partisan:node(),
    Sock = dial_raw(Config),
    {ok, Challenge} = recv_raw_frame(Sock),
    Neg0 = partisan_peer_auth:client_init([hmac_secret], Node, Node, binding(Sock)),
    {reply, Response, Neg1} = partisan_peer_auth:client_handle(Challenge, Neg0),
    Wire = iolist_to_binary(partisan_peer_auth:enframe(Response)),
    [ok = send(Sock, <<Byte>>) || <<Byte>> <= Wire],
    {ok, Result} = recv_raw_frame(Sock),
    ?assertMatch({ok, #{method := hmac_secret}}, partisan_peer_auth:client_handle(Result, Neg1)),
    close(Sock).

%% A peer that sends bytes after its response, before it has read the result,
%% is closed, although the response is valid. The node does not carry bytes
%% from the phase into the existing protocol.
bytes_after_response_close_connection(Config) ->
    Node = partisan:node(),
    Sock = dial_raw(Config),
    {ok, Challenge} = recv_raw_frame(Sock),
    Neg0 = partisan_peer_auth:client_init([hmac_secret], Node, Node, binding(Sock)),
    {reply, Response, _} = partisan_peer_auth:client_handle(Challenge, Neg0),
    Hello = term_to_binary({hello, Node, partisan_config:default_channel()}),
    ok = send(Sock, [partisan_peer_auth:enframe(Response), partisan_peer_auth:enframe(Hello)]),
    {ok, Result} = recv_raw_frame(Sock),
    ?assertEqual({auth_result, error}, partisan_peer_auth:decode(Result, true)),
    ?assertMatch({error, _}, recv_raw_frame(Sock)).

%% A length over the limit closes the connection on its 4 bytes. The peer
%% sends no byte of the frame, so the node cannot have waited for it.
oversized_length_closes_before_the_frame(Config) ->
    Sock = dial_raw(Config),
    {ok, _Challenge} = recv_raw_frame(Sock),
    Over = partisan_peer_auth:max_frame_size() + 1,
    ok = send(Sock, <<Over:32>>),
    {ok, Result} = recv_raw_frame(Sock),
    ?assertEqual({auth_result, error}, partisan_peer_auth:decode(Result, true)),
    ?assertMatch({error, _}, recv_raw_frame(Sock)),
    ?assert(node_is_up()).

%% The node holds at most `peer_auth_max_pending' connections that have not
%% authenticated, and closes one more at once. A place is free again when a
%% connection ends or authenticates.
pending_connections_are_limited(Config) ->
    ok = wait_until(fun() -> partisan_peer_auth:pending() =:= 0 end),
    ok = partisan_config:set(peer_auth_max_pending, 2),
    try
        S1 = dial(Config),
        S2 = dial(Config),
        ?assertMatch({auth_challenge, _, _, _, _}, recv_frame(S1)),
        ?assertMatch({auth_challenge, _, _, _, _}, recv_frame(S2)),
        ?assertEqual(2, partisan_peer_auth:pending()),
        %% One more gets no challenge. Over TLS the node closes it before the
        %% TLS handshake, so the connect itself can fail.
        case catch dial(Config) of
            {'EXIT', _} -> ok;
            S3 -> ?assertMatch({error, _}, recv(S3))
        end,
        ?assertEqual(2, partisan_peer_auth:pending()),
        %% A connection that ends frees its place.
        close(S1),
        ok = wait_until(fun() -> partisan_peer_auth:pending() =:= 1 end),
        %% A connection that authenticates frees its place.
        Node = partisan:node(),
        S4 = dial(Config),
        ?assertMatch(#{method := hmac_secret}, authenticate(S4, Node, Node)),
        ok = wait_until(fun() -> partisan_peer_auth:pending() =:= 1 end),
        close(S2),
        close(S4),
        ok = wait_until(fun() -> partisan_peer_auth:pending() =:= 0 end)
    after
        ok = partisan_config:set(peer_auth_max_pending, 1024)
    end.

%% The gate of `net_kernel:allow/1'. A node that is not allowed is refused
%% when it connects, although it holds the key, and is not dialled.
allowed_nodes(Config) ->
    Node = partisan:node(),
    ok = partisan_peer_auth:allow([?FAKE]),
    try
        Sock = dial(Config),
        ?assertMatch(#{method := hmac_secret}, authenticate(Sock, Node, Node)),
        ok = send_term(Sock, {hello, Node, partisan_config:default_channel()}),
        ?assertEqual({error, closed}, recv(Sock)),
        %% The node does not dial itself either: it is not in the list.
        ?assertMatch({error, _}, start_client(partisan:node_spec(), listen_addr())),
        receive {'EXIT', _, _} -> ok after 0 -> ok end,

        ok = partisan_peer_auth:allow([Node]),
        {ok, Pid} = start_client(partisan:node_spec(), listen_addr()),
        ?assertMatch({connected, _, _, _, _}, connected()),
        stop_client(Pid)
    after
        ok = partisan_config:set(peer_auth_allowed_nodes, all)
    end.

%% A monotonic channel drops a frame when a newer one waits. The frames of
%% the handshake are not dropped: the client authenticates with messages in
%% its mailbox.
monotonic_channel_authenticates(_Config) ->
    Channel = partisan_config:default_channel(),
    Opts = (partisan_config:channel_opts(Channel))#{monotonic => true},
    {ok, Pid} = partisan_peer_service_client:start_link(
        partisan:node_spec(), listen_addr(), Channel, Opts, self()
    ),
    [Pid ! {noise, I} || I <- lists:seq(1, 50)],
    ?assertMatch({connected, _, _, _, _}, connected()),
    stop_client(Pid).

%% The connection processes themselves do not give a frame to
%% `binary_to_term/1,2' before authentication. A call trace on every process
%% of the VM looks for a call whose argument is the frame the test sent:
%% to the node's listener, and from a raw listener to a client.
no_term_decode_in_connection_processes(Config) ->
    Term = term_to_binary({hello, ?FAKE}),
    {Atoms, _} = atom_frame(5),
    erlang:trace_pattern({erlang, binary_to_term, '_'}, true, [local]),
    erlang:trace(all, true, [call]),
    try
        lists:foreach(
            fun(Frame) ->
                Sock = dial(Config),
                ?assertMatch({auth_challenge, _, _, _, _}, recv_frame(Sock)),
                ok = send(Sock, Frame),
                ?assertEqual({auth_result, error}, recv_frame(Sock)),
                close(Sock),
                Server = raw_server(Config, fun(S) -> send(S, Frame) end),
                {ok, Pid} = start_client(fake_spec(Server), Server),
                ?assertEqual(
                    {shutdown, {auth_failed, authentication_required}},
                    exit_reason(Pid)
                )
            end,
            [Term, Atoms]
        )
    after
        erlang:trace(all, false, [call]),
        erlang:trace_pattern({erlang, binary_to_term, '_'}, false, [local])
    end,
    Decoded = fun Drain(Acc) ->
        receive
            {trace, Pid, call, {erlang, binary_to_term, [Arg | _]}} when
                Arg =:= Term; Arg =:= Atoms
            ->
                Drain([Pid | Acc]);
            {trace, _, call, _} ->
                Drain(Acc)
        after 200 -> Acc
        end
    end,
    ?assertEqual([], Decoded([])),
    %% The trace does see such a call: with the phase off the node decodes
    %% the same frame.
    ok = partisan_config:set(peer_auth_methods, []),
    erlang:trace_pattern({erlang, binary_to_term, '_'}, true, [local]),
    erlang:trace(all, true, [call]),
    try
        Sock = dial(Config),
        ?assertEqual({hello, partisan:node()}, recv_term(Sock)),
        ok = send(Sock, term_to_binary({hello, partisan:node(), partisan_config:default_channel()})),
        ?assertMatch({state, _, _}, recv_term(Sock)),
        close(Sock)
    after
        erlang:trace(all, false, [call]),
        erlang:trace_pattern({erlang, binary_to_term, '_'}, false, [local])
    end,
    Any = fun Drain(N) ->
        receive
            {trace, P, call, {erlang, binary_to_term, _}} when P =/= self() -> Drain(N + 1);
            {trace, _, call, _} -> Drain(N)
        after 200 -> N
        end
    end,
    ?assert(Any(0) > 0).

%% =============================================================================
%% CHANNEL BINDING
%% =============================================================================

%% The proofs of a TLS 1.3 connection cover the exporter value of the session,
%% and those of a TCP connection cover no binding. The test takes the value
%% from its own end of the socket with `ssl' directly, so the phase succeeds
%% only if `partisan_peer_socket:channel_binding/1' gave the node the same
%% value. The server reports the binding in its telemetry event.
proofs_cover_channel_binding(Config) ->
    Expected =
        case ?config(tls, Config) of
            true -> tls_exporter;
            false -> none
        end,
    Test = self(),
    Handler = {?MODULE, make_ref()},
    ok = telemetry:attach(
        Handler,
        [partisan, connection, server, auth],
        fun(_, _, Metadata, _) -> Test ! {auth_event, Metadata} end,
        undefined
    ),
    Node = partisan:node(),
    Sock = dial(Config),
    try
        ?assertMatch(
            #{method := hmac_secret, channel_binding := Expected},
            authenticate(Sock, Node, Node)
        ),
        receive
            {auth_event, Metadata} ->
                ?assertMatch(
                    #{result := ok, channel_binding := Expected}, Metadata
                )
        after ?RECV_TIMEOUT ->
            ct:fail(no_auth_event)
        end
    after
        telemetry:detach(Handler),
        close(Sock)
    end,
    %% A client that leaves the binding out of its proof is refused on TLS.
    Sock2 = dial(Config),
    Neg0 = partisan_peer_auth:client_init([hmac_secret], Node, Node, none),
    {ok, Challenge} = recv(Sock2),
    {reply, Response, _} = partisan_peer_auth:client_handle(Challenge, Neg0),
    ok = send(Sock2, Response),
    case Expected of
        tls_exporter ->
            ?assertEqual({auth_result, error}, recv_frame(Sock2));
        none ->
            ?assertMatch({auth_result, ok, _}, recv_frame(Sock2))
    end,
    close(Sock2).

%% A party on the path. The test accepts the connection of a
%% `partisan_peer_service_client', opens its own connection to the node, and
%% copies every frame from one to the other without change. The party holds
%% no key.
%%
%% Over TCP the relay works: both ends authenticate, and the party can then
%% read and write the connection. This is the limit of the phase without TLS.
%% Over TLS 1.3 the party holds two sessions, the proofs cover two exporter
%% values, and the node refuses the client.
relayed_exchange(Config) ->
    Relay = raw_server(Config, fun(Down) ->
        Up = dial(Config),
        relay(Down, Up)
    end),
    %% The client dials the party at an address it believes is the node's.
    Spec = (partisan:node_spec())#{listen_addrs := [Relay]},
    {ok, Pid} = start_client(Spec, Relay),
    case ?config(tls, Config) of
        true ->
            ?assertEqual({shutdown, {auth_failed, rejected}}, exit_reason(Pid));
        false ->
            ?assertMatch({connected, _, _, _, _}, connected()),
            stop_client(Pid)
    end.

%% A client that allows only TLS 1.2 gets a session without a binding, and the
%% phase succeeds on it: the client says that it is not capable.
%%
%% A client on the same TLS 1.2 session that says it is capable is what the
%% node sees when a party on the path took TLS 1.3 away from two ends that
%% allow it. The node allows TLS 1.3, so it refuses.
earlier_tls_version_has_no_binding(Config) ->
    case ?config(tls, Config) of
        false ->
            {skip, "TLS only"};
        true ->
            Node = partisan:node(),
            Dial = fun() -> dial(Config, [{versions, ['tlsv1.2']}]) end,
            Sock = Dial(),
            ?assertEqual(none, binding(Sock)),
            ?assertMatch(
                #{method := hmac_secret, channel_binding := none},
                authenticate(Sock, Node, Node)
            ),
            close(Sock),

            Sock2 = Dial(),
            {ok, Challenge} = recv(Sock2),
            %% The node says it is capable on this TLS 1.2 session.
            ?assertMatch(
                {auth_challenge, _, _, _, true},
                partisan_peer_auth:decode(Challenge, true)
            ),
            %% An honest capable client stops here.
            Capable = partisan_peer_auth:client_init(
                [hmac_secret], Node, Node, binding(Sock2, capable)
            ),
            ?assertEqual(
                {error, channel_binding_downgrade},
                partisan_peer_auth:client_handle(Challenge, Capable)
            ),
            close(Sock2)
    end.

%% Copies frames between two raw sockets until one of them closes.
relay(A, B) ->
    Self = self(),
    Copy = fun Copy(From, To) ->
        case recv(From, 3000) of
            {ok, Data} ->
                _ = send(To, Data),
                Copy(From, To);
            {error, _} ->
                Self ! relay_done
        end
    end,
    spawn_link(fun() -> Copy(A, B) end),
    spawn_link(fun() -> Copy(B, A) end),
    receive
        relay_done -> ok
    end,
    close(A),
    close(B).

%% =============================================================================
%% HELPERS: THE PHASE, PLAYED BY THE TEST
%% =============================================================================

%% Runs the client's part of the phase on a raw socket, as `Node', against
%% the server `Peer'. Returns what the client concludes.
authenticate(Sock, Node, Peer) ->
    Neg0 = partisan_peer_auth:client_init(
        [hmac_secret], Node, Peer, binding(Sock)
    ),
    {ok, Challenge} = recv(Sock),
    {reply, Response, Neg1} = partisan_peer_auth:client_handle(Challenge, Neg0),
    ok = send(Sock, Response),
    {ok, Result} = recv(Sock),
    {ok, Authn} = partisan_peer_auth:client_handle(Result, Neg1),
    Authn.

%% =============================================================================
%% HELPERS: CLIENT PROCESS
%% =============================================================================

start_client(Spec, ListenAddr) ->
    Channel = partisan_config:default_channel(),
    partisan_peer_service_client:start_link(
        Spec, ListenAddr, Channel, partisan_config:channel_opts(Channel), self()
    ).

stop_client(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok.

wait_until(Fun) ->
    wait_until(Fun, 50).

wait_until(_Fun, 0) ->
    timeout;
wait_until(Fun, N) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(100),
            wait_until(Fun, N - 1)
    end.

connected() ->
    receive
        Msg when element(1, Msg) =:= connected ->
            Msg
    after ?RECV_TIMEOUT ->
        ct:fail(not_connected)
    end.

exit_reason(Pid) ->
    receive
        {'EXIT', Pid, Reason} ->
            Reason
    after ?RECV_TIMEOUT ->
        ct:fail(client_did_not_exit)
    end.

fake_spec(ListenAddr) ->
    (partisan:node_spec())#{name := ?FAKE, listen_addrs := [ListenAddr]}.

listen_addr() ->
    [#{ip := _, port := _} = Addr | _] = partisan_config:get(listen_addrs),
    Addr.

node_is_up() ->
    lists:keymember(partisan, 1, application:which_applications()) andalso
        is_list(partisan:nodes()).

%% =============================================================================
%% HELPERS: RAW SOCKETS
%% =============================================================================

%% Opens a raw connection to the node's peer listener.
dial(Config) ->
    dial(Config, []).

%% A connection that does not frame: the test writes and reads the 4-byte
%% lengths itself.
dial_raw(Config) ->
    {Mod, Sock} = dial(Config),
    ok =
        case Mod of
            ssl -> ssl:setopts(Sock, [{packet, raw}]);
            gen_tcp -> inet:setopts(Sock, [{packet, raw}])
        end,
    {Mod, Sock}.

recv_raw_frame({Mod, Sock}) ->
    maybe
        {ok, <<Size:32>>} ?= Mod:recv(Sock, 4, ?RECV_TIMEOUT),
        Mod:recv(Sock, Size, ?RECV_TIMEOUT)
    end.

dial(Config, TLSOpts) ->
    #{ip := Ip, port := Port} = listen_addr(),
    Opts = [binary, {packet, 4}, {active, false}],
    case ?config(tls, Config) of
        true ->
            {ok, Sock} = ssl:connect(
                Ip, Port, [{verify, verify_none} | Opts] ++ TLSOpts, ?RECV_TIMEOUT
            ),
            {ssl, Sock};
        false ->
            {ok, Sock} = gen_tcp:connect(Ip, Port, Opts, ?RECV_TIMEOUT),
            {gen_tcp, Sock}
    end.

%% Starts a raw listener that accepts one connection and runs `Fun' on it.
%% Returns its listen address.
raw_server(Config, Fun) ->
    {ok, LSock} = gen_tcp:listen(0, [
        binary, {packet, 4}, {active, false}, {ip, {127, 0, 0, 1}}
    ]),
    {ok, Port} = inet:port(LSock),
    TLS = ?config(tls, Config),
    TLSOpts = proplists:get_value(tls_server_options, ?config(env, Config), []),
    spawn_link(fun() ->
        {ok, Sock0} = gen_tcp:accept(LSock, ?RECV_TIMEOUT),
        Sock =
            case TLS of
                true ->
                    {ok, S} = ssl:handshake(Sock0, TLSOpts, ?RECV_TIMEOUT),
                    {ssl, S};
                false ->
                    {gen_tcp, Sock0}
            end,
        _ = Fun(Sock),
        %% Keep the connection open: the client decides when it ends.
        _ = recv(Sock, 10000),
        close(Sock)
    end),
    #{ip => {127, 0, 0, 1}, port => Port}.

%% The channel binding of a raw socket, taken with `ssl' directly and not
%% with `partisan_peer_socket'.
binding({gen_tcp, _}) ->
    none;
binding({ssl, Sock}) ->
    case ssl:connection_information(Sock, [protocol]) of
        {ok, [{protocol, 'tlsv1.3'}]} ->
            {ok, [Value]} = ssl:export_key_materials(
                Sock, [<<"EXPORTER-Channel-Binding">>], [no_context], [32], false
            ),
            {tls_exporter, Value};
        {ok, [{protocol, _}]} ->
            none
    end.

binding(Sock, Override) ->
    case binding(Sock) of
        none -> Override;
        Other -> Other
    end.

send({Mod, Sock}, Data) ->
    Mod:send(Sock, Data).

send_term(Sock, Term) ->
    send(Sock, term_to_binary(Term)).

recv(Sock) ->
    recv(Sock, ?RECV_TIMEOUT).

recv({Mod, Sock}, Timeout) ->
    Mod:recv(Sock, 0, Timeout).

recv_term(Sock) ->
    {ok, Data} = recv(Sock),
    binary_to_term(Data).

%% Reads a frame of the authentication phase. `partisan_peer_auth' does not
%% export `decode/2'; `partisan.hrl' compiles it with `export_all'.
recv_frame(Sock) ->
    {ok, Data} = recv(Sock),
    partisan_peer_auth:decode(Data, true).

close({Mod, Sock}) ->
    Mod:close(Sock).

%% =============================================================================
%% HELPERS: FRAMES
%% =============================================================================

unseen_name() ->
    Unique = integer_to_binary(erlang:unique_integer([positive])),
    <<"zz_unseen_", Unique/binary, "@host">>.

%% The external term format of a list of `N' atoms that no node holds, and
%% the names of the atoms.
atom_frame(N) ->
    Prefix = unseen_name(),
    Names = [
        <<Prefix/binary, "_", (integer_to_binary(I))/binary>>
     || I <- lists:seq(1, N)
    ],
    Atoms = [<<119, (byte_size(Name)), Name/binary>> || Name <- Names],
    {iolist_to_binary([<<131, 108, N:32>>, Atoms, <<106>>]), Names}.

atom_exists(Name) ->
    try binary_to_existing_atom(Name, utf8) of
        _ -> true
    catch
        error:badarg -> false
    end.

%% =============================================================================
%% HELPERS: TLS
%% =============================================================================

tls_env(false, _Config) ->
    [];
tls_env(true, Config) ->
    Dir = ?config(priv_dir, Config),
    Cert = filename:join(Dir, "cert.pem"),
    Key = filename:join(Dir, "key.pem"),
    Cmd = lists:flatten(
        io_lib:format(
            "openssl req -x509 -newkey rsa:2048 -nodes -keyout ~s -out ~s "
            "-days 2 -subj /CN=localhost 2>&1",
            [Key, Cert]
        )
    ),
    _ = os:cmd(Cmd),
    true = filelib:is_regular(Cert),
    [
        {tls_server_options, [{certfile, Cert}, {keyfile, Key}]},
        {tls_client_options, [{verify, verify_none}]}
    ].
