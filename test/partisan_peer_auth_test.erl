%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

%% Tests for the peer authentication phase (ADR-000005): the two verifiers and
%% the negotiator. They run in one VM without sockets, so one
%% `partisan_config' serves both ends. A test that gives the two ends
%% different trust material switches the configuration before each step.
%%
%% Not covered here: the wiring into the connection processes, the socket
%% frame cap and the timeout. `partisan_peer_auth_SUITE' covers those.
-module(partisan_peer_auth_test).

-include_lib("eunit/include/eunit.hrl").

-define(SERVER, 'auth_server@host').
-define(CLIENT, 'auth_client@host').
-define(KEY_A, <<"0123456789abcdef0123456789abcdef">>).
-define(KEY_B, <<"fedcba9876543210fedcba9876543210">>).

%% =============================================================================
%% HMAC VERIFIER
%% =============================================================================

%% RFC 4231, test case 3: a key of 20 bytes 0xaa and data of 50 bytes 0xdd.
hmac_known_answer_test() ->
    Key = binary:copy(<<16#aa>>, 20),
    Data = binary:copy(<<16#dd>>, 50),
    {ok, Proof} = partisan_peer_auth_hmac:respond(Data, [Key]),
    ?assertEqual(
        binary:decode_hex(
            <<"773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe">>
        ),
        Proof
    ).

hmac_verify_test() ->
    Msg = <<"message">>,
    {ok, Proof} = partisan_peer_auth_hmac:respond(Msg, [?KEY_A]),
    Verify = fun(M, P, Keys) ->
        partisan_peer_auth_hmac:verify(<<"peer">>, M, P, Keys)
    end,
    ?assert(Verify(Msg, Proof, [?KEY_A])),
    %% A proof made with a key that is not first in the list.
    ?assert(Verify(Msg, Proof, [?KEY_B, ?KEY_A])),
    ?assertNot(Verify(Msg, Proof, [?KEY_B])),
    ?assertNot(Verify(<<"other">>, Proof, [?KEY_A])),
    %% A proof of the wrong size must return false, not raise: this is the
    %% case `crypto:hash_equals/2' fails on.
    ?assertNot(Verify(Msg, binary:part(Proof, 0, 31), [?KEY_A])),
    ?assertNot(Verify(Msg, <<>>, [?KEY_A])),
    ?assertNot(Verify(Msg, <<Proof/binary, 0>>, [?KEY_A])).

hmac_validate_test() ->
    ?assertEqual([?KEY_A], partisan_peer_auth_hmac:validate_keyring([?KEY_A])),
    ?assertEqual([], partisan_peer_auth_hmac:validate_keyring([])),
    ?assertError(_, partisan_peer_auth_hmac:validate_keyring([<<"short">>])),
    ?assertError(_, partisan_peer_auth_hmac:validate_keyring(?KEY_A)),
    ?assertError(_, partisan_peer_auth_hmac:validate_keyring(["0123456789abcdef"])),
    ?assertEqual(undefined, partisan_peer_auth_hmac:validate_secret(undefined)),
    ?assertError(_, partisan_peer_auth_hmac:validate_secret(<<"short">>)),
    ?assertError(_, partisan_peer_auth_hmac:validate_secret("0123456789abcdef")).

%% =============================================================================
%% CRYPTOSIGN VERIFIER
%% =============================================================================

%% RFC 8032, section 7.1, test 1.
cryptosign_known_answer_test() ->
    Private = binary:decode_hex(
        <<"9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60">>
    ),
    Public = binary:decode_hex(
        <<"d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a">>
    ),
    Signature = binary:decode_hex(
        <<
            "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155"
            "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
        >>
    ),
    Config = #{private_key => Private, trusted_peers => #{<<"n">> => Public}},
    ?assertEqual(
        {ok, Signature}, partisan_peer_auth_cryptosign:respond(<<>>, Config)
    ),
    ?assert(
        partisan_peer_auth_cryptosign:verify(<<"n">>, <<>>, Signature, Config)
    ).

cryptosign_verify_test() ->
    #{public_key := PubA, private_key := PrivA} =
        partisan_peer_auth_cryptosign:generate_key(),
    #{public_key := PubB, private_key := PrivB} =
        partisan_peer_auth_cryptosign:generate_key(),
    Config = #{
        private_key => PrivA,
        trusted_peers => #{<<"a">> => PubA, <<"b">> => PubB}
    },
    Msg = <<"message">>,
    {ok, SigA} = partisan_peer_auth_cryptosign:respond(Msg, Config),
    {ok, SigB} = partisan_peer_auth_cryptosign:respond(
        Msg, Config#{private_key := PrivB}
    ),
    Verify = fun(Peer, M, Sig) ->
        partisan_peer_auth_cryptosign:verify(Peer, M, Sig, Config)
    end,
    ?assert(Verify(<<"a">>, Msg, SigA)),
    ?assert(Verify(<<"b">>, Msg, SigB)),
    %% A trusted node that claims the name of another trusted node.
    ?assertNot(Verify(<<"b">>, Msg, SigA)),
    %% A name that is not trusted.
    ?assertNot(Verify(<<"c">>, Msg, SigA)),
    ?assertNot(Verify(<<"a">>, <<"other">>, SigA)),
    ?assertNot(Verify(<<"a">>, Msg, <<>>)),
    ?assertNot(Verify(<<"a">>, Msg, binary:part(SigA, 0, 63))).

cryptosign_validate_test() ->
    #{public_key := Pub, private_key := Priv} =
        partisan_peer_auth_cryptosign:generate_key(),
    Hex = binary:encode_hex(Pub),
    Expected = #{
        private_key => Priv,
        trusted_peers => #{<<"a@h">> => Pub, <<"b@h">> => Pub, <<"c@h">> => Pub}
    },
    ?assertEqual(
        Expected,
        partisan_peer_auth_cryptosign:validate(#{
            private_key => binary:encode_hex(Priv, lowercase),
            trusted_peers => #{
                'a@h' => Pub,
                <<"b@h">> => Hex,
                'c@h' => binary_to_list(Hex)
            }
        })
    ),
    ?assertEqual(undefined, partisan_peer_auth_cryptosign:validate(undefined)),
    Bad = [
        #{private_key => Priv},
        #{private_key => <<"short">>, trusted_peers => #{}},
        #{private_key => Priv, trusted_peers => #{'a@h' => <<"short">>}},
        #{private_key => Priv, trusted_peers => #{1 => Pub}},
        #{private_key => binary:copy(<<"z">>, 64), trusted_peers => #{}},
        [Priv]
    ],
    [
        ?assertError(_, partisan_peer_auth_cryptosign:validate(B))
     || B <- Bad
    ].

%% =============================================================================
%% NEGOTIATOR: SUCCESSFUL EXCHANGES
%% =============================================================================

hmac_secret_exchange_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    ?assertEqual(
        {
            {ok, #{method => hmac_secret, peer => name(?CLIENT), channel_binding => none}},
            {ok, #{method => hmac_secret, peer => name(?SERVER), channel_binding => none}}
        },
        exchange({[hmac_secret], Conf}, {[hmac_secret], Conf})
    ).

gossip_keyring_exchange_test() ->
    Conf = conf(#{peer_auth_keyring => [?KEY_A, ?KEY_B]}),
    ?assertMatch(
        {{ok, #{method := gossip_keyring}}, {ok, #{method := gossip_keyring}}},
        exchange({[gossip_keyring], Conf}, {[gossip_keyring], Conf})
    ).

cryptosign_exchange_test() ->
    {SConf, CConf, _} = cryptosign_confs(),
    ?assertEqual(
        {
            {ok, #{method => cryptosign, peer => name(?CLIENT), channel_binding => none}},
            {ok, #{method => cryptosign, peer => name(?SERVER), channel_binding => none}}
        },
        exchange({[cryptosign], SConf}, {[cryptosign], CConf})
    ).

%% The server's order decides among the methods both ends accept.
negotiation_test() ->
    Conf = conf(#{
        peer_auth_secret => ?KEY_A, peer_auth_keyring => [?KEY_A]
    }),
    Method = fun(S, C) ->
        {{ok, #{method := M}}, {ok, #{method := M}}} =
            exchange({S, Conf}, {C, Conf}),
        M
    end,
    ?assertEqual(
        gossip_keyring,
        Method([gossip_keyring, hmac_secret], [hmac_secret, gossip_keyring])
    ),
    ?assertEqual(
        hmac_secret,
        Method([gossip_keyring, hmac_secret, anonymous], [hmac_secret])
    ),
    %% A client with an empty list answers a challenge with `anonymous'.
    ?assertEqual(anonymous, Method([hmac_secret, anonymous], [])),
    ?assertEqual(anonymous, Method([anonymous], [anonymous, hmac_secret])).

%% =============================================================================
%% NEGOTIATOR: REFUSALS
%% =============================================================================

wrong_secret_test() ->
    ?assertEqual(
        {{error, invalid_proof}, {error, rejected}},
        exchange(
            {[hmac_secret], conf(#{peer_auth_secret => ?KEY_A})},
            {[hmac_secret], conf(#{peer_auth_secret => ?KEY_B})}
        )
    ).

no_common_method_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A, peer_auth_keyring => [?KEY_A]}),
    %% A node that requires authentication against one that has none.
    ?assertMatch(
        {_, {error, no_common_method}},
        exchange({[hmac_secret], Conf}, {[], Conf})
    ),
    ?assertMatch(
        {_, {error, no_common_method}},
        exchange({[hmac_secret], Conf}, {[gossip_keyring], Conf})
    ).

%% A client that names a method the server did not offer.
method_not_offered_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    {_, Server} = server_init([hmac_secret], Conf),
    Frame = encode(
        {auth_response, <<"anonymous">>, name(?CLIENT), nonce(), <<>>, false}
    ),
    ?assertMatch(
        {error, unsupported_method, _},
        partisan_peer_auth:server_handle(Frame, Server)
    ).

%% A server that does not hold the key cannot complete the phase: the client
%% checks the server's proof.
rogue_server_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    {Challenge, _} = server_init([hmac_secret], Conf),
    {reply, Response, Client} = client_handle(
        Challenge, client_init([hmac_secret], Conf), Conf
    ),
    {auth_response, _, _, _, ClientProof, _} = decode(Response),
    Result = fun(Proof) ->
        client_handle(encode({auth_result, ok, Proof}), Client, Conf)
    end,
    ?assertEqual({error, invalid_proof}, Result(crypto:strong_rand_bytes(32))),
    ?assertEqual({error, invalid_proof}, Result(<<>>)),
    %% Reflection: the client's own proof sent back as the server's.
    ?assertEqual({error, invalid_proof}, Result(ClientProof)).

%% A response recorded in one exchange is not valid in another: the server's
%% nonce differs.
replay_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    {Challenge, Server1} = server_init([hmac_secret], Conf),
    {reply, Response, _} = client_handle(
        Challenge, client_init([hmac_secret], Conf), Conf
    ),
    ?assertMatch({ok, _, _}, partisan_peer_auth:server_handle(Response, Server1)),
    {_, Server2} = server_init([hmac_secret], Conf),
    ?assertMatch(
        {error, invalid_proof, _},
        partisan_peer_auth:server_handle(Response, Server2)
    ).

%% A party on the path removes the stronger method from the offer. Both ends
%% accept the weaker one, but the proof covers the offered list, so the
%% server refuses.
downgrade_test() ->
    {SConf0, CConf0, _} = cryptosign_confs(),
    %% Appended, so that it overrides the `undefined' of `conf/1'.
    Add = fun(Conf) -> Conf ++ [{peer_auth_secret, ?KEY_A}] end,
    SConf = Add(SConf0),
    CConf = Add(CConf0),
    Methods = [cryptosign, hmac_secret],
    {Challenge, Server} = server_init(Methods, SConf),
    {auth_challenge, [_, Weak], SName, SNonce, Cap} = decode(Challenge),
    Altered = encode({auth_challenge, [Weak], SName, SNonce, Cap}),
    {reply, Response, _} = client_handle(
        Altered, client_init(Methods, CConf), CConf
    ),
    ?assertMatch({auth_response, <<"hmac_secret">>, _, _, _, _}, decode(Response)),
    ok = set(SConf),
    ?assertMatch(
        {error, invalid_proof, _},
        partisan_peer_auth:server_handle(Response, Server)
    ).

%% A trusted node cannot take the name of another trusted node.
cryptosign_impersonation_test() ->
    {SConf, CConf, _} = cryptosign_confs(),
    {Challenge, Server} = server_init([cryptosign], SConf),
    ok = set(CConf),
    Client = partisan_peer_auth:client_init([cryptosign], 'auth_other@host', ?SERVER),
    {reply, Response, _} = partisan_peer_auth:client_handle(Challenge, Client),
    ok = set(SConf),
    ?assertMatch(
        {error, invalid_proof, _},
        partisan_peer_auth:server_handle(Response, Server)
    ).

%% The server is not the node the client dialled.
unexpected_peer_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    ok = set(Conf),
    {Challenge, _} = partisan_peer_auth:server_init([hmac_secret], 'auth_other@host'),
    ?assertEqual(
        {error, unexpected_peer},
        client_handle(Challenge, client_init([hmac_secret], Conf), Conf)
    ).

%% Rotation of a keyring. A node signs with its first key, so a new key goes
%% to the end of every list before it goes to the front of any.
keyring_rotation_test() ->
    Ring = fun(Keys) -> conf(#{peer_auth_keyring => Keys}) end,
    Run = fun(S, C) ->
        exchange({[gossip_keyring], Ring(S)}, {[gossip_keyring], Ring(C)})
    end,
    ?assertMatch({{ok, _}, {ok, _}}, Run([?KEY_A, ?KEY_B], [?KEY_A])),
    ?assertMatch({{ok, _}, {ok, _}}, Run([?KEY_A], [?KEY_A, ?KEY_B])),
    ?assertMatch({{ok, _}, {ok, _}}, Run([?KEY_B, ?KEY_A], [?KEY_A, ?KEY_B])),
    ?assertMatch({{ok, _}, {ok, _}}, Run([?KEY_B], [?KEY_B, ?KEY_A])),
    %% The order the procedure forbids: the server signs with a key the
    %% client does not hold yet.
    ?assertMatch({{ok, _}, {error, invalid_proof}}, Run([?KEY_B, ?KEY_A], [?KEY_A])).

%% =============================================================================
%% NEGOTIATOR: CHANNEL BINDING
%% =============================================================================

%% Runs the phase between a server and a client that give the bindings `SB'
%% and `CB', with one shared key. Returns what each end concludes.
bound_exchange(SB, CB) ->
    ok = set(conf(#{peer_auth_secret => ?KEY_A})),
    {Challenge, Server} = partisan_peer_auth:server_init([hmac_secret], ?SERVER, SB),
    Client0 = partisan_peer_auth:client_init([hmac_secret], ?CLIENT, ?SERVER, CB),
    {reply, Response, Client1} = partisan_peer_auth:client_handle(Challenge, Client0),
    case partisan_peer_auth:server_handle(Response, Server) of
        {ok, Result, Authn} ->
            {{ok, Authn}, partisan_peer_auth:client_handle(Result, Client1)};
        {error, Reason, Result} ->
            {{error, Reason}, partisan_peer_auth:client_handle(Result, Client1)}
    end.

%% Two ends of one session give the same binding, and the phase succeeds.
same_binding_test() ->
    B = {tls_exporter, crypto:strong_rand_bytes(32)},
    ?assertMatch(
        {
            {ok, #{channel_binding := tls_exporter}},
            {ok, #{channel_binding := tls_exporter}}
        },
        bound_exchange(B, B)
    ).

%% A party on the path holds one session with the client and another with the
%% server, and relays the frames between them unchanged. The two ends then
%% give different bindings, and the server refuses the client's proof although
%% the key is right.
relayed_exchange_is_refused_test() ->
    B1 = {tls_exporter, crypto:strong_rand_bytes(32)},
    B2 = {tls_exporter, crypto:strong_rand_bytes(32)},
    ?assertEqual(
        {{error, invalid_proof}, {error, rejected}}, bound_exchange(B1, B2)
    ).

%% The party cannot remove the binding either: one end bound and the other
%% not is a different binding. This is a client on TLS 1.3 and a server
%% reached over TCP or an earlier TLS version, and the reverse.
binding_cannot_be_removed_test() ->
    B = {tls_exporter, crypto:strong_rand_bytes(32)},
    ?assertEqual(
        {{error, invalid_proof}, {error, rejected}}, bound_exchange(none, B)
    ),
    ?assertEqual(
        {{error, invalid_proof}, {error, rejected}}, bound_exchange(B, none)
    ).

%% The server's proof covers the binding too. A server that accepts the
%% client's proof but holds another binding cannot be simulated with the real
%% server, which refuses first, so the test makes the server's proof by hand
%% from an exchange with the other binding.
server_proof_covers_binding_test() ->
    ok = set(conf(#{peer_auth_secret => ?KEY_A})),
    B1 = {tls_exporter, crypto:strong_rand_bytes(32)},
    B2 = {tls_exporter, crypto:strong_rand_bytes(32)},
    {Challenge, Server} = partisan_peer_auth:server_init([hmac_secret], ?SERVER, B1),
    Client0 = partisan_peer_auth:client_init([hmac_secret], ?CLIENT, ?SERVER, B2),
    {reply, Response, Client1} = partisan_peer_auth:client_handle(Challenge, Client0),
    {auth_response, Method, CName, CNonce, _, CCap} = decode(Response),
    %% The proof an honest server with binding B1 gives for this exchange.
    Message = partisan_peer_auth:message(<<"server">>, Method, Server, CName, CNonce, CCap),
    {ok, Proof} = partisan_peer_auth_hmac:respond(Message, [?KEY_A]),
    ?assertEqual(
        {error, invalid_proof},
        partisan_peer_auth:client_handle(encode({auth_result, ok, Proof}), Client1)
    ).

%% A party on the path makes each end negotiate a transport without a
%% binding, although both allow one. Each end says that it is capable, and
%% the client stops when it reads the server's statement.
downgrade_of_binding_is_refused_test() ->
    ok = set(conf(#{peer_auth_secret => ?KEY_A})),
    {Challenge, Server} = partisan_peer_auth:server_init([hmac_secret], ?SERVER, capable),
    Client = partisan_peer_auth:client_init([hmac_secret], ?CLIENT, ?SERVER, capable),
    ?assertEqual(
        {error, channel_binding_downgrade},
        partisan_peer_auth:client_handle(Challenge, Client)
    ),
    %% The party clears the server's statement so that the client continues.
    %% The server then refuses: its proof input says it is capable, and the
    %% client's says it is not.
    {auth_challenge, Offered, SName, SNonce, true} = decode(Challenge),
    Cleared = encode({auth_challenge, Offered, SName, SNonce, false}),
    {reply, Response, _} = partisan_peer_auth:client_handle(Cleared, Client),
    ?assertMatch(
        {error, invalid_proof, _},
        partisan_peer_auth:server_handle(Response, Server)
    ),
    %% The party clears the client's statement too. The proof covers it.
    {auth_response, M, CName, CNonce, Proof, true} = decode(Response),
    Cleared2 = encode({auth_response, M, CName, CNonce, Proof, false}),
    ?assertMatch(
        {error, invalid_proof, _},
        partisan_peer_auth:server_handle(Cleared2, Server)
    ).

%% The server makes the same check, for a client that does not stop. The
%% client here is the test: it signs as an honest capable client that skipped
%% its own check.
server_refuses_downgrade_test() ->
    ok = set(conf(#{peer_auth_secret => ?KEY_A})),
    {Challenge, Server} = partisan_peer_auth:server_init([hmac_secret], ?SERVER, capable),
    {auth_challenge, _, _, _, true} = decode(Challenge),
    CNonce = nonce(),
    Message = partisan_peer_auth:message(
        <<"client">>, <<"hmac_secret">>, Server, name(?CLIENT), CNonce, true
    ),
    {ok, Proof} = partisan_peer_auth_hmac:respond(Message, [?KEY_A]),
    Response = encode(
        {auth_response, <<"hmac_secret">>, name(?CLIENT), CNonce, Proof, true}
    ),
    ?assertMatch(
        {error, channel_binding_downgrade, _},
        partisan_peer_auth:server_handle(Response, Server)
    ).

%% A node that allows a binding and a node that does not: the session has no
%% binding and the phase succeeds, in both arrangements.
capable_and_incapable_ends_authenticate_test() ->
    ?assertMatch(
        {{ok, #{channel_binding := none}}, {ok, #{channel_binding := none}}},
        bound_exchange(capable, none)
    ),
    ?assertMatch(
        {{ok, #{channel_binding := none}}, {ok, #{channel_binding := none}}},
        bound_exchange(none, capable)
    ).

invalid_binding_test() ->
    ?assertError(_, partisan_peer_auth:server_init([hmac_secret], ?SERVER, {tls_exporter, <<1, 2>>})),
    ?assertError(_, partisan_peer_auth:client_init([hmac_secret], ?CLIENT, ?SERVER, tls)).

%% =============================================================================
%% NEGOTIATOR: BOUNDED DECODE
%% =============================================================================

%% The reported attack: a frame that names atoms the node does not hold. The
%% node must refuse it and create no atom.
unknown_atoms_create_no_atom_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    {_, Server} = server_init([hmac_secret], Conf),
    Client = client_init([hmac_secret], Conf),
    Frame = atom_frame(200),
    ?assert(byte_size(Frame) =< partisan_peer_auth:max_frame_size()),
    Before = erlang:system_info(atom_count),
    ?assertMatch(
        {error, invalid_frame, _},
        partisan_peer_auth:server_handle(Frame, Server)
    ),
    ?assertEqual(
        {error, authentication_required},
        partisan_peer_auth:client_handle(Frame, Client)
    ),
    ?assertEqual(Before, erlang:system_info(atom_count)),
    %% The same frame does create atoms under the decode of the existing
    %% protocol, so the assertion above is not vacuous.
    _ = binary_to_term(Frame),
    ?assertEqual(Before + 200, erlang:system_info(atom_count)).

frame_too_large_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    {_, Server} = server_init([hmac_secret], Conf),
    Max = partisan_peer_auth:max_frame_size(),
    Frame = fun(Size) ->
        %% A binary term has 6 bytes of header.
        F = term_to_binary(binary:copy(<<0>>, Size - 6)),
        Size = byte_size(F),
        F
    end,
    %% At the limit the frame decodes, and fails as a frame of the wrong shape.
    ?assertMatch(
        {error, invalid_frame, _},
        partisan_peer_auth:server_handle(Frame(Max), Server)
    ),
    ?assertMatch(
        {error, frame_too_large, _},
        partisan_peer_auth:server_handle(Frame(Max + 1), Server)
    ),
    ?assertEqual(
        {error, frame_too_large},
        partisan_peer_auth:client_handle(
            Frame(Max + 1), client_init([hmac_secret], Conf)
        )
    ).

compressed_frame_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    {_, Server} = server_init([hmac_secret], Conf),
    %% 4 MB that deflate to less than the frame limit.
    Bomb = term_to_binary(binary:copy(<<0>>, 4000000), [{compressed, 9}]),
    ?assert(byte_size(Bomb) =< partisan_peer_auth:max_frame_size()),
    ?assertMatch(
        {error, invalid_frame, _},
        partisan_peer_auth:server_handle(Bomb, Server)
    ),
    ?assertEqual(
        {error, authentication_required},
        partisan_peer_auth:client_handle(Bomb, client_init([hmac_secret], Conf))
    ),
    %% A client that accepts `anonymous' decodes the first frame of a server
    %% that runs no phase, and refuses a compressed one.
    ?assertEqual(
        {error, compressed_frame},
        partisan_peer_auth:client_handle(
            Bomb, client_init([hmac_secret, anonymous], Conf)
        )
    ).

%% A frame of the existing protocol does not pass a node that requires
%% authentication, in either role.
existing_protocol_refused_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    {_, Server} = server_init([hmac_secret], Conf),
    ?assertMatch(
        {error, invalid_frame, _},
        partisan_peer_auth:server_handle(
            term_to_binary({hello, ?CLIENT, undefined}), Server
        )
    ),
    ?assertEqual(
        {error, authentication_required},
        partisan_peer_auth:client_handle(
            term_to_binary({hello, ?SERVER}), client_init([hmac_secret], Conf)
        )
    ).

%% A client that accepts `anonymous' hands the first frame of a server that
%% runs no phase back to its caller.
existing_protocol_accepted_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    Hello = {hello, ?SERVER},
    ?assertEqual(
        {ok, #{method => anonymous, peer => undefined, channel_binding => none}, Hello},
        partisan_peer_auth:client_handle(
            term_to_binary(Hello), client_init([hmac_secret, anonymous], Conf)
        )
    ),
    %% With a non-empty list the decode is still bounded.
    ?assertEqual(
        {error, undecodable_frame},
        partisan_peer_auth:client_handle(
            atom_frame(3), client_init([hmac_secret, anonymous], Conf)
        )
    ),
    %% With an empty list the client decodes as the existing protocol does.
    ?assertMatch(
        {ok, #{method := anonymous}, [_, _, _]},
        partisan_peer_auth:client_handle(atom_frame(3), client_init([], Conf))
    ).

%% The node takes the frames of the phase out of a raw socket itself. A
%% length over the limit is refused on its 4 bytes, before the frame comes.
deframe_test() ->
    Max = partisan_peer_auth:max_frame_size(),
    Frame = binary:copy(<<"a">>, 10),
    Wire = iolist_to_binary(partisan_peer_auth:enframe(Frame)),
    ?assertEqual(<<10:32, Frame/binary>>, Wire),
    ?assertEqual({ok, Frame, <<>>}, partisan_peer_auth:deframe(Wire)),
    ?assertEqual(
        {ok, Frame, <<"next">>},
        partisan_peer_auth:deframe(<<Wire/binary, "next">>)
    ),
    %% Every proper prefix is an incomplete frame.
    [
        ?assertEqual(more, partisan_peer_auth:deframe(binary:part(Wire, 0, N)))
     || N <- lists:seq(0, byte_size(Wire) - 1)
    ],
    %% At the limit, and one over. The one over is refused on its length
    %% alone, and with every amount of the frame present.
    AtMax = binary:copy(<<0>>, Max),
    ?assertEqual(
        {ok, AtMax, <<>>}, partisan_peer_auth:deframe(<<Max:32, AtMax/binary>>)
    ),
    Over = Max + 1,
    ?assertEqual({error, frame_too_large}, partisan_peer_auth:deframe(<<Over:32>>)),
    ?assertEqual(
        {error, frame_too_large},
        partisan_peer_auth:deframe(<<Over:32, AtMax/binary, 0>>)
    ),
    ?assertEqual(
        {error, frame_too_large},
        partisan_peer_auth:deframe(<<16#FFFFFFFF:32, "x">>)
    ),
    ?assertEqual({ok, <<>>, <<>>}, partisan_peer_auth:deframe(<<0:32>>)).

%% Frames of the right tag and the wrong shape, and random bytes, return an
%% error. None raises.
malformed_frames_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    {Challenge, Server} = server_init([hmac_secret], Conf),
    Client0 = client_init([hmac_secret], Conf),
    {reply, _, Client1} = client_handle(Challenge, Client0, Conf),
    N = nonce(),
    Name = name(?SERVER),
    Big = binary:copy(<<"x">>, 2000),
    F = fun(Bin) -> <<(byte_size(Bin)):16, Bin/binary>> end,
    Raw = fun(Type, Fields) -> iolist_to_binary(["PAUTH1", Type, Fields]) end,
    Valid = encode({auth_response, <<"hmac_secret">>, name(?CLIENT), N, <<>>, false}),
    Frames =
        [
            %% Fields of the wrong size.
            encode({auth_response, <<"hmac_secret">>, name(?CLIENT), <<1, 2, 3>>, <<>>, false}),
            encode({auth_response, <<"hmac_secret">>, name(?CLIENT), N, Big, false}),
            encode({auth_response, <<"hmac_secret">>, Big, N, <<>>, false}),
            encode({auth_response, Big, name(?CLIENT), N, <<>>, false}),
            encode({auth_challenge, [<<"hmac_secret">>], Name, <<1>>, false}),
            encode({auth_challenge, [<<"hmac_secret">>], Big, N, false}),
            encode({auth_result, ok, Big}),
            %% A capability byte that is neither 0 nor 1, and none.
            Raw(2, [F(<<"hmac_secret">>), F(name(?CLIENT)), F(N), F(<<>>), 2]),
            Raw(2, [F(<<"hmac_secret">>), F(name(?CLIENT)), F(N), F(<<>>)]),
            Raw(1, [1, F(<<"hmac_secret">>), F(Name), F(N), 7]),
            %% A field missing, a field too many, bytes after the last field.
            Raw(2, [F(<<"hmac_secret">>), F(name(?CLIENT)), F(N)]),
            Raw(2, [F(<<"hmac_secret">>), F(name(?CLIENT)), F(N), F(<<>>), F(<<>>)]),
            <<Valid/binary, 0>>,
            binary:part(Valid, 0, byte_size(Valid) - 1),
            %% A field length that runs past the frame.
            Raw(3, [<<500:16, "short">>]),
            %% Method counts of 0 and of more than the limit.
            Raw(1, [0, F(Name), F(N)]),
            Raw(1, [9, lists:duplicate(9, F(<<"hmac_secret">>)), F(Name), F(N)]),
            Raw(1, [2, F(<<"hmac_secret">>), F(Name), F(N)]),
            Raw(4, [F(<<>>)]),
            %% An unknown type, and no type.
            Raw(9, []),
            <<"PAUTH1">>,
            <<"PAUTH">>,
            <<>>,
            <<131>>,
            term_to_binary({auth_result, ok, <<>>})
        ] ++
            [crypto:strong_rand_bytes(rand:uniform(64)) || _ <- lists:seq(1, 500)] ++
            [
                Raw(rand:uniform(5), crypto:strong_rand_bytes(rand:uniform(64)))
             || _ <- lists:seq(1, 2000)
            ],
    lists:foreach(
        fun(Frame) ->
            ?assertMatch(
                {error, _, _}, partisan_peer_auth:server_handle(Frame, Server)
            ),
            ?assertMatch(
                {error, _}, partisan_peer_auth:client_handle(Frame, Client0)
            ),
            ?assertMatch(
                {error, _}, partisan_peer_auth:client_handle(Frame, Client1)
            )
        end,
        Frames
    ).

%% A node that requires authentication does not call `binary_to_term/1,2' on
%% a frame before the phase succeeds, in either role. A call trace
%% checks it for a whole successful exchange and for frames in the
%% external term format.
no_term_decode_before_authentication_test() ->
    Conf = conf(#{peer_auth_secret => ?KEY_A}),
    ok = set(Conf),
    %% Runs `Fun' in a process that this process traces, and counts its calls
    %% to `binary_to_term/1,2'. A process cannot trace itself.
    Calls = fun(Fun) ->
        erlang:trace_pattern({erlang, binary_to_term, '_'}, true, [local]),
        Self = self(),
        Pid = spawn_link(fun() ->
            receive go -> ok end,
            _ = Fun(),
            Self ! {done, self()}
        end),
        erlang:trace(Pid, true, [call]),
        Pid ! go,
        receive {done, Pid} -> ok end,
        erlang:trace_pattern({erlang, binary_to_term, '_'}, false, [local]),
        Drain = fun Drain(N) ->
            receive
                {trace, Pid, call, {erlang, binary_to_term, _}} -> Drain(N + 1)
            after 100 -> N
            end
        end,
        Drain(0)
    end,
    Term = term_to_binary({hello, ?SERVER}),
    AtomFrame = atom_frame(5),
    Strict = fun() ->
        {Challenge, Server} = partisan_peer_auth:server_init([hmac_secret], ?SERVER),
        Client0 = partisan_peer_auth:client_init([hmac_secret], ?CLIENT, ?SERVER),
        {reply, Response, Client1} = partisan_peer_auth:client_handle(Challenge, Client0),
        {ok, Result, _} = partisan_peer_auth:server_handle(Response, Server),
        {ok, _} = partisan_peer_auth:client_handle(Result, Client1),
        [
            begin
                {error, _, _} = partisan_peer_auth:server_handle(Frame, Server),
                {error, _} = partisan_peer_auth:client_handle(Frame, Client0),
                {error, _} = partisan_peer_auth:client_handle(Frame, Client1)
            end
         || Frame <- [Term, AtomFrame]
        ]
    end,
    ?assertEqual(0, Calls(Strict)),
    %% The trace does see a call: a client that accepts `anonymous' decodes
    %% the same frame.
    Lenient = fun() ->
        Client = partisan_peer_auth:client_init([hmac_secret, anonymous], ?CLIENT, ?SERVER),
        {ok, _, _} = partisan_peer_auth:client_handle(Term, Client)
    end,
    ?assertEqual(1, Calls(Lenient)).

%% Every frame the negotiator builds fits the frame limit with the longest
%% node names an atom can hold.
longest_names_fit_test() ->
    Long = fun(Char) ->
        binary_to_atom(unicode:characters_to_binary(lists:duplicate(255, Char)))
    end,
    %% U+10348 takes 4 bytes in UTF-8.
    ServerNode = Long(16#10348),
    ClientNode = Long(16#10349),
    ok = set(conf(#{peer_auth_secret => ?KEY_A})),
    Methods = [cryptosign, gossip_keyring, hmac_secret, anonymous],
    {Challenge, Server} = partisan_peer_auth:server_init(Methods, ServerNode),
    Client = partisan_peer_auth:client_init([hmac_secret], ClientNode, ServerNode),
    {reply, Response, Client1} = partisan_peer_auth:client_handle(Challenge, Client),
    {ok, Result, _} = partisan_peer_auth:server_handle(Response, Server),
    ?assertMatch({ok, _}, partisan_peer_auth:client_handle(Result, Client1)),
    Max = partisan_peer_auth:max_frame_size(),
    [?assert(byte_size(F) =< Max) || F <- [Challenge, Response, Result]].

%% =============================================================================
%% CONFIGURATION
%% =============================================================================

validate_methods_test() ->
    All = [cryptosign, gossip_keyring, hmac_secret, anonymous],
    ?assertEqual(All, partisan_peer_auth:validate_methods(All)),
    ?assertEqual([], partisan_peer_auth:validate_methods([])),
    ?assertError(_, partisan_peer_auth:validate_methods([password])),
    ?assertError(_, partisan_peer_auth:validate_methods(hmac_secret)),
    ?assertError(_, partisan_peer_auth:validate_methods([hmac_secret, hmac_secret])).

validate_config_test() ->
    ok = set(conf(#{peer_auth_methods => [hmac_secret, anonymous]})),
    ?assertError(
        {peer_auth, {not_configured, peer_auth_secret}},
        partisan_peer_auth:validate_config()
    ),
    ok = set(conf(#{peer_auth_methods => [gossip_keyring]})),
    ?assertError(
        {peer_auth, {not_configured, peer_auth_keyring}},
        partisan_peer_auth:validate_config()
    ),
    ok = set(conf(#{peer_auth_methods => [cryptosign]})),
    ?assertError(
        {peer_auth, {not_configured, peer_auth_cryptosign}},
        partisan_peer_auth:validate_config()
    ),
    ok = set(conf(#{
        peer_auth_methods => [hmac_secret], peer_auth_secret => ?KEY_A
    })),
    ?assertEqual(ok, partisan_peer_auth:validate_config()),
    ok = set(conf(#{})),
    ?assertEqual(ok, partisan_peer_auth:validate_config()).

%% The one definition of an authenticated peer plane. Each row is a
%% configuration and whether the node admits only authenticated peers.
authenticates_peers_test() ->
    Verify = [{verify, verify_peer}],
    Mutual = [{fail_if_no_peer_cert, true} | Verify],
    Rows = [
        %% {Methods, TLS, ServerOpts, ClientOpts, Required, Certificates}
        {[], false, [], [], false, false},
        {[anonymous], false, [], [], false, false},
        {[hmac_secret, anonymous], false, [], [], false, false},
        {[hmac_secret], false, [], [], true, false},
        {[hmac_secret], true, [], [], true, false},
        {[], true, Mutual, Verify, false, true},
        {[hmac_secret, anonymous], true, Mutual, Verify, false, true},
        %% TLS that does not admit only verified peers.
        {[], true, [], [], false, false},
        %% A server that verifies peers requires a certificate unless the
        %% option is `false' (probed on OTP 27, 28 and 29).
        {[], true, Verify, Verify, false, true},
        {[], true, [{fail_if_no_peer_cert, false} | Verify], Verify, false, false},
        {[], true, Mutual, [], false, false},
        {[], true, [{fail_if_no_peer_cert, true}], Verify, false, false},
        {[], true, [{verify, verify_none}, {fail_if_no_peer_cert, true}], Verify, false, false},
        %% The options without `tls' do nothing.
        {[], false, Mutual, Verify, false, false}
    ],
    try
        lists:foreach(
            fun({Methods, TLS, Server, Client, Required, Certs} = Row) ->
                ok = set(conf(#{
                    peer_auth_methods => Methods,
                    peer_auth_secret => ?KEY_A
                })),
                ok = partisan_config:set(tls, TLS),
                ok = partisan_config:set(tls_server_options, Server),
                ok = partisan_config:set(tls_client_options, Client),
                ?assertEqual({Row, Required}, {Row, partisan_peer_auth:is_required()}),
                ?assertEqual(
                    {Row, Certs},
                    {Row, partisan_peer_auth:requires_tls_certificates()}
                ),
                ?assertEqual(
                    {Row, Required orelse Certs},
                    {Row, partisan_peer_auth:authenticates_peers()}
                )
            end,
            Rows
        )
    after
        ok = set(conf(#{})),
        ok = partisan_config:set(tls, false),
        ok = partisan_config:set(tls_server_options, []),
        ok = partisan_config:set(tls_client_options, [])
    end.

config_defaults_test() ->
    ok = partisan_config:init(),
    ?assertEqual([], partisan_config:get(peer_auth_methods)),
    ?assertEqual([], partisan_config:get(peer_auth_keyring)),
    ?assertEqual(undefined, partisan_config:get(peer_auth_secret)),
    ?assertEqual(undefined, partisan_config:get(peer_auth_cryptosign)),
    ?assertEqual(5000, partisan_config:get(peer_auth_timeout)),
    %% With the phase off a socket starts at the limit of the existing
    %% protocol.
    %% With the phase off a socket has the framing of the existing protocol
    %% from the start. With it on, the socket starts raw.
    Established = [
        {packet, 4}, {packet_size, partisan_config:get(max_message_size)}
    ],
    ?assertEqual(Established, partisan_peer_auth:established_socket_opts()),
    ?assertEqual(Established, partisan_peer_auth:initial_socket_opts([])),
    ?assertEqual(
        [{packet, raw}], partisan_peer_auth:initial_socket_opts([anonymous])
    ),
    ?assertEqual(all, partisan_config:get(peer_auth_allowed_nodes)),
    ?assertEqual(1024, partisan_config:get(peer_auth_max_pending)).

%% The gate of `net_kernel:allow/1'.
allow_test() ->
    ok = partisan_config:set(peer_auth_allowed_nodes, all),
    try
        ?assertEqual(all, partisan_peer_auth:allowed()),
        ?assert(partisan_peer_auth:is_allowed(?CLIENT)),
        ?assertEqual(error, partisan_peer_auth:allow(<<"not a list">>)),
        ?assertEqual(error, partisan_peer_auth:allow([<<"not an atom">>])),
        ?assertEqual(all, partisan_peer_auth:allowed()),
        %% The first call ends `all': only the listed nodes pass.
        ok = partisan_peer_auth:allow([?CLIENT]),
        ?assertEqual([?CLIENT], partisan_peer_auth:allowed()),
        ?assert(partisan_peer_auth:is_allowed(?CLIENT)),
        ?assertNot(partisan_peer_auth:is_allowed(?SERVER)),
        %% A later call adds.
        ok = partisan_peer_auth:allow([?SERVER, ?CLIENT]),
        ?assert(partisan_peer_auth:is_allowed(?SERVER)),
        ?assertEqual(lists:sort([?CLIENT, ?SERVER]), partisan_peer_auth:allowed()),
        ?assertError(_, partisan_config:set(peer_auth_allowed_nodes, [<<"x">>])),
        ?assertError(_, partisan_config:set(peer_auth_allowed_nodes, none))
    after
        ok = partisan_config:set(peer_auth_allowed_nodes, all)
    end.

%% The count of connections that have not authenticated, and its limit.
pending_limit_test() ->
    ok = partisan_peer_auth:init(),
    ok = partisan_config:set(peer_auth_max_pending, 2),
    try
        ?assertEqual(0, partisan_peer_auth:pending()),
        ?assertEqual(ok, partisan_peer_auth:enter_pending()),
        ?assertEqual(ok, partisan_peer_auth:enter_pending()),
        ?assertEqual({error, limit}, partisan_peer_auth:enter_pending()),
        %% A refused connection is not counted.
        ?assertEqual(2, partisan_peer_auth:pending()),
        ok = partisan_peer_auth:leave_pending(),
        ?assertEqual(ok, partisan_peer_auth:enter_pending()),
        ok = partisan_peer_auth:leave_pending(),
        ok = partisan_peer_auth:leave_pending(),
        ?assertEqual(0, partisan_peer_auth:pending()),
        ?assertError(_, partisan_config:set(peer_auth_max_pending, 0))
    after
        ok = partisan_config:set(peer_auth_max_pending, 1024)
    end.

%% =============================================================================
%% HELPERS
%% =============================================================================

%% A full configuration: every key, so that one test leaves nothing behind
%% for the next.
conf(Overrides) ->
    maps:to_list(
        maps:merge(
            #{
                peer_auth_methods => [],
                peer_auth_secret => undefined,
                peer_auth_keyring => [],
                peer_auth_cryptosign => undefined
            },
            Overrides
        )
    ).

set(Conf) ->
    lists:foreach(fun({K, V}) -> ok = partisan_config:set(K, V) end, Conf).

cryptosign_confs() ->
    #{public_key := SPub, private_key := SPriv} =
        partisan_peer_auth_cryptosign:generate_key(),
    #{public_key := CPub, private_key := CPriv} =
        partisan_peer_auth_cryptosign:generate_key(),
    Trusted = #{?SERVER => SPub, ?CLIENT => CPub, 'auth_other@host' => SPub},
    Conf = fun(Priv) ->
        conf(#{
            peer_auth_cryptosign => #{
                private_key => Priv, trusted_peers => Trusted
            }
        })
    end,
    {Conf(SPriv), Conf(CPriv), Trusted}.

server_init(Methods, Conf) ->
    ok = set(Conf),
    partisan_peer_auth:server_init(Methods, ?SERVER).

client_init(Methods, Conf) ->
    ok = set(Conf),
    partisan_peer_auth:client_init(Methods, ?CLIENT, ?SERVER).

client_handle(Frame, Client, Conf) ->
    ok = set(Conf),
    partisan_peer_auth:client_handle(Frame, Client).

%% Runs the phase between a server and a client that each have their own
%% configuration. Returns what each end concludes.
exchange({SMethods, SConf}, {CMethods, CConf}) ->
    {Challenge, Server} = server_init(SMethods, SConf),
    case client_handle(Challenge, client_init(CMethods, CConf), CConf) of
        {reply, Response, Client} ->
            ok = set(SConf),
            case partisan_peer_auth:server_handle(Response, Server) of
                {ok, Result, Authn} ->
                    {{ok, Authn}, client_handle(Result, Client, CConf)};
                {error, Reason, Result} ->
                    {{error, Reason}, client_handle(Result, Client, CConf)}
            end;
        {error, _} = Error ->
            {undefined, Error}
    end.

name(Node) ->
    atom_to_binary(Node, utf8).

%% `partisan_peer_auth' does not export these two. `partisan.hrl' compiles
%% every module that includes it with `export_all'.
encode(Frame) ->
    partisan_peer_auth:encode(Frame).

decode(Frame) ->
    partisan_peer_auth:decode(Frame, true).

nonce() ->
    crypto:strong_rand_bytes(32).

%% The external term format of a list of `N' atoms that no node holds.
atom_frame(N) ->
    Unique = integer_to_binary(erlang:unique_integer([positive])),
    Atoms = [
        begin
            Name = <<"zz_unseen_", Unique/binary, "_", (integer_to_binary(I))/binary>>,
            <<119, (byte_size(Name)), Name/binary>>
        end
     || I <- lists:seq(1, N)
    ],
    iolist_to_binary([<<131, 108, N:32>>, Atoms, <<106>>]).
