%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

%% Tests for the peer authentication phase (ADR-000005) between real nodes.
%%
%% Each node is an Erlang VM started with `peer', with its own configuration
%% and so its own trust material. The nodes run the default peer service
%% manager over TCP and are not connected by Erlang distribution: the test
%% controls them over standard I/O.
%%
%% The `mixed_version' group needs a build of a release that does not have the
%% phase. It reads the `ebin' directory of that build from the environment
%% variable `PARTISAN_OLD_EBIN' and is skipped without it. `make
%% peer-auth-mixed-test' makes the build from the tag in `PARTISAN_OLD_TAG'.
%%
%% The `managers' group runs each of the other peer service managers with
%% the phase on. The `tls' group runs the nodes over TLS 1.3 with a
%% self-signed certificate and no certificate verification, so the keys are
%% what authenticates the nodes and the proofs cover the TLS channel binding.
%%
%% The `mtls' group runs the nodes over mutual TLS with certificate
%% verification and no authentication method: each node has a certificate of
%% a cluster certificate authority, verifies its peers against that authority
%% in both roles, and refuses a client without a certificate. The group makes
%% two authorities with `openssl', so that a node can hold a certificate the
%% cluster does not trust.
%%
%% Not covered: a cluster of more than three nodes, certificate revocation,
%% and hostname verification by name (the nodes dial the address 127.0.0.1,
%% which each certificate lists).
-module(partisan_peer_auth_cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

-define(KEY_A, <<"0123456789abcdef0123456789abcdef">>).
-define(KEY_B, <<"fedcba9876543210fedcba9876543210">>).
-define(STRICT(Key), [{peer_auth_methods, [hmac_secret]}, {peer_auth_secret, Key}]).
-define(WIDE(Key), [
    {peer_auth_methods, [hmac_secret, anonymous]}, {peer_auth_secret, Key}
]).
-define(OFF, []).
%% How long a case waits for something that must happen.
-define(WAIT, 15000).
%% How long a case watches for something that must not happen. A node retries
%% a connection every second, so this covers several attempts.
-define(HOLD, 4000).

all() ->
    [
        {group, cluster},
        {group, managers},
        {group, tls},
        {group, mtls},
        {group, mixed_version}
    ].

groups() ->
    [
        {cluster, [], [
            cluster_forms_with_shared_key,
            cluster_forms_with_cryptosign,
            cluster_forms_with_keyring_during_rotation,
            node_with_wrong_key_is_refused,
            node_without_phase_is_refused,
            node_with_untrusted_key_is_refused,
            node_not_allowed_is_refused,
            rollout_keeps_cluster_connected
        ]},
        {managers, [], [
            hyparview_with_shared_key,
            hyparview_refuses_wrong_key,
            static_with_shared_key,
            static_refuses_wrong_key,
            client_server_with_shared_key,
            client_server_refuses_wrong_key
        ]},
        {tls, [], [
            cluster_forms_with_shared_key,
            cluster_forms_with_cryptosign,
            node_with_wrong_key_is_refused,
            node_without_phase_is_refused
        ]},
        {mtls, [], [
            mtls_cluster_forms,
            mtls_refuses_certificate_of_another_authority,
            mtls_refuses_client_without_certificate,
            mtls_refuses_plain_and_uncertified_sockets,
            mtls_without_required_certificate_admits_client,
            mtls_with_cryptosign_cluster_forms
        ]},
        {mixed_version, [], [
            old_and_new_cluster_with_phase_off,
            old_node_and_node_that_accepts_anonymous,
            old_node_and_node_that_requires_authentication
        ]}
    ].

init_per_group(mixed_version, Config) ->
    case os:getenv("PARTISAN_OLD_EBIN") of
        false ->
            {skip, "PARTISAN_OLD_EBIN is not set"};
        Dir ->
            true = filelib:is_regular(filename:join(Dir, "partisan.app")),
            %% The build must not have the phase.
            false = filelib:is_regular(
                filename:join(Dir, "partisan_peer_auth.beam")
            ),
            [{old_ebin, Dir} | Config]
    end;
init_per_group(mtls, Config) ->
    Dir = ?config(priv_dir, Config),
    Cluster = make_ca(Dir, "cluster"),
    Other = make_ca(Dir, "other"),
    Certs = [
        {{cluster, N}, make_cert(Dir, Cluster, "cluster_" ++ atom_to_list(N))}
     || N <- [n1, n2, n3]
    ],
    Stranger = {{other, n3}, make_cert(Dir, Other, "other_n3")},
    [
        {common_env, []},
        {cluster_ca, Cluster},
        {other_ca, Other},
        {certs, [Stranger | Certs]}
        | Config
    ];
init_per_group(tls, Config) ->
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
    %% Every node of the group gets these options. `start/2' reads them from
    %% the process dictionary of the case process, which CT copies from here.
    TLS = [
        {tls, true},
        {tls_server_options, [
            {certfile, Cert}, {keyfile, Key}, {versions, ['tlsv1.3']}
        ]},
        {tls_client_options, [{verify, verify_none}, {versions, ['tlsv1.3']}]}
    ],
    [{common_env, TLS} | Config];
init_per_group(_, Config) ->
    [{common_env, []} | Config].

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    %% Options that every node of the case gets, beside its own.
    put(common_env, ?config(common_env, Config)),
    Config.

end_per_testcase(_Case, _Config) ->
    %% Every node is linked to the case process and stops with it.
    ok.

%% =============================================================================
%% CLUSTER
%% =============================================================================

cluster_forms_with_shared_key(_Config) ->
    Nodes = [start(N, ?STRICT(?KEY_A)) || N <- [n1, n2, n3]],
    ok = form_cluster(Nodes).

cluster_forms_with_cryptosign(_Config) ->
    Names = [n1, n2, n3],
    Keys = [
        {N, partisan_peer_auth_cryptosign:generate_key()}
     || N <- Names
    ],
    Trusted = maps:from_list([
        {name(N), binary:encode_hex(Public)}
     || {N, #{public_key := Public}} <- Keys
    ]),
    Nodes = [
        start(N, [
            {peer_auth_methods, [cryptosign]},
            {peer_auth_cryptosign, #{
                private_key => Private, trusted_peers => Trusted
            }}
        ])
     || {N, #{private_key := Private}} <- Keys
    ],
    ok = form_cluster(Nodes).

%% The middle of a keyring rotation: the nodes hold the same keys and sign
%% with different ones.
cluster_forms_with_keyring_during_rotation(_Config) ->
    Ring = fun(Keys) ->
        [{peer_auth_methods, [gossip_keyring]}, {peer_auth_keyring, Keys}]
    end,
    Nodes = [
        start(n1, Ring([?KEY_A, ?KEY_B])),
        start(n2, Ring([?KEY_B, ?KEY_A])),
        start(n3, Ring([?KEY_A, ?KEY_B]))
    ],
    ok = form_cluster(Nodes).

%% A node with another key does not connect to the cluster, and the cluster
%% does not connect to it, whichever end dials.
node_with_wrong_key_is_refused(_Config) ->
    [N1, N2] = Nodes = [start(N, ?STRICT(?KEY_A)) || N <- [n1, n2]],
    ok = form_cluster(Nodes),
    N3 = start(n3, ?STRICT(?KEY_B)),
    ok = refused(N3, N1),
    ok = refused(N1, N3),
    ok = connected_both_ways(N1, N2).

%% A node that runs no phase and a node that requires authentication do not
%% connect, whichever end dials.
node_without_phase_is_refused(_Config) ->
    [N1, N2] = Nodes = [start(N, ?STRICT(?KEY_A)) || N <- [n1, n2]],
    ok = form_cluster(Nodes),
    N3 = start(n3, ?OFF),
    ok = refused(N3, N1),
    ok = refused(N1, N3),
    ok = connected_both_ways(N1, N2).

%% A node whose key the others do not list is refused under `cryptosign',
%% although it lists theirs.
node_with_untrusted_key_is_refused(_Config) ->
    Key = fun() -> partisan_peer_auth_cryptosign:generate_key() end,
    #{public_key := Pub1, private_key := Priv1} = Key(),
    #{public_key := Pub3, private_key := Priv3} = Key(),
    Conf = fun(Private, Trusted) ->
        [
            {peer_auth_methods, [cryptosign]},
            {peer_auth_cryptosign, #{
                private_key => Private, trusted_peers => Trusted
            }}
        ]
    end,
    N1 = start(n1, Conf(Priv1, #{name(n1) => Pub1})),
    N3 = start(n3, Conf(Priv3, #{name(n1) => Pub1, name(n3) => Pub3})),
    ok = refused(N3, N1),
    ok = refused(N1, N3).

%% The gate of `net_kernel:allow/1'. A node that holds the key and is not in
%% the list of another node does not connect to it, whichever end dials. A
%% node in the list does.
node_not_allowed_is_refused(_Config) ->
    Allow = fun(Nodes) ->
        [{peer_auth_allowed_nodes, [name(N) || N <- Nodes]} | ?STRICT(?KEY_A)]
    end,
    N1 = start(n1, Allow([n1, n2])),
    N2 = start(n2, ?STRICT(?KEY_A)),
    N3 = start(n3, ?STRICT(?KEY_A)),
    ok = join(N2, N1),
    ok = connected_both_ways(N1, N2),
    ok = refused(N3, N1),
    ok = refused(N1, N3).

%% =============================================================================
%% MANAGERS
%% =============================================================================

hyparview_with_shared_key(_Config) ->
    ok = manager_cluster(hyparview(?STRICT(?KEY_A)), hyparview(?STRICT(?KEY_A))).

hyparview_refuses_wrong_key(_Config) ->
    ok = manager_refusal(hyparview(?STRICT(?KEY_A)), hyparview(?STRICT(?KEY_B))).

static_with_shared_key(_Config) ->
    ok = manager_cluster(static(?STRICT(?KEY_A)), static(?STRICT(?KEY_A))).

static_refuses_wrong_key(_Config) ->
    ok = manager_refusal(static(?STRICT(?KEY_A)), static(?STRICT(?KEY_B))).

client_server_with_shared_key(_Config) ->
    ok = manager_cluster(
        client_server(server, ?STRICT(?KEY_A)),
        client_server(client, ?STRICT(?KEY_A))
    ).

client_server_refuses_wrong_key(_Config) ->
    ok = manager_refusal(
        client_server(server, ?STRICT(?KEY_A)),
        client_server(client, ?STRICT(?KEY_B))
    ).

hyparview(Env) ->
    [{peer_service_manager, partisan_hyparview_peer_service_manager} | Env].

static(Env) ->
    [{peer_service_manager, partisan_static_peer_service_manager} | Env].

client_server(Tag, Env) ->
    [
        {peer_service_manager, partisan_client_server_peer_service_manager},
        {tag, Tag}
        | Env
    ].

%% Two nodes with the given options join each other. Each then holds a
%% connection to the other and has it as a member.
%%
%% The check is not a call over Partisan, as in the other groups: with these
%% managers a call between two nodes of this suite does not complete with the
%% phase off either. A join under the static and client-server managers
%% connects the joining node only, so each node joins the other.
manager_cluster(Env1, Env2) ->
    N1 = start(n1, Env1),
    N2 = start(n2, Env2),
    ok = join(N2, N1),
    ok = join(N1, N2),
    ok = wait(fun() ->
        is_connected(N1, N2) andalso is_connected(N2, N1) andalso
            is_member(N1, N2) andalso is_member(N2, N1)
    end).

%% Two nodes with different keys join each other. Neither becomes a member of
%% the other, and both keep running: the manager survives the exit of a
%% connection process that failed to authenticate, and still answers.
%%
%% The control for the membership check is `manager_cluster/2': with the same
%% key the same joins do make each node a member of the other.
manager_refusal(Env1, Env2) ->
    N1 = start(n1, Env1),
    N2 = start(n2, Env2),
    _ = join(N2, N1),
    _ = join(N1, N2),
    ok = hold(fun() -> not is_member(N1, N2) andalso not is_member(N2, N1) end),
    ?assert(is_up(N1)),
    ?assert(is_up(N2)),
    ok.

is_member(Node, Other) ->
    lists:member(name_of(Other), members(Node)).

%% The rollout of the cluster security guide on a cluster that runs: each
%% node first accepts `anonymous' beside the method, then requires the method.
%% The two nodes stay connected in both directions at every step, including
%% the steps where their lists differ.
rollout_keeps_cluster_connected(_Config) ->
    [N1, N2] = Nodes = [start(N, ?OFF) || N <- [n1, n2]],
    ok = form_cluster(Nodes),
    Steps = [
        {N1, ?WIDE(?KEY_A)},
        {N2, ?WIDE(?KEY_A)},
        {N1, ?STRICT(?KEY_A)},
        {N2, ?STRICT(?KEY_A)}
    ],
    lists:foreach(
        fun({Node, Env}) ->
            Other = hd(Nodes -- [Node]),
            ok = restart(Node, Env),
            ok = join(Node, Other),
            ok = connected_both_ways(N1, N2)
        end,
        Steps
    ),
    %% After the last step the cluster refuses a node that runs no phase.
    N3 = start(n3, ?OFF),
    ok = refused(N3, N1).

%% =============================================================================
%% MUTUAL TLS
%% =============================================================================

%% Nodes with certificates of the cluster authority form a cluster with no
%% authentication method, and each reports that it authenticates its peers.
mtls_cluster_forms(Config) ->
    Nodes = [start(N, mtls(Config, cluster, {cluster, N})) || N <- [n1, n2, n3]],
    [
        ?assertEqual(
            {true, false, true},
            {
                call(Node, partisan_peer_auth, requires_tls_certificates, []),
                call(Node, partisan_peer_auth, is_required, []),
                call(Node, partisan_peer_auth, authenticates_peers, [])
            }
        )
     || Node <- Nodes
    ],
    ok = form_cluster(Nodes).

%% A node with a certificate of another authority does not connect to the
%% cluster, and the cluster does not connect to it. The node trusts the
%% cluster's authority, so each refusal is the cluster node's: it refuses the
%% client certificate when the node dials, and the server certificate when it
%% dials the node.
mtls_refuses_certificate_of_another_authority(Config) ->
    [N1, N2] =
        Nodes = [start(N, mtls(Config, cluster, {cluster, N})) || N <- [n1, n2]],
    ok = form_cluster(Nodes),
    N3 = start(n3, mtls(Config, cluster, {other, n3})),
    ok = refused(N3, N1),
    ok = refused(N1, N3),
    ok = connected_both_ways(N1, N2).

%% A node that presents no client certificate does not connect to the
%% cluster. This is `fail_if_no_peer_cert'.
mtls_refuses_client_without_certificate(Config) ->
    [N1, N2] =
        Nodes = [start(N, mtls(Config, cluster, {cluster, N})) || N <- [n1, n2]],
    ok = form_cluster(Nodes),
    Env = lists:keyreplace(
        tls_client_options,
        1,
        mtls(Config, cluster, {cluster, n3}),
        {tls_client_options, [
            {cacertfile, ?config(cluster_ca, Config)}, {verify, verify_peer}
        ]}
    ),
    N3 = start(n3, Env),
    %% Only the direction in which the node is the client: as a server it
    %% holds a valid certificate, and the cluster may connect to it.
    ok = join(N3, N1),
    ok = hold(fun() -> not reaches(N3, N1) end),
    ?assertNot(lists:member(name_of(N3), members(N1))),
    ?assert(is_up(N1)),
    ok = connected_both_ways(N1, N2).

%% The reported attack against a node with mutual TLS. A host without a
%% certificate sends the frame of the report over plain TCP, and over TLS
%% without a client certificate. The node sends it no frame of the protocol
%% and holds none of the atoms the frame names.
mtls_refuses_plain_and_uncertified_sockets(Config) ->
    N1 = start(n1, mtls(Config, cluster, {cluster, n1})),
    [#{ip := Ip, port := Port} | _] =
        maps:get(listen_addrs, call(N1, partisan, node_spec, [])),
    Opts = [binary, {packet, 4}, {active, false}],
    {Frame, Names} = atom_frame(100),

    {ok, Plain} = gen_tcp:connect(Ip, Port, Opts, 5000),
    _ = gen_tcp:send(Plain, Frame),
    ?assertMatch({error, _}, gen_tcp:recv(Plain, 0, 5000)),
    _ = gen_tcp:close(Plain),

    %% With TLS 1.3 the client's handshake can complete before the server
    %% refuses the missing certificate, so the refusal shows on the connect
    %% or on the first read.
    TLS = [{verify, verify_none} | Opts],
    case ssl:connect(Ip, Port, TLS, 5000) of
        {ok, Sock} ->
            _ = ssl:send(Sock, Frame),
            ?assertMatch({error, _}, ssl:recv(Sock, 0, 5000)),
            _ = ssl:close(Sock);
        {error, _} ->
            ok
    end,

    ?assertEqual(
        [],
        [
            N
         || N <- Names,
            call(N1, ?MODULE, atom_exists, [N])
        ]
    ),
    ?assert(is_up(N1)).

%% The control for the two cases above. A server that verifies certificates
%% and has `{fail_if_no_peer_cert, false}' admits a client that presents
%% none, and the node reports that it does not authenticate its peers.
%%
%% Without the option the server refuses that client, as with `true': the
%% last part of the case checks it, and that the node then reports that it
%% does authenticate.
mtls_without_required_certificate_admits_client(Config) ->
    Set = fun(Env, Fun) ->
        {tls_server_options, Server} = lists:keyfind(tls_server_options, 1, Env),
        lists:keyreplace(
            tls_server_options,
            1,
            Env,
            {tls_server_options,
                Fun(lists:keydelete(fail_if_no_peer_cert, 1, Server))}
        )
    end,
    Lax = fun(Env) ->
        Set(Env, fun(Server) -> [{fail_if_no_peer_cert, false} | Server] end)
    end,
    Unset = fun(Env) -> Set(Env, fun(Server) -> Server end) end,
    N1 = start(n1, Lax(mtls(Config, cluster, {cluster, n1}))),
    ?assertNot(call(N1, partisan_peer_auth, authenticates_peers, [])),
    [#{ip := Ip, port := Port} | _] =
        maps:get(listen_addrs, call(N1, partisan, node_spec, [])),
    {ok, Sock} = ssl:connect(
        Ip,
        Port,
        [binary, {packet, 4}, {active, false}, {verify, verify_none}],
        5000
    ),
    {ok, Hello} = ssl:recv(Sock, 0, 5000),
    ?assertEqual({hello, name_of(N1)}, binary_to_term(Hello)),
    _ = ssl:close(Sock),

    N2 = start(n2, Unset(mtls(Config, cluster, {cluster, n2}))),
    ?assert(call(N2, partisan_peer_auth, authenticates_peers, [])),
    [#{ip := Ip2, port := Port2} | _] =
        maps:get(listen_addrs, call(N2, partisan, node_spec, [])),
    case
        ssl:connect(
            Ip2,
            Port2,
            [binary, {packet, 4}, {active, false}, {verify, verify_none}],
            5000
        )
    of
        {ok, Sock2} ->
            ?assertMatch({error, _}, ssl:recv(Sock2, 0, 5000)),
            _ = ssl:close(Sock2);
        {error, _} ->
            ok
    end.

%% Mutual TLS and a method together: the certificates admit the nodes, and
%% `cryptosign' binds each node name to a key. The proofs cover the channel
%% binding of each TLS 1.3 session.
mtls_with_cryptosign_cluster_forms(Config) ->
    Names = [n1, n2, n3],
    Keys = [
        {N, partisan_peer_auth_cryptosign:generate_key()}
     || N <- Names
    ],
    Trusted = maps:from_list([
        {name(N), Public}
     || {N, #{public_key := Public}} <- Keys
    ]),
    Nodes = [
        start(
            N,
            [
                {peer_auth_methods, [cryptosign]},
                {peer_auth_cryptosign, #{
                    private_key => Private, trusted_peers => Trusted
                }}
                | mtls(Config, cluster, {cluster, N})
            ]
        )
     || {N, #{private_key := Private}} <- Keys
    ],
    ok = form_cluster(Nodes).

%% The options of a node that trusts the authority `Trust' and holds the
%% certificate `Cert'.
mtls(Config, Trust, Cert) ->
    CA =
        case Trust of
            cluster -> ?config(cluster_ca, Config);
            other -> ?config(other_ca, Config)
        end,
    {CertFile, KeyFile} = proplists:get_value(Cert, ?config(certs, Config)),
    Own = [{certfile, CertFile}, {keyfile, KeyFile}, {cacertfile, CA}],
    [
        {tls, true},
        {tls_server_options, [
            {verify, verify_peer}, {fail_if_no_peer_cert, true} | Own
        ]},
        {tls_client_options, [{verify, verify_peer} | Own]}
    ].

%% =============================================================================
%% MIXED VERSION
%% =============================================================================

%% With the phase off, a node with the phase and a node of a release without
%% it form a cluster.
old_and_new_cluster_with_phase_off(Config) ->
    Old = start_old(old, Config),
    New = start(new, ?OFF),
    ok = join(New, Old),
    ok = connected_both_ways(Old, New),
    Old2 = start_old(old2, Config),
    ok = join(Old2, New),
    ok = connected_both_ways(Old2, New).

%% A node whose list is not empty sends a challenge, which a release without
%% the phase does not understand. The node connects to the old node, since as
%% a client it accepts the old node's `hello'. The old node does not connect
%% to it. This is why every node is upgraded before any list is set.
old_node_and_node_that_accepts_anonymous(Config) ->
    Old = start_old(old, Config),
    New = start(new, ?WIDE(?KEY_A)),
    ok = join(New, Old),
    ok = wait(fun() -> is_connected(New, Old) end),
    ok = hold(fun() -> not reaches(Old, New) end),
    ?assert(is_up(Old)),
    ?assert(is_up(New)).

old_node_and_node_that_requires_authentication(Config) ->
    Old = start_old(old, Config),
    New = start(new, ?STRICT(?KEY_A)),
    ok = refused(New, Old),
    ok = refused(Old, New).

%% =============================================================================
%% HELPERS: ASSERTIONS
%% =============================================================================

%% Joins every node to the first, and checks that each pair is connected in
%% both directions and can call the other.
form_cluster([First | Rest] = Nodes) ->
    [ok = join(Node, First) || Node <- Rest],
    [ok = connected_both_ways(A, B) || A <- Nodes, B <- Nodes, A < B],
    Names = lists:sort([name_of(N) || N <- Nodes]),
    [
        ok = wait(fun() -> members(Node) =:= Names end)
     || Node <- Nodes
    ],
    ok.

%% Each node holds a connection to the other and gets an answer from it. A
%% call goes over the caller's connection and its answer over the callee's.
connected_both_ways(A, B) ->
    ok = wait(fun() -> is_connected(A, B) andalso is_connected(B, A) end),
    ok = wait(fun() -> remote_name(A, B) =:= name_of(B) end),
    ok = wait(fun() -> remote_name(B, A) =:= name_of(A) end).

%% `Dialler' joins `Target'. Neither node gets an answer from the other,
%% neither becomes a member of the other, and both keep running.
%%
%% The check is not `is_connected/2': the default manager counts a client
%% process as a connection from the moment its socket connects, so a node
%% that retries a refused peer is `connected' for an instant on each attempt.
refused(Dialler, Target) ->
    ok = join(Dialler, Target),
    ok = hold(fun() ->
        not reaches(Dialler, Target) andalso not reaches(Target, Dialler)
    end),
    ?assertNot(lists:member(name_of(Target), members(Dialler))),
    ?assertNot(lists:member(name_of(Dialler), members(Target))),
    ?assert(is_up(Dialler)),
    ?assert(is_up(Target)),
    ok.

wait(Fun) ->
    wait(Fun, erlang:monotonic_time(millisecond) + ?WAIT).

wait(Fun, Deadline) ->
    case Fun() of
        true ->
            ok;
        false ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true ->
                    timeout;
                false ->
                    timer:sleep(100),
                    wait(Fun, Deadline)
            end
    end.

%% `Fun' returns `true' on every check for `?HOLD' milliseconds.
hold(Fun) ->
    hold(Fun, erlang:monotonic_time(millisecond) + ?HOLD).

hold(Fun, Deadline) ->
    case Fun() of
        true ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true ->
                    ok;
                false ->
                    timer:sleep(100),
                    hold(Fun, Deadline)
            end;
        false ->
            violated
    end.

%% =============================================================================
%% HELPERS: CERTIFICATES
%% =============================================================================

%% Makes a certificate authority in `Dir'. Returns the path of its
%% certificate. Its key is beside it, with the extension `.key'.
make_ca(Dir, Name) ->
    Cert = filename:join(Dir, Name ++ "_ca.pem"),
    Key = filename:join(Dir, Name ++ "_ca.key"),
    openssl(
        "req -x509 -newkey rsa:2048 -nodes -days 2 -keyout ~s "
        "-subj /CN=~s-ca "
        "-addext basicConstraints=critical,CA:TRUE "
        "-addext keyUsage=critical,keyCertSign,cRLSign -out ~s",
        [Key, Name, Cert]
    ),
    Cert.

%% Makes a key and a certificate that the authority `CA' signs, for a node
%% that serves and dials the address 127.0.0.1. Returns their paths.
make_cert(Dir, CA, Name) ->
    Key = filename:join(Dir, Name ++ ".key"),
    Req = filename:join(Dir, Name ++ ".csr"),
    Cert = filename:join(Dir, Name ++ ".pem"),
    Ext = filename:join(Dir, Name ++ ".ext"),
    ok = file:write_file(Ext, [
        "subjectAltName=IP:127.0.0.1,DNS:localhost\n",
        "extendedKeyUsage=serverAuth,clientAuth\n",
        "basicConstraints=CA:FALSE\n"
    ]),
    openssl(
        "req -newkey rsa:2048 -nodes -keyout ~s -subj /CN=~s -out ~s",
        [Key, Name, Req]
    ),
    %% `-set_serial' and not `-CAcreateserial': openssl names the serial file
    %% after the authority's path, and fails with a long path.
    openssl(
        "x509 -req -in ~s -CA ~s -CAkey ~s -set_serial ~b -days 2 "
        "-extfile ~s -out ~s",
        [
            Req,
            CA,
            filename:rootname(CA) ++ ".key",
            erlang:unique_integer([positive]),
            Ext,
            Cert
        ]
    ),
    {Cert, Key}.

%% Runs openssl. Fails with its output when a file it was to write is
%% missing or empty: openssl can print an error and exit 0.
openssl(Format, Args) ->
    Cmd = lists:flatten(io_lib:format("openssl " ++ Format ++ " 2>&1", Args)),
    Output = os:cmd(Cmd),
    Out = lists:last(Args),
    case filelib:file_size(Out) of
        0 -> ct:fail({openssl, Cmd, Output});
        _ -> Output
    end.

%% The external term format of a list of `N' atoms that no node holds, and
%% the names of the atoms.
atom_frame(N) ->
    Unique = integer_to_binary(erlang:unique_integer([positive])),
    Names = [
        <<"zz_mtls_unseen_", Unique/binary, "_", (integer_to_binary(I))/binary>>
     || I <- lists:seq(1, N)
    ],
    Atoms = [<<119, (byte_size(Name)), Name/binary>> || Name <- Names],
    {iolist_to_binary([<<131, 108, N:32>>, Atoms, <<106>>]), Names}.

%% Runs on a node of the suite, which has this module on its code path.
atom_exists(Name) ->
    try binary_to_existing_atom(Name, utf8) of
        _ -> true
    catch
        error:badarg -> false
    end.

%% =============================================================================
%% HELPERS: NODES
%% =============================================================================

%% A node is `{Name, Peer}': its Partisan node name and its `peer' handle.
start(Short, Env) ->
    start(Short, Env, code:get_path()).

start_old(Short, Config) ->
    New = filename:join(code:lib_dir(partisan), "ebin"),
    Path = [?config(old_ebin, Config) | code:get_path() -- [New]],
    {Name, Peer} = Node = start(Short, ?OFF, Path),
    %% The node runs the old build.
    ?assertEqual(
        non_existing, peer:call(Peer, code, which, [partisan_peer_auth])
    ),
    ?assertEqual(Name, call(Node, partisan, node, [])),
    Node.

start(Short, Env, Path) ->
    Args = lists:append([["-pa", Dir] || Dir <- Path, filelib:is_dir(Dir)]),
    {ok, Peer, _} = peer:start_link(#{connection => standard_io, args => Args}),
    Name = name(Short),
    ok = peer:call(Peer, logger, set_primary_config, [level, error]),
    Common =
        case get(common_env) of
            undefined -> [];
            List -> List
        end,
    ok = boot(Peer, [{name, Name}, {peer_ip, {127, 0, 0, 1}} | Common ++ Env]),
    {Name, Peer}.

%% Stops Partisan on the node and starts it with other authentication
%% options, as a node restarted with a new configuration.
restart({Name, Peer}, Env) ->
    ok = peer:call(Peer, application, stop, [partisan]),
    Keys = [
        peer_auth_methods,
        peer_auth_secret,
        peer_auth_keyring,
        peer_auth_cryptosign
    ],
    [ok = peer:call(Peer, application, unset_env, [partisan, K]) || K <- Keys],
    boot(Peer, [{name, Name} | Env]).

boot(Peer, Env) ->
    _ = peer:call(Peer, application, load, [partisan]),
    [ok = peer:call(Peer, application, set_env, [partisan, K, V]) || {K, V} <- Env],
    {ok, _} = peer:call(Peer, application, ensure_all_started, [partisan]),
    ok.

name(Short) ->
    list_to_atom(atom_to_list(Short) ++ "_auth_cluster@127.0.0.1").

name_of({Name, _}) ->
    Name.

call({_, Peer}, M, F, A) ->
    peer:call(Peer, M, F, A, 20000).

join(Node, Target) ->
    Spec = call(Target, partisan, node_spec, []),
    call(Node, partisan_peer_service, join, [Spec]).

is_connected(Node, {Other, _}) ->
    call(Node, partisan_peer_connections, is_connected, [Other]).

members(Node) ->
    {ok, Members} = call(Node, partisan_peer_service, members, []),
    lists:sort(Members).

is_up(Node) ->
    Apps = call(Node, application, which_applications, []),
    lists:keymember(partisan, 1, Apps).

reaches(From, To) ->
    remote_name(From, To) =:= name_of(To).

%% Asks `To' for its node name, from `From', over Partisan.
remote_name(From, {To, _}) ->
    try
        call(From, partisan_rpc, call, [To, partisan, node, [], 2000])
    catch
        _:_ -> error
    end.
