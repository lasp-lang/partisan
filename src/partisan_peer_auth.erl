%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(partisan_peer_auth).

-moduledoc """
The negotiator of the peer authentication phase (ADR-000005).

The phase runs on a new connection before `hello`. This module builds and
reads its frames and decides its outcome. It does no I/O:
`partisan_peer_service_server` and `partisan_peer_service_client` own the
socket and call this module with each frame they receive.

## Frames

The accepting node is the server and the connecting node is the client.

1. Server to client: a challenge with `Methods`, `ServerName` and
   `ServerNonce`.
2. Client to server: a response with `Method`, `ClientName`, `ClientNonce` and
   `ClientProof`.
3. Server to client: a result with `ServerProof`, or an error result followed
   by a close.

The client then sends the `hello` of the existing protocol. The server sends
no `hello` after the phase: the challenge carried its name. Each end sends
its frame of the phase only after it has read the other's, so no end has
bytes of the existing protocol in hand when it changes how it reads the
socket.

Names and method names are UTF-8 bytes and nonces are 32 random bytes. The
client picks the first entry of `Methods` that it also accepts. Each proof
covers the role of the prover, the chosen method, `Methods`, both names, both
nonces and the channel binding of the connection, so a proof is not valid in
the other direction, in another exchange, after a change to the offered list,
or on another transport session.

## Channel binding

On a TLS 1.3 connection every proof covers the TLS exporter value of the
session (RFC 9266, `tls-exporter`). A party that terminates TLS on the path
holds two sessions with two values, so a proof it relays from one to the other
does not verify. No frame carries the binding or its type: each end takes them
from its own socket.

A connection over TCP, or over an earlier TLS version, has no binding. Each
end says in its frame whether it allows TLS 1.3, and the proofs cover both
statements. When both ends allow TLS 1.3 and the session is not TLS 1.3, a
party on the path made each end negotiate an earlier version, and the phase
fails with `channel_binding_downgrade`. A connection between a node that
allows TLS 1.3 and a node that does not has no binding and succeeds.

A frame is not a term in the external term format. It is the bytes `PAUTH1`, a
type byte, and length-prefixed fields.

## Decoding

Until the phase succeeds, a node that requires authentication accepts a frame
of at most 4096 bytes and reads it with binary patterns. It does not call
`binary_to_term/1,2` on it. Bytes that are not the expected frame fail the
phase.

A client that accepts `anonymous` also accepts the `hello` of a server that
runs no phase. With a non-empty method list it decodes that frame with
`binary_to_term(Frame, [safe])` and refuses a compressed term.

## The method list

`peer_auth_methods` is the ordered list of methods a node accepts, strongest
first: `cryptosign`, `gossip_keyring`, `hmac_secret` and `anonymous`.
`anonymous` is no authentication. A server accepts whichever listed method the
client names, so the weakest method in the list is what the list guarantees:
`[cryptosign, hmac_secret]` admits every holder of the shared key, under any
name. An empty list, the default, turns the phase
off: the node sends `hello` at once as a server, and as a client it answers a
challenge with `anonymous`. A list without `anonymous` makes the node refuse a
peer that does not authenticate.
""".

-include("partisan.hrl").

-define(MAX_FRAME_SIZE, 4096).
-define(NONCE_SIZE, 32).
%% An atom holds at most 255 characters, of at most 4 bytes each in UTF-8.
-define(MAX_NAME_SIZE, 1020).
-define(MAX_PROOF_SIZE, 512).
-define(MAX_METHODS, 8).
-define(MAX_METHOD_SIZE, 32).
-define(LABEL, <<"partisan-peer-auth-v1">>).
-define(MAGIC, "PAUTH1").
-define(CHALLENGE, 1).
-define(RESPONSE, 2).
-define(RESULT_OK, 3).
-define(RESULT_ERROR, 4).

-record(server, {
    methods :: [{binary(), method()}],
    offered :: [binary()],
    name :: binary(),
    nonce :: binary(),
    binding :: binding()
}).

-record(client, {
    %% `false' for a node with an empty method list, which keeps the decode
    %% of the existing protocol.
    bounded :: boolean(),
    methods :: [{binary(), method()}],
    name :: binary(),
    peer :: binary(),
    binding :: binding(),
    %% `{result, Method, Message}': the client waits for the server's proof
    %% over `Message'. The stage holds no trust material: the client loads it
    %% again to check the proof.
    stage :: challenge | {result, method(), binary()}
}).

-type method() :: partisan_peer_authentication:method() | anonymous.
%% The channel binding of a connection: a value that both ends of one
%% transport session hold and that no other session has. See
%% `partisan_peer_socket:channel_binding/1'.
%%
%% - `{tls_exporter, Value}': the session has a binding.
%% - `capable': the session has no binding, but this end allows a transport
%%   that has one. Two capable ends always get a binding, so this value on
%%   both ends means that a party on the path took it away.
%% - `none': the session has no binding and this end cannot have one.
-type binding() :: none | capable | {tls_exporter, binary()}.
-type authn() :: #{
    method := method(),
    %% Whether the proofs covered a channel binding. Always `none' for
    %% `anonymous', which has no proof.
    channel_binding := none | tls_exporter,
    %% The name the peer authenticated under. `undefined' for `anonymous'.
    %% Only `cryptosign' proves that the peer may use the name: with a
    %% shared-key method every holder of the key can use any name.
    peer := binary() | undefined
}.
-opaque server() :: #server{}.
-opaque client() :: #client{}.
-type reason() ::
    frame_too_large
    | compressed_frame
    | undecodable_frame
    | invalid_frame
    | unsupported_method
    | no_common_method
    | invalid_proof
    | channel_binding_downgrade
    | unexpected_peer
    | authentication_required
    | rejected
    | {verifier, term()}.

-export_type([authn/0]).
-export_type([binding/0]).
-export_type([client/0]).
-export_type([method/0]).
-export_type([reason/0]).
-export_type([server/0]).

-export([authenticates_peers/0]).
-export([client_handle/2]).
-export([client_init/3]).
-export([client_init/4]).
-export([allow/1]).
-export([allowed/0]).
-export([enter_pending/0]).
-export([init/0]).
-export([deframe/1]).
-export([enframe/1]).
-export([error_frame/0]).
-export([established_socket_opts/0]).
-export([initial_socket_opts/1]).
-export([is_allowed/1]).
-export([leave_pending/0]).
-export([pending/0]).
-export([is_required/0]).
-export([requires_tls_certificates/0]).
-export([max_frame_size/0]).
-export([methods/0]).
-export([server_handle/2]).
-export([server_init/2]).
-export([server_init/3]).
-export([timeout/0]).
-export([validate_config/0]).
-export([validate_methods/1]).

%% =============================================================================
%% API
%% =============================================================================

-doc """
Returns the configured `peer_auth_methods`.
""".
-spec methods() -> [method()].

methods() ->
    partisan_config:get(peer_auth_methods, []).

-doc """
Returns `true` when this node admits only peers that have authenticated.

This is the one definition of an authenticated peer plane. It holds when
`is_required/0` or `requires_tls_certificates/0` holds. The startup notice of
Partisan uses it, and an application that refuses to run on an open peer plane
calls it instead of reading the options itself.

The function reads the configuration. It says what this node enforces on the
connections it accepts and opens, not what its peers enforce.
""".
-spec authenticates_peers() -> boolean().

authenticates_peers() ->
    is_required() orelse requires_tls_certificates().

-doc """
Returns `true` when this node refuses a peer that does not complete the
authentication phase: `peer_auth_methods` is not empty and does not contain
`anonymous`.
""".
-spec is_required() -> boolean().

is_required() ->
    case methods() of
        [] ->
            false;
        Methods ->
            not lists:member(anonymous, Methods)
    end.

-doc """
Returns `true` when this node uses TLS and admits only peers that present a
certificate it verifies, in both roles.

That is `tls` set, `{verify, verify_peer}` in `tls_server_options` and in
`tls_client_options`, and no `{fail_if_no_peer_cert, false}` in
`tls_server_options`. A server that verifies peers refuses a client that
presents no certificate, unless that option is `false`: a probe on OTP 27, 28
and 29, over TLS 1.2 and 1.3, got a refusal without the option and with
`true`, and a connection with `false`.
`partisan_peer_auth_cluster_SUITE` covers both on real nodes.

The function does not check which certificate authority the node trusts. A
node that trusts a public authority admits every holder of a certificate of
that authority.
""".
-spec requires_tls_certificates() -> boolean().

requires_tls_certificates() ->
    Server = partisan_config:get(tls_server_options, []),
    Client = partisan_config:get(tls_client_options, []),
    partisan_config:get(tls, false) =:= true andalso
        proplists:get_value(verify, Server) =:= verify_peer andalso
        proplists:get_value(fail_if_no_peer_cert, Server) =/= false andalso
        proplists:get_value(verify, Client) =:= verify_peer.

-doc """
Returns the time in milliseconds a node waits for the phase to complete.
""".
-spec timeout() -> pos_integer().

timeout() ->
    partisan_config:get(peer_auth_timeout, ?DEFAULT_PEER_AUTH_TIMEOUT).

-doc """
Returns the largest frame, in bytes, a node accepts before the phase succeeds.
""".
-spec max_frame_size() -> pos_integer().

max_frame_size() ->
    ?MAX_FRAME_SIZE.

-doc """
Returns the framing options a peer socket starts with, for a node with the
method list `Methods`: raw when the phase is on, where the node takes the
frames itself with `deframe/1`, and those of `established_socket_opts/0` when
it is off.
""".
-spec initial_socket_opts([method()]) -> [gen_tcp:option()].

initial_socket_opts([]) ->
    established_socket_opts();
initial_socket_opts([_ | _]) ->
    [{packet, raw}].

-doc """
Returns the framing options of a peer socket that carries the existing
protocol: a 4-byte length before each frame, and `max_message_size` as the
frame limit.
""".
-spec established_socket_opts() -> [gen_tcp:option()].

established_socket_opts() ->
    [{packet, 4}, {packet_size, partisan_config:get(max_message_size)}].

-doc """
Takes one frame of the phase from the bytes a raw socket has delivered.

A frame on the wire is a 4-byte length and that many bytes, as with the
`{packet, 4}` socket option. Until the phase succeeds a node reads the socket
raw and takes the frames itself, so that the frame limit does not depend on
the socket: `ssl` does not apply `packet_size` on OTP 27.

- `{ok, Frame, Rest}`: a whole frame, and the bytes after it.
- `more`: the frame is not whole yet. `Bytes` is at most 4 bytes more than
  `max_frame_size/0`.
- `{error, frame_too_large}`: the length is over `max_frame_size/0`. The
  result does not wait for the frame's bytes.
""".
-spec deframe(Bytes :: binary()) ->
    {ok, binary(), binary()} | more | {error, frame_too_large}.

deframe(<<Size:32, _/binary>>) when Size > ?MAX_FRAME_SIZE ->
    {error, frame_too_large};
deframe(<<Size:32, Frame:Size/binary, Rest/binary>>) ->
    {ok, Frame, Rest};
deframe(Bytes) when is_binary(Bytes) ->
    more.

-doc """
Returns the frame that tells a peer that the phase failed. A server sends it
for a failure that `server_handle/2` does not see, such as a frame over the
limit.
""".
-spec error_frame() -> binary().

error_frame() ->
    encode({auth_result, error}).

-doc """
Returns a frame of the phase with its 4-byte length, for a raw socket.
""".
-spec enframe(binary()) -> iodata().

enframe(Frame) when is_binary(Frame) ->
    [<<(byte_size(Frame)):32>>, Frame].

-doc """
Returns `true` when this node may connect to and accept `Node`:
`peer_auth_allowed_nodes` is `all`, the default, or contains `Node`.

This is the gate of `net_kernel:allow/1`. It applies with every method, and
with the phase off. Only `cryptosign` proves that a peer may use the name it
presents, so with the other methods the gate stops a mistake and not an
attacker that holds the shared key.
""".
-spec is_allowed(node()) -> boolean().

is_allowed(Node) ->
    case partisan_config:get(peer_auth_allowed_nodes, all) of
        all ->
            true;
        Nodes ->
            lists:member(Node, Nodes)
    end.

-doc """
Adds `Nodes` to `peer_auth_allowed_nodes`, as `net_kernel:allow/1` does.

Before the first call, and with the default configuration, the node allows
every node. After it, the node allows the listed nodes only. The node does not
close the connections it already has.
""".
-spec allow([node()]) -> ok | error.

allow(Nodes) when is_list(Nodes) ->
    case lists:all(fun erlang:is_atom/1, Nodes) of
        true ->
            Current =
                case partisan_config:get(peer_auth_allowed_nodes, all) of
                    all -> [];
                    List -> List
                end,
            partisan_config:set(
                peer_auth_allowed_nodes, lists:usort(Current ++ Nodes)
            );
        false ->
            error
    end;
allow(_) ->
    error.

-doc """
Returns `peer_auth_allowed_nodes`: `all` or the list of allowed nodes.
""".
-spec allowed() -> all | [node()].

allowed() ->
    partisan_config:get(peer_auth_allowed_nodes, all).

-doc """
Creates the count of connections that have not authenticated. Partisan calls
it when it reads its configuration.
""".
-spec init() -> ok.

init() ->
    persistent_term:put({?MODULE, pending}, counters:new(1, [atomics])).

-doc """
Counts one more accepted connection that has not authenticated.

Returns `{error, limit}`, and does not count the connection, when the node
already holds `peer_auth_max_pending` of them. The caller then closes the
connection. A caller that gets `ok` calls `leave_pending/0` once, when the
connection authenticates or ends.
""".
-spec enter_pending() -> ok | {error, limit}.

enter_pending() ->
    Counter = pending_counter(),
    ok = counters:add(Counter, 1, 1),
    Max = partisan_config:get(
        peer_auth_max_pending, ?DEFAULT_PEER_AUTH_MAX_PENDING
    ),
    case counters:get(Counter, 1) > Max of
        true ->
            ok = counters:sub(Counter, 1, 1),
            {error, limit};
        false ->
            ok
    end.

-doc """
Ends the count of one connection. See `enter_pending/0`.
""".
-spec leave_pending() -> ok.

leave_pending() ->
    counters:sub(pending_counter(), 1, 1).

-doc """
Returns the number of accepted connections that have not authenticated.
""".
-spec pending() -> non_neg_integer().

pending() ->
    counters:get(pending_counter(), 1).

pending_counter() ->
    case persistent_term:get({?MODULE, pending}, undefined) of
        undefined ->
            ok = init(),
            persistent_term:get({?MODULE, pending});
        Counter ->
            Counter
    end.

-doc """
Returns `Methods` when it is a valid value for `peer_auth_methods`. Fails with
`badarg` otherwise.
""".
-spec validate_methods(term()) -> [method()].

validate_methods(Methods) when is_list(Methods) ->
    Valid =
        lists:all(fun is_method/1, Methods) andalso
            length(lists:usort(Methods)) =:= length(Methods),
    Valid orelse error({badarg, peer_auth_methods}),
    Methods;
validate_methods(_) ->
    error({badarg, peer_auth_methods}).

-doc """
Checks that every method in `peer_auth_methods` has its trust material.
Fails with `{peer_auth, Reason}` when one does not.
""".
-spec validate_config() -> ok.

validate_config() ->
    lists:foreach(
        fun
            (anonymous) ->
                ok;
            (Method) ->
                case (verifier(Method)):init(Method) of
                    {ok, _} ->
                        ok;
                    {error, Reason} ->
                        error({peer_auth, Reason})
                end
        end,
        methods()
    ).

-doc """
Starts the phase on the accepting side.

`Methods` is not empty. Returns the challenge frame to send, and the state for
`server_handle/2`.
""".
-spec server_init([method(), ...], node()) -> {binary(), server()}.

server_init(Methods, Node) ->
    server_init(Methods, Node, none).

-doc """
Starts the phase on the accepting side of a connection with the channel
binding `Binding`.

Every proof of the exchange covers `Binding`. The exchange succeeds only when
the client gives the same binding to `client_init/4`, which two ends do only
when they share one transport session.
""".
-spec server_init([method(), ...], node(), binding()) -> {binary(), server()}.

server_init([_ | _] = Methods, Node, Binding) when is_atom(Node) ->
    Named = named(Methods),
    Offered = [Name || {Name, _} <- Named],
    Server = #server{
        methods = Named,
        offered = Offered,
        name = atom_to_binary(Node, utf8),
        nonce = crypto:strong_rand_bytes(?NONCE_SIZE),
        binding = valid_binding(Binding)
    },
    Frame = encode(
        {auth_challenge, Offered, Server#server.name, Server#server.nonce,
            capable(Binding)}
    ),
    {Frame, Server}.

-doc """
Handles the frame a server receives in answer to its challenge.

Returns the result frame to send in both cases. On `ok` the phase succeeded.
On `error` the caller sends the frame and closes the connection.
""".
-spec server_handle(binary(), server()) ->
    {ok, binary(), authn()} | {error, reason(), binary()}.

server_handle(Data, #server{} = Server) ->
    try
        {Authn, Proof} = server_verify(decode(Data, true), Server),
        {ok, encode({auth_result, ok, Proof}), Authn}
    catch
        throw:Reason ->
            {error, Reason, encode({auth_result, error})}
    end.

-doc """
Starts the phase on the connecting side, for a connection to `Peer`.

`Methods` may be empty. The node then accepts `anonymous` only, and decodes
frames as the existing protocol does.
""".
-spec client_init([method()], node(), node()) -> client().

client_init(Methods, Node, Peer) ->
    client_init(Methods, Node, Peer, none).

-doc """
Starts the phase on the connecting side of a connection with the channel
binding `Binding`. See `server_init/3`.
""".
-spec client_init([method()], node(), node(), binding()) -> client().

client_init(Methods, Node, Peer, Binding) when
    is_list(Methods), is_atom(Node), is_atom(Peer)
->
    Effective =
        case Methods of
            [] -> [anonymous];
            _ -> Methods
        end,
    #client{
        bounded = Methods =/= [],
        methods = named(Effective),
        name = atom_to_binary(Node, utf8),
        peer = atom_to_binary(Peer, utf8),
        binding = valid_binding(Binding),
        stage = challenge
    }.

-doc """
Handles a frame a client receives before the phase completes.

- `{reply, Frame, Client}`: send `Frame` and wait for the next frame.
- `{ok, Authn}`: the phase succeeded.
- `{ok, Authn, Term}`: the server runs no phase and the client accepts
  `anonymous`. `Term` is the decoded frame, the first of the existing
  protocol.
- `{error, Reason}`: the phase failed. The caller closes the connection.
""".
-spec client_handle(binary(), client()) ->
    {reply, binary(), client()}
    | {ok, authn()}
    | {ok, authn(), term()}
    | {error, reason()}.

client_handle(Data, #client{} = Client) ->
    try
        client_step(decode(Data, Client#client.bounded), Client)
    catch
        throw:Reason ->
            {error, Reason}
    end.

%% =============================================================================
%% PRIVATE: SERVER
%% =============================================================================

%% Returns the authn() of a verified response and this node's proof. Throws a
%% reason() otherwise.
server_verify(
    {auth_response, Method, CName, CNonce, CProof, CCapable},
    #server{} = Server
) when
    is_boolean(CCapable),
    is_binary(Method),
    byte_size(Method) =< ?MAX_METHOD_SIZE,
    is_binary(CName),
    byte_size(CName) =< ?MAX_NAME_SIZE,
    is_binary(CNonce),
    byte_size(CNonce) =:= ?NONCE_SIZE,
    is_binary(CProof),
    byte_size(CProof) =< ?MAX_PROOF_SIZE
->
    case lists:keyfind(Method, 1, Server#server.methods) of
        {_, anonymous} when CProof =:= <<>> ->
            {authn(anonymous, undefined, none), <<>>};
        {_, anonymous} ->
            throw(invalid_frame);
        {_, Name} ->
            Module = verifier(Name),
            State = verifier_init(Module, Name),
            Message = fun(Role) ->
                message(Role, Method, Server, CName, CNonce, CCapable)
            end,
            Module:verify(CName, Message(<<"client">>), CProof, State) orelse
                throw(invalid_proof),
            %% After the proof, which covers `CCapable'.
            ok = check_downgrade(Server#server.binding, CCapable),
            Proof = verifier_respond(Module, Message(<<"server">>), State),
            {authn(Name, CName, Server#server.binding), Proof};
        false ->
            throw(unsupported_method)
    end;
server_verify(_, _) ->
    throw(invalid_frame).

%% =============================================================================
%% PRIVATE: CLIENT
%% =============================================================================

client_step(
    {auth_challenge, Offered, SName, SNonce, SCapable},
    #client{stage = challenge} = Client
) when
    is_boolean(SCapable),
    is_list(Offered),
    is_binary(SName),
    byte_size(SName) =< ?MAX_NAME_SIZE,
    is_binary(SNonce),
    byte_size(SNonce) =:= ?NONCE_SIZE
->
    valid_offer(Offered) orelse throw(invalid_frame),
    SName =:= Client#client.peer orelse throw(unexpected_peer),

    {Method, Name} = pick(Offered, Client#client.methods),
    CNonce = crypto:strong_rand_bytes(?NONCE_SIZE),

    case Name of
        anonymous ->
            Frame = encode(
                {auth_response, Method, Client#client.name, CNonce, <<>>,
                    false}
            ),
            Stage = {result, anonymous, <<>>},
            {reply, Frame, Client#client{stage = Stage}};
        _ ->
            %% `SCapable' is not authenticated yet. The server's proof covers
            %% it, and the client checks that proof before the phase succeeds.
            ok = check_downgrade(Client#client.binding, SCapable),
            CCapable = capable(Client#client.binding),
            Module = verifier(Name),
            State = verifier_init(Module, Name),
            Message = fun(Role) ->
                message(
                    Role,
                    Method,
                    Offered,
                    SName,
                    SNonce,
                    Client#client.name,
                    CNonce,
                    Client#client.binding,
                    SCapable,
                    CCapable
                )
            end,
            CProof = verifier_respond(Module, Message(<<"client">>), State),
            Frame = encode(
                {auth_response, Method, Client#client.name, CNonce, CProof,
                    CCapable}
            ),
            Stage = {result, Name, Message(<<"server">>)},
            {reply, Frame, Client#client{stage = Stage}}
    end;
client_step({auth_challenge, _, _, _, _}, #client{stage = challenge}) ->
    throw(invalid_frame);
client_step({other, Data}, #client{stage = challenge} = Client) ->
    %% Not a frame of the phase: the server runs no phase. A client that
    %% requires authentication stops here and does not decode the bytes.
    case lists:keymember(anonymous, 2, Client#client.methods) of
        true ->
            Term = decode_existing(Data, Client#client.bounded),
            {ok, authn(anonymous, undefined, none), Term};
        false ->
            throw(authentication_required)
    end;
client_step(
    {auth_result, ok, <<>>}, #client{stage = {result, anonymous, _}}
) ->
    {ok, authn(anonymous, undefined, none)};
client_step(
    {auth_result, ok, SProof},
    #client{stage = {result, Name, Message}} = Client
) when
    Name =/= anonymous,
    is_binary(SProof),
    byte_size(SProof) =< ?MAX_PROOF_SIZE
->
    Module = verifier(Name),
    State = verifier_init(Module, Name),
    Module:verify(Client#client.peer, Message, SProof, State) orelse
        throw(invalid_proof),
    {ok, authn(Name, Client#client.peer, Client#client.binding)};
client_step({auth_result, error}, #client{stage = {result, _, _}}) ->
    throw(rejected);
client_step(_, #client{}) ->
    throw(invalid_frame).

%% The first offered method that the client accepts.
pick([Method | Rest], Methods) ->
    case lists:keyfind(Method, 1, Methods) of
        {Method, Name} ->
            {Method, Name};
        false ->
            pick(Rest, Methods)
    end;
pick([], _) ->
    throw(no_common_method).

valid_offer(Offered) ->
    valid_offer(Offered, 0).

valid_offer([Method | Rest], N) when
    is_binary(Method), byte_size(Method) =< ?MAX_METHOD_SIZE, N < ?MAX_METHODS
->
    valid_offer(Rest, N + 1);
valid_offer([], N) ->
    N > 0;
valid_offer(_, _) ->
    false.

%% =============================================================================
%% PRIVATE: FRAMES AND MESSAGES
%% =============================================================================

%% A frame of the phase is not a term in the external term format. A node
%% reads it with the binary patterns below and never gives bytes from a peer
%% that has not authenticated to `binary_to_term/1,2'.
%%
%% A frame is `?MAGIC', a type byte, and the fields of the type. A field is a
%% 16-bit length and that many bytes. A challenge carries a count byte before
%% its method fields. A challenge and a response end with one byte, 1 when the
%% sender is capable of a channel binding and 0 when it is not.
encode({auth_challenge, Offered, SName, SNonce, Capable}) ->
    frame(?CHALLENGE, [
        <<(length(Offered)):8>>,
        [field16(O) || O <- Offered],
        field16(SName),
        field16(SNonce),
        flag(Capable)
    ]);
encode({auth_response, Method, CName, CNonce, Proof, Capable}) ->
    frame(?RESPONSE, [
        field16(Method),
        field16(CName),
        field16(CNonce),
        field16(Proof),
        flag(Capable)
    ]);
encode({auth_result, ok, Proof}) ->
    frame(?RESULT_OK, [field16(Proof)]);
encode({auth_result, error}) ->
    frame(?RESULT_ERROR, []).

frame(Type, Fields) ->
    Frame = iolist_to_binary([?MAGIC, Type, Fields]),
    %% Holds for every frame this module builds: at most 4 method names, two
    %% names of at most 1020 bytes, a nonce and a proof of at most 64 bytes.
    true = byte_size(Frame) =< ?MAX_FRAME_SIZE,
    Frame.

field16(Bin) ->
    <<(byte_size(Bin)):16, Bin/binary>>.

flag(true) -> 1;
flag(false) -> 0.

unflag(<<1>>) -> true;
unflag(<<0>>) -> false;
unflag(_) -> throw(invalid_frame).

%% Returns the frame as a tuple, or `{other, Data}' for bytes that are not a
%% frame of the phase. Throws a reason() for a frame of the phase that is
%% malformed, and for a frame over the limit.
decode(Data, true) when byte_size(Data) > ?MAX_FRAME_SIZE ->
    throw(frame_too_large);
decode(<<?MAGIC, Type, Rest/binary>>, _) ->
    try
        decode_frame(Type, Rest)
    catch
        %% Bytes left after the last field.
        error:{badmatch, _} ->
            throw(invalid_frame)
    end;
decode(Data, _) ->
    {other, Data}.

decode_frame(?CHALLENGE, <<Count, Rest0/binary>>) when
    Count > 0, Count =< ?MAX_METHODS
->
    {Offered, Rest1} = fields(Count, Rest0),
    {[SName, SNonce], Flag} = fields(2, Rest1),
    {auth_challenge, Offered, SName, SNonce, unflag(Flag)};
decode_frame(?RESPONSE, Rest) ->
    {[Method, CName, CNonce, Proof], Flag} = fields(4, Rest),
    {auth_response, Method, CName, CNonce, Proof, unflag(Flag)};
decode_frame(?RESULT_OK, Rest) ->
    {[Proof], <<>>} = fields(1, Rest),
    {auth_result, ok, Proof};
decode_frame(?RESULT_ERROR, <<>>) ->
    {auth_result, error};
decode_frame(_, _) ->
    throw(invalid_frame).

fields(0, Rest) ->
    {[], Rest};
fields(N, <<Size:16, Field:Size/binary, Rest0/binary>>) ->
    {Fields, Rest} = fields(N - 1, Rest0),
    {[Field | Fields], Rest};
fields(_, _) ->
    throw(invalid_frame).

%% Decodes the first frame of a server that runs no phase, for a client that
%% accepts `anonymous'. With a non-empty method list the decode is bounded: the
%% frame is at most `?MAX_FRAME_SIZE' (checked by `decode/2'), is not
%% compressed, and creates no atom. `safe' does not bound what a compressed
%% term inflates to.
decode_existing(<<131, 80, _/binary>>, true) ->
    throw(compressed_frame);
decode_existing(Data, true) ->
    try
        binary_to_term(Data, [safe])
    catch
        error:badarg ->
            throw(undecodable_frame)
    end;
decode_existing(Data, false) ->
    binary_to_term(Data).

message(Role, Method, #server{} = Server, CName, CNonce, CCapable) ->
    message(
        Role,
        Method,
        Server#server.offered,
        Server#server.name,
        Server#server.nonce,
        CName,
        CNonce,
        Server#server.binding,
        capable(Server#server.binding),
        CCapable
    ).

%% The bytes a proof covers. Each field carries its length, so no two
%% different field lists give the same bytes.
%%
%% The binding goes in as its type and its value. Each end takes both from its
%% own socket and neither travels in a frame, so a party on the path cannot
%% remove the binding: the ends then sign different bytes.
%%
%% The two capability flags do travel in frames, and go in as each end saw
%% them, so a party that changes one makes the ends sign different bytes.
message(
    Role, Method, Offered, SName, SNonce, CName, CNonce, Binding, SCap, CCap
) ->
    {BindingType, BindingValue} =
        case Binding of
            {tls_exporter, Value} -> {<<"tls-exporter">>, Value};
            _ -> {<<"none">>, <<>>}
        end,
    iolist_to_binary([
        ?LABEL,
        field(Role),
        field(Method),
        <<(length(Offered)):32>>,
        [field(O) || O <- Offered],
        field(SName),
        field(SNonce),
        field(CName),
        field(CNonce),
        field(BindingType),
        field(BindingValue),
        <<(flag(SCap)), (flag(CCap))>>
    ]).

%% Whether this end allows a transport that has a channel binding.
capable(none) -> false;
capable(capable) -> true;
capable({tls_exporter, _}) -> true.

%% Both ends are capable of a binding and the session has none: a party on the
%% path made each end negotiate a transport without one. This is the check of
%% the "y" flag of SCRAM (RFC 5802, section 6).
check_downgrade(capable, true) ->
    throw(channel_binding_downgrade);
check_downgrade(_, _) ->
    ok.

valid_binding(none) ->
    none;
valid_binding(capable) ->
    capable;
valid_binding({tls_exporter, Value} = Binding) when
    is_binary(Value), byte_size(Value) =:= 32
->
    Binding.

authn(Method, Peer, Binding) ->
    #{
        method => Method,
        peer => Peer,
        channel_binding =>
            case Binding of
                {tls_exporter, _} -> tls_exporter;
                _ -> none
            end
    }.

field(Bin) ->
    <<(byte_size(Bin)):32, Bin/binary>>.

%% =============================================================================
%% PRIVATE: METHODS
%% =============================================================================

named(Methods) ->
    [{atom_to_binary(Method, utf8), Method} || Method <- Methods].

is_method(Method) ->
    lists:member(Method, [cryptosign, gossip_keyring, hmac_secret, anonymous]).

verifier(cryptosign) ->
    partisan_peer_auth_cryptosign;
verifier(gossip_keyring) ->
    partisan_peer_auth_hmac;
verifier(hmac_secret) ->
    partisan_peer_auth_hmac.

verifier_init(Module, Method) ->
    case Module:init(Method) of
        {ok, State} ->
            State;
        {error, Reason} ->
            throw({verifier, Reason})
    end.

verifier_respond(Module, Message, State) ->
    case Module:respond(Message, State) of
        {ok, Proof} ->
            Proof;
        {error, Reason} ->
            throw({verifier, Reason})
    end.
