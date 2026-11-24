%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(partisan_peer_auth_cryptosign).

-moduledoc """
The Ed25519 verifier for the `cryptosign` peer authentication method
(ADR-000005).

A proof is the Ed25519 signature of the message under the node's private key.
A node accepts a proof when it verifies under the public key that
`peer_auth_cryptosign` lists for the name the peer claims. A proof therefore
shows which node the peer is.

`peer_auth_cryptosign` is a map:

```
#{
    private_key => PrivateKey,
    trusted_peers => #{Node => PublicKey}
}
```

A key is 32 raw bytes, or 64 hexadecimal characters as a binary or a string.
`Node` is a node name as an atom or a binary. `generate_key/0` makes a key pair.
""".

-behaviour(partisan_peer_authentication).

-define(KEY_SIZE, 32).
-define(SIGNATURE_SIZE, 64).

-type config() :: #{
    private_key := binary(),
    trusted_peers := #{binary() => binary()}
}.

-export([init/1]).
-export([respond/2]).
-export([verify/4]).
-export([generate_key/0]).
-export([validate/1]).

%% =============================================================================
%% API
%% =============================================================================

-doc """
Returns a new Ed25519 key pair as raw bytes.
""".
-spec generate_key() -> #{public_key := binary(), private_key := binary()}.

generate_key() ->
    {Public, Private} = crypto:generate_key(eddsa, ed25519),
    #{public_key => Public, private_key => Private}.

-doc """
Returns the stored form of a `peer_auth_cryptosign` value: raw keys, and peer
names as binaries. Returns `undefined` for `undefined`. Fails with `badarg`
when the value is malformed.
""".
-spec validate(term()) -> config() | undefined.

validate(undefined) ->
    undefined;
validate(#{private_key := Private, trusted_peers := Peers}) when
    is_map(Peers)
->
    #{
        private_key => key(Private),
        trusted_peers => maps:fold(
            fun(Name, Public, Acc) ->
                maps:put(name(Name), key(Public), Acc)
            end,
            #{},
            Peers
        )
    };
validate(_) ->
    error({badarg, peer_auth_cryptosign}).

%% =============================================================================
%% PARTISAN_PEER_AUTHENTICATION CALLBACKS
%% =============================================================================

-spec init(cryptosign) -> {ok, config()} | {error, term()}.

init(cryptosign) ->
    case partisan_config:get(peer_auth_cryptosign, undefined) of
        #{private_key := _, trusted_peers := _} = Config ->
            {ok, Config};
        _ ->
            {error, {not_configured, peer_auth_cryptosign}}
    end.

-spec respond(binary(), config()) -> {ok, binary()}.

respond(Message, #{private_key := Private}) ->
    {ok, crypto:sign(eddsa, none, Message, [Private, ed25519])}.

-spec verify(binary(), binary(), binary(), config()) -> boolean().

verify(Peer, Message, Proof, #{trusted_peers := Peers}) when
    byte_size(Proof) =:= ?SIGNATURE_SIZE
->
    case maps:find(Peer, Peers) of
        {ok, Public} ->
            crypto:verify(eddsa, none, Message, Proof, [Public, ed25519]);
        error ->
            false
    end;
verify(_Peer, _Message, _Proof, _Config) ->
    false.

%% =============================================================================
%% PRIVATE
%% =============================================================================

key(Key) when is_binary(Key), byte_size(Key) =:= ?KEY_SIZE ->
    Key;
key(Key) when is_binary(Key), byte_size(Key) =:= 2 * ?KEY_SIZE ->
    try
        binary:decode_hex(Key)
    catch
        error:badarg ->
            error({badarg, peer_auth_cryptosign})
    end;
key(Key) when is_list(Key) ->
    try unicode:characters_to_binary(Key) of
        Bin when is_binary(Bin), byte_size(Bin) =:= 2 * ?KEY_SIZE ->
            key(Bin);
        _ ->
            error({badarg, peer_auth_cryptosign})
    catch
        error:badarg ->
            error({badarg, peer_auth_cryptosign})
    end;
key(_) ->
    error({badarg, peer_auth_cryptosign}).

name(Name) when is_atom(Name) ->
    atom_to_binary(Name, utf8);
name(Name) when is_binary(Name) ->
    Name;
name(_) ->
    error({badarg, peer_auth_cryptosign}).
