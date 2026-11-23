%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(partisan_peer_auth_hmac).

-moduledoc """
The shared-key verifier for the `gossip_keyring` and `hmac_secret` peer
authentication methods (ADR-000005).

A proof is the HMAC-SHA256 of the message under a shared key. A proof shows
that the peer holds a key of the cluster. It does not show which node the peer
is.

`gossip_keyring` reads `peer_auth_keyring`, an ordered list of keys. The node
signs with the first key and accepts a proof made with any key in the list.
To rotate, add the new key at the end of the list on every node, then move it
to the front on every node, then remove the old key.

`hmac_secret` reads `peer_auth_secret`, one key. It is a keyring of one key.
""".

-behaviour(partisan_peer_authentication).

-define(MIN_KEY_SIZE, 16).
-define(MAC_SIZE, 32).

-export([init/1]).
-export([respond/2]).
-export([verify/4]).
-export([validate_keyring/1]).
-export([validate_secret/1]).

%% =============================================================================
%% API
%% =============================================================================

-doc """
Returns `Keyring` when it is a valid value for `peer_auth_keyring`: a list of
binaries of at least 16 bytes each. Fails with `badarg` otherwise.
""".
-spec validate_keyring(term()) -> [binary()].

validate_keyring(Keyring) when is_list(Keyring) ->
    lists:all(fun is_key/1, Keyring) orelse
        error({badarg, peer_auth_keyring}),
    Keyring;
validate_keyring(_) ->
    error({badarg, peer_auth_keyring}).

-doc """
Returns `Secret` when it is a valid value for `peer_auth_secret`: `undefined`
or a binary of at least 16 bytes. Fails with `badarg` otherwise.
""".
-spec validate_secret(term()) -> binary() | undefined.

validate_secret(undefined) ->
    undefined;
validate_secret(Secret) ->
    is_key(Secret) orelse error({badarg, peer_auth_secret}),
    Secret.

%% =============================================================================
%% PARTISAN_PEER_AUTHENTICATION CALLBACKS
%% =============================================================================

-spec init(gossip_keyring | hmac_secret) ->
    {ok, [binary(), ...]} | {error, term()}.

init(gossip_keyring) ->
    case partisan_config:get(peer_auth_keyring, []) of
        [_ | _] = Keyring ->
            {ok, Keyring};
        _ ->
            {error, {not_configured, peer_auth_keyring}}
    end;
init(hmac_secret) ->
    case partisan_config:get(peer_auth_secret, undefined) of
        Secret when is_binary(Secret) ->
            {ok, [Secret]};
        _ ->
            {error, {not_configured, peer_auth_secret}}
    end.

-spec respond(binary(), [binary(), ...]) -> {ok, binary()}.

respond(Message, [Primary | _]) ->
    {ok, mac(Primary, Message)}.

-spec verify(binary(), binary(), binary(), [binary(), ...]) -> boolean().

verify(_Peer, Message, Proof, Keyring) when byte_size(Proof) =:= ?MAC_SIZE ->
    %% `crypto:hash_equals/2' compares in constant time. It fails with `badarg'
    %% on binaries of different sizes, which the guard excludes.
    %% Every key is tried, so the time does not tell which key matched.
    lists:foldl(
        fun(Key, Acc) -> crypto:hash_equals(mac(Key, Message), Proof) or Acc end,
        false,
        Keyring
    );
verify(_Peer, _Message, _Proof, _Keyring) ->
    false.

%% =============================================================================
%% PRIVATE
%% =============================================================================

mac(Key, Message) ->
    crypto:mac(hmac, sha256, Key, Message).

is_key(Key) ->
    is_binary(Key) andalso byte_size(Key) >= ?MIN_KEY_SIZE.
