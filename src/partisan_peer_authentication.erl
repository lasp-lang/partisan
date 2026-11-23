%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(partisan_peer_authentication).

-moduledoc """
The behaviour of a peer authentication method (ADR-000005).

A callback module computes and checks proofs. It does no I/O:
`partisan_peer_auth` builds the message a proof covers and owns the frames, and
the connection processes own the socket.

A proof covers a message that `partisan_peer_auth` builds from both nonces, both
node names, the role of the prover, the chosen method and the offered method
list. A callback module treats the message as opaque bytes.
""".

-type method() :: cryptosign | gossip_keyring | hmac_secret.
-type state() :: term().

-export_type([method/0]).
-export_type([state/0]).

-doc """
Loads the trust material of `Method` from the configuration.

Returns `{error, Reason}` when the material is missing or malformed. The
connection then fails.
""".
-callback init(Method :: method()) -> {ok, state()} | {error, Reason :: term()}.

-doc """
Returns this node's proof over `Message`.
""".
-callback respond(Message :: binary(), state()) ->
    {ok, Proof :: binary()} | {error, Reason :: term()}.

-doc """
Returns `true` when `Proof` is a valid proof over `Message` for the peer that
claims the name `Peer`.

`Peer` is the UTF-8 node name the peer sent. It comes from an unauthenticated
frame: a callback module must not convert it to an atom.
""".
-callback verify(
    Peer :: binary(), Message :: binary(), Proof :: binary(), state()
) -> boolean().
