%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(partisan_plumtree_broadcast_handler).

-include("partisan.hrl").

-moduledoc """
Behaviour for a `partisan_plumtree_broadcast` handler: the application-specific
half of an epidemic broadcast.

A handler defines how a broadcast payload is produced, de-duplicated, applied,
recovered and reconciled. Each handler runs in its own broadcast group — its own
spanning tree and process — and `partisan_broadcast:broadcast/2` routes to the
group by handler module.

## Delivery paths

A handler chooses one of two apply paths:

- **Synchronous** — implement `merge/2`. The broadcast process both de-duplicates
  and applies the payload, and waits for the apply to finish.
- **Non-blocking** — implement `claim/2` and `handle_broadcast/2`. `claim/2` runs
  on the broadcast process and does only the fast, atomic novelty check; the heavy
  apply runs off the tree in the handler's own process, so a slow apply never
  blocks the tree or the other handlers. When `claim/2` is exported it is used in
  place of `merge/2`.

`broadcast_channel/0`, `claim/2` and `handle_broadcast/2` are optional; the rest
are required.
""".

-doc """
Returns the `{MessageId, Payload}` to disseminate for a given application
broadcast.

`MessageId` identifies the message for de-duplication and for lazy-push (`i_have`)
advertisement; `Payload` is what peers apply.
""".
-callback broadcast_data(any()) -> {MessageId :: any(), Payload :: any()}.

-doc """
Returns the channel on which this handler's broadcasts are sent.

Optional; defaults to the default channel. See
`partisan_plumtree_broadcast:broadcast/2`.
""".
-callback broadcast_channel() -> partisan:channel().

-doc """
De-duplicates and applies a received broadcast, synchronously.

Given the message id and payload, applies the message to local state and returns
`true` if it was novel or `false` if it had already been received. Runs on the
broadcast process, which waits for it — prefer the non-blocking `claim/2` +
`handle_broadcast/2` path when the apply is expensive.
""".
-callback merge(any(), any()) -> boolean().

-doc """
Returns `true` if the message with the given id has already been received.

The broadcast process calls this on a lazy-push (`i_have`) advertisement to decide
whether to request the message.
""".
-callback is_stale(any()) -> boolean().

-doc """
Re-supplies the payload for a previously advertised message id.

Returns `{ok, Payload}` for the requested message, `stale` if a later message has
already subsumed it, or `{error, Reason}`. The broadcast process calls this to
answer a `graft` from a peer repairing its tree.
""".
-callback graft(any()) -> stale | {ok, any()} | {error, any()}.

-doc """
Triggers an anti-entropy exchange between this handler and the one on `Node`.

The exchange reconciles messages missing on either side. It should run as a
background process and need not account for messages in flight when it starts, or
broadcast during its operation — a later exchange covers those. Return `ignore` to
decline.
""".
-callback exchange(node()) -> ok | {ok, pid()} | {error, term()} | ignore.

-doc """
Atomically decides whether `MessageId` is novel and records it as seen — the fast
half of the non-blocking delivery path (PDDR-000001).

Returns `true` if newly claimed, in which case the caller applies the payload via
`handle_broadcast/2`, or `false` if already seen. Runs ON the broadcast process,
so it must be fast and confine its side effects to the seen-set (for example
`ets:insert_new/2` on the handler's own table). Because the id is recorded before
the asynchronous apply runs, a redelivery is still recognised as a duplicate, so
at-least-once idempotent delivery is preserved.
""".
-callback claim(MessageId :: any(), Payload :: any()) -> boolean().

-doc """
Applies a previously claimed broadcast payload — the heavy half of the
non-blocking delivery path.

Runs in the handler's own process: the broadcast server delivers the payload as a
`gen_server` cast, `{'$partisan_apply', MessageId, Payload}`, and never waits on
it, so the apply may do heavy work (CRDT merge, persistence) without blocking the
broadcast tree or the other handlers.
""".
-callback handle_broadcast(MessageId :: any(), Payload :: any()) -> ok.

-optional_callbacks([broadcast_channel/0, claim/2, handle_broadcast/2]).
