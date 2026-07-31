%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(partisan_broadcast_engine).

-moduledoc """
Behaviour for a broadcast group's **tree engine**.

A broadcast group (see `partisan_plumtree_broadcast`) delegates tree construction
and repair to an engine implementing this behaviour. The group shell owns
everything else — the process, mailbox, membership poll, handler dispatch, the
rolling-upgrade router, ticks and transport — and the engine owns only the tree
topology and the transitions that maintain it.

The engine is **pure with respect to I/O**: callbacks never send. They return an
updated engine state together with a list of actions, which the shell executes via
its transport in the returned order. This keeps the protocol testable and lets an
alternate engine be swapped in behind the same interface.

## Dispatch modes

An engine runs in one of two modes, which the shell reads from `dispatch_mode/0`:

- **Typed** (the default) — the shell decodes each wire message and calls a
  Plumtree-shaped callback (`handle_broadcast/8`, `handle_ihave/7`,
  `handle_graft/7`, …), passing the handler's own verdicts (novelty, staleness).
  Plumtree is the first typed engine, `partisan_plumtree_engine`.
- **Raw** — for an engine that runs its own wire protocol and de-duplication (for
  example `partisan_thicket_engine`). The shell hands it whole inbound messages via
  `handle_message/2` and periodic ticks via `repair_tick/1`, and executes the
  `t:raw_action/0` list it returns. Such a group hosts a single handler module,
  supplied by the shell, so raw actions carry no `Mod`.

A raw engine implements `init/1`, `update_members/2`, `broadcast/4` and the query
callbacks; it omits the typed `handle_*` / `lazy_tick` / `select_exchange_peer`
callbacks. An engine that does not export `dispatch_mode/0` runs in typed mode.
""".

-doc "Opaque engine state; its internal shape is private to the engine module.".
-type state() :: term().

-type nodeset() :: ordsets:ordset(node()).

-doc """
A typed engine's action: transmit `Msg` on behalf of handler `Mod` to `Peer` (the
peer's same-named group). The shell executes each in the order returned.
""".
-type action() :: {send, node(), Msg :: term(), module()}.

-doc """
A raw engine's action. The group's single handler is supplied by the shell, so
these carry no `Mod`:

- `send` transmits a wire message to `Peer`;
- `deliver` hands a received payload to the handler to store and apply;
- `fetch` asks the shell to re-supply `MessageId` to `Peer` via the handler's
  `graft/1`, stamping the given piggyback load on the reply.
""".
-type raw_action() ::
    {send, node(), Msg :: term()}
    | {deliver, MessageId :: term(), Payload :: term()}
    | {fetch, node(), MessageId :: term(), Root :: node(), Load :: term()}.

-export_type([state/0]).
-export_type([action/0]).
-export_type([raw_action/0]).

-doc """
Builds the initial engine state.

`Opts` carries at least `#{members := [node()]}`. The engine may allocate private
resources here (for example an ETS table for outstanding lazy pushes); they are
owned by the calling group process.
""".
-callback init(Opts :: map()) -> state().

-doc """
Reconciles the tree after membership changed to `Members` — attach joiners, drop
leavers.
""".
-callback update_members(Members :: nodeset(), state()) ->
    {state(), [action()]}.

-doc """
Originates a broadcast from this node (the tree root is self): choose the eager and
lazy targets to disseminate to.
""".
-callback broadcast(
    MessageId :: any(), Payload :: any(), Mod :: module(), state()
) ->
    {state(), [action()]}.

-doc """
Handles a received broadcast (typed dispatch).

`Novel` is the handler's verdict — new versus already seen — and it drives the
eager-versus-prune decision, the one point of variation between tree protocols.
""".
-callback handle_broadcast(
    Novel :: boolean(),
    MessageId :: any(),
    Payload :: any(),
    Mod :: module(),
    Round :: non_neg_integer(),
    Root :: node(),
    From :: node(),
    state()
) -> {state(), [action()]}.

-doc """
Handles a received lazy-push summary (`i_have`, typed dispatch). `Stale` is the
handler's verdict on whether the advertised message is already held.
""".
-callback handle_ihave(
    Stale :: boolean(),
    MessageId :: any(),
    Mod :: module(),
    Round :: non_neg_integer(),
    Root :: node(),
    From :: node(),
    state()
) -> {state(), [action()]}.

-doc "Handles an `ignored_i_have` — a peer's acknowledgement of a lazy push.".
-callback handle_ignored_ihave(
    MessageId :: any(),
    Mod :: module(),
    Round :: non_neg_integer(),
    Root :: node(),
    From :: node(),
    state()
) -> {state(), [action()]}.

-doc "Handles a received `prune`: demote the sender from eager to lazy.".
-callback handle_prune(Root :: node(), From :: node(), state()) ->
    {state(), [action()]}.

-doc """
Handles a received `graft` (typed dispatch). `GraftResult` is the handler's
`graft/1` result for the requested message.
""".
-callback handle_graft(
    GraftResult :: stale | {ok, any()} | {error, any()},
    MessageId :: any(),
    Mod :: module(),
    Round :: non_neg_integer(),
    Root :: node(),
    From :: node(),
    state()
) -> {state(), [action()]}.

-doc "Periodic flush of outstanding lazy pushes, emitting `i_have` summaries.".
-callback lazy_tick(state()) -> {state(), [action()]}.

-doc """
Chooses a peer for a periodic anti-entropy exchange, or `undefined` to skip this
round.
""".
-callback select_exchange_peer(Connected :: nodeset(), state()) ->
    node() | undefined.

-doc "Query (debug): the eager and lazy peers for the tree rooted at `Root`.".
-callback get_peers(Root :: node(), state()) -> {nodeset(), nodeset()}.

-doc "Query: the eager peers for the tree rooted at `Root`.".
-callback all_eager_peers(Root :: node(), state()) -> nodeset().

-doc "Query: the lazy peers for the tree rooted at `Root`.".
-callback all_lazy_peers(Root :: node(), state()) -> nodeset().

-doc "Query: the engine's view of all known members.".
-callback all_members(state()) -> nodeset().

-doc """
Declares the engine's dispatch mode. Absent (not exported) means `typed`; a raw
engine exports this returning `raw`.
""".
-callback dispatch_mode() -> typed | raw.

-doc """
Handles one inbound wire message (raw dispatch), returning actions for the shell to
execute.
""".
-callback handle_message(Msg :: term(), state()) -> {state(), [raw_action()]}.

-doc "Periodic repair and summary tick (raw dispatch).".
-callback repair_tick(state()) -> {state(), [raw_action()]}.

-optional_callbacks([
    dispatch_mode/0,
    handle_message/2,
    repair_tick/1,
    %% typed-only callbacks a raw engine omits
    handle_broadcast/8,
    handle_ihave/7,
    handle_ignored_ihave/6,
    handle_prune/3,
    handle_graft/7,
    lazy_tick/1,
    select_exchange_peer/2
]).
