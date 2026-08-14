%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(partisan_thicket_engine).

-moduledoc """
Thicket broadcast engine — T interior-node-disjoint spanning trees over one
overlay, with each node's forwarding load bounded (Ferreira, Leitão & Rodrigues,
_Thicket: A Protocol for Building and Maintaining Multiple Trees in a P2P Overlay_,
SRDS 2010). This is broadcast engine #2 behind `partisan_broadcast_engine`; Plumtree
is engine #1.

## The problem it solves

A single epidemic-broadcast tree (Plumtree) concentrates forwarding work on its
interior nodes. When many sources broadcast over the same overlay — one tree per
source — the *same* well-placed nodes tend to be interior in *every* tree, so a few
nodes carry the whole cluster's fan-out while the rest stay leaves. Thicket spreads
that load: it builds the T trees so that, as far as the overlay allows, each node is
interior in only a few of them, and it caps how many with `max_load`.

## The idea

There is one tree per broadcast source, identified by the source node
(`t:tree/0` is `t:node/0`, the analogue of Plumtree's `Root`). A node is **interior**
in a tree when it forwards that tree's traffic — it has more than one active peer in
it — and a **leaf** when it merely receives. Thicket keeps the trees
*interior-node-disjoint*: a node interior in one tree is preferentially a leaf in the
others, so forwarding is shared out. Two quantities make this work and are piggybacked
on every message: a node's own per-tree forwarding fan-out (its `load`), and its
neighbours' most recent loads (`load_est`), so peers can steer new interior duty
toward the least-loaded node that can take it.

## Per-node state (the `#thicket{}` record)

- **`active :: #{tree() => nodeset()}`** — for each tree, the neighbours this node
  exchanges that tree's traffic with (its parent and children). `|active(t)| > 1`
  means interior in `t`; `= 1` means a leaf; `= 0` means disconnected from `t`.
- **`backup`** — neighbours currently used in no tree; the spare capacity that repair
  and branching draw on.
- **`received :: #{msg_id() => tree()}`** — delivered message *ids* only (the paper's
  `receivedMsgs`), for loop-avoidance and announcement filtering. **No payloads** —
  see _Payload delegation_ below.
- **`announcements`** — `{id, tree, sender}` triples learned from summaries: who can
  re-supply a message this node is missing. This set doubles as the repair
  server-directory, and is bounded by an age window (see _Bounded announcements_).
- **`load_est :: #{{node(), tree()} => load}`** — the last forwarding load each
  neighbour reported for each tree; the persistent directory repair and
  reconfiguration steer by.
- **`repair` / `rejected` / `unsummarised`** — the per-tree repair countdown, the
  servers that recently rejected a graft (so a different one is tried next), and the
  recently-received ids still due to be advertised in summaries.

## The four mechanisms

Thicket is four interacting mechanisms, matching the paper's four algorithms:

1. **Construction.** On a *novel* DATA message a node disconnected from that tree
   adopts the sender as its tree link (`adopt_and_forward/6`), and a node not yet
   interior anywhere may *branch*, taking on children up to the fan-out
   (`tree_branching/2`). Adoption and branching reuse a link already carrying another
   tree when no idle one is free (§4.4 link-reassignment), which is what lets a late
   source's tree still span the overlay.
2. **Repair.** Periodically a node advertises its recent ids to its backup peers as a
   `summary`; a peer missing an advertised id records an announcement and arms a
   repair timer; when the timer fires it `graft`s the best server it knows of
   (`tick/1` → `fire_repairs/1` → `graft_best/2` → `best_announcer/2`). Repair is what
   turns the randomised construction — which leaves gaps — into full coverage.
3. **Reconfiguration.** The load-aware `balance/4`: on a novel message from its current
   parent, a node whose parent is heavily loaded swaps to a lighter announced
   alternative — but only one that is a *currently-missing upstream* server, the §4.4
   precondition that proves the swap cannot form a cycle.
4. **Overlay dynamics.** `neighbor_up/2` and `neighbor_down/2` fold membership changes
   into the peer sets; repair then recovers any tree a failed neighbour was carrying.

## Invariants

- **Interior-load bound (safety).** No node is ever interior in more than `max_load`
  trees. Every step that could raise interior load — branching, adoption, accepting a
  graft — is gated on the current `interior_load/1`, so the bound holds by
  construction, for every configuration and message ordering.
- **Coverage (liveness).** Every node eventually delivers every broadcast. This holds
  when `max_load ≥ T` (the cap a ceiling at or above the tree count) over up to four
  divergent trees — a regime that necessarily includes `T > f`, where
  interior-node-disjoint trees cannot fit and coverage rests on link-reassignment and
  on branching into the headroom `max_load` allows. `max_load < T` still holds the
  load bound but does not guarantee coverage.

## Two implementation decisions worth knowing

- **Payload delegation.** The engine stores no payloads. It keeps only delivered
  *ids* for loop-avoidance and delegates payload storage and re-supply to the
  registered broadcast handler, exactly as Partisan's Plumtree engine does. When
  repair must re-supply a specific missing message the engine emits a
  `{fetch, Peer, MsgId, Tree, Load}` action, which the shell resolves through the
  handler's `graft/1`. Re-supply is therefore per-missing-id — never a dump of a
  tree's whole history — and no engine structure grows with the payload volume.
- **Bounded announcements.** Because `announcements` doubles as the repair
  server-directory, the paper's drop-on-receipt pruning would starve repair here.
  Instead each announcement carries a rounds-left age, refreshed on re-advertisement
  and dropped when it expires, bounding the set to a sliding window.

## Purity and testing

Every event returns `{state(), [action()]}`, where an action is `{send, Peer, Msg}`,
`{deliver, MsgId, Payload}` or `{fetch, Peer, MsgId, Tree, Load}`; the engine never
performs I/O. That makes the whole protocol exercisable in a deterministic
simulation — `prop_partisan_thicket_engine`, the primary validator, since an
off-by-default engine is invisible to `partisan_SUITE`.

## Configuration

Configure via `new/3` `Opts`, following the paper's guidance (§4.6 and the §5.1
experimental setup), which is also the regime the property model validates:

- **`max_load`** (default 7) — the ceiling on how many trees a node may be interior
  in. Treat it as a generous safety ceiling, not a tight budget: repair settles most
  nodes to be interior in a single tree, and `max_load` only catches the few that
  must do more for coverage. Keep it comfortably above the tree count (the paper runs
  `max_load = 7` with 5 trees). **Not recommended:** `max_load = 1` with more than one
  tree.
- **`fanout`** (default 5) — the number of children a node branches to when building
  a tree.
- **`trees`** (default 1) — the number of trees `T`, used to size the degree-budget
  reservations. Bound it by the fanout (`T ≤ f`): with fanout `f` only about `f`
  interior-node-disjoint trees fit over one overlay, so asking for more forces nodes
  to forward for several trees (bounded by `max_load`). The paper uses `T = f = 5`.

The overlay should give each node a degree on the order of `f × T`, so it has enough
neighbours to play its role in every tree (the paper uses degree 25 for `f = T = 5`).
`new/3` rejects clearly-invalid values (`max_load < 1`, `fanout < 2`, `trees < 1`).

## Reading guide

The EVENTS section — `broadcast/3`, `handle/2`, `tick/1` — is the whole protocol at a
glance; each dispatches into the PRIVATE section, whose functions run construction →
forwarding → reconfiguration → repair → peer-set bookkeeping, in that order. `new/3`
and the record it builds define the state everything else operates on.
""".

-type tree() :: node().
-type msg_id() :: term().
-type nodeset() :: ordsets:ordset(node()).
%% Piggybacked load: for each tree, this sender's forwarding fanout.
-type load() :: #{tree() => non_neg_integer()}.
-type action() ::
    {send, node(), Msg :: term()}
    | {deliver, msg_id(), Payload :: term()}
    %% ask the shell to re-supply MsgId (via the handler's graft/1) to Peer on
    %% Tree — the engine holds no payloads, so recovery goes through the handler.
    %% `Load' is our piggybacked load map to stamp on the re-supplied data, so the
    %% shell need not reach back into engine state to build the message.
    | {fetch, Peer :: node(), msg_id(), tree(), load()}.

-record(thicket, {
    node :: node(),
    max_load :: pos_integer(),
    fanout :: pos_integer(),
    %% number of trees (broadcast groups) the deployment runs — a known Thicket
    %% protocol parameter. Used to reserve degree budget so a node keeps enough
    %% backup peers to become a leaf in every tree it does not yet know about.
    n_trees = 1 :: pos_integer(),
    %% t.activePeers: neighbours used to receive/forward in each tree.
    active = #{} :: #{tree() => nodeset()},
    %% backupPeers: neighbours used in no tree.
    backup = ordsets:new() :: nodeset(),
    %% announcements: {MsgId, Tree, Sender, Age} learned from summaries, for
    %% repair. `Age' is a rounds-left TTL: each tick decrements it and drops
    %% expired entries, so the set is bounded by a sliding window rather than
    %% growing for all time. Re-advertisement (a repeated summary) refreshes the
    %% age, so an entry lives while its message is actively advertised and for a
    %% window after — long enough to keep serving as the repair server-directory
    %% (this engine's stand-in for the paper's separate directory), which is why
    %% the paper's `removeMuid' (drop on receipt) cannot be applied verbatim here.
    announcements = [] :: [
        {msg_id(), tree(), node(), Age :: non_neg_integer()}
    ],
    %% receivedMsgs (paper): delivered message ids, tagged with their tree (the
    %% source). Loop-avoidance + announcement filtering only — NO payloads are
    %% held here; the handler owns payload storage (see @doc, the `fetch' action).
    received = #{} :: #{msg_id() => tree()},
    %% recently received ids to advertise in summaries, each with a rounds-left
    %% TTL so a message is re-advertised over several summary rounds (the paper's
    %% Summary is periodic over a recent window, not one-shot).
    unsummarised = [] :: [{msg_id(), tree(), non_neg_integer()}],
    %% loadEstimate(peer, tree): most recent forwarding load of a neighbour.
    load_est = #{} :: #{{node(), tree()} => non_neg_integer()},
    %% per-tree repair countdown (ticks until repair fires); absent = disarmed.
    repair = #{} :: #{tree() => non_neg_integer()},
    %% per-tree set of servers that recently rejected our repair graft, so we try
    %% a different announced server next; cleared for a tree once we (re)connect.
    rejected = #{} :: #{tree() => nodeset()}
}).

-type state() :: #thicket{}.
-export_type([state/0, action/0]).

%% lifecycle / events
-export([new/2]).
-export([new/3]).
-export([broadcast/3]).
-export([handle/2]).
-export([tick/1]).
-export([neighbor_up/2]).
-export([neighbor_down/2]).
%% queries (for tests / debug)
-export([interior_load/1]).
-export([is_interior/2]).
-export([delivered/1]).
-export([active_peers/2]).
-export([backup_peers/1]).
-export([own_load_map/1]).
-export([announcement_count/1]).
%% broadcast-engine adapter (ADR-000004 seam; raw dispatch mode)
-export([dispatch_mode/0]).
-export([init/1]).
-export([update_members/2]).
-export([broadcast/4]).
-export([handle_message/2]).
-export([repair_tick/1]).
-export([get_peers/2]).
-export([all_eager_peers/2]).
-export([all_lazy_peers/2]).
-export([all_members/1]).

-define(REPAIR_TICKS, 3).
%% Rounds a received message is re-advertised in summaries, so repair converges
%% regardless of delivery ordering (a single advertisement can miss a peer that
%% was momentarily an active rather than a backup neighbour). Wide enough that a
%% multi-hop repair chain can complete within one message's advertisement window.
-define(SUMMARY_TTL, 8).
%% Rounds an announcement is retained after its last (re-)advertisement before it
%% is garbage-collected (the paper's age-based purging, ref [12]). Kept comfortably
%% longer than a repair cycle (?REPAIR_TICKS) with retries so the repair
%% server-directory an announcement provides survives long enough to reconnect a
%% node, while still bounding `announcements' to a sliding window.
-define(ANNOUNCE_TTL, 10).

%% =============================================================================
%% LIFECYCLE
%% =============================================================================

-spec new(node(), [node()]) -> state().

new(Node, Members) ->
    new(Node, Members, #{}).

%% @doc Create engine state for `Node' over the given overlay `Members'. `Opts'
%% keys `max_load' (default 7), `fanout' (default 5) and `trees' (default 1) are
%% documented, with their recommended values, in the module header. Clearly-invalid
%% values are rejected with a `badarg' error.
-spec new(node(), [node()], map()) -> state().

new(Node, Members, Opts) ->
    MaxLoad = maps:get(max_load, Opts, 7),
    Fanout = maps:get(fanout, Opts, 5),
    Trees = maps:get(trees, Opts, 1),
    ok = validate_opts(MaxLoad, Fanout, Trees),
    Backup = ordsets:del_element(Node, ordsets:from_list(Members)),
    #thicket{
        node = Node,
        max_load = MaxLoad,
        fanout = Fanout,
        n_trees = Trees,
        backup = Backup
    }.

%% @private Reject configurations that break the engine's assumptions. The T =< f
%% and max_load >= 2 recommendations are documented rather than enforced here, so
%% that the property model can still exercise the interior-load bound at the
%% `max_load = 1' extreme; only structurally-invalid values are refused.
validate_opts(MaxLoad, Fanout, Trees) ->
    (is_integer(MaxLoad) andalso MaxLoad >= 1) orelse
        erlang:error({badarg, {max_load, MaxLoad}}),
    (is_integer(Fanout) andalso Fanout >= 2) orelse
        erlang:error({badarg, {fanout, Fanout}}),
    (is_integer(Trees) andalso Trees >= 1) orelse
        erlang:error({badarg, {trees, Trees}}),
    ok.

%% =============================================================================
%% EVENTS
%% =============================================================================

%% @doc Originate a broadcast: this node is the source (tree = self).
-spec broadcast(msg_id(), term(), state()) -> {state(), [action()]}.

broadcast(MsgId, Payload, #thicket{node = Node} = S0) ->
    Tree = Node,
    %% A source is unavoidably interior in its own tree. If we are already at the
    %% cap from relaying OTHER trees, free a slot first so sourcing does not push
    %% interior-load past max_load; shed peers repair via the normal mechanism.
    {S1, ShedActions} = make_room_for_source(Tree, S0),
    S2 = ensure_source_branch(Tree, S1),
    S3 = remember(MsgId, Tree, S2),
    Fwd = forward(MsgId, Payload, Tree, Node, S3),
    %% The source delivers locally too, so its handler holds the payload and can
    %% re-supply the tree on a later graft (the engine caches nothing).
    {S3, [{deliver, MsgId, Payload} | ShedActions ++ Fwd]}.

%% @doc Handle an inbound Thicket message.
-spec handle(term(), state()) -> {state(), [action()]}.

handle({data, MsgId, Payload, Tree, Load, Sender}, S0) ->
    S1 = update_load_est(Sender, Load, S0),
    case is_received(MsgId, S1) of
        true ->
            %% duplicate: prune the redundant eager link back to Sender
            S2 = to_backup(Sender, Tree, S1),
            {S2, [
                {send, Sender, {prune, own_load_map(S2), Tree, S1#thicket.node}}
            ]};
        false ->
            %% novel: deliver + record, settle the repair timer for this tree, and
            %% forget rejections now that we are receiving on this tree again.
            S2 = remember(MsgId, Tree, S1),
            S3 = clear_rejected(Tree, settle_repair(Tree, S2)),
            Deliver = {deliver, MsgId, Payload},
            {S6, Actions} = adopt_and_forward(
                MsgId, Payload, Tree, Load, Sender, S3
            ),
            {S6, [Deliver | Actions]}
    end;
handle({prune, Load, Tree, Sender}, S0) ->
    S1 = update_load_est(Sender, Load, S0),
    S2 = to_backup(Sender, Tree, S1),
    %% If this Prune is a graft rejection that leaves us disconnected from the
    %% tree, remember the rejecting server (so we pick a different one) and
    %% immediately re-attempt repair (paper §4.3: "move st back to backupPeers and
    %% attempt to repair t by picking new targets"). `graft_best' decides whether a
    %% target exists — an announced server or the tree's source as a fallback — and
    %% re-arms the timer if none is reachable yet, so we do not gate on there being
    %% an explicit announcement. A Prune that merely trims a redundant link (we
    %% still have a parent/children) needs no repair.
    case ordsets:size(active_peers(Tree, S2)) =:= 0 of
        true -> graft_best(Tree, mark_rejected(Sender, Tree, S2));
        false -> {S2, []}
    end;
handle({summary, Ids, Load, Sender}, S0) ->
    %% Ids :: [{MsgId, Tree}]. Record announcements + arm repair for missing.
    %% Summaries repeat (?SUMMARY_TTL), so an identical (Id,Tree,Sender)
    %% announcement is de-duplicated and its age refreshed rather than appended.
    S1 = update_load_est(Sender, Load, S0),
    Missing = [{Id, T} || {Id, T} <- Ids, not is_received(Id, S1)],
    S2 = lists:foldl(
        fun({Id, T}, Acc) ->
            Ann = refresh_announcement(
                Id, T, Sender, Acc#thicket.announcements
            ),
            arm_repair(T, Acc#thicket{announcements = Ann})
        end,
        S1,
        Missing
    ),
    {S2, []};
handle({graft, Missing, Load, Tree, Sender}, S0) ->
    %% A peer wants to (re)join tree via us, and lists in `Missing' the ids it has
    %% heard announced but not received. Accept iff we can do so without exceeding
    %% max_load or becoming interior in a new tree, AND without exhausting our
    %% degree budget on two fronts:
    %%   (1) a per-TREE child cap (2*fanout): no single tree may absorb our whole
    %%       neighbour set as a star; excess grafters attach deeper in the tree
    %%       instead. Without it a popular node (typically a source) balloons one
    %%       tree, drains its backup, and can never join the trees it is missing.
    %%   (2) a global reserve of one backup peer per OTHER tree we know about, so
    %%       we can still become a leaf in each of them.
    %% Only serve a tree we are actually IN (a leaf or interior member) — we must
    %% be able to re-supply its messages. This also makes speculative repair grafts
    %% (see graft_best) safe: a probe to a peer not in the tree is cleanly rejected
    %% rather than accepted by a node that has nothing to give. Tree growth to new
    %% nodes happens via `tree_branching' on the data path, not by grafting a node
    %% into a tree it has never seen.
    S1 = update_load_est(Sender, Load, S0),
    Reserve = max(0, max(known_trees(S1), S1#thicket.n_trees) - 1),
    ChildCap = 2 * S1#thicket.fanout,
    CanServe =
        %% Serve any neighbour not already our peer in this tree — including one we
        %% already use in ANOTHER tree (§4.4 link-reassignment): that reuse is what
        %% lets a node walled off from a tree (its only server-neighbour busy) still
        %% be reached. The interior-load bound is enforced by the load gate below,
        %% independently of whether the link was idle.
        available_for(Sender, Tree, S1) andalso
            in_tree(Tree, S1) andalso
            (is_interior(Tree, S1) orelse
                interior_load(S1) < S1#thicket.max_load) andalso
            ordsets:size(S1#thicket.backup) > Reserve andalso
            ordsets:size(active_peers(Tree, S1)) < ChildCap,
    case CanServe of
        true ->
            S2 = to_active(Sender, Tree, S1),
            %% Re-supply, via the handler, only the SPECIFIC ids the grafter named
            %% and that we actually hold — one `fetch' per id, never a dump of the
            %% tree's history. Ids the grafter has not yet heard of it requests on a
            %% later repair, once our summaries have advertised them.
            Load1 = own_load_map(S2),
            Resupply = [
                {fetch, Sender, Id, Tree, Load1}
             || Id <- Missing, is_received(Id, S2)
            ],
            {S2, Resupply};
        false ->
            {S1, [
                {send, Sender, {prune, own_load_map(S1), Tree, S1#thicket.node}}
            ]}
    end.

%% @doc Periodic tick: fire due repairs, then (if under load) send summaries.
-spec tick(state()) -> {state(), [action()]}.

tick(S0) ->
    {S1, RepairActions} = fire_repairs(S0),
    {S2, SummaryActions} = maybe_summarise(S1),
    %% Age announcements AFTER repair/summary so this tick still sees the full
    %% directory; expired entries are dropped, bounding the set to a window.
    S3 = age_announcements(S2),
    {S3, RepairActions ++ SummaryActions}.

%% @doc Overlay: a neighbour became available.
-spec neighbor_up(node(), state()) -> state().

neighbor_up(Node, #thicket{backup = B} = S) ->
    S#thicket{backup = ordsets:add_element(Node, B)}.

%% @doc Overlay: a neighbour failed/left. Repair recovers any lost tree.
-spec neighbor_down(Node :: node(), state()) -> state().

neighbor_down(Node, S0) ->
    Active = maps:map(
        fun(_T, Peers) -> ordsets:del_element(Node, Peers) end,
        S0#thicket.active
    ),
    Ann = [
        A
     || {_, _, Sndr, _Age} = A <- S0#thicket.announcements, Sndr =/= Node
    ],
    %% Purge every remaining reference to the failed node so nothing accumulates
    %% across up/down churn and no stale estimate or rejection outlives it: drop
    %% its per-tree load estimates and remove it from every tree's rejected set.
    %% A node that returns re-advertises its load on its next message and is no
    %% longer pre-emptively excluded from repair.
    LoadEst = maps:filter(
        fun({P, _T}, _N) -> P =/= Node end, S0#thicket.load_est
    ),
    Rejected = maps:map(
        fun(_T, Set) -> ordsets:del_element(Node, Set) end, S0#thicket.rejected
    ),
    S0#thicket{
        active = Active,
        backup = ordsets:del_element(Node, S0#thicket.backup),
        announcements = Ann,
        load_est = LoadEst,
        rejected = Rejected
    }.

%% =============================================================================
%% QUERIES
%% =============================================================================

%% @doc Number of trees this node is interior in = |{t : |active(t)| > 1}|.
-spec interior_load(state()) -> non_neg_integer().

interior_load(#thicket{active = Active}) ->
    maps:fold(
        fun(_T, Peers, Acc) ->
            case ordsets:size(Peers) > 1 of
                true -> Acc + 1;
                false -> Acc
            end
        end,
        0,
        Active
    ).

-spec is_interior(tree(), state()) -> boolean().

is_interior(Tree, #thicket{active = Active}) ->
    ordsets:size(maps:get(Tree, Active, ordsets:new())) > 1.

%% @private Are we a member (leaf or interior) of this tree — i.e. do we hold a
%% live link in it, and hence its messages to re-supply? The source is always a
%% member of its own tree.
in_tree(Tree, #thicket{node = Node} = S) ->
    Tree =:= Node orelse ordsets:size(active_peers(Tree, S)) > 0.

-spec delivered(state()) -> [msg_id()].

delivered(#thicket{received = R}) ->
    maps:keys(R).

-spec active_peers(tree(), state()) -> nodeset().

active_peers(Tree, #thicket{active = Active}) ->
    maps:get(Tree, Active, ordsets:new()).

-spec backup_peers(state()) -> nodeset().

backup_peers(#thicket{backup = B}) ->
    B.

%% @doc Number of live announcements retained (for tests/debug). Bounded by the
%% age-based GC window, so it does not grow with the total message count.
-spec announcement_count(state()) -> non_neg_integer().

announcement_count(#thicket{announcements = Ann}) ->
    length(Ann).

%% @doc This node's per-tree forwarding load (|active(t)| - 1), for piggybacking.
-spec own_load_map(state()) -> load().

own_load_map(#thicket{active = Active}) ->
    maps:map(fun(_T, Peers) -> max(0, ordsets:size(Peers) - 1) end, Active).

%% =============================================================================
%% BROADCAST-ENGINE ADAPTER (ADR-000004 seam)
%%
%% Thicket runs behind {@link partisan_plumtree_broadcast} in <b>raw dispatch</b>
%% mode: the shell hands the engine whole wire messages (`handle_message/2') and
%% ticks (`repair_tick/1') rather than the Plumtree-shaped typed callbacks, and
%% executes the `{send | deliver | fetch, ...}' actions this engine returns. A raw
%% group hosts a SINGLE handler module, which the shell supplies for `deliver'
%% (store/apply) and `fetch' (re-supply via `graft/1'), so the engine itself stays
%% Mod-agnostic and pure. These wrappers adapt the engine's native API to that
%% contract; the protocol lives in the EVENTS section above.
%% =============================================================================

%% @doc Tell the shell to drive this engine by raw wire messages, not the
%% Plumtree-shaped typed callbacks. (Absent ⇒ the shell assumes `typed'.)
-spec dispatch_mode() -> raw.

dispatch_mode() ->
    raw.

%% @doc Build initial state from the shell's `Opts' (carries `members' plus the
%% `max_load'/`fanout'/`trees' documented in the module header).
-spec init(map()) -> state().

init(Opts) ->
    new(partisan:node(), maps:get(members, Opts, []), Opts).

%% @doc Membership changed: add joiners as backup neighbours, drop leavers.
-spec update_members([node()], state()) -> {state(), [action()]}.

update_members(Members, #thicket{node = Self} = S0) ->
    Want = ordsets:del_element(Self, ordsets:from_list(Members)),
    Have = neighbours(S0),
    Joiners = ordsets:subtract(Want, Have),
    Leavers = ordsets:subtract(Have, Want),
    S1 = lists:foldl(fun neighbor_up/2, S0, ordsets:to_list(Joiners)),
    S2 = lists:foldl(fun neighbor_down/2, S1, ordsets:to_list(Leavers)),
    {S2, []}.

%% @doc Originate a broadcast. The group's single handler `Mod' is irrelevant to
%% the pure engine (the shell owns handler dispatch), so it is ignored here.
-spec broadcast(msg_id(), term(), module(), state()) -> {state(), [action()]}.

broadcast(MsgId, Payload, _Mod, S) ->
    broadcast(MsgId, Payload, S).

%% @doc Handle an inbound wire message (the shell's raw dispatch path).
-spec handle_message(term(), state()) -> {state(), [action()]}.

handle_message(Msg, S) ->
    handle(Msg, S).

%% @doc Periodic repair/summary tick (the shell's raw dispatch path).
-spec repair_tick(state()) -> {state(), [action()]}.

repair_tick(S) ->
    tick(S).

%% @doc Debug query: {tree peers, spare peers} for `Root' — active peers in the
%% tree and the idle backup set, the nearest Thicket analogue of eager/lazy.
-spec get_peers(tree(), state()) -> {nodeset(), nodeset()}.

get_peers(Root, S) ->
    {active_peers(Root, S), backup_peers(S)}.

-spec all_eager_peers(tree(), state()) -> nodeset().

all_eager_peers(Root, S) ->
    active_peers(Root, S).

-spec all_lazy_peers(tree(), state()) -> nodeset().

all_lazy_peers(_Root, S) ->
    backup_peers(S).

%% @doc Debug query: all overlay members this node knows (self + every neighbour).
-spec all_members(state()) -> nodeset().

all_members(#thicket{node = Self} = S) ->
    ordsets:add_element(Self, neighbours(S)).

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private Source of a tree: move f peers into the tree as its first children.
%% Prefers idle (backup) links but reuses links already carrying another tree when
%% backup is thin (§4.4) — a LATE source, emitting after earlier trees have drained
%% everyone's backup, would otherwise branch to almost no one and its tree would
%% never span the overlay. Reuse is load-safe: the source is interior only in its
%% own tree, and each reused child joins this tree as a leaf.
ensure_source_branch(Tree, #thicket{active = Active} = S) ->
    case maps:get(Tree, Active, undefined) of
        undefined ->
            Chosen = pick_children(S#thicket.fanout, Tree, S),
            Peers = ordsets:from_list(Chosen),
            S#thicket{
                active = Active#{Tree => Peers},
                backup = ordsets:subtract(S#thicket.backup, Peers)
            };
        _ ->
            S
    end.

%% @private Ensure there is interior-load headroom for this node's own source
%% tree. If we are not yet interior in `Tree' and already at the cap, shed one
%% other interior tree (detach its active peers to backup and Prune them); the
%% detached peers reconnect through repair. Returns any Prune actions to emit.
make_room_for_source(Tree, S) ->
    case is_interior(Tree, S) orelse interior_load(S) < S#thicket.max_load of
        true ->
            {S, []};
        false ->
            case shed_candidate(Tree, S) of
                {ok, Shed} ->
                    Peers = active_peers(Shed, S),
                    S1 = lists:foldl(
                        fun(P, Acc) -> to_backup(P, Shed, Acc) end, S, Peers
                    ),
                    Prunes = [
                        {send, P,
                            {prune, own_load_map(S1), Shed, S1#thicket.node}}
                     || P <- Peers
                    ],
                    {S1, Prunes};
                error ->
                    {S, []}
            end
    end.

%% @private An interior tree to shed (any tree other than Tree we forward in).
shed_candidate(Tree, #thicket{active = Active}) ->
    Interior = [
        T
     || {T, Peers} <- maps:to_list(Active),
        T =/= Tree,
        ordsets:size(Peers) > 1
    ],
    case lists:sort(Interior) of
        [T | _] -> {ok, T};
        [] -> error
    end.

%% @private Novel-DATA tree attachment (paper Algorithm 2, lines 16-22). The
%% load-critical rule is the adoption guard: we adopt `Sender' as our tree link
%% ONLY when not yet connected to this tree (`active(Tree)' empty). Adopting a
%% second parent is what would make an already-connected node newly interior;
%% guarding adoption on `not Connected' keeps interior-load bounded. We adopt
%% `Sender' even if we already use it in ANOTHER tree (§4.4 link-reassignment):
%% requiring it to be an idle backup peer would strand a tree whose data arrives
%% over a link already carrying a different tree — exactly what happens to a late
%% source once earlier trees have drained everyone's backup. Reuse is load-safe
%% here because we only add ourselves as a LEAF (`active(Tree)' goes 0 -> 1).
%% Branching (load-capped), forwarding to `active∖Sender', and the balance
%% reconfiguration always run on a novel message, so coverage propagates regardless.
adopt_and_forward(MsgId, Payload, Tree, Load, Sender, S0) ->
    Connected = ordsets:size(active_peers(Tree, S0)) > 0,
    S1 =
        case (not Connected) andalso is_neighbour(Sender, S0) of
            true -> to_active(Sender, Tree, S0);
            false -> S0
        end,
    S2 = tree_branching(Tree, S1),
    Fwd = forward(MsgId, Payload, Tree, Sender, S2),
    {S3, Bal} = balance(Tree, Load, Sender, S2),
    {S3, Fwd ++ Bal}.

%% @private Interior tree branching (paper Algorithm 2, line 33): become interior
%% (add up to f-1 children) while under the interior-load cap (`interior_load <
%% max_load'), so branching can never push us past `max_load' (the bound is
%% preserved by construction: `< max_load' before ⇒ `=< max_load' after).
%%
%% The paper's disjoint-tree heuristic branches only at `interior_load == 0', which
%% keeps most nodes interior in a single tree — ideal in its regime (`T =< f', where
%% interior-node-disjoint trees actually fit). But when `T > f' disjoint trees are
%% impossible: some nodes MUST be interior in several trees for the later trees to
%% span the overlay at all, and a `== 0' guard strands them as shallow stars (a
%% late source reaches only its immediate children, none of whom — already interior
%% elsewhere — will extend it). Gating on the cap instead lets a tree grow into the
%% headroom `max_load' already permits, which is the §4.4 spirit: use spare interior
%% capacity to keep every tree connected, never exceeding the bound.
tree_branching(Tree, S) ->
    case interior_load(S) < S#thicket.max_load of
        false ->
            S;
        true ->
            Peers0 = maps:get(Tree, S#thicket.active, ordsets:new()),
            Chosen = ordsets:from_list(
                pick_children(S#thicket.fanout - 1, Tree, S)
            ),
            Peers = ordsets:union(Peers0, Chosen),
            S#thicket{
                active = (S#thicket.active)#{Tree => Peers},
                backup = ordsets:subtract(S#thicket.backup, Chosen)
            }
    end.

%% @private Pick up to `Need' children for `Tree' from this node's neighbours,
%% excluding any already active in `Tree'. Idle (backup) links are used first; a
%% link already carrying another tree is reused only when there are not enough idle
%% ones (§4.4 link-reassignment). Reuse lets a tree keep branching once backup is
%% drained; it is load-safe because a child joins as a leaf and the branching node
%% is gated to become interior in at most one tree (see tree_branching).
pick_children(Need, _Tree, _S) when Need =< 0 ->
    [];
pick_children(Need, Tree, S) ->
    InTree = active_peers(Tree, S),
    Idle = [
        P
     || P <- ordsets:to_list(S#thicket.backup),
        not ordsets:is_element(P, InTree)
    ],
    case length(Idle) >= Need of
        true ->
            lists:sublist(Idle, Need);
        false ->
            Reuse = [
                P
             || P <- ordsets:to_list(neighbours(S)),
                not ordsets:is_element(P, InTree),
                not lists:member(P, Idle)
            ],
            lists:sublist(Idle ++ Reuse, Need)
    end.

%% @private Forward DATA to active(tree) except Sender, piggybacking load.
forward(MsgId, Payload, Tree, Sender, S) ->
    Peers = ordsets:del_element(Sender, active_peers(Tree, S)),
    Load = own_load_map(S),
    Self = S#thicket.node,
    [{send, P, {data, MsgId, Payload, Tree, Load, Self}} || P <- Peers].

%% @private Balance / tree reconfiguration (paper §4.4, Algorithm 3 lines 18-23).
%% On a NOVEL message from our current parent `Sender' in `Tree', if we hold an
%% earlier announcement for `Tree' from a graftable peer `A' whose interior-tree
%% count AFTER taking this tree (`IncTreeLoad') would be strictly less than
%% Sender's current interior count, swap our parent from Sender to A: prune Sender
%% and graft A. The swap keeps our own `active(Tree)' size unchanged, so we never
%% become interior in more trees (the paper's "without becoming interior in more
%% trees" condition holds by construction). Because the announcement is already
%% recorded when this data arrives, the paper's "announcement received before the
%% data" ordering is satisfied. Repeated over the message stream this drives the
%% system toward most nodes being interior in a single tree and, by moving
%% forwarding duty onto lower-load peers, spreads interior roles more widely.
balance(Tree, SenderLoad, Sender, S) ->
    IsParent = ordsets:is_element(Sender, active_peers(Tree, S)),
    case IsParent andalso balance_target(Tree, Sender, S) of
        {ok, A} ->
            %% nInterior(IncTreeLoad(loadEstimate_A, Tree)): A's interior count if
            %% it took this tree — its current count, plus one iff not already
            %% interior in Tree.
            IncA =
                case load_est_in(A, Tree, S) > 0 of
                    true -> 0;
                    false -> 1
                end,
            case (est_total(A, S) + IncA) < n_interior(SenderLoad) of
                true ->
                    S1 = to_active(A, Tree, to_backup(Sender, Tree, S)),
                    Self = S1#thicket.node,
                    {S1, [
                        {send, Sender, {prune, own_load_map(S1), Tree, Self}},
                        {send, A,
                            {graft, missing_ids(Tree, S1), own_load_map(S1),
                                Tree, Self}}
                    ]};
                false ->
                    {S, []}
            end;
        _ ->
            {S, []}
    end.

%% @private A reconfiguration target for Tree: an announced peer we can graft —
%% in our backup, not the current parent, not a recent rejecter, and — critically —
%% the announcement must be for a message we are STILL MISSING (`not is_received').
%%
%% That last guard is the paper's §4.4 precondition: an announcement for a
%% currently-missing message, recorded before this data, proves `A' is a live
%% UPSTREAM alternative, which is what avoids grafting a descendant (a cycle).
%% Enforcing it makes reconfiguration sound but stops it from churning links off
%% stale announcements, so spreading does not rest on Balance. That job belongs
%% to repair (summary-, source-fallback- and speculative-graft; see graft_best)
%% and to §4.4 link-reassignment; ADR-000004 carries the coverage analysis.
balance_target(Tree, Sender, S) ->
    Rej = maps:get(Tree, S#thicket.rejected, ordsets:new()),
    Cands = [
        A
     || {Id, T, A, _Age} <- S#thicket.announcements,
        T =:= Tree,
        not is_received(Id, S),
        A =/= Sender,
        ordsets:is_element(A, S#thicket.backup),
        not ordsets:is_element(A, Rej)
    ],
    case Cands of
        [A | _] -> {ok, A};
        [] -> error
    end.

%% @private Fire repair timers that reached zero and still have missing msgs:
%% graft the best announcer for the tree.
fire_repairs(S0) ->
    Due = [T || {T, 0} <- maps:to_list(S0#thicket.repair)],
    lists:foldl(
        fun(Tree, {S, Acc}) ->
            {S1, As} = graft_best(Tree, S),
            {S1, As ++ Acc}
        end,
        {countdown_repairs(S0), []},
        Due
    ).

%% @private Select the best announced target for a tree and graft it, applying
%% the paper's `removeBest' (the chosen announcement is consumed so a rejecting
%% peer is not re-picked on retry). If no eligible target exists yet, re-arm the
%% repair timer so a later summary/tick can retry — never silently give up.
%%
%% This fires only when the repair timer expires, i.e. we have been MISSING a
%% message for the tree — so any active peer we currently hold is stale (it did
%% not supply the message; typically we are in a partitioned component whose peers
%% are equally cut off). We therefore graft a fresh parent even when active(Tree)
%% is non-empty. To preserve the interior-load bound, if adding the fresh parent
%% would make us newly interior (active grows 1 -> 2) while already at max_load,
%% we first detach one stale peer — a swap, not an addition. When disconnected
%% (active empty) the graft simply makes us a leaf.
graft_best(Tree, S0) ->
    case best_announcer(Tree, S0) of
        {ok, P} ->
            do_graft(P, Tree, S0);
        error ->
            %% No KNOWN server (no announcement, nothing in load_est). If we are
            %% disconnected from the tree, speculatively probe a backup peer we
            %% have not just tried: if it is in the tree it serves and re-supplies
            %% us; otherwise its `in_tree' guard rejects and we mark it and try
            %% another next round. This is how a disconnected node discovers a
            %% nearby server it was never told about — recovering, soundly, the
            %% coverage spreading that the (currently-missing-gated) balance no
            %% longer provides. We only probe while disconnected, so we never trade
            %% a live parent for a likely-rejecting guess.
            case
                ordsets:size(active_peers(Tree, S0)) =:= 0 andalso
                    speculative_target(Tree, S0)
            of
                {ok, P} -> do_graft(P, Tree, S0);
                _ -> {rearm_repair(Tree, clear_rejected(Tree, S0)), []}
            end
    end.

%% @private Graft `P' as a (re)connection to `Tree'. If we already hold exactly
%% one active peer it is our stale parent (the repair timer only fires because it
%% stopped delivering), so SWAP — detach it and graft `P'. This keeps us a leaf
%% (no interior-load growth) and leaves us disconnected if the graft is rejected,
%% so the Prune handler retries another server instead of stranding us behind a
%% dead link. When disconnected the graft simply makes us a leaf. We do NOT
%% consume `P''s announcement (unlike the paper's `removeBest'): periodic
%% summaries keep the set bounded and re-filtering on load/backup avoids stranding.
do_graft(P, Tree, S0) ->
    Cur = active_peers(Tree, S0),
    {S1, Prunes} =
        case ordsets:size(Cur) =:= 1 of
            true ->
                [Victim | _] = Cur,
                {to_backup(Victim, Tree, S0), [
                    {send, Victim,
                        {prune, own_load_map(S0), Tree, S0#thicket.node}}
                ]};
            false ->
                {S0, []}
        end,
    S2 = to_active(P, Tree, S1),
    S3 = disarm_repair(Tree, S2),
    Graft =
        {send, P,
            {graft, missing_ids(Tree, S3), own_load_map(S3), Tree,
                S3#thicket.node}},
    {S3, Prunes ++ [Graft]}.

%% @private A neighbour we have not recently had rejected for this tree, to probe
%% speculatively when no known server exists. Prefer an IDLE (backup) link first,
%% so we only reuse a link already carrying another tree (§4.4 reassignment) when
%% no idle one is available — reuse is the fallback, not the default.
speculative_target(Tree, S) ->
    Rej = maps:get(Tree, S#thicket.rejected, ordsets:new()),
    Ok = fun(P) ->
        available_for(P, Tree, S) andalso not ordsets:is_element(P, Rej)
    end,
    case [P || P <- ordsets:to_list(S#thicket.backup), Ok(P)] of
        [P | _] ->
            {ok, P};
        [] ->
            case [P || P <- ordsets:to_list(neighbours(S)), Ok(P)] of
                [P | _] -> {ok, P};
                [] -> error
            end
    end.

%% @private Every tick, decrement armed repair countdowns (min 0).
countdown_repairs(#thicket{repair = R} = S) ->
    S#thicket{repair = maps:map(fun(_T, N) -> max(0, N - 1) end, R)}.

%% @private Periodic summary: advertise recently-received messages to backup
%% peers so they can repair trees they are missing. Each message is advertised
%% for ?SUMMARY_TTL rounds (the paper's Summary re-covers a recent window), so a
%% peer that missed one advertisement still learns of it on a later round — this
%% is what makes repair converge under any delivery ordering.
%%
%% Note: unlike the paper we do NOT silence a node once its interior-load reaches
%% max_load. A saturated node is interior in some tree yet a leaf in others, and
%% must still advertise those others or repair starves at tight caps. Grafts are
%% already steered toward spare capacity by the {@link best_announcer} selection,
%% so the summary throttle was redundant for load-balancing and harmful for
%% coverage.
maybe_summarise(#thicket{unsummarised = []} = S) ->
    {S, []};
maybe_summarise(S) ->
    U = S#thicket.unsummarised,
    Ids = [{Id, T} || {Id, T, _Ttl} <- U],
    Load = own_load_map(S),
    Self = S#thicket.node,
    Actions = [
        {send, P, {summary, Ids, Load, Self}}
     || P <- S#thicket.backup
    ],
    U1 = [{Id, T, Ttl - 1} || {Id, T, Ttl} <- U, Ttl > 1],
    {S#thicket{unsummarised = U1}, Actions}.

%% @private
arm_repair(Tree, #thicket{repair = R} = S) ->
    case maps:is_key(Tree, R) of
        true -> S;
        false -> S#thicket{repair = R#{Tree => ?REPAIR_TICKS}}
    end.

disarm_repair(Tree, #thicket{repair = R} = S) ->
    S#thicket{repair = maps:remove(Tree, R)}.

%% @private After receiving on `Tree', disarm repair only if we have no remaining
%% GAP — an id announced for the tree that we still have not received. If a gap
%% remains (we attached via a later message but missed an earlier one), keep repair
%% armed so it fetches the gap; otherwise a per-id resupply never recovers an early
%% message once a later one has arrived, because that arrival would disarm repair.
settle_repair(Tree, S) ->
    case missing_ids(Tree, S) of
        [] -> disarm_repair(Tree, S);
        _ -> arm_repair(Tree, S)
    end.

%% @private receiving a message confirms (re)connection to the tree, so forget
%% which servers rejected us — they may have spare capacity for a future repair.
clear_rejected(Tree, #thicket{rejected = Rej} = S) ->
    S#thicket{rejected = maps:remove(Tree, Rej)}.

%% @private force the repair countdown back to the full interval (retry later).
rearm_repair(Tree, #thicket{repair = R} = S) ->
    S#thicket{repair = R#{Tree => ?REPAIR_TICKS}}.

%% @private remember that Sender rejected our graft for Tree (avoid re-picking it).
mark_rejected(Sender, Tree, #thicket{rejected = Rej} = S) ->
    Set = ordsets:add_element(Sender, maps:get(Tree, Rej, ordsets:new())),
    S#thicket{rejected = Rej#{Tree => Set}}.

%% @private Best announced repair target for a tree (paper §4.3 selection): a
%% backup peer that either (a) is interior in fewer than max_load trees, so it
%% can take on this tree as new interior duty, OR (b) is already interior in this
%% tree, so serving us adds no new interior tree even at max_load. Omitting (b)
%% starves repair of the one server always able to help — most sharply the tree's
%% own source, which sits at load=1 in its tree and would otherwise be filtered
%% out under a tight cap. Ties broken toward the least-loaded candidate.
best_announcer(Tree, S) ->
    ML = S#thicket.max_load,
    Rej = maps:get(Tree, S#thicket.rejected, ordsets:new()),
    Announced = [
        Sndr
     || {_Id, T, Sndr, _Age} <- S#thicket.announcements, T =:= Tree
    ],
    %% Candidate servers come from three sources, in preference order handled by
    %% the load sort below:
    %%   * announced peers — a peer that told us it holds a message we are missing;
    %%   * backup peers the piggybacked `load_est' shows are already interior in
    %%     this tree — a persistent, bounded server directory that does NOT vanish
    %%     when announcements are pruned on receipt (paper `removeMuid'), so repair
    %%     keeps distributing across intermediate nodes instead of collapsing onto
    %%     the source;
    %%   * the tree's source itself (the tree id IS its source node), a standing
    %%     fallback that can always serve its own tree — this is what stops a node
    %%     being stranded when every other lead has a busy link.
    %% The per-tree child cap keeps the source/any node from becoming a star, and
    %% the load ordering prefers lower-load servers over the source.
    FromLoad = [
        P
     || P <- ordsets:to_list(neighbours(S)), load_est_in(P, Tree, S) > 0
    ],
    Cands = [
        C
     || C <- lists:usort([Tree | Announced ++ FromLoad]),
        C =/= S#thicket.node,
        available_for(C, Tree, S),
        not ordsets:is_element(C, Rej),
        (C =:= Tree orelse est_total(C, S) < ML orelse
            load_est_in(C, Tree, S) > 0)
    ],
    case
        lists:sort(fun(A, B) -> est_total(A, S) =< est_total(B, S) end, Cands)
    of
        [Best | _] -> {ok, Best};
        [] -> error
    end.

%% @private estimated forwarding load of a peer in a specific tree (0 if unknown).
load_est_in(Peer, Tree, #thicket{load_est = LE}) ->
    maps:get({Peer, Tree}, LE, 0).

%% @private count of distinct trees this node is aware of (participates in or has
%% heard announcements for) — used to size the degree-budget reserve.
known_trees(#thicket{active = Active, announcements = Ann}) ->
    length(lists:usort(maps:keys(Active) ++ [T || {_Id, T, _S, _Age} <- Ann])).

%% @private
to_active(Node, Tree, #thicket{active = Active, backup = Backup} = S) ->
    Peers = ordsets:add_element(Node, maps:get(Tree, Active, ordsets:new())),
    S#thicket{
        active = Active#{Tree => Peers},
        backup = ordsets:del_element(Node, Backup)
    }.

%% @private Detach `Node' from `Tree'. It returns to the global backup set (used in
%% no tree) ONLY if it is not still active in some OTHER tree — because a link may
%% serve several trees at once (trees are interior-node-DISJOINT, not edge-disjoint;
%% §4.4 link-reassignment), the backup set must keep meaning "in no tree" or its
%% membership check would wrongly free a link that is still carrying another tree.
to_backup(Node, Tree, #thicket{active = Active, backup = Backup} = S) ->
    Peers = ordsets:del_element(Node, maps:get(Tree, Active, ordsets:new())),
    Active1 = Active#{Tree => Peers},
    Backup1 =
        case active_in_some_tree(Node, Active1) of
            true -> Backup;
            false -> ordsets:add_element(Node, Backup)
        end,
    S#thicket{active = Active1, backup = Backup1}.

%% @private Is `Node' an active peer in any tree of `Active'?
active_in_some_tree(Node, Active) ->
    maps:fold(
        fun
            (_T, _Peers, true) -> true;
            (_T, Peers, false) -> ordsets:is_element(Node, Peers)
        end,
        false,
        Active
    ).

%% @private All current overlay neighbours: those in backup (no tree) plus those
%% active in at least one tree. A link-reassigning repair may reuse any of these,
%% not only the idle (backup) ones.
neighbours(#thicket{backup = Backup, active = Active}) ->
    maps:fold(
        fun(_T, Peers, Acc) -> ordsets:union(Acc, Peers) end, Backup, Active
    ).

%% @private Is `Peer' a current overlay neighbour (idle or serving some tree)?
is_neighbour(Peer, S) ->
    ordsets:is_element(Peer, neighbours(S)).

%% @private A neighbour is available to (re)join `Tree' via us if it is not already
%% our active peer there — whether it is idle (backup) or already serving another
%% tree. This is the §4.4 relaxation that lets a busy link be reused for a tree a
%% node would otherwise be walled off from; the interior-load bound is preserved
%% separately by the load gate at each graft, not by this membership test.
available_for(Peer, Tree, S) ->
    is_neighbour(Peer, S) andalso
        not ordsets:is_element(Peer, active_peers(Tree, S)).

%% @private record a delivered message id (no payload — the handler stores that),
%% tag it with its tree, and (re)seed its summary TTL.
remember(MsgId, Tree, #thicket{received = R, unsummarised = U} = S) ->
    S#thicket{
        received = R#{MsgId => Tree},
        unsummarised = [
            {MsgId, Tree, ?SUMMARY_TTL} | lists:keydelete(MsgId, 1, U)
        ]
    }.

%% @private Ids this node has heard announced for `Tree' but not yet received —
%% what a repair graft asks the server to re-supply. Bounded by what we are
%% actually missing (received ids are filtered out), so a graft never triggers a
%% dump of the tree's whole history.
missing_ids(Tree, S) ->
    lists:usort([
        Id
     || {Id, T, _Sndr, _Age} <- S#thicket.announcements,
        T =:= Tree,
        not is_received(Id, S)
    ]).

%% @private Add or refresh an announcement `{Id, Tree, Sender}', (re)setting its
%% age to the full TTL. Any prior entry for the same (Id, Tree, Sender) is dropped
%% first, so a repeated summary refreshes the age instead of appending a duplicate.
refresh_announcement(Id, Tree, Sender, Ann) ->
    Others = [
        E
     || {I, T, Sn, _Age} = E <- Ann,
        not (I =:= Id andalso T =:= Tree andalso Sn =:= Sender)
    ],
    [{Id, Tree, Sender, ?ANNOUNCE_TTL} | Others].

%% @private Decrement every announcement's age by one and drop those that reach
%% zero (age-based garbage collection, paper ref [12]). This is what bounds the
%% `announcements' set to a sliding window; the directory role it plays for repair
%% survives for ?ANNOUNCE_TTL rounds after the last summary that advertised it.
age_announcements(#thicket{announcements = Ann} = S) ->
    Ann1 = [{Id, T, Sndr, Age - 1} || {Id, T, Sndr, Age} <- Ann, Age > 1],
    S#thicket{announcements = Ann1}.

%% @private
is_received(MsgId, #thicket{received = R}) ->
    maps:is_key(MsgId, R).

%% @private update the load estimate for a peer from a piggybacked load map.
update_load_est(Peer, Load, #thicket{load_est = LE} = S) when is_map(Load) ->
    LE1 = maps:fold(
        fun(Tree, N, Acc) -> Acc#{{Peer, Tree} => N} end, LE, Load
    ),
    S#thicket{load_est = LE1};
update_load_est(_Peer, _Load, S) ->
    S.

%% @private total interior-tree count estimate for a peer.
est_total(Peer, #thicket{load_est = LE}) ->
    maps:fold(
        fun
            ({P, _T}, N, Acc) when P =:= Peer, N > 0 -> Acc + 1;
            (_, _, Acc) -> Acc
        end,
        0,
        LE
    ).

%% @private number of trees a piggybacked load map marks as interior.
n_interior(Load) when is_map(Load) ->
    maps:fold(
        fun
            (_T, N, Acc) when N > 0 -> Acc + 1;
            (_, _, Acc) -> Acc
        end,
        0,
        Load
    );
n_interior(_) ->
    0.
