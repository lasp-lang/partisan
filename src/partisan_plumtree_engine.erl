%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(partisan_plumtree_engine).

-moduledoc """
The Plumtree tree engine — a self-optimising epidemic broadcast that combines an
eager-push spanning tree with lazy-push redundancy (Leitão, Pereira & Rodrigues,
_Epidemic Broadcast Trees_, SRDS 2007). This is broadcast engine #1 behind
`partisan_broadcast_engine`; Thicket is engine #2 (`partisan_thicket_engine`).

## The idea

A pure flood is reliable but wasteful — every node receives every message on every
link. A pure spanning tree is efficient but fragile — one failed interior node cuts
off a whole subtree. Plumtree gets both: it lets a spanning tree *emerge* from an
initial flood and keeps the pruned links as a cheap standby.

Each node partitions its peers, per broadcast source (`Root`), into two sets:

- **eager peers** — the tree links. A message is pushed to them in full, immediately.
- **lazy peers** — the standby links. Instead of the payload, a node sends them an
  `i_have(MessageId)` advertisement.

A node forwards a full message only to its eager peers, so the eager links form the
spanning tree. The tree builds and heals itself with two rules:

- **Prune.** When a node receives a message it has *already seen* over an eager link,
  that link is a redundant second path into the tree. The node demotes the sender to
  lazy (`prune`), removing the duplicate edge.
- **Graft.** A lazy peer's `i_have` names a message the node has not received. If,
  after a short wait, the message still has not arrived over the tree, the node
  `graft`s that peer back to eager and asks for the payload — filling a gap the tree
  left, and re-attaching a subtree orphaned by a failure.

Over a stream of broadcasts the eager set converges to a low-cost spanning tree while
the lazy set stays ready to repair it.

## Per-node state (the `#engine{}` record)

- **`eager_sets` / `lazy_sets` :: `#{Root => nodeset()}`** — the eager and lazy peers
  for each source's tree.
- **`common_eagers` / `common_lazys`** — the default partition a newly-seen root starts
  from (initially all members eager, none lazy), so a first message from a new source
  floods and then self-prunes into a tree.
- **`all_members`** — the current membership, the universe the peer sets are drawn from.
- **`outstanding_tab`** — an ETS table of lazy pushes queued but not yet sent as
  `i_have`; `lazy_tick/1` flushes it. It is owned by the group process and created in
  `init/1`.

## The mechanism, callback by callback

The engine is pure: each callback returns `{engine_state(), [action()]}` and never
sends. The group shell (`partisan_plumtree_broadcast`) computes the handler's verdicts
— is this message novel? is this `i_have` stale? — and executes the returned actions.

- **`broadcast/4`** — originate: eager-push to the tree and schedule lazy pushes.
- **`handle_broadcast/8`** — a message arrived over an eager link. If *novel*, deliver
  it, forward it on down the tree, and schedule lazy pushes; if a *duplicate*, `prune`
  the sender.
- **`handle_ihave/7`** — a lazy `i_have` arrived. If the id is not already held, record
  it and arm a graft timer; if stale, ignore it.
- **`handle_graft/7`** — a peer asks us to re-supply a message and re-join the tree:
  promote it to eager and send the payload the handler returned.
- **`handle_prune/3`** — a peer tells us its eager link to us was redundant: demote it
  to lazy.
- **`lazy_tick/1`** — flush the outstanding lazy pushes as `i_have` summaries.
- **`select_exchange_peer/2`** — pick a peer for a periodic anti-entropy exchange, the
  backstop that heals anything the eager/lazy mechanics missed.

`update_members/2` reconciles the peer sets when membership changes; `neighbors_down`
folds a departed peer out of every tree and lets graft re-attach whatever it carried.

## Where this sits

The engine owns *tree topology and repair* only. The group shell owns the process, the
mailbox, the membership poll, handler dispatch (novelty via `claim`/`merge`, re-supply
via `graft`), the transport, and the ticks. That split is what lets Thicket drop in
behind the same behaviour with no change to the shell (see
`partisan_broadcast_engine`).
""".

-behaviour(partisan_broadcast_engine).

-include("partisan_logger.hrl").

-type nodeset() :: ordsets:ordset(node()).
-type action() :: partisan_broadcast_engine:action().

-record(engine, {
    node :: node(),
    common_eagers :: nodeset(),
    common_lazys :: nodeset(),
    eager_sets :: #{node() := nodeset()},
    lazy_sets :: #{node() := nodeset()},
    all_members :: nodeset(),
    outstanding_tab :: ets:tid()
}).

-type engine() :: #engine{}.

-export([init/1]).
-export([update_members/2]).
-export([broadcast/4]).
-export([handle_broadcast/8]).
-export([handle_ihave/7]).
-export([handle_ignored_ihave/6]).
-export([handle_prune/3]).
-export([handle_graft/7]).
-export([lazy_tick/1]).
-export([select_exchange_peer/2]).
-export([get_peers/2]).
-export([all_eager_peers/2]).
-export([all_lazy_peers/2]).
-export([all_members/1]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

-spec init(map()) -> engine().

init(Opts) ->
    Members = ordsets:from_list(maps:get(members, Opts, [])),
    %% Per-instance outstanding-lazy-push table, owned by the group process.
    Tab = ets:new(plumtree_outstanding, [duplicate_bag]),
    E0 = #engine{
        node = partisan:node(),
        all_members = ordsets:new(),
        common_eagers = ordsets:new(),
        common_lazys = ordsets:new(),
        eager_sets = maps:new(),
        lazy_sets = maps:new(),
        outstanding_tab = Tab
    },
    %% Start off with pure gossip: all members eager, none lazy.
    reset_peers(Members, Members, [], E0).

-spec update_members(nodeset(), engine()) -> {engine(), [action()]}.

update_members(Members, E) ->
    {apply_membership(Members, E), []}.

-spec broadcast(any(), any(), module(), engine()) -> {engine(), [action()]}.

broadcast(MessageId, Message, Mod, E) ->
    Node = E#engine.node,
    {E1, Actions} = eager_push(MessageId, Message, Mod, 0, Node, Node, E),
    E2 = schedule_lazy_push(MessageId, Mod, 0, Node, Node, E1),
    {E2, Actions}.

-spec handle_broadcast(
    boolean(),
    any(),
    any(),
    module(),
    non_neg_integer(),
    node(),
    node(),
    engine()
) -> {engine(), [action()]}.

handle_broadcast(false, _MessageId, _Message, Mod, _Round, Root, From, E) ->
    %% stale msg: demote sender from eager to lazy and prune it
    E1 = add_lazy(From, Root, E),
    {E1, [{send, From, {prune, Root, E#engine.node}, Mod}]};
handle_broadcast(true, MessageId, Message, Mod, Round, Root, From, E) ->
    %% valid msg: promote sender to eager, forward, and schedule lazy pushes
    E1 = add_eager(From, Root, E),
    {E2, Actions} = eager_push(
        MessageId, Message, Mod, Round + 1, Root, From, E1
    ),
    E3 = schedule_lazy_push(MessageId, Mod, Round + 1, Root, From, E2),
    {E3, Actions}.

-spec handle_ihave(
    boolean(), any(), module(), non_neg_integer(), node(), node(), engine()
) -> {engine(), [action()]}.

handle_ihave(true, MessageId, Mod, Round, Root, From, E) ->
    %% stale i_have — ack it
    Msg = {ignored_i_have, MessageId, Mod, Round, Root, E#engine.node},
    {E, [{send, From, Msg, Mod}]};
handle_ihave(false, MessageId, Mod, Round, Root, From, E) ->
    %% valid i_have — graft (TODO: don't graft immediately)
    E1 = add_eager(From, Root, E),
    Msg = {graft, MessageId, Mod, Round, Root, E#engine.node},
    {E1, [{send, From, Msg, Mod}]}.

-spec handle_ignored_ihave(
    any(), module(), non_neg_integer(), node(), node(), engine()
) -> {engine(), [action()]}.

handle_ignored_ihave(MessageId, Mod, Round, Root, From, E) ->
    ok = ack_outstanding(
        E#engine.outstanding_tab, MessageId, Mod, Round, Root, From
    ),
    {E, []}.

-spec handle_prune(node(), node(), engine()) -> {engine(), [action()]}.

handle_prune(Root, From, E) ->
    {add_lazy(From, Root, E), []}.

-spec handle_graft(
    stale | {ok, any()} | {error, any()},
    any(),
    module(),
    non_neg_integer(),
    node(),
    node(),
    engine()
) -> {engine(), [action()]}.

handle_graft(stale, MessageId, Mod, Round, Root, From, E) ->
    %% A causally-newer broadcast exists; the outstanding entry for it will be
    %% acked instead.
    ok = ack_outstanding(
        E#engine.outstanding_tab, MessageId, Mod, Round, Root, From
    ),
    {E, []};
handle_graft({ok, Message}, MessageId, Mod, Round, Root, From, E) ->
    %% Do not ack here: allow the i_have to be sent once more and let the
    %% subsequent ignore serve as the ack.
    E1 = add_eager(From, Root, E),
    Msg = {broadcast, MessageId, Message, Mod, Round, Root, E#engine.node},
    {E1, [{send, From, Msg, Mod}]};
handle_graft({error, Reason}, _MessageId, Mod, _Round, _Root, _From, E) ->
    ?LOG_ERROR(#{
        description => "Unable to graft message",
        callback_mod => Mod,
        reason => Reason
    }),
    {E, []}.

-spec lazy_tick(engine()) -> {engine(), [action()]}.

lazy_tick(E) ->
    Node = E#engine.node,
    %% Fold the outstanding table into `i_have' send actions, preserving the
    %% original per-peer connected-skip behaviour (skip all of a disconnected
    %% peer's messages this tick).
    {_, Rev} = ets:foldl(
        fun({Peer, {MessageId, Mod, Round, Root}}, {Skip, Acc}) ->
            IHave =
                {send, Peer, {i_have, MessageId, Mod, Round, Root, Node}, Mod},
            case Skip of
                {Peer, true} ->
                    {Skip, [IHave | Acc]};
                {Peer, false} ->
                    {Skip, Acc};
                _ ->
                    case partisan:is_connected(Peer) of
                        true -> {{Peer, true}, [IHave | Acc]};
                        false -> {{Peer, false}, Acc}
                    end
            end
        end,
        {undefined, []},
        E#engine.outstanding_tab
    ),
    {E, lists:reverse(Rev)}.

-spec select_exchange_peer(nodeset(), engine()) -> node() | undefined.

select_exchange_peer(Connected, E) ->
    Root = random_root(E, Connected),
    random_peer(Root, E, Connected).

-spec get_peers(node(), engine()) -> {nodeset(), nodeset()}.

get_peers(Root, E) ->
    {all_eager_peers(Root, E), all_lazy_peers(Root, E)}.

-spec all_eager_peers(node(), engine()) -> nodeset().

all_eager_peers(Root, E) ->
    all_peers(Root, E#engine.eager_sets, E#engine.common_eagers).

-spec all_lazy_peers(node(), engine()) -> nodeset().

all_lazy_peers(Root, E) ->
    all_peers(Root, E#engine.lazy_sets, E#engine.common_lazys).

-spec all_members(engine()) -> nodeset().

all_members(E) ->
    E#engine.all_members.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
apply_membership(Members, E) ->
    #engine{
        all_members = AllMembers,
        common_eagers = EagerPeers0,
        common_lazys = LazyPeers
    } = E,

    New = ordsets:subtract(Members, AllMembers),
    Removed = ordsets:subtract(AllMembers, Members),

    E1 =
        case ordsets:size(New) > 0 of
            false ->
                E;
            true ->
                %% Plumtree paper (page 9): a newly-detected member is added to
                %% eagerPushPeers.
                EagerPeers = ordsets:union(EagerPeers0, New),
                reset_peers(Members, EagerPeers, LazyPeers, E)
        end,
    neighbors_down(Removed, E1).

%% @private
neighbors_down(Removed, #engine{} = E) ->
    #engine{
        all_members = AllMembers,
        common_eagers = CommonEagers,
        eager_sets = EagerSets,
        common_lazys = CommonLazys,
        lazy_sets = LazySets
    } = E,

    NewAllMembers = ordsets:subtract(AllMembers, Removed),
    NewCommonEagers = ordsets:subtract(CommonEagers, Removed),
    NewCommonLazys = ordsets:subtract(CommonLazys, Removed),

    NewEagerSets = maps:from_list([
        {Root, ordsets:subtract(Existing, Removed)}
     || {Root, Existing} <- maps:to_list(EagerSets)
    ]),
    NewLazySets = maps:from_list([
        {Root, ordsets:subtract(Existing, Removed)}
     || {Root, Existing} <- maps:to_list(LazySets)
    ]),

    Tab = E#engine.outstanding_tab,
    ok = ordsets:fold(
        fun(Peer, Acc) ->
            _ = ets:delete(Tab, Peer),
            Acc
        end,
        ok,
        Removed
    ),

    E#engine{
        all_members = NewAllMembers,
        common_eagers = NewCommonEagers,
        common_lazys = NewCommonLazys,
        eager_sets = NewEagerSets,
        lazy_sets = NewLazySets
    }.

%% @private
eager_push(MessageId, Message, Mod, Round, Root, From, E) ->
    Peers = eager_peers(Root, From, E),
    Msg = {broadcast, MessageId, Message, Mod, Round, Root, E#engine.node},
    {E, [{send, Peer, Msg, Mod} || Peer <- Peers]}.

%% @private
schedule_lazy_push(MessageId, Mod, Round, Root, From, E) ->
    Peers = lazy_peers(Root, From, E),
    ok = add_all_outstanding(
        E#engine.outstanding_tab, MessageId, Mod, Round, Root, Peers
    ),
    E.

%% @private
random_root(#engine{all_members = Members}, Connected) ->
    random_other_node(Members, Connected).

%% @private
random_peer(Root, #engine{all_members = All} = E, Connected) ->
    Node = E#engine.node,
    Mode = partisan_config:get(exchange_selection, optimized),

    Other =
        case Mode of
            normal ->
                ordsets:del_element(Node, All);
            optimized ->
                Eagers = all_eager_peers(Root, E),
                Lazys = all_lazy_peers(Root, E),
                Union = ordsets:union([Eagers, Lazys]),
                ordsets:del_element(Node, ordsets:subtract(All, Union))
        end,

    case ordsets:size(Other) of
        0 ->
            random_other_node(ordsets:del_element(Node, All), Connected);
        _ ->
            random_other_node(Other, Connected)
    end.

%% @private
random_other_node(OrdSet0, Connected) ->
    OrdSet = ordsets:intersection(OrdSet0, Connected),
    case ordsets:size(OrdSet) of
        0 -> undefined;
        Size -> lists:nth(rand:uniform(Size), ordsets:to_list(OrdSet))
    end.

%% @private
ack_outstanding(Tab, MessageId, Mod, Round, Root, From) ->
    true = ets:delete_object(Tab, {From, {MessageId, Mod, Round, Root}}),
    ok.

%% @private
add_all_outstanding(Tab, MessageId, Mod, Round, Root, Peers) ->
    Message = {MessageId, Mod, Round, Root},
    Objects = [{Peer, Message} || Peer <- ordsets:to_list(Peers)],
    true = ets:insert(Tab, Objects),
    ok.

%% @private
add_eager(From, Root, E) ->
    update_peers(
        From, Root, fun ordsets:add_element/2, fun ordsets:del_element/2, E
    ).

%% @private
add_lazy(From, Root, E) ->
    update_peers(
        From, Root, fun ordsets:del_element/2, fun ordsets:add_element/2, E
    ).

%% @private
update_peers(From, Root, EagerUpdate, LazyUpdate, E) ->
    CurrentEagers = all_eager_peers(Root, E),
    CurrentLazys = all_lazy_peers(Root, E),
    NewEagers = EagerUpdate(From, CurrentEagers),
    NewLazys = LazyUpdate(From, CurrentLazys),
    set_peers(Root, NewEagers, NewLazys, E).

%% @private
set_peers(Root, Eagers, Lazys, #engine{} = E) ->
    #engine{eager_sets = EagerSets, lazy_sets = LazySets} = E,
    E#engine{
        eager_sets = maps:put(Root, Eagers, EagerSets),
        lazy_sets = maps:put(Root, Lazys, LazySets)
    }.

%% @private
eager_peers(Root, From, #engine{} = E) ->
    #engine{eager_sets = EagerSets, common_eagers = CommonEagers} = E,
    all_filtered_peers(Root, From, EagerSets, CommonEagers).

%% @private
lazy_peers(Root, From, #engine{} = E) ->
    #engine{lazy_sets = LazySets, common_lazys = CommonLazys} = E,
    all_filtered_peers(Root, From, LazySets, CommonLazys).

%% @private
all_filtered_peers(Root, From, Sets, Common) ->
    ordsets:del_element(From, all_peers(Root, Sets, Common)).

%% @private
all_peers(Root, Sets, Default) ->
    case maps:find(Root, Sets) of
        {ok, Peers} -> Peers;
        error -> Default
    end.

%% @private
reset_peers(AllMembers, EagerPeers, LazyPeers, #engine{} = E) ->
    ThisNode = partisan:node(),
    E#engine{
        common_eagers = ordsets:del_element(ThisNode, EagerPeers),
        common_lazys = ordsets:del_element(ThisNode, LazyPeers),
        eager_sets = maps:new(),
        lazy_sets = maps:new(),
        all_members = AllMembers
    }.
