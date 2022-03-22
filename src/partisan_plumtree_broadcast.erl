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
-module(partisan_plumtree_broadcast).

-behaviour(gen_server).

%% API
-export([start_link/0,
         start_link/5,
         broadcast/2,
         update/1,
         broadcast_members/0,
         broadcast_members/1,
         exchanges/0,
         exchanges/1,
         cancel_exchanges/1]).

%% Debug API
-export([debug_get_peers/2,
         debug_get_peers/3,
         debug_get_tree/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("partisan.hrl").

-define(SERVER, ?MODULE).

-type nodename()        :: any().
-type message_id()      :: any().
-type message_round()   :: non_neg_integer().
-type outstanding()     :: {message_id(), module(), message_round(), nodename()}.
-type exchange()        :: {module(), node(), reference(), pid()}.
-type exchanges()       :: [exchange()].

-record(state, {
          %% Initially trees rooted at each node are the same.
          %% Portions of that tree belonging to this node are
          %% shared in this set.
          common_eagers :: ordsets:ordset(nodename()) | undefined,

          %% Initially trees rooted at each node share the same lazy links.
          %% Typically this set will contain a single element. However, it may
          %% contain more in large clusters and may be empty for clusters with
          %% less than three nodes.
          common_lazys  :: ordsets:ordset(nodename()) | undefined,

          %% A mapping of sender node (root of each broadcast tree)
          %% to this node's portion of the tree. Elements are
          %% added to this structure as messages rooted at a node
          %% propagate to this node. Nodes that are never the
          %% root of a message will never have a key added to
          %% `eager_sets'
          eager_sets    :: [{nodename(), ordsets:ordset(nodename())}] | undefined,

          %% A Mapping of sender node (root of each spanning tree)
          %% to this node's set of lazy peers. Elements are added
          %% to this structure as messages rooted at a node
          %% propagate to this node. Nodes that are never the root
          %% of a message will never have a key added to `lazy_sets'
          lazy_sets     :: [{nodename(), ordsets:ordset(nodename())}] | undefined,

          %% Lazy messages that have not been acked. Messages are added to
          %% this set when a node is sent a lazy message (or when it should be
          %% sent one sometime in the future). Messages are removed when the lazy
          %% pushes are acknowledged via graft or ignores. Entries are keyed by their
          %% destination
          outstanding   :: [{nodename(), outstanding()}],

          %% Set of registered modules that may handle messages that
          %% have been broadcast
          mods          :: [module()],

          %% List of outstanding exchanges
          exchanges     :: exchanges(),

          %% Set of all known members. Used to determine
          %% which members have joined and left during a membership update
          all_members   :: ordsets:ordset(nodename()) | undefined,

          %% Lazy tick period in milliseconds. On every tick all outstanding
          %% lazy pushes are sent out
          lazy_tick_period :: non_neg_integer(),

          %% Exchange tick period in milliseconds that may or may not occur
          exchange_tick_period :: non_neg_integer()

         }).
-type state()           :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the broadcast server on this node. The initial membership list is
%% fetched from the ring. If the node is a singleton then the initial eager and lazy
%% sets are empty. If there are two nodes, each will be in the others eager set and the
%% lazy sets will be empty. When number of members is less than 5, each node will initially
%% have one other node in its eager set and lazy set. If there are more than five nodes
%% each node will have at most two other nodes in its eager set and one in its lazy set, initially.
%% In addition, after the broadcast server is started, a callback is registered with ring_events
%% to generate membership updates as the ring changes.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    LazyTickPeriod = partisan_config:get(lazy_tick_period, ?DEFAULT_LAZY_TICK_PERIOD),
    ExchangeTickPeriod = partisan_config:get(exchange_tick_period, ?DEFAULT_EXCHANGE_TICK_PERIOD),
    {ok, Members} = partisan_peer_service:members(),
    ?UTIL:log(info, "peer sampling service members: ~p", [Members]),
    %% the peer service has already sampled the members, we start off
    %% with pure gossip (ie. all members are in the eager push list and lazy
    %% list is empty)
    InitEagers = Members,
    InitLazys = [],
    ?UTIL:log(debug, "init peers, eager: ~p, lazy: ~p",
                      [InitEagers, InitLazys]),
    Mods = partisan_config:get(broadcast_mods, []),
    Res = start_link(Members, InitEagers, InitLazys, Mods,
                     [{lazy_tick_period, LazyTickPeriod},
                      {exchange_tick_period, ExchangeTickPeriod}]),
    partisan_peer_service:add_sup_callback(fun ?MODULE:update/1),
    Res.

%% @doc Starts the broadcast server on this node. `InitMembers' must be a list
%% of all members known to this node when starting the broadcast server.
%% `InitEagers' are the initial peers of this node for all broadcast trees.
%% `InitLazys' is a list of random peers not in `InitEagers' that will be used
%% as the initial lazy peer shared by all trees for this node. If the number
%% of nodes in the cluster is less than 3, `InitLazys' should be an empty list.
%% `InitEagers' and `InitLazys' must also be subsets of `InitMembers'. `Mods' is
%% a list of modules that may be handlers for broadcasted messages. All modules in
%% `Mods' should implement the `partisan_plumtree_broadcast_handler' behaviour.
%% `Opts' is a proplist with the following possible options:
%%      Flush all outstanding lazy pushes period (in milliseconds)
%%          {`lazy_tick_period', non_neg_integer()}
%%      Possibly perform an exchange period (in milliseconds)
%%          {`exchange_tick_period', non_neg_integer()}
%%
%% NOTE: When starting the server using start_link/2 no automatic membership update from
%% ring_events is registered. Use start_link/0.
-spec start_link([nodename()], [nodename()], [nodename()], [module()],
                 proplists:proplist()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(InitMembers, InitEagers, InitLazys, Mods, Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE,
                          [InitMembers, InitEagers, InitLazys, Mods, Opts], []).

%% @doc Broadcasts a message originating from this node. The message will be delivered to
%% each node at least once. The `Mod' passed is responsible for handling the message on remote
%% nodes as well as providing some other information both locally and and on other nodes.
%% `Mod' must be loaded on all members of the clusters and implement the
%% `riak_core_broadcast_handler' behaviour.
-spec broadcast(any(), module()) -> ok.
broadcast(Broadcast, Mod) ->
    {MessageId, Payload} = Mod:broadcast_data(Broadcast),
    gen_server:cast(?SERVER, {broadcast, MessageId, Payload, Mod}).

%% @doc Notifies broadcast server of membership update
update(LocalState0) ->
    LocalState = partisan_peer_service:decode(LocalState0),
    gen_server:cast(?SERVER, {update, LocalState}).

%% @doc Returns the broadcast servers view of full cluster membership.
%% Wait indefinitely for a response is returned from the process
-spec broadcast_members() -> ordsets:ordset(nodename()).
broadcast_members() ->
    broadcast_members(infinity).

%% @doc Returns the broadcast servers view of full cluster membership.
%% Waits `Timeout' ms for a response from the server
-spec broadcast_members(infinity | pos_integer()) -> ordsets:ordset(nodename()).
broadcast_members(Timeout) ->
    gen_server:call(?SERVER, broadcast_members, Timeout).

%% @doc return a list of exchanges, started by broadcast on thisnode, that are running
-spec exchanges() -> exchanges().
exchanges() ->
    exchanges(myself()).

%% @doc returns a list of exchanges, started by broadcast on `Node', that are running
%% -spec exchanges(node()) -> exchanges().
exchanges(Node) ->
    gen_server:call({?SERVER, Node}, exchanges, infinity).

%% @doc cancel exchanges started by this node.
-spec cancel_exchanges(all              |
                       {peer, node()}   |
                       {mod, module()}  |
                       reference()      |
                       pid()) -> exchanges().
cancel_exchanges(WhichExchanges) ->
    gen_server:call(?SERVER, {cancel_exchanges, WhichExchanges}, infinity).

%%%===================================================================
%%% Debug API
%%%===================================================================

%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Wait indefinitely for a response is returned from the process
-spec debug_get_peers(node(), node()) -> {ordsets:ordset(node()), ordsets:ordset(node())}.
debug_get_peers(Node, Root) ->
    debug_get_peers(Node, Root, infinity).

%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Waits `Timeout' ms for a response from the server
-spec debug_get_peers(node(), node(), infinity | pos_integer()) ->
                             {ordsets:ordset(node()), ordsets:ordset(node())}.
debug_get_peers(Node, Root, Timeout) ->
    gen_server:call({?SERVER, Node}, {get_peers, Root}, Timeout).

%% @doc return peers for all `Nodes' for tree rooted at `Root'
%% Wait indefinitely for a response is returned from the process
-spec debug_get_tree(node(), [node()]) ->
                            [{node(), {ordsets:ordset(node()), ordsets:ordset(node())}}].
debug_get_tree(Root, Nodes) ->
    [begin
         Peers = try debug_get_peers(Node, Root)
                 catch _:_ -> down
                 end,
         {Node, Peers}
     end || Node <- Nodes].

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([[any()], ...]) -> {ok, state()}.
init([AllMembers, InitEagers, InitLazys, Mods, Opts]) ->
    LazyTickPeriod = proplists:get_value(lazy_tick_period, Opts),
    ExchangeTickPeriod = proplists:get_value(exchange_tick_period, Opts),
    schedule_lazy_tick(LazyTickPeriod),
    schedule_exchange_tick(ExchangeTickPeriod),
    State1 =  #state{
                 outstanding = orddict:new(),
                 mods = lists:usort(Mods),
                 exchanges=[],
                 lazy_tick_period = LazyTickPeriod,
                 exchange_tick_period = ExchangeTickPeriod
                },
    State2 = reset_peers(AllMembers, InitEagers, InitLazys, State1),
    {ok, State2}.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.
handle_call({get_peers, Root}, _From, State) ->
    EagerPeers = all_peers(Root, State#state.eager_sets, State#state.common_eagers),
    LazyPeers = all_peers(Root, State#state.lazy_sets, State#state.common_lazys),
    {reply, {EagerPeers, LazyPeers}, State};
handle_call(broadcast_members, _From, State=#state{all_members=AllMembers}) ->
    {reply, AllMembers, State};
handle_call(exchanges, _From, State=#state{exchanges=Exchanges}) ->
    {reply, Exchanges, State};
handle_call({cancel_exchanges, WhichExchanges}, _From, State) ->
    Cancelled = cancel_exchanges(WhichExchanges, State#state.exchanges),
    {reply, Cancelled, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({broadcast, MessageId, Message, Mod}, State) ->
    ?UTIL:log(debug, "received {broadcast, ~p, Msg, ~p}",
                      [MessageId, Mod]),
    State1 = eager_push(MessageId, Message, Mod, State),
    State2 = schedule_lazy_push(MessageId, Mod, State1),
    {noreply, State2};
handle_cast({broadcast, MessageId, Message, Mod, Round, Root, From}, State) ->
    ?UTIL:log(debug, "received {broadcast, ~p, Msg, ~p, ~p, ~p, ~p}",
                      [MessageId, Mod, Round, Root, From]),
    Valid = Mod:merge(MessageId, Message),
    State1 = handle_broadcast(Valid, MessageId, Message, Mod, Round, Root, From, State),
    {noreply, State1};
handle_cast({prune, Root, From}, State) ->
    ?UTIL:log(debug, "received ~p", [{prune, Root, From}]),
    ?UTIL:log(debug, "moving peer ~p from eager to lazy", [From]),
    State1 = add_lazy(From, Root, State),
    {noreply, State1};
handle_cast({i_have, MessageId, Mod, Round, Root, From}, State) ->
    ?UTIL:log(debug, "received ~p", [{i_have, MessageId, Mod, Round, Root, From}]),
    Stale = Mod:is_stale(MessageId),
    State1 = handle_ihave(Stale, MessageId, Mod, Round, Root, From, State),
    {noreply, State1};
handle_cast({ignored_i_have, MessageId, Mod, Round, Root, From}, State) ->
    ?UTIL:log(debug, "received ~p", [{ignored_i_have, MessageId, Mod, Round, Root, From}]),
    State1 = ack_outstanding(MessageId, Mod, Round, Root, From, State),
    {noreply, State1};
handle_cast({graft, MessageId, Mod, Round, Root, From}, State) ->
    ?UTIL:log(debug, "received ~p", [{graft, MessageId, Mod, Round, Root, From}]),
    Result = Mod:graft(MessageId),
    ?UTIL:log(debug, "graft(~p): ~p", [MessageId, Result]),
    State1 = handle_graft(Result, MessageId, Mod, Round, Root, From, State),
    {noreply, State1};
handle_cast({update, Members}, State=#state{all_members=BroadcastMembers,
                                            common_eagers=EagerPeers0,
                                            common_lazys=LazyPeers}) ->
    ?UTIL:log(debug, "received ~p", [{update, Members}]),
    CurrentMembers = ordsets:from_list(Members),
    New = ordsets:subtract(CurrentMembers, BroadcastMembers),
    Removed = ordsets:subtract(BroadcastMembers, CurrentMembers),
    ?UTIL:log(debug, "    new members: ~p", [ordsets:to_list(New)]),
    ?UTIL:log(debug, "    removed members: ~p", [ordsets:to_list(Removed)]),
    State1 = case ordsets:size(New) > 0 of
                 false ->
                     State;
                 true ->
                     %% as per the paper (page 9):
                     %% "When a new member is detected, it is simply added to the set
                     %%  of eagerPushPeers"
                     EagerPeers = ordsets:union(EagerPeers0, New),
                     ?UTIL:log(debug, "    new peers, eager: ~p, lazy: ~p",
                                       [EagerPeers, LazyPeers]),
                     reset_peers(CurrentMembers, EagerPeers, LazyPeers, State)
             end,
    State2 = neighbors_down(Removed, State1),
    {noreply, State2}.

%% @private
-spec handle_info('exchange_tick' | 'lazy_tick' | {'DOWN', _, 'process', _, _}, state()) ->
    {noreply, state()}.
handle_info(lazy_tick,
            #state{lazy_tick_period = Period} = State) ->
    _ = send_lazy(State),
    schedule_lazy_tick(Period),
    {noreply, State};
handle_info(exchange_tick,
            #state{exchange_tick_period = Period} = State) ->
    State1 = maybe_exchange(State),
    schedule_exchange_tick(Period),
    {noreply, State1};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, State=#state{exchanges=Exchanges}) ->
    Exchanges1 = lists:keydelete(Ref, 3, Exchanges),
    {noreply, State#state{exchanges=Exchanges1}}.

%% @private
-spec terminate(term(), state()) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
handle_broadcast(false, _MessageId, _Message, Mod, _Round, Root, From, State) -> %% stale msg
    %% remove sender from eager and set as lazy
    ?UTIL:log(debug, "moving peer ~p from eager to lazy", [From]),
    State1 = add_lazy(From, Root, State),
    _ = send({prune, Root, myself()}, Mod, From),
    State1;
handle_broadcast(true, MessageId, Message, Mod, Round, Root, From, State) -> %% valid msg
    %% remove sender from lazy and set as eager
    State1 = add_eager(From, Root, State),
    State2 = eager_push(MessageId, Message, Mod, Round+1, Root, From, State1),
    schedule_lazy_push(MessageId, Mod, Round+1, Root, From, State2).

handle_ihave(true, MessageId, Mod, Round, Root, From, State) -> %% stale i_have
    _ = send({ignored_i_have, MessageId, Mod, Round, Root, myself()}, Mod, From),
    State;
handle_ihave(false, MessageId, Mod, Round, Root, From, State) -> %% valid i_have
    %% TODO: don't graft immediately
    _ = send({graft, MessageId, Mod, Round, Root, myself()}, Mod, From),
    add_eager(From, Root, State).

handle_graft(stale, MessageId, Mod, Round, Root, From, State) ->
    %% There has been a subsequent broadcast that is causally newer than this message
    %% according to Mod. We ack the outstanding message since the outstanding entry
    %% for the newer message exists
    ack_outstanding(MessageId, Mod, Round, Root, From, State);
handle_graft({ok, Message}, MessageId, Mod, Round, Root, From, State) ->
    %% we don't ack outstanding here because the broadcast may fail to be delivered
    %% instead we will allow the i_have to be sent once more and let the subsequent
    %% ignore serve as the ack.
    State1 = add_eager(From, Root, State),
    _ = send({broadcast, MessageId, Message, Mod, Round, Root, myself()}, Mod, From),
    State1;
handle_graft({error, Reason}, _MessageId, Mod, _Round, _Root, _From, State) ->
    lager:error("unable to graft message from ~p. reason: ~p", [Mod, Reason]),
    State.

neighbors_down(Removed, State=#state{common_eagers=CommonEagers, eager_sets=EagerSets,
                                     common_lazys=CommonLazys, lazy_sets=LazySets,
                                     outstanding=Outstanding}) ->
    NewCommonEagers = ordsets:subtract(CommonEagers, Removed),
    NewCommonLazys  = ordsets:subtract(CommonLazys, Removed),
    %% TODO: once we have delayed grafting need to remove timers
    NewEagerSets = ordsets:from_list([{Root, ordsets:subtract(Existing, Removed)} ||
                                         {Root, Existing} <- ordsets:to_list(EagerSets)]),
    NewLazySets  = ordsets:from_list([{Root, ordsets:subtract(Existing, Removed)} ||
                                         {Root, Existing} <- ordsets:to_list(LazySets)]),
    %% delete outstanding messages to removed peers
    NewOutstanding = ordsets:fold(fun(RPeer, OutstandingAcc) ->
                                          orddict:erase(RPeer, OutstandingAcc)
                                  end,
                                  Outstanding, Removed),
    State#state{common_eagers=NewCommonEagers,
                common_lazys=NewCommonLazys,
                eager_sets=NewEagerSets,
                lazy_sets=NewLazySets,
                outstanding=NewOutstanding}.

eager_push(MessageId, Message, Mod, State) ->
    eager_push(MessageId, Message, Mod, 0, myself(), myself(), State).

eager_push(MessageId, Message, Mod, Round, Root, From, State) ->
    Peers = eager_peers(Root, From, State),
    ?UTIL:log(debug, "eager push to peers: ~p", [Peers]),
    _ = send({broadcast, MessageId, Message, Mod, Round, Root, myself()}, Mod, Peers),
    State.

schedule_lazy_push(MessageId, Mod, State) ->
    schedule_lazy_push(MessageId, Mod, 0, myself(), myself(), State).

schedule_lazy_push(MessageId, Mod, Round, Root, From, State) ->
    Peers = lazy_peers(Root, From, State),
    ?UTIL:log(debug, "scheduling lazy push to peers ~p: ~p",
               [Peers, {MessageId, Mod, Round, Root, From}]),
    add_all_outstanding(MessageId, Mod, Round, Root, Peers, State).

send_lazy(#state{outstanding=Outstanding}) ->
    [send_lazy(Peer, Messages) || {Peer, Messages} <- orddict:to_list(Outstanding)].

send_lazy(Peer, Messages) ->
    [send_lazy(MessageId, Mod, Round, Root, Peer) ||
        {MessageId, Mod, Round, Root} <- ordsets:to_list(Messages)].

send_lazy(MessageId, Mod, Round, Root, Peer) ->
    ?UTIL:log(debug, "sending lazy push ~p",
               [{i_have, MessageId, Mod, Round, Root, myself()}]),
    send({i_have, MessageId, Mod, Round, Root, myself()}, Mod, Peer).

maybe_exchange(State) ->
    Root = random_root(State),
    Peer = random_peer(Root, State),
    maybe_exchange(Peer, State).

maybe_exchange(undefined, State) ->
    State;
maybe_exchange(Peer, State=#state{mods=[Mod | _], exchanges=Exchanges}) ->
    %% limit the number of exchanges this node can start concurrently.
    %% the exchange must (currently?) implement any "inbound" concurrency limits
    ExchangeLimit = partisan_config:get(broadcast_start_exchange_limit, 1),
    BelowLimit = not (length(Exchanges) >= ExchangeLimit),
    FreeMod = lists:keyfind(Mod, 1, Exchanges) =:= false,
    case BelowLimit and FreeMod of
        true -> exchange(Peer, State);
        false -> State
    end;
maybe_exchange(_Peer, State=#state{mods=[]}) ->
    %% No registered handler.
    State.

exchange(Peer, State=#state{mods=[Mod | Mods], exchanges=Exchanges}) ->
    State1 = case Mod:exchange(Peer) of
                 {ok, Pid} ->
                     ?UTIL:log(debug, "started ~p exchange with ~p (~p)", [Mod, Peer, Pid]),
                     Ref = monitor(process, Pid),
                     State#state{exchanges=[{Mod, Peer, Ref, Pid} | Exchanges]};
                 {error, _Reason} ->
                     State
             end,
    State1#state{mods=Mods ++ [Mod]}.

cancel_exchanges(all, Exchanges) ->
    kill_exchanges(Exchanges);
cancel_exchanges(WhichProc, Exchanges) when is_reference(WhichProc) orelse
                                            is_pid(WhichProc) ->
    KeyPos = case is_reference(WhichProc) of
              true -> 3;
              false -> 4
          end,
    case lists:keyfind(WhichProc, KeyPos, Exchanges) of
        false -> [];
        Exchange ->
            kill_exchange(Exchange),
            [Exchange]
    end;
cancel_exchanges(Which, Exchanges) ->
    Filter = exchange_filter(Which),
    ToCancel = [Ex || Ex <- Exchanges, Filter(Ex)],
    kill_exchanges(ToCancel).

kill_exchanges(Exchanges) ->
    _ = [kill_exchange(Exchange) || Exchange <- Exchanges],
    Exchanges.

kill_exchange({_, _, _, ExchangePid}) ->
    exit(ExchangePid, cancel_exchange).


exchange_filter({peer, Peer}) ->
    fun({_, ExchangePeer, _, _}) ->
            Peer =:= ExchangePeer
    end;
exchange_filter({mod, Mod}) ->
    fun({ExchangeMod, _, _, _}) ->
            Mod =:= ExchangeMod
    end.

%% picks random root uniformly
random_root(#state{all_members=Members}) ->
    random_other_node(Members).

%% picks random peer favoring peers not in eager or lazy set and ensuring
%% peer is not this node
random_peer(Root, State=#state{all_members=All}) ->
    Mode = partisan_config:get(exchange_selection, optimized),
    Other = case Mode of
        %% Normal; randomly select a peer from the known membership at
        %% this node.
        normal ->
            ordsets:del_element(myself(), All);
        %% Optimized; attempt to find a peer that's not in the broadcast
        %% tree, to increase probability of selecting a lagging node.
        optimized ->
            Eagers = all_eager_peers(Root, State),
            Lazys  = all_lazy_peers(Root, State),
            Union  = ordsets:union([Eagers, Lazys]),
            ordsets:del_element(myself(), ordsets:subtract(All, Union))
    end,
    Selected = case ordsets:size(Other) of
        0 ->
            random_other_node(ordsets:del_element(myself(), All));
        _ ->
            random_other_node(Other)
    end,
    Selected.

%% picks random node from ordset
random_other_node(OrdSet) ->
    Size = ordsets:size(OrdSet),
    case Size of
        0 -> undefined;
        _ ->
            lists:nth(rand:uniform(Size),
                     ordsets:to_list(OrdSet))
    end.

ack_outstanding(MessageId, Mod, Round, Root, From, State=#state{outstanding=All}) ->
    Existing = existing_outstanding(From, All),
    Updated = set_outstanding(From,
                              ordsets:del_element({MessageId, Mod, Round, Root}, Existing),
                              All),
    State#state{outstanding=Updated}.

add_all_outstanding(MessageId, Mod, Round, Root, Peers, State) ->
    lists:foldl(fun(Peer, SAcc) -> add_outstanding(MessageId, Mod, Round, Root, Peer, SAcc) end,
                State,
                ordsets:to_list(Peers)).

add_outstanding(MessageId, Mod, Round, Root, Peer, State=#state{outstanding=All}) ->
    Existing = existing_outstanding(Peer, All),
    Updated = set_outstanding(Peer,
                              ordsets:add_element({MessageId, Mod, Round, Root}, Existing),
                              All),
    State#state{outstanding=Updated}.

set_outstanding(Peer, Outstanding, All) ->
    case ordsets:size(Outstanding) of
        0 -> orddict:erase(Peer, All);
        _ -> orddict:store(Peer, Outstanding, All)
    end.

existing_outstanding(Peer, All) ->
    case orddict:find(Peer, All) of
        error -> ordsets:new();
        {ok, Outstanding} -> Outstanding
    end.

add_eager(From, Root, State) ->
    update_peers(From, Root, fun ordsets:add_element/2, fun ordsets:del_element/2, State).

add_lazy(From, Root, State) ->
    update_peers(From, Root, fun ordsets:del_element/2, fun ordsets:add_element/2, State).

update_peers(From, Root, EagerUpdate, LazyUpdate, State) ->
    CurrentEagers = all_eager_peers(Root, State),
    CurrentLazys = all_lazy_peers(Root, State),
    NewEagers = EagerUpdate(From, CurrentEagers),
    NewLazys  = LazyUpdate(From, CurrentLazys),
    set_peers(Root, NewEagers, NewLazys, State).

set_peers(Root, Eagers, Lazys, State=#state{eager_sets=EagerSets, lazy_sets=LazySets}) ->
    NewEagers = orddict:store(Root, Eagers, EagerSets),
    NewLazys = orddict:store(Root, Lazys, LazySets),
    State#state{eager_sets=NewEagers, lazy_sets=NewLazys}.

all_eager_peers(Root, State) ->
    all_peers(Root, State#state.eager_sets, State#state.common_eagers).

all_lazy_peers(Root, State) ->
    all_peers(Root, State#state.lazy_sets, State#state.common_lazys).

eager_peers(Root, From, #state{eager_sets=EagerSets, common_eagers=CommonEagers}) ->
    all_filtered_peers(Root, From, EagerSets, CommonEagers).

lazy_peers(Root, From, #state{lazy_sets=LazySets, common_lazys=CommonLazys}) ->
    all_filtered_peers(Root, From, LazySets, CommonLazys).

all_filtered_peers(Root, From, Sets, Common) ->
    All = all_peers(Root, Sets, Common),
    ordsets:del_element(From, All).

all_peers(Root, Sets, Default) ->
    case orddict:find(Root, Sets) of
        error -> Default;
        {ok, Peers} -> Peers
    end.

send(Msg, Mod, Peers) when is_list(Peers) ->
    [send(Msg, Mod, P) || P <- Peers];
send(Msg, Mod, P) ->
    PeerServiceManager = partisan_config:get(partisan_peer_service_manager, ?DEFAULT_PEER_SERVICE_MANAGER),
    instrument_transmission(Msg, Mod),
    PeerServiceManager:cast_message(P, ?SERVER, Msg).
    %% TODO: add debug logging
    %% gen_server:cast({?SERVER, P}, Msg).

schedule_lazy_tick(Period) ->
    schedule_tick(lazy_tick, broadcast_lazy_timer, Period).

schedule_exchange_tick(Period) ->
    schedule_tick(exchange_tick, broadcast_exchange_timer, Period).

schedule_tick(Message, Timer, Default) ->
    TickMs = partisan_config:get(Timer, Default),
    erlang:send_after(TickMs, ?MODULE, Message).

reset_peers(AllMembers, EagerPeers, LazyPeers, State) ->
    State#state{
      common_eagers = ordsets:del_element(myself(), ordsets:from_list(EagerPeers)),
      common_lazys  = ordsets:del_element(myself(), ordsets:from_list(LazyPeers)),
      eager_sets    = orddict:new(),
      lazy_sets     = orddict:new(),
      all_members   = ordsets:from_list(AllMembers)
     }.

%% @private
myself() ->
    partisan_peer_service_manager:myself().

%% @private
instrument_transmission(Message, Mod) ->
    case partisan_config:get(transmission_logging_mfa, undefined) of
        undefined ->
            ok;
        {Module, Function, Args} ->
            ToLog = try
                Mod:extract_log_type_and_payload(Message)
            catch
                _:Error ->
                    lager:info("Couldn't extract log type and payload. Reason ~p", [Error]),
                    []
            end,

            lists:foreach(
                fun({Type, Payload}) ->
                    erlang:apply(Module, Function, Args ++ [Type, Payload])
                end,
                ToLog
            )
    end.
