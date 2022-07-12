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

-include("partisan.hrl").
-include("partisan_logger.hrl").

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

-define(SERVER, ?MODULE).

-type message_id()      :: any().
-type message_round()   :: non_neg_integer().
%% Lazy messages that have not been acked. Messages are added to
%% this set when a node is sent a lazy message (or when it should be
%% sent one sometime in the future). Messages are removed when the lazy
%% pushes are acknowleged via graft or ignores. Entries are keyed by their
%% destination
%% These are stored in the ?PLUMTREE_OUTSTANDING ets table under using nodename
%% as key.
%% PLUMTREE_OUTSTANDING is created and owned by partisan_sup
-type outstanding()     :: {message_id(), module(), message_round(), node()}.
-type exchange()        :: {module(), node(), reference(), pid()}.
-type exchanges()       :: [exchange()].
-type selector()        ::  all
                            | {peer, node()}
                            | {mod, module()}
                            | reference()
                            | pid().


-record(state, {
    %% Initially trees rooted at each node are the same.
    %% Portions of that tree belonging to this node are
    %% shared in this set.
    common_eagers :: nodeset() | undefined,

    %% Initially trees rooted at each node share the same lazy links.
    %% Typically this set will contain a single element. However, it may
    %% contain more in large clusters and may be empty for clusters with
    %% less than three nodes.
    common_lazys  :: nodeset() | undefined,

    %% A mapping of sender node (root of each broadcast tree)
    %% to this node's portion of the tree. Elements are
    %% added to this structure as messages rooted at a node
    %% propogate to this node. Nodes that are never the
    %% root of a message will never have a key added to
    %% `eager_sets'
    eager_sets    :: #{node() := nodeset()} | undefined,

    %% A Mapping of sender node (root of each spanning tree)
    %% to this node's set of lazy peers. Elements are added
    %% to this structure as messages rooted at a node
    %% propogate to this node. Nodes that are never the root
    %% of a message will never have a key added to `lazy_sets'
    lazy_sets     :: #{node() := nodeset()} | undefined,

    %% Set of registered modules that may handle messages that
    %% have been broadcast
    mods          :: [module()],

    %% List of outstanding exchanges
    exchanges     :: exchanges(),

    %% Set of all known members. Used to determine
    %% which members have joined and left during a membership update
    all_members   :: nodeset() | undefined,

    %% Lazy tick period in milliseconds. On every tick all outstanding
    %% lazy pushes are sent out
    lazy_tick_period :: non_neg_integer(),

    %% Exchange tick period in milliseconds that may or may not occur
    exchange_tick_period :: non_neg_integer()

}).

-type nodeset()         :: ordsets:ordset(node()).

-type state()           :: #state{}.



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Starts the broadcast server on this node. The initial membership list is
%% fetched from the ring. If the node is a singleton then the initial eager and
%% lazy sets are empty. If there are two nodes, each will be in the others
%% eager set and the lazy sets will be empty. When number of members is less
%% than 5, each node will initially have one other node in its eager set and
%% lazy set. If there are more than five nodes each node will have at most two
%% other nodes in its eager set and one in its lazy set, initially.
%% In addition, after the broadcast server is started, a callback is registered
%% with ring_events to generate membership updates as the ring changes.
%% @end
%% -----------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.

start_link() ->
    LazyTickPeriod = partisan_config:get(
        lazy_tick_period, ?DEFAULT_LAZY_TICK_PERIOD
    ),
    ExchangeTickPeriod = partisan_config:get(
        exchange_tick_period, ?DEFAULT_EXCHANGE_TICK_PERIOD
    ),
    Opts = #{
        lazy_tick_period => LazyTickPeriod,
        exchange_tick_period => ExchangeTickPeriod
    },

    {ok, Members} = partisan_peer_service:members(),

    ?LOG_DEBUG("Peer sampling service members: ~p", [Members]),

    %% The peer service has already sampled the members, we start off
    %% with pure gossip (ie. all members are in the eager push list and lazy
    %% list is empty)
    InitEagers = Members,
    InitLazys = [],

    ?LOG_DEBUG("Init peers, eager: ~p, lazy: ~p", [InitEagers, InitLazys]),

    Mods = partisan_config:get(broadcast_mods, []),

    start_link(Members, InitEagers, InitLazys, Mods, Opts).


%% -----------------------------------------------------------------------------
%% @doc Starts the broadcast server on this node. `InitMembers' must be a list
%% of all members known to this node when starting the broadcast server.
%% `InitEagers' are the initial peers of this node for all broadcast trees.
%% `InitLazys' is a list of random peers not in `InitEagers' that will be used
%% as the initial lazy peer shared by all trees for this node. If the number
%% of nodes in the cluster is less than 3, `InitLazys' should be an empty list.
%% `InitEagers' and `InitLazys' must also be subsets of `InitMembers'. `Mods' is
%% a list of modules that may be handlers for broadcasted messages. All modules
%% in `Mods' should implement the `partisan_plumtree_broadcast_handler'
%% behaviour.
%% `Opts' is a proplist or map with the following possible options:
%%  <ul>
%%  <li> `lazy_tick_period :: non_neg_integer()' - Flush all outstanding lazy pushes period (in milliseconds)</li>
%%  <li> `exchange_tick_period :: non_neg_integer()' - Possibly perform an exchange period (in milliseconds)</li>
%% </ul>
%%
%% NOTE: When starting the server using start_link/2 no automatic membership update from
%% ring_events is registered. Use start_link/0.
%% @end
%% -----------------------------------------------------------------------------

-spec start_link(
    InitMembers :: [node()],
    InitEagers :: [node()],
    InitLazys :: [node()],
    Mods :: [module()],
    Opts :: proplists:proplist() | map()) ->
    {ok, pid()} | ignore | {error, term()}.

start_link(InitMembers, InitEagers, InitLazys, Mods, Opts) when is_list(Opts) ->
    start_link(InitMembers, InitEagers, InitLazys, Mods, maps:from_list(Opts));

start_link(InitMembers, InitEagers, InitLazys, Mods, Opts) when is_map(Opts) ->
    StartOpts = [
        {spawn_opt, ?PARALLEL_SIGNAL_OPTIMISATION([])}
    ],
    gen_server:start_link(
        {local, ?SERVER}, ?MODULE,
        [InitMembers, InitEagers, InitLazys, Mods, Opts],
        StartOpts
    ).


%% -----------------------------------------------------------------------------
%% @doc Broadcasts a message originating from this node. The message will be
%% delivered to each node at least once. The `Mod' passed is responsible for
%% handling the message on remote nodes as well as providing some other
%% information both locally and and on other nodes.
%% `Mod' must be loaded on all members of the clusters and implement the
%% `partisan_plumtree_broadcast_handler' behaviour.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast(any(), module()) -> ok.

broadcast(Broadcast, Mod) ->
    {MessageId, Payload} = Mod:broadcast_data(Broadcast),
    gen_server:cast(?SERVER, {broadcast, MessageId, Payload, Mod}).


%% -----------------------------------------------------------------------------
%% @doc Notifies broadcast server of membership update
%% @end
%% -----------------------------------------------------------------------------
-spec update([node()]) -> ok.

update(LocalState0) ->
    LocalState = partisan_peer_service:decode(LocalState0),
    gen_server:cast(?SERVER, {update, LocalState}).


%% -----------------------------------------------------------------------------
%% @doc Returns the broadcast servers view of full cluster membership.
%% Wait indefinitely for a response is returned from the process.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_members() -> nodeset().

broadcast_members() ->
    broadcast_members(infinity).


%% -----------------------------------------------------------------------------
%% @doc Returns the broadcast servers view of full cluster membership.
%% Waits `Timeout' ms for a response from the server.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_members(infinity | pos_integer()) -> nodeset().

broadcast_members(Timeout) ->
    gen_server:call(?SERVER, broadcast_members, Timeout).


%% -----------------------------------------------------------------------------
%% @doc return a list of exchanges, started by broadcast on thisnode, that are
%% running.
%% @end
%% -----------------------------------------------------------------------------
-spec exchanges() -> exchanges().

exchanges() ->
    exchanges(partisan:node()).


%% -----------------------------------------------------------------------------
%% @doc returns a list of exchanges, started by broadcast on `Node', that are
%% running.
%% @end
%% -----------------------------------------------------------------------------
-spec exchanges(node()) -> partisan_plumtree_broadcast:exchanges().

exchanges(Node) ->
    gen_server:call({?SERVER, Node}, exchanges, infinity).


%% -----------------------------------------------------------------------------
%% @doc cancel exchanges started by this node.
%% @end
%% -----------------------------------------------------------------------------
-spec cancel_exchanges(selector()) -> exchanges().

cancel_exchanges(WhichExchanges) ->
    gen_server:call(?SERVER, {cancel_exchanges, WhichExchanges}, infinity).



%% =============================================================================
%% DEBUG API
%% =============================================================================



%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Wait indefinitely for a response is returned from the process
-spec debug_get_peers(node(), node()) ->
    {nodeset(), nodeset()}.

debug_get_peers(Node, Root) ->
    debug_get_peers(Node, Root, infinity).


%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Waits `Timeout' ms for a response from the server
-spec debug_get_peers(node(), node(), infinity | pos_integer()) ->
    {nodeset(), nodeset()}.

debug_get_peers(Node, Root, Timeout) ->
    gen_server:call({?SERVER, Node}, {get_peers, Root}, Timeout).


%% @doc return peers for all `Nodes' for tree rooted at `Root'
%% Wait indefinitely for a response is returned from the process
-spec debug_get_tree(node(), [node()]) ->
    [{node(), {nodeset(), nodeset()}}].

debug_get_tree(Root, Nodes) ->
    [begin
         Peers = try debug_get_peers(Node, Root)
                 catch _:_ -> down
                 end,
         {Node, Peers}
     end || Node <- Nodes].



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



-spec init([[any()], ...]) -> {ok, state()}.

init([Members, InitEagers0, InitLazys0, Mods, Opts]) ->
    %% We subscribe to the membership change events
    partisan_peer_service:add_sup_callback(fun ?MODULE:update/1),

    LazyTickPeriod = maps:get(lazy_tick_period, Opts),
    ExchangeTickPeriod = maps:get(exchange_tick_period, Opts),
    schedule_lazy_tick(LazyTickPeriod),
    schedule_exchange_tick(ExchangeTickPeriod),

    State1 =  #state{
        mods = lists:usort(Mods),
        exchanges = [],
        lazy_tick_period = LazyTickPeriod,
        exchange_tick_period = ExchangeTickPeriod
    },

    AllMembers = ordsets:from_list(Members),
    InitEagers = ordsets:from_list(InitEagers0),
    InitLazys = ordsets:from_list(InitLazys0),

    State2 = reset_peers(AllMembers, InitEagers, InitLazys, State1),
    {ok, State2}.


-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.

handle_call({get_peers, Root}, _From, State) ->
    EagerPeers = all_peers(
        Root, State#state.eager_sets, State#state.common_eagers
    ),
    LazyPeers = all_peers(
        Root, State#state.lazy_sets, State#state.common_lazys
    ),
    {reply, {EagerPeers, LazyPeers}, State};

handle_call(broadcast_members, _From, State=#state{all_members=AllMembers}) ->
    {reply, AllMembers, State};

handle_call(exchanges, _From, State=#state{exchanges=Exchanges}) ->
    {reply, Exchanges, State};

handle_call({cancel_exchanges, WhichExchanges}, _From, State) ->
    Cancelled = cancel_exchanges(WhichExchanges, State#state.exchanges),
    {reply, Cancelled, State}.


-spec handle_cast(term(), state()) -> {noreply, state()}.

handle_cast({broadcast, MessageId, Message, Mod}, State) ->
    ?LOG_DEBUG("received {broadcast, ~p, Msg, ~p}", [MessageId, Mod]),
    State1 = eager_push(MessageId, Message, Mod, State),
    State2 = schedule_lazy_push(MessageId, Mod, State1),
    {noreply, State2};

handle_cast({broadcast, MessageId, Message, Mod, Round, Root, From}, State) ->
    ?LOG_DEBUG(
        "received {broadcast, ~p, Msg, ~p, ~p, ~p, ~p}",
        [MessageId, Mod, Round, Root, From]
    ),
    Valid = Mod:merge(MessageId, Message),
    State1 = handle_broadcast(Valid, MessageId, Message, Mod, Round, Root, From, State),
    {noreply, State1};

handle_cast({prune, Root, From}, State) ->
    ?LOG_DEBUG("received ~p", [{prune, Root, From}]),
    ?LOG_DEBUG("moving peer ~p from eager to lazy", [From]),
    State1 = add_lazy(From, Root, State),
    {noreply, State1};

handle_cast({i_have, MessageId, Mod, Round, Root, From}, State) ->
    ?LOG_DEBUG("received ~p", [{i_have, MessageId, Mod, Round, Root, From}]),
    Stale = Mod:is_stale(MessageId),
    State1 = handle_ihave(Stale, MessageId, Mod, Round, Root, From, State),
    {noreply, State1};

handle_cast({ignored_i_have, MessageId, Mod, Round, Root, From}, State) ->
    ?LOG_DEBUG(#{
        description => "received ~p",
        message => {ignored_i_have, MessageId, Mod, Round, Root, From}
    }),
    ok = ack_outstanding(MessageId, Mod, Round, Root, From),
    {noreply, State};

handle_cast({graft, MessageId, Mod, Round, Root, From}, State) ->
    ?LOG_DEBUG("received ~p", [{graft, MessageId, Mod, Round, Root, From}]),
    Result = Mod:graft(MessageId),
    ?LOG_DEBUG("graft(~p): ~p", [MessageId, Result]),
    State1 = handle_graft(Result, MessageId, Mod, Round, Root, From, State),
    {noreply, State1};

handle_cast({update, MemberList}, #state{} = State) ->
    ?LOG_DEBUG("received ~p", [{update, MemberList}]),

    #state{
        all_members = BroadcastMembers,
        common_eagers = EagerPeers0,
        common_lazys = LazyPeers
    } = State,

    Members = ordsets:from_list(MemberList),
    New = ordsets:subtract(Members, BroadcastMembers),
    Removed = ordsets:subtract(BroadcastMembers, Members),

    ?LOG_DEBUG("new members: ~p", [ordsets:to_list(New)]),
    ?LOG_DEBUG("removed members: ~p", [ordsets:to_list(Removed)]),

    State1 = case ordsets:size(New) > 0 of
        false ->
            State;
        true ->
            %% as per the paper (page 9):
            %% "When a new member is detected, it is simply added to the set
            %%  of eagerPushPeers"
            EagerPeers = ordsets:union(EagerPeers0, New),

            ?LOG_DEBUG(
                "new peers, eager: ~p, lazy: ~p", [EagerPeers, LazyPeers]
            ),

            reset_peers(Members, EagerPeers, LazyPeers, State)
    end,
    State2 = neighbors_down(Removed, State1),
    {noreply, State2}.


-spec handle_info(
    'exchange_tick' | 'lazy_tick' | {'DOWN', _, 'process', _, _}, state()) ->
    {noreply, state()}.

handle_info(lazy_tick, #state{lazy_tick_period = Period} = State) ->
    ok = send_lazy(),
    schedule_lazy_tick(Period),
    {noreply, State};

handle_info(exchange_tick, #state{exchange_tick_period = Period} = State) ->
    State1 = maybe_exchange(State),
    schedule_exchange_tick(Period),
    {noreply, State1};

handle_info(
    {'DOWN', Ref, process, _Pid, _Reason}, State=#state{exchanges=Exchanges}) ->
    Exchanges1 = lists:keydelete(Ref, 3, Exchanges),
    {noreply, State#state{exchanges=Exchanges1}}.


-spec terminate(term(), state()) -> term().

terminate(_Reason, _State) ->
    ok.


-spec code_change(term() | {down, term()}, state(), term()) -> {ok, state()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



handle_broadcast(
    false, _MessageId, _Message, Mod, _Round, Root, From, State) ->
    %% stale msg
    %% remove sender from eager and set as lazy
    ?LOG_DEBUG("moving peer ~p from eager to lazy", [From]),
    State1 = add_lazy(From, Root, State),
    ok = send({prune, Root, partisan:node()}, Mod, From),
    State1;

handle_broadcast(true, MessageId, Message, Mod, Round, Root, From, State) ->
    %% valid msg
    %% remove sender from lazy and set as eager
    State1 = add_eager(From, Root, State),
    State2 = eager_push(MessageId, Message, Mod, Round + 1, Root, From, State1),
    schedule_lazy_push(MessageId, Mod, Round + 1, Root, From, State2).

handle_ihave(true, MessageId, Mod, Round, Root, From, State) ->
    %% stale i_have
    ok = send(
        {ignored_i_have, MessageId, Mod, Round, Root, partisan:node()},
        Mod,
        From
    ),
    State;
handle_ihave(false, MessageId, Mod, Round, Root, From, State) ->
    %% valid i_have
    %% TODO: don't graft immediately
    ok = send(
        {graft, MessageId, Mod, Round, Root, partisan:node()}, Mod, From
    ),
    add_eager(From, Root, State).

handle_graft(stale, MessageId, Mod, Round, Root, From, State) ->
    %% There has been a subsequent broadcast that is causally newer than this
    %% message according to Mod. We ack the outstanding message since the
    %% outstanding entry for the newer message exists.
    ok = ack_outstanding(MessageId, Mod, Round, Root, From),
    State;

handle_graft({ok, Message}, MessageId, Mod, Round, Root, From, State) ->
    %% We don't ack outstanding here because the broadcast may fail to be
    %% delivered.
    %% Instead we will allow the i_have to be sent once more and let the
    %% subsequent ignore serve as the ack.
    State1 = add_eager(From, Root, State),
    ok = send(
        {broadcast, MessageId, Message, Mod, Round, Root, partisan:node()},
        Mod,
        From
    ),
    State1;

handle_graft({error, Reason}, _MessageId, Mod, _Round, _Root, _From, State) ->
    ?LOG_ERROR(#{
        description => "Unable to graft message",
        callback_mod => Mod,
        reason => Reason
    }),
    State.


neighbors_down(Removed, #state{} = State) ->
    #state{
        all_members = AllMembers,
        common_eagers = CommonEagers,
        eager_sets = EagerSets,
        common_lazys = CommonLazys,
        lazy_sets = LazySets
    } = State,

    NewAllMembers = ordsets:subtract(AllMembers, Removed),
    NewCommonEagers = ordsets:subtract(CommonEagers, Removed),
    NewCommonLazys  = ordsets:subtract(CommonLazys, Removed),

    %% TODO: once we have delayed grafting need to remove timers
    NewEagerSets = maps:from_list([
        {Root, ordsets:subtract(Existing, Removed)}
        || {Root, Existing} <- maps:to_list(EagerSets)
    ]),
    NewLazySets  = maps:from_list([
        {Root, ordsets:subtract(Existing, Removed)}
        || {Root, Existing} <- maps:to_list(LazySets)
    ]),

    %% delete outstanding messages to removed peers
    ok = ordsets:fold(
        fun(Peer, Acc) ->
            %% PLUMTREE_OUTSTANDING is a duplicate bag, so delete will delete
            %% all messages for the removed Peer
            _ = ets:delete(?PLUMTREE_OUTSTANDING, Peer),
            Acc
        end,
        ok,
        Removed
    ),

    State#state{
        all_members = NewAllMembers,
        common_eagers = NewCommonEagers,
        common_lazys = NewCommonLazys,
        eager_sets = NewEagerSets,
        lazy_sets = NewLazySets
    }.

eager_push(MessageId, Message, Mod, State) ->
    eager_push(
        MessageId, Message, Mod, 0, partisan:node(), partisan:node(), State
    ).

eager_push(MessageId, Message, Mod, Round, Root, From, State) ->
    Peers = eager_peers(Root, From, State),
    ?LOG_DEBUG("eager push to peers: ~p", [Peers]),
    ok = send(
        {broadcast, MessageId, Message, Mod, Round, Root, partisan:node()},
        Mod,
        Peers
    ),
    State.

schedule_lazy_push(MessageId, Mod, State) ->
    schedule_lazy_push(
        MessageId, Mod, 0, partisan:node(), partisan:node(), State
    ).

schedule_lazy_push(MessageId, Mod, Round, Root, From, State) ->
    Peers = lazy_peers(Root, From, State),
    ?LOG_DEBUG(
        "scheduling lazy push to peers ~p: ~p",
        [Peers, {MessageId, Mod, Round, Root, From}]
    ),
    ok = add_all_outstanding(MessageId, Mod, Round, Root, Peers),
    State.


send_lazy() ->
    _ = ets:foldl(
        fun
            ({Peer, Message}, {Peer, true} = Acc) ->
                ok = send_lazy(Message, Peer),
                Acc;
            ({Peer, _}, {Peer, false} = Acc) ->
                %% We skip sending
                Acc;
            ({Peer, Message}, _) ->
                case partisan:is_connected(Peer) of
                    true ->
                        ok = send_lazy(Message, Peer),
                        {Peer, true};
                    false ->
                        %% We skip sending
                        {Peer, false}
                end
        end,
        undefined,
        ?PLUMTREE_OUTSTANDING
    ),
    ok.


-spec send_lazy(outstanding(), node()) -> ok.

send_lazy({MessageId, Mod, Round, Root}, Peer) ->
    ?LOG_DEBUG(#{
        description => "sending lazy push ~p",
        message => {i_have, MessageId, Mod, Round, Root, partisan:node()}
    }),
    send({i_have, MessageId, Mod, Round, Root, partisan:node()}, Mod, Peer).


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
            ?LOG_DEBUG("started ~p exchange with ~p (~p)", [Mod, Peer, Pid]),
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
    Node = partisan:node(),
    Mode = partisan_config:get(exchange_selection, optimized),

    Other = case Mode of
        normal ->
            %% Normal; randomly select a peer from the known membership at
            %% this node.
            ordsets:del_element(Node, All);
        optimized ->
            %% Optimized; attempt to find a peer that's not in the broadcast
            %% tree, to increase probability of selecting a lagging node.
            Eagers = all_eager_peers(Root, State),
            Lazys  = all_lazy_peers(Root, State),
            Union  = ordsets:union([Eagers, Lazys]),
            ordsets:del_element(Node, ordsets:subtract(All, Union))
    end,

    case ordsets:size(Other) of
        0 ->
            random_other_node(ordsets:del_element(Node, All));
        _ ->
            random_other_node(Other)
    end.


%% picks random node from ordset
random_other_node(OrdSet) ->
    Size = ordsets:size(OrdSet),

    case Size of
        0 -> undefined;
        _ ->
            lists:nth(rand:uniform(Size), ordsets:to_list(OrdSet))
    end.


ack_outstanding(MessageId, Mod, Round, Root, From) ->
    true = ets:delete_object(
        ?PLUMTREE_OUTSTANDING, {From, {MessageId, Mod, Round, Root}}
    ),
    ok.


add_all_outstanding(MessageId, Mod, Round, Root, Peers) ->
    Message = {MessageId, Mod, Round, Root},
    Objects = [{Peer, Message} || Peer <- ordsets:to_list(Peers)],
    true = ets:insert(?PLUMTREE_OUTSTANDING, Objects),
    ok.


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


set_peers(Root, Eagers, Lazys, #state{} = State) ->
    #state{eager_sets = EagerSets, lazy_sets = LazySets} = State,
    NewEagers = maps:put(Root, Eagers, EagerSets),
    NewLazys = maps:put(Root, Lazys, LazySets),
    State#state{eager_sets = NewEagers, lazy_sets = NewLazys}.


all_eager_peers(Root, State) ->
    all_peers(Root, State#state.eager_sets, State#state.common_eagers).


all_lazy_peers(Root, State) ->
    all_peers(Root, State#state.lazy_sets, State#state.common_lazys).


eager_peers(Root, From, #state{} = State) ->
    #state{eager_sets = EagerSets, common_eagers = CommonEagers} = State,
    all_filtered_peers(Root, From, EagerSets, CommonEagers).


lazy_peers(Root, From, #state{} = State) ->
    #state{lazy_sets = LazySets, common_lazys = CommonLazys} = State,
    all_filtered_peers(Root, From, LazySets, CommonLazys).


all_filtered_peers(Root, From, Sets, Common) ->
    All = all_peers(Root, Sets, Common),
    ordsets:del_element(From, All).


all_peers(Root, Sets, Default) ->
    case maps:find(Root, Sets) of
        {ok, Peers} -> Peers;
        error -> Default
    end.


-spec send(Msg :: message(), Mod :: module(), Peers :: [node()] | node()) -> ok.

send(Msg, Mod, Peers) when is_list(Peers) ->
    _ = [send(Msg, Mod, P) || P <- Peers],
    ok;

send(Msg, Mod, Peer) ->
    instrument_transmission(Msg, Mod),
    partisan:cast_message(Peer, ?SERVER, Msg, #{}).


schedule_lazy_tick(Period) ->
    schedule_tick(lazy_tick, lazy_tick_period, Period).


schedule_exchange_tick(Period) ->
    schedule_tick(exchange_tick, exchange_tick_period, Period).


schedule_tick(Message, Timer, Default) ->
    TickMs = partisan_config:get(Timer, Default),
    erlang:send_after(TickMs, ?MODULE, Message).


-spec reset_peers(nodeset(), nodeset(), nodeset(), state()) -> state().

reset_peers(AllMembers, EagerPeers, LazyPeers, State) ->
    MyNode = partisan:node(),
    State#state{
        common_eagers = ordsets:del_element(MyNode, EagerPeers),
        common_lazys  = ordsets:del_element(MyNode, LazyPeers),
        eager_sets    = maps:new(),
        lazy_sets     = maps:new(),
        all_members   = AllMembers
    }.


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
                    ?LOG_INFO(
                        "Couldn't extract log type and payload. Reason ~p",
                        [Error]
                    ),
                    []
            end,

            lists:foreach(
                fun({Type, Payload}) ->
                    erlang:apply(Module, Function, Args ++ [Type, Payload])
                end,
                ToLog
            )
    end.
