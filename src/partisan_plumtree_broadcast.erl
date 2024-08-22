%% -----------------------------------------------------------------------------
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
%% -----------------------------------------------------------------------------
%% -----------------------------------------------------------------------------
%% @doc This module implements the
%% <a href="https://www.dpss.inesc-id.pt/~ler/reports/srds07.pdf">
%% Plumtree Protocol
%% </a>
%% which is the component that materialises Partisan's gossip scheme.
%%
%% The component is implemented as as a {@link gen_server} that has two
%% functions:
%% <dl>
%% <dt><b>Tree construction</b></dt>
%% <dd>This component is in charge of selecting which links of the random
%% overlay network will be used to forward the message payload using an eager
%% push strategy. It implements a tree construction mechanisms that is as
%% simple as possible, with minimal overhead in terms of control messages.</dd>
%% <dt><b>Tree repair</b></dt>
%% <dd>This component is in charge of repairing the tree when failures occur.
%% The process ensures that, despite failures, all nodes remain covered by the
%% spanning tree. Therefore, it should be able to detect and heal partitions of
%% the tree. The overhead imposed by this operation should also be as low as
%% possible.</dd>
%% </dl>
%%
%% == Overview ==
%% The protocol operates as any pure gossip protocol, in the sense that, in
%% order to broadcast a message, each node gossips with `f' nodes provided by a
%% peer sampling service (where `f' is the protocol fanout). However, each node
%% uses a combination of eager push and lazy push gossip.
%%
%% <em>Eager push</em> is used just for a subset of the `f' nodes, while
%% <em>lazy push</em> is used for the remaining nodes. The links used for eager
%% push are selected in such a way that their closure effectively builds a
%% broadcast tree embedded in the random overlay network. Lazy push links are
%% used to ensure gossip reliability when nodes fail and also to quickly heal
%% the broadcast tree.
%% Furthermore, the set of (random) peers is not changed at each gossip round.
%% Instead, the same peers are used until failures are detected.
%%
%% The protocol uses a Partisan Channel (See @{link broadcast_channel}), i.e.
%% based on TCP, to support the message exchange, as they
%% offer extra reliability and an additional source of failure detection.
%%
%% Plumtree depends on an overlay network which is maintained by a peer
%% sampling service ({@link partisan_peer_service_manager}) and relies on the
%% peer sampling services/s exhibiting a property: <em>Symmetric (partial)
%% views</em>. If the links that form the spanning tree are symmetric,
%% then the tree may be shared by multiple sources. Symmetric partial views
%% render the task of creating bi-directional trees easier, and reduce the
%% amount of peers that each node has to maintain.
%%
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_plumtree_broadcast).

-behaviour(partisan_gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-define(EVENT_MANAGER, partisan_peer_service_events).
-define(SERVER, ?MODULE).



-type message_id()      ::  any().
-type message_round()   ::  non_neg_integer().
%% Lazy messages that have not been acked. Messages are added to
%% this set when a node is sent a lazy message (or when it should be
%% sent one sometime in the future). Messages are removed when the lazy
%% pushes are acknowledged via graft or ignores. Entries are keyed by their
%% destination
%% These are stored in the ?PLUMTREE_OUTSTANDING ets table under using nodename
%% as key.
%% PLUMTREE_OUTSTANDING is created and owned by partisan_sup
-type outstanding()     ::  {message_id(), module(), message_round(), node()}.
-type exchange()        ::  {module(), node(), reference(), pid()}.
-type exchanges()       ::  [exchange()].
-type selector()        ::  all
                            | {peer, node()}
                            | {mod, module()}
                            | reference()
                            | pid().
-type opts()            ::  opts_map() | opts_list().
-type opts_map()        ::  #{
                                lazy_tick_period => non_neg_integer(),
                                exchange_tick_period => non_neg_integer()
                            }.
-type opts_list()       ::  [
                                {lazy_tick_period, non_neg_integer()}
                                | {exchange_tick_period, non_neg_integer()}
                            ].
-type info_opt()        ::  node_spec | metadata | distance.

-record(state, {
    %% This node
    node :: node(),

    %% Initially trees rooted at each node are the same.
    %% Portions of that tree belonging to this node are
    %% shared in this set.
    common_eagers :: nodeset(),

    %% Initially trees rooted at each node share the same lazy links.
    %% Typically this set will contain a single element. However, it may
    %% contain more in large clusters and may be empty for clusters with
    %% less than three nodes.
    common_lazys  :: nodeset(),

    %% A mapping of sender node (root of each broadcast tree)
    %% to this node's portion of the tree. Elements are
    %% added to this structure as messages rooted at a node
    %% propagate to this node. Nodes that are never the
    %% root of a message will never have a key added to
    %% `eager_sets'
    eager_sets    :: #{node() := nodeset()},

    %% A Mapping of sender node (root of each spanning tree)
    %% to this node's set of lazy peers. Elements are added
    %% to this structure as messages rooted at a node
    %% propagate to this node. Nodes that are never the root
    %% of a message will never have a key added to `lazy_sets'
    lazy_sets     :: #{node() := nodeset()},

    %% Set of registered modules that may handle messages that
    %% have been broadcast
    mods          :: [module()],

    %% List of outstanding exchanges
    exchanges     :: exchanges(),

    %% Set of all known members. Used to determine
    %% which members have joined and left during a membership update
    all_members   :: nodeset(),

    %% Lazy tick period in milliseconds. On every tick all outstanding
    %% lazy pushes are sent out
    lazy_tick_period :: non_neg_integer(),

    %% Exchange tick period in milliseconds that may or may not occur
    exchange_tick_period :: non_neg_integer()

}).

-type state()           :: #state{}.
-type nodeset()         :: ordsets:ordset(node()).

-export_type([info_opt/0]).
-export_type([nodeset/0]).

%% API
-export([broadcast/2]).
-export([broadcast_channel/1]).
-export([broadcast_members/0]).
-export([broadcast_members/1]).
-export([cancel_exchanges/1]).
-export([exchanges/0]).
-export([exchanges/1]).
-export([exchanges/2]).
-export([start_link/0]).
-export([start_link/5]).
-export([update/1]).

%% Debug API
-export([get_peers/1]).
-export([get_peers/2]).
-export([get_eager_peers/1]).
-export([get_lazy_peers/1]).
-export([debug_get_peers/2]).
-export([debug_get_peers/3]).
-export([debug_get_peers/4]).
-export([debug_get_tree/2]).
-export([debug_get_tree/3]).
-export([debug_get_tree/4]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).


-eqwalizer({nowarn_function, start_link/5}).

%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Starts the broadcast server on this node.
%%
%% The initial membership list is fetched from the configured @{link
%% partisan_peer_service}.
%%
%% If the node is a singleton then the initial eager and lazy sets are empty.
%% If there are two nodes, each will be in the others
%% eager set and the lazy sets will be empty. When number of members is less
%% than 5, each node will initially have one other node in its eager set and
%% lazy set. If there are more than five nodes each node will have at most two
%% other nodes in its eager set and one in its lazy set, initially.
%%
%% In addition, after the broadcast server is started, all callbacks defined in
%% the configuration option `broadcast_mods' are registered.
%% By default the list of callbacks includes the module
%% {@link partisan_plumtree_backend} which is used by to generate membership
%% updates as the ring changes.
%%
%% @TODO we should spawn 1 broadcast server per channel and or channel
%% partition
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
%% @doc Starts the broadcast server on this node.
%% `Members' must be a list of all members known to this node when starting
%% the broadcast server.
%% `Eagers' are the initial peers of this node for all broadcast trees.
%% `Lazys' is a list of random peers not in `Eagers' that will be used
%% as the initial lazy peer shared by all trees for this node. If the number
%% of nodes in the cluster is less than 3, `Lazys' should be an empty list.
%% `Eagers' and `Lazys' must also be subsets of `Members'. `Mods' is
%% a list of modules that may be handlers for broadcasted messages. All modules
%% in `Mods' should implement the `partisan_plumtree_broadcast_handler'
%% behaviour.
%%
%% `Opts' is a proplist or map with the following possible options:
%%  <ul>
%%  <li> `lazy_tick_period :: non_neg_integer()' - Flush all outstanding lazy pushes period (in milliseconds)</li>
%%  <li> `exchange_tick_period :: non_neg_integer()' - Possibly perform an exchange period (in milliseconds)</li>
%% </ul>
%%
%% NOTE: When starting the server using start_link/2 no automatic membership
%% update from ring_events is registered. Use {@link start_link/0}.
%% @end
%% -----------------------------------------------------------------------------

-spec start_link(
    Members :: [node()],
    Eagers :: [node()],
    Lazys :: [node()],
    Mods :: [module()],
    Opts :: opts()) ->
    {ok, pid()} | ignore | {error, term()}.

start_link(Members, Eagers, Lazys, Mods, Opts)
when is_list(Members), is_list(Eagers), is_list(Lazys), is_list(Mods),
is_list(Opts) ->
    start_link(Members, Eagers, Lazys, Mods, maps:from_list(Opts));

start_link(Members, Eagers, Lazys, Mods, Opts)
when is_list(Members), is_list(Eagers), is_list(Lazys), is_list(Mods),
is_map(Opts) ->
    Args = [Members, Eagers, Lazys, Mods, Opts],
    StartOpts = [
        {spawn_opt, ?PARALLEL_SIGNAL_OPTIMISATION([])}
    ],
    partisan_gen_server:start_link({local, ?SERVER}, ?MODULE, Args, StartOpts).


%% -----------------------------------------------------------------------------
%% @doc Broadcasts a message originating from this node.
%% The message will be delivered to each node at least once. The `Mod' passed
%% must be loaded on all members of the cluster and implement the
%% `partisan_plumtree_broadcast_handler' behaviour which is responsible for
%% handling the message on remote nodes as well as providing some other
%% information both locally and on other nodes.
%%
%% The broadcast will be sent over the channel defined by
%% {@link broadcast_channel/1}.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast(any(), module()) -> ok.

broadcast(Broadcast, Mod) ->
    {MessageId, Payload} = Mod:broadcast_data(Broadcast),
    partisan_gen_server:cast(?SERVER, {broadcast, MessageId, Payload, Mod}).


%% -----------------------------------------------------------------------------
%% @doc Returns the channel to be used when sending broadcasting a message
%% on behalf of module `Mod'.
%%
%% The channel defined by the callback `Mod:broadcast_channel()' or default
%% channel i.e. {@link partisan:default_channel/0} if the callback is not
%% implemented.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_channel(Mod :: module()) -> partisan:channel().

broadcast_channel(Mod) ->
    partisan_util:apply(Mod, broadcast_channel, [], ?DEFAULT_CHANNEL).


%% -----------------------------------------------------------------------------
%% @doc Notifies broadcast server of membership update
%% This is the function is added to partisan_peer_service_events using
%% partisan_peer_service:add_sup_callback(fun ?MODULE:update/1),
%% @end
%% -----------------------------------------------------------------------------
-spec update([node()]) -> ok | no_return().

update(LocalState0) ->
    MemberList = partisan_peer_service:decode(LocalState0),
    partisan_gen_server:cast(?SERVER, {update, MemberList}).


%% -----------------------------------------------------------------------------
%% @doc Returns the broadcast servers view of full cluster membership.
%% Wait indefinitely for a response is returned from the process.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_members() -> nodeset() | no_return().

broadcast_members() ->
    broadcast_members(infinity).


%% -----------------------------------------------------------------------------
%% @doc Returns the broadcast servers view of full cluster membership.
%% Waits `Timeout' ms for a response from the server.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_members(infinity | pos_integer()) -> nodeset() | no_return().

broadcast_members(Timeout) ->
    partisan_gen_server:call(?SERVER, broadcast_members, Timeout).


%% -----------------------------------------------------------------------------
%% @doc return a list of exchanges, started by broadcast on this node, that are
%% running.
%% @end
%% -----------------------------------------------------------------------------
-spec exchanges() -> exchanges() | no_return().

exchanges() ->
    partisan_gen_server:call(?SERVER, exchanges, infinity).


%% -----------------------------------------------------------------------------
%% @doc Returns a list of running exchanges, started on `Node'.
%% @end
%% -----------------------------------------------------------------------------
-spec exchanges(node()) -> exchanges() | no_return().

exchanges(Node) ->
    exchanges(Node, infinity).


%% -----------------------------------------------------------------------------
%% @doc Returns a list of running exchanges, started on `Node'.
%% @end
%% -----------------------------------------------------------------------------
-spec exchanges(node(), timeout()) -> exchanges() | no_return().

exchanges(Node, Timeout) ->
    partisan_gen_server:call({?SERVER, Node}, exchanges, Timeout).


%% -----------------------------------------------------------------------------
%% @doc Cancel exchanges started by this node.
%% @end
%% -----------------------------------------------------------------------------
-spec cancel_exchanges(selector()) -> exchanges() | no_return().

cancel_exchanges(Selector) ->
    partisan_gen_server:call(?SERVER, {cancel_exchanges, Selector}, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_peers(Root :: node()) -> list() | no_return().

get_peers(Root) ->
    partisan_gen_server:call(?SERVER, {get_peers, Root}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_peers(Root :: node(), Opts :: [partisan:info_opt()]) ->
    list() | no_return().

get_peers(Root, Opts) when is_list(Opts) ->
    partisan_gen_server:call(?SERVER, {get_peers, Root, Opts}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_eager_peers(Root :: node()) -> list() | no_return().

get_eager_peers(Root) ->
    partisan_gen_server:call(?SERVER, {get_eager_peers, Root}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_lazy_peers(Root :: node()) -> list() | no_return().

get_lazy_peers(Root) ->
    partisan_gen_server:call(?SERVER, {get_lazy_peers, Root}).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



-spec init(list()) -> {ok, state()}.

init([Members, InitEagers0, InitLazys0, Mods, Opts]) ->
    %% We subscribe to the membership change events
    partisan_peer_service:add_sup_callback(fun ?MODULE:update/1),

    LazyTickPeriod = maps:get(lazy_tick_period, Opts),
    ExchangeTickPeriod = maps:get(exchange_tick_period, Opts),
    schedule_lazy_tick(LazyTickPeriod),
    schedule_exchange_tick(ExchangeTickPeriod),

    State1 =  #state{
        node = partisan:node(),
        all_members = ordsets:new(),
        common_lazys = ordsets:new(),
        common_eagers = ordsets:new(),
        eager_sets = maps:new(),
        lazy_sets = maps:new(),
        mods = lists:usort(Mods),
        exchanges = [],
        lazy_tick_period = LazyTickPeriod,
        exchange_tick_period = ExchangeTickPeriod
    },

    AllMembers = ordsets:from_list(Members),
    InitEagers = ordsets:from_list(InitEagers0),
    InitLazys = ordsets:from_list(InitLazys0),

    %% We finish initialising the state
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

handle_call({get_peers, Root, InfoOpts}, _From, State) ->
    EagerPeers = all_peers(
        Root, State#state.eager_sets, State#state.common_eagers
    ),
    LazyPeers = all_peers(
        Root, State#state.lazy_sets, State#state.common_lazys
    ),
    Info = try_node_info(Root, InfoOpts),

    {reply, {EagerPeers, LazyPeers, Info}, State};

handle_call({get_eager_peers, Root}, _From, State) ->
    EagerPeers = all_peers(
        Root, State#state.eager_sets, State#state.common_eagers
    ),
    {reply, EagerPeers, State};

handle_call({get_lazy_peers, Root}, _From, State) ->
    LazyPeers = all_peers(
        Root, State#state.lazy_sets, State#state.common_lazys
    ),
    {reply, LazyPeers, State};

handle_call(broadcast_members, _From, State=#state{all_members = AllMembers}) ->
    {reply, AllMembers, State};

handle_call(exchanges, _From, State=#state{exchanges = Exchanges}) ->
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
    Valid = partisan_util:safe_apply(Mod, merge, [MessageId, Message], false),
    State1 = handle_broadcast(Valid, MessageId, Message, Mod, Round, Root, From, State),
    {noreply, State1};

handle_cast({prune, Root, From}, State) ->
    ?LOG_DEBUG("received ~p", [{prune, Root, From}]),
    ?LOG_DEBUG("moving peer ~p from eager to lazy", [From]),
    State1 = add_lazy(From, Root, State),
    {noreply, State1};

handle_cast({i_have, MessageId, Mod, Round, Root, From}, State) ->
    ?LOG_DEBUG("received ~p", [{i_have, MessageId, Mod, Round, Root, From}]),
    Stale = partisan_util:safe_apply(Mod, is_stale, [MessageId], false),
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
    Result = partisan_util:safe_apply(Mod, graft, [MessageId], false),
    ?LOG_DEBUG("graft(~p): ~p", [MessageId, Result]),
    State1 = handle_graft(Result, MessageId, Mod, Round, Root, From, State),
    {noreply, State1};

handle_cast({update, NodeList}, #state{} = State) when is_list(NodeList) ->
    ?LOG_DEBUG("received ~p", [{update, NodeList}]),

    #state{
        all_members = AllMembers,
        common_eagers = EagerPeers0,
        common_lazys = LazyPeers
    } = State,

    Members = ordsets:from_list(NodeList),
    New = ordsets:subtract(Members, AllMembers),
    Removed = ordsets:subtract(AllMembers, Members),

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
            %% eqwalizer:ignore Members
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

handle_info(exchange_tick, #state{exchange_tick_period = Period} = State) ->
    State1 = maybe_exchange(State),
    schedule_exchange_tick(Period),
    {noreply, State1};

handle_info(
    {'DOWN', Ref, process, _Pid, _Reason}, State=#state{exchanges=Exchanges}) ->
    %% An exchange has terminated
    Exchanges1 = lists:keydelete(Ref, 3, Exchanges),
    {noreply, State#state{exchanges=Exchanges1}};

handle_info({gen_event_EXIT, {?EVENT_MANAGER, _}, Reason}, State)
when Reason == normal; Reason == shutdown ->
    {noreply, State};

handle_info({gen_event_EXIT, {?EVENT_MANAGER, _}, {swapped, _, _}}, State) ->
    {noreply, State};

handle_info({gen_event_EXIT, {?EVENT_MANAGER, _}, Reason}, State) ->
    ?LOG_INFO(#{
        description => "Event handler terminated. Adding new handler.",
        reason => Reason,
        manager => ?EVENT_MANAGER
    }),
    partisan_peer_service:add_sup_callback(fun ?MODULE:update/1),
    {noreply, State};

handle_info(Event, State) ->
    ?LOG_INFO(#{description => "Unhandled info event", event => Event}),
    {noreply, State}.



-spec terminate(term(), state()) -> term().

terminate(_Reason, _State) ->
    ok.


-spec code_change(term() | {down, term()}, state(), term()) -> {ok, state()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% DEBUG API
%% =============================================================================



%% @private
try_node_info(Node, Opts) ->
    case partisan:node() of
        Node ->
            partisan:node_info(Opts);
        Peer ->
            case partisan_rpc:call(Peer, partisan, node_info, [Opts], 5000) of
                {badrpc, _} ->
                    #{};
                Info ->
                    Info
            end
    end.


%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Wait indefinitely for a response is returned from the process
-spec debug_get_peers(node(), node()) -> {nodeset(), nodeset()} | no_return().

debug_get_peers(Node, Root) ->
    debug_get_peers(Node, Root, infinity).


%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Waits `Timeout' ms for a response from the server
-spec debug_get_peers(node(), node(), infinity | pos_integer()) ->
    {nodeset(), nodeset()} | no_return().

debug_get_peers(Node, Root, Timeout) ->
    partisan_gen_server:call({?SERVER, Node}, {get_peers, Root}, Timeout).


%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Waits `Timeout' ms for a response from the server
-spec debug_get_peers(node(), node(), [info_opt()], infinity | pos_integer()) ->
    {nodeset(), nodeset(), Info :: map()} | no_return().

debug_get_peers(Node, Root, Opts, Timeout) ->
    partisan_gen_server:call({?SERVER, Node}, {get_peers, Root, Opts}, Timeout).


%% -----------------------------------------------------------------------------
%% @doc return peers for all `Nodes' for tree rooted at `Root'
%% Wait indefinitely for a response is returned from the process
%% @end
%% -----------------------------------------------------------------------------
-spec debug_get_tree(node(), [node()]) ->
    [{node(), {nodeset(), nodeset()} | down}].

debug_get_tree(Root, Nodes) ->
    debug_get_tree(Root, Nodes, infinity).


%% -----------------------------------------------------------------------------
%% @doc return peers for all `Nodes' for tree rooted at `Root'
%% Wait `Timeout' for a response is returned from the process
%% @end
%% -----------------------------------------------------------------------------
-spec debug_get_tree(node(), [node()], timeout()) ->
    [{node(), {nodeset(), nodeset()} | down}].

debug_get_tree(Root, Nodes, Timeout) ->
    [
        begin
            try
                {Node, debug_get_peers(Node, Root, Timeout)}
            catch
                _:Reason ->
                    ?LOG_INFO(#{
                        description =>
                            "Call to get remote root tree failed.",
                        peer => Node,
                        root => Root,
                        reason => Reason
                    }),
                    {Node, down}
            end
        end
        || Node <- Nodes
     ].


%% -----------------------------------------------------------------------------
%% @doc return peers for all `Nodes' for tree rooted at `Root'
%% Wait `Timeout' for a response is returned from the process
%% @end
%% -----------------------------------------------------------------------------
-spec debug_get_tree(node(), [node()], [info_opt()], timeout()) ->
    [{node(), {nodeset(), nodeset(), Info :: map()} | down}].

debug_get_tree(Root, Nodes, Opts, Timeout) ->
    [
        begin
            try
                {Node, debug_get_peers(Node, Root, Opts, Timeout)}
            catch
                _:Reason ->
                    ?LOG_INFO(#{
                        description =>
                            "Call to get remote root tree failed.",
                        peer => Node,
                        root => Root,
                        reason => Reason
                    }),
                    {Node, down}
            end
        end
        || Node <- Nodes
     ].


%% =============================================================================
%% PRIVATE
%% =============================================================================


%% @private
handle_broadcast(
    false, _MessageId, _Message, Mod, _Round, Root, From, State) ->
    %% stale msg
    %% remove sender from eager and set as lazy
    ?LOG_DEBUG("moving peer ~p from eager to lazy", [From]),
    State1 = add_lazy(From, Root, State),
    ok = send({prune, Root, State#state.node}, Mod, From),
    State1;

handle_broadcast(true, MessageId, Message, Mod, Round, Root, From, State) ->
    %% valid msg
    %% remove sender from lazy and set as eager
    State1 = add_eager(From, Root, State),
    State2 = eager_push(MessageId, Message, Mod, Round + 1, Root, From, State1),
    schedule_lazy_push(MessageId, Mod, Round + 1, Root, From, State2).


%% @private
handle_ihave(true, MessageId, Mod, Round, Root, From, State) ->
    %% stale i_have
    ok = send(
        {ignored_i_have, MessageId, Mod, Round, Root, State#state.node},
        Mod,
        From
    ),
    State;

handle_ihave(false, MessageId, Mod, Round, Root, From, State) ->
    %% valid i_have
    %% TODO: don't graft immediately
    ok = send(
        {graft, MessageId, Mod, Round, Root, State#state.node}, Mod, From
    ),
    add_eager(From, Root, State).


%% @private
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
        {broadcast, MessageId, Message, Mod, Round, Root, State#state.node},
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


%% @private
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


%% @private
eager_push(MessageId, Message, Mod, State) ->
    Node = State#state.node,
    eager_push(
        MessageId, Message, Mod, 0, Node, Node, State
    ).


%% @private
eager_push(MessageId, Message, Mod, Round, Root, From, State) ->
    Peers = eager_peers(Root, From, State),
    ?LOG_DEBUG("eager push to peers: ~p", [Peers]),
    ok = send(
        {broadcast, MessageId, Message, Mod, Round, Root, State#state.node},
        Mod,
        Peers
    ),
    State.


%% @private
schedule_lazy_push(MessageId, Mod, State) ->
    Node = State#state.node,
    schedule_lazy_push(
        MessageId, Mod, 0, Node, Node, State
    ).


%% @private
schedule_lazy_push(MessageId, Mod, Round, Root, From, State) ->
    Peers = lazy_peers(Root, From, State),
    ?LOG_DEBUG(
        "scheduling lazy push to peers ~p: ~p",
        [Peers, {MessageId, Mod, Round, Root, From}]
    ),
    ok = add_all_outstanding(MessageId, Mod, Round, Root, Peers),
    State.


%% @private
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
                        %% This will send even when Mod:broadcast_channel is
                        %% not connected as it will use the default channel.
                        %% TODO make this option configurable so that we can
                        %% ask Partisan to skip in case broadcast_channel is
                        %% not connected.
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


%% @private
-spec send_lazy(outstanding(), node()) -> ok.

send_lazy({MessageId, Mod, Round, Root}, Peer) ->
    Node = partisan:node(),

    ?LOG_DEBUG(#{
        description => "sending lazy push ~p",
        message => {i_have, MessageId, Mod, Round, Root, Node}
    }),
    send({i_have, MessageId, Mod, Round, Root, Node}, Mod, Peer).


%% @private
maybe_exchange(State) ->
    %% This checks for any channel connection, not specifically the broadcast
    %% channel.
    Connected = partisan_peer_connections:nodes(),
    Root = random_root(State, Connected),
    Peer = random_peer(Root, State, Connected),
    maybe_exchange(Peer, State).

maybe_exchange(undefined, State) ->
    State;

maybe_exchange(_, #state{mods = []} = State) ->
    State;

maybe_exchange(Peer, State) ->
    %% limit the number of exchanges this node can start concurrently.
    %% the exchange must (currently?) implement any "inbound" concurrency limits
    Limit = partisan_config:get(broadcast_start_exchange_limit),

    case length(State#state.exchanges) >= Limit of
        true ->
            State;
        false ->
            maybe_exchange(Peer, State, State#state.mods)
    end.


%% @private
maybe_exchange(_Peer, State, []) ->
    State;

maybe_exchange(Peer, #state{mods = [_|Mods]} = State, [H|T]) ->
    %% We place the current Mod at the end of the list i.e. results in a
    %% roundrobin algorithm for when limit =/= length(Mods)
    NewState = State#state{mods = Mods ++ [H]},

    case lists:keyfind(H, 1, State#state.exchanges) of
        {H, _, _, _} ->
            %% We skip current Mod as there is already an exchange for it
            ?LOG_DEBUG(
                "Ignoring exchange request for ~p with ~p, "
                "there is already another exchange running "
                "for the same handler.",
                [H, Peer]
            ),
            maybe_exchange(Peer, NewState, T);
        false ->
            maybe_exchange(Peer, exchange(Peer, State, H), T)
    end.


%% @private
exchange(Peer, #state{exchanges = Exchanges} = State, Mod) ->
    case catch Mod:exchange(Peer) of
        ignore ->
            ?LOG_DEBUG(
                "~p ignored exchange request with ~p.", [Mod, Peer]
            ),
            State;

        {ok, Pid} ->
            ?LOG_DEBUG(
                "Started ~p exchange with ~p (~p).", [Mod, Peer, Pid]
            ),
            Ref = monitor(process, Pid),
            State#state{exchanges = [{Mod, Peer, Ref, Pid} | Exchanges]};

        {error, _Reason} ->
            State;
        _ -> 
            State
    end.


%% @private
cancel_exchanges(all, Exchanges) ->
    kill_exchanges(Exchanges);

cancel_exchanges(WhichProc, Exchanges)
when is_reference(WhichProc) orelse is_pid(WhichProc) ->
    KeyPos = case is_reference(WhichProc) of
        true -> 3;
        false -> 4
    end,
    case lists:keyfind(WhichProc, KeyPos, Exchanges) of
        false ->
            [];
        Exchange ->
            kill_exchange(Exchange),
            [Exchange]
    end;

cancel_exchanges(Which, Exchanges) ->
    Filter = exchange_filter(Which),
    ToCancel = [Ex || Ex <- Exchanges, Filter(Ex)],
    kill_exchanges(ToCancel).


%% @private
kill_exchanges(Exchanges) ->
    _ = [kill_exchange(Exchange) || Exchange <- Exchanges],
    Exchanges.


%% @private
kill_exchange({_, _, _, ExchangePid}) ->
    exit(ExchangePid, cancel_exchange).


%% @private
exchange_filter({peer, Peer}) ->
    fun({_, ExchangePeer, _, _}) ->
            Peer =:= ExchangePeer
    end;
exchange_filter({mod, Mod}) ->
    fun({ExchangeMod, _, _, _}) ->
            Mod =:= ExchangeMod
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc picks random root uniformly
%% @end
%% -----------------------------------------------------------------------------
random_root(#state{all_members=Members}, Connected) ->
    random_other_node(Members, Connected).


%% -----------------------------------------------------------------------------
%% @doc picks random peer favoring peers not in eager or lazy set and ensuring
%% peer is not this node
%% @end
%% -----------------------------------------------------------------------------
random_peer(Root, State=#state{all_members = All}, Connected) ->
    Node = State#state.node,
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
            random_other_node(ordsets:del_element(Node, All), Connected);
        _ ->
            random_other_node(Other, Connected)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc picks random node from ordset
%% @end
%% -----------------------------------------------------------------------------
random_other_node(OrdSet0, Connected) ->
    OrdSet = ordsets:intersection(OrdSet0, Connected),
    Size = ordsets:size(OrdSet),

    case Size of
        0 ->
            undefined;
        _ ->
            lists:nth(rand:uniform(Size), ordsets:to_list(OrdSet))
    end.


%% @private
ack_outstanding(MessageId, Mod, Round, Root, From) ->
    true = ets:delete_object(
        ?PLUMTREE_OUTSTANDING, {From, {MessageId, Mod, Round, Root}}
    ),
    ok.


%% @private
add_all_outstanding(MessageId, Mod, Round, Root, Peers) ->
    Message = {MessageId, Mod, Round, Root},
    Objects = [{Peer, Message} || Peer <- ordsets:to_list(Peers)],
    %% These will be removed when ACKed
    true = ets:insert(?PLUMTREE_OUTSTANDING, Objects),
    ok.


%% @private
add_eager(From, Root, State) ->
    update_peers(
        From, Root, fun ordsets:add_element/2, fun ordsets:del_element/2, State
    ).


%% @private
add_lazy(From, Root, State) ->
    update_peers(
        From, Root, fun ordsets:del_element/2, fun ordsets:add_element/2, State
    ).


%% @private
update_peers(From, Root, EagerUpdate, LazyUpdate, State) ->
    CurrentEagers = all_eager_peers(Root, State),
    CurrentLazys = all_lazy_peers(Root, State),
    NewEagers = EagerUpdate(From, CurrentEagers),
    NewLazys  = LazyUpdate(From, CurrentLazys),
    set_peers(Root, NewEagers, NewLazys, State).


%% @private
set_peers(Root, Eagers, Lazys, #state{} = State) ->
    #state{eager_sets = EagerSets, lazy_sets = LazySets} = State,
    NewEagers = maps:put(Root, Eagers, EagerSets),
    NewLazys = maps:put(Root, Lazys, LazySets),
    State#state{eager_sets = NewEagers, lazy_sets = NewLazys}.


%% @private
all_eager_peers(Root, State) ->
    all_peers(Root, State#state.eager_sets, State#state.common_eagers).


%% @private
all_lazy_peers(Root, State) ->
    all_peers(Root, State#state.lazy_sets, State#state.common_lazys).


%% @private
eager_peers(Root, From, #state{} = State) ->
    #state{eager_sets = EagerSets, common_eagers = CommonEagers} = State,
    all_filtered_peers(Root, From, EagerSets, CommonEagers).


%% @private
lazy_peers(Root, From, #state{} = State) ->
    #state{lazy_sets = LazySets, common_lazys = CommonLazys} = State,
    all_filtered_peers(Root, From, LazySets, CommonLazys).


%% @private
all_filtered_peers(Root, From, Sets, Common) ->
    All = all_peers(Root, Sets, Common),
    ordsets:del_element(From, All).


%% @private
all_peers(Root, Sets, Default) ->
    case maps:find(Root, Sets) of
        {ok, Peers} -> Peers;
        error -> Default
    end.


%% @private
-spec send(
    Msg :: partisan:message(),
    Mod :: module(),
    Peers :: [node()] | node()) -> ok.

send(Msg, Mod, Peers) when is_list(Peers) ->
    _ = [send(Msg, Mod, P) || P <- Peers],
    ok;

send(Msg, Mod, Peer) ->
    instrument_transmission(Msg, Mod),
    Opts = [{channel, broadcast_channel(Mod)}],
    partisan_gen_server:cast({?SERVER, Peer}, Msg, Opts).


%% @private
schedule_lazy_tick(Period) ->
    schedule_tick(lazy_tick, lazy_tick_period, Period).


%% @private
schedule_exchange_tick(Period) ->
    schedule_tick(exchange_tick, exchange_tick_period, Period).


%% @private
schedule_tick(Message, Timer, Default) ->
    TickMs = partisan_config:get(Timer, Default),
    erlang:send_after(TickMs, ?MODULE, Message).


%% @private
-spec reset_peers(nodeset(), nodeset(), nodeset(), state()) -> state().

reset_peers(AllMembers, EagerPeers, LazyPeers, State) ->
    Node = State#state.node,
    State#state{
        common_eagers = ordsets:del_element(Node, EagerPeers),
        common_lazys  = ordsets:del_element(Node, LazyPeers),
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
