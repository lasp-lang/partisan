%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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

%% @doc This module realises the {@link partisan_peer_service_manager}
%% behaviour implementing a peer-to-peer partial mesh topology using the
%% protocol described in the paper
%% <a href="https://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf">HyParView:
%% a membership protocol for reliable gossip-based broadcast</a>
%% by João Leitão, José Pereira and Luís Rodrigues.
%%
%% The following content contains abstracts from the paper.
%%
%% == Characteristics ==
%% <ul>
%% <li>Uses TCP/IP as an unreliable failure detector (unreliable because it can
%% generate false positives e.g. when the network becomes suddenly
%% congested).</li>
%% <li>It can sustain high level of node failres while ensuring connectivity
%% of the overlay. Nodes are considered "failed" when the TCP/IP connection is
%% dropped.</li>
%% <li>Nodes maintain partial views of the network. Every node will contain and
%% <em>active view</em> that forms a connected grah, and a
%% <em>passive view</em> of backup links that
%% are used to repair graph connectivity under failure. Some links to passive
%% nodes are kept open for fast replacement of failed nodes in the active
%% view. So the view is probabilistic, meaning that the protocol doesn't
%% prevent (nor detects) the cluter to be split into several subclusters with
%% no connections to each other.</li>
%% <li>HyParView sacrificies strong membership for high availability and
%% connectivity: the algorithm constantly works towards and ensures that
%% eventually the clsuter membership is a fully-connected component.
%% However, at any point in time different nodes may have different,
%% inconsistent views of the cluster membership. As a consequence, HyParView is
%% not designed to work with systems that require strong membership properties,
%% eg. consensus protocols like Paxos or Raft.</li>
%% <li>Point-to-point messaging for connected nodes with a minimum of 1 hop via
%% transitive message delivery (as not all nodes directly connected). Delivery
%% is probabilistic.</li>
%% <li>No explicit leave operation, becuase the overlay is able to react fast
%% enough to node failures. Hence when a node wishes to leave the system it is
%% simply treated as if hte node have failed.</li>
%% <li>Scalability to up-to 2,000 nodes.</li>
%% </ul>
%%
%% == HyParView Membership Protocol ==
%%
%% == Partial View ==
%% A partial view is a small subset of the entire system (cluster) membership,
%% a set of node specifications maintained locally at each node.
%%
%% A node specification i.e. `partisan:node_spec()' allows a node to be
%% reached by other nodes.
%%
%% A membership protocol is in charge of initializing and maintaining the
%% partial views at each node in face of dynamic changes in the system. For
%% instance, when a new node joins the system, its identifier should be added
%% to the partial view of (some) other nodes and it has to create its own
%% partial view, including identifiers of nodes already in the system. Also, if
%% a node fails or leaves the system, its identifier should be removed from all
%% partial views as soon as possible.
%%
%% Partial views establish neighboring associations among nodes. Therefore,
%% partial views define an overlay network, in other words, partial views
%% establish an directed graph that captures the neighbor relation between all
%% nodes executing the protocol. In this graph nodes are represented by a
%% vertex while a neighbor relation is represented by an arc from the node who
%% contains the target node in his partial view.
%%
%% == Membership Protocol ==
%% The Hybrid Partial View (HyParView) membership protocol is in charge of
%% maintaining two distinct views at each node: a small active view, of size
%% `log(n) + c', and a larger passive view, of size `k(log(n) + c)'.
%%
%% It then selects which members of this view should be promoted to the active
%% view.
%%
%% === Active View ===
%% Each node maintains a small symmetric ctive view the size of fanout + 1.
%% Being symmetric means means that if node <b>q</b> is in the active view of
%% node <b>p</b> then node <b>p</b> is also in the active view of node <b>q</b>.
%%
%% The active views af all cluster nodes create an overlay that is used for
%% message dissemination. Each node keeps an open TCP connection to every other
%% node in its active view.
%%
%% Broadcast is performed deterministically by flooding the graph defined by
%% the active views across the cluster. When a node receives a message for the
%% first time, it broadcasts the message to all nodes of its active view (
%% except, obviously, to the node that has sent the message).
%% While this graph is generated at random, gossip is deterministic as long as
%% the graph remains unchanged.
%%
%% ==== Active View Management ====
%% A reactive strategy is used to maintain the active view. Nodes can be added
%% to the active view when they join the system. Also, nodes are removed from
%% the active view when they fail. When a node <b>p</b> suspects that one of the
%% nodes present in its active view has failed (by either disconnecting or
%% blocking), it selects a random node <b>q</b> from its passive view and attempts
%% to establish a TCP connection with <b>q</b>. If the connection fails to
%% establish, node <b>q</b> is considered failed and removed from <b>p’s</b>
%% passive view; another node <b>q′</b> is selected at random and a new attempt
%% is made.
%%
%% When the connection is established with success, p sends to q a Neighbor
%% request with its own identifier and a priority level. The priority level of
%% the request may take two values, depending on the number of nodes present in
%% the active view of p: if p has no elements in its active view the priority
%% is high; the priority is low otherwise.
%%
%% A node q that receives a high priority neighbor request will always accept
%% the request, even if it has to drop a random member from its active view (
%% again, the member that is dropped will receive a Disconnect notification).
%% If a node q receives a low priority Neighbor request, it will only accept
%% the request if it has a free slot in its active view, otherwise it will
%% refuse the request.
%%
%% If the node q accepts the Neighbor request, p will remove q’s identifier
%% from its passive view and add it to the active view. If q rejects the
%% Neighbor request, the initiator will select another node from its passive
%% view and repeat the whole procedure (without removing q from its passive
%% view).
%%
%% Each node tests its entire active view every
%% time it forwards a message. Therefore, the entire broadcast overlay is
%% implicitly tested at every broadcast, which allows a very fast failure
%% detection.
%%
%% === Passive View ===
%% In addition to the active view, each node maintains a larger passive view
%% of backup nodes that can be promoted to the active view when one of the
%% nodes in the active view fails.
%%
%% The passive view is not used for message dissemination. Instead, the goal of
%% the passive view is to maintain a list of nodes that can be used to replace
%% failed members of the active view. The passive view is maintained using a
%% cyclic strategy. Periodically, each node performs a shuffle operation with
%% one of its neighbors in order to update its passive view.
%%
%% ==== Passive View Management ====
%%
%% The passive view is maintained using a cyclic strategy. Periodically, each
%% node perform a shuffle operation with one of its peers at random. The
%% purpose of the shuffle operation is to update the passive views of the nodes
%% involved in the exchange. The node p that initiates the exchange creates an
%% exchange list with the following contents: p’s own identifier, ka nodes from
%% its active view and kp nodes from its passive view (where ka and kp are
%% protocol parameters). It then sends the list in a Shuffle request to a
%% random neighbor of its active view. Shuffle requests are propagated using a
%% random walk and have an associated “time to live”, just like the ForwardJoin
%% requests.
%%
%% A node q that receives a Shuffle request will first decrease its time to
%% live. If the time to live of the message is greater than zero and the number
%% of nodes in q’s active view is greater than 1, the node will select a random
%% node from its active view, different from the one he received this shuffle
%% message from, and simply forwards the Shuffle request. Otherwise, node q
%% accepts the Shuffle request and send back, using a temporary TCP connection,
%% a ShuffleReply message that includes a number of nodes selected at random
%% from q’s passive view equal to the number of nodes received in the Shuffle
%% request.
%%
%% Then, both nodes integrate the elements they received in the Shuffle/
%% ShuffleReply mes- sage into their passive views (naturally, they exclude
%% their own identifier and nodes that are part of the active or passive
%% views). Because the passive view has a fixed length, it might get full; in
%% that case, some identifiers will have to be removed in order to free space
%% to include the new ones. A node will first attempt to remove identifiers
%% sent to the peer. If no such identifiers remain in the passive view, it will
%% remove identifiers at random.
%%
%% == Configuration ==
%% The following are the HyParView configuration parameters managed by
%% {@link partisan_config}. The params are passed as `{hyparview, Config}'
%% where `Config' is a property list or map where the keys are the following:
%%
%% <dl>
%% <dt>`active_max_size'</dt><dd>Defaults to 6.</dd>
%% <dt>`active_min_size'</dt><dd>Defaults to 3.</dd>
%% <dt>`active_rwl'</dt><dd>Active View Random Walk Length. Defaults
%% to 6.</dd>
%% <dt>`passive_max_size'</dt><dd>Defaults to 30.</dd>
%% <dt>`passive_rwl'</dt><dd>Passive View Random Walk Length.
%% Defaults to 6.</dd>
%% <dt>`random_promotion'</dt><dd>A boolean indicating if random promotion is
%% enabled. Defaults `true'.</dd>
%% <dt>`random_promotion_interval'</dt><dd>Time after which the
%% protocol attempts to promote a node in the passive view to the active
%% view.Defaults to 5000.</dd>
%% <dt>`shuffle_interval'</dt><dd>Defaults to 10000.</dd>
%% <dt>`shuffle_k_active'</dt><dd>Number of peers to include in the
%% shuffle exchange. Defaults to 3.</dd>
%% <dt>`shuffle_k_passive'</dt><dd>Number of peers to include in the
%% shuffle exchange. Defaults to 4.</dd>
%% </dl>
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_hyparview_peer_service_manager).

-behaviour(gen_server).
-behaviour(partisan_peer_service_manager).

-include("partisan_logger.hrl").
-include("partisan.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").
-author("Bruno Santiago Vazquez <brunosantiagovazquez@gmail.com>").

%% Defaults
-define(SHUFFLE_INTERVAL, 10000).
-define(RANDOM_PROMOTION_INTERVAL, 5000).

-record(state, {
    name                    ::  node(),
    node_spec               ::  partisan:node_spec(),
    config                  ::  config(),
    active                  ::  active(),
    passive                 ::  passive(),
    reserved                ::  reserved(),
    out_links               ::  list(),
    tag                     ::  tag(),
    epoch                   ::  epoch(),
    sent_message_map        ::  message_id_store(),
    recv_message_map        ::  message_id_store(),
    partitions              ::  partisan_peer_service_manager:partitions()
}).

-type t()                   ::  #state{}.
-type active()              ::  sets:set(partisan:node_spec()).
-type passive()             ::  sets:set(partisan:node_spec()).
-type reserved()            ::  #{atom() := partisan:node_spec()}.
-type tag()                 ::  atom().
%% The epoch indicates how many times the node is restarted.
-type epoch()               ::  non_neg_integer().
%% The epoch_count indicates how many disconnect messages are generated.
-type epoch_count()         ::  non_neg_integer().
-type message_id()          ::  {epoch(), epoch_count()}.
-type message_id_store()    ::  #{partisan:node_spec() := message_id()}.
-type call()                ::  {join, partisan:node_spec()}
                                | {leave, partisan:node_spec()}
                                | {update_members, [partisan:node_spec()]}
                                | {resolve_partition, reference()}
                                | {inject_partition,
                                    partisan:node_spec(),
                                    integer()}
                                | {reserve, tag()}
                                | active
                                | passive
                                | {active, tag()}
                                | {send_message, node(), term()}
                                %% | {forward_message, node(), ...}
                                %% | {receive_message, node(), ...}
                                | members
                                | members_for_orchestration
                                | get_local_state
                                | connections
                                | partitions.
-type cast()                ::  {join, partisan:node_spec()}
                                | {receive_message,
                                    partisan:node_spec(),
                                    partisan:channel(),
                                    term()}
                                | {disconnect, partisan:node_spec()}.

%% PARTISAN_PEER_SERVICE_MANAGER CALLBACKS
-export([cast_message/2]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([decode/1]).
-export([forward_message/2]).
-export([forward_message/3]).
-export([forward_message/4]).
-export([get_local_state/0]).
-export([inject_partition/2]).
-export([join/1]).
-export([leave/0]).
-export([leave/1]).
-export([members/0]).
-export([members_for_orchestration/0]).
-export([on_down/2]).
-export([on_down/3]).
-export([on_up/2]).
-export([on_up/3]).
-export([partitions/0]).
-export([receive_message/3]).
-export([reserve/1]).
-export([resolve_partition/1]).
-export([send_message/2]).
-export([start_link/0]).
-export([supports_capability/1]).
-export([sync_join/1]).
-export([update_members/1]).

%% DEBUG API
-export([active/0]).
-export([active/1]).
-export([passive/0]).

%% GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% temporary exceptions
-export([delete_state_from_disk/0]).


%% -----------------------------------------------------------------------------
%% Notes on HyParView protocol
%%
%% <- `join' - A node that receives a `join' request will start by adding the
%% new node to its active view, even if it has to drop a random node from it (
%% `disconnect'). Then it will send to all other nodes in its active view a
%% `forward_join' request containing the new node identifier
%% <- `forward_join' - A message send by a node to all its active view members
%% when it accepts a join request by another node. This message will be
%% propagated in the overlay using a random walk. Associated to the join
%% procedure, there are two configuration parameters, named Active Random Walk
%% Length (ARWL), that specifies the maximum number of hops a `forward_join'
%% request is propagated, and Passive Random Walk Length (PRWL), that specifies
%% at which point in the walk the node is inserted in a passive view. To use
%% these parameters, the `forward_join' request carries a “time to live” field
%% that is initially set to ARWL and decreased at every hop.
%% <- `disconnect' - a Disconnect notification is sent to the node that has been
%% dropped from the active view. This happens when another node joins the
%% active view taking the place of the dropped one (see join).
%% <- `neighbor'
%% <- `neighbor_request'
%% <- `neighbor_rejected'
%% <- `neighbor_accepted'
%% <- `shuffle_reply'
%% <- `shuffle' - Part of the passive view maintenance. Periodically, each node
%% performs a shuffle operation with one of its neighbors in order to update
%% its passive view. One interesting aspect of our shuffle mechanism is that
%% the identifiers that are exchanged in a shuffle operation are not only from
%% the passive view: a node also sends its own identifier and some nodes
%% collected from its active view to its neighbor. This increases the
%% probability of having nodes that are active in the passive views and ensures
%% that failed nodes are eventually expunged from all passive views.
%%
%% -----------------------------------------------------------------------------


%% =============================================================================
%% PARTISAN_PEER_SERVICE_MANAGER CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Start the peer service manager.
%% @end
%% -----------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% -----------------------------------------------------------------------------
%% @doc Return membership list.
%% @end
%% -----------------------------------------------------------------------------
members() ->
    gen_server:call(?MODULE, members, infinity).


%% -----------------------------------------------------------------------------
%% @doc Return membership list.
%% @end
%% -----------------------------------------------------------------------------
members_for_orchestration() ->
    gen_server:call(?MODULE, members_for_orchestration, infinity).


%% -----------------------------------------------------------------------------
%% @doc Decode state.
%% @end
%% -----------------------------------------------------------------------------
decode({state, Active, _Epoch}) ->
    decode(Active);

decode(Active) ->
    sets:to_list(Active).


%% -----------------------------------------------------------------------------
%% @doc Return local node's view of cluster membership.
%% @end
%% -----------------------------------------------------------------------------
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).


%% -----------------------------------------------------------------------------
%% @doc Register a trigger to fire when a connection drops.
%% @end
%% -----------------------------------------------------------------------------
on_down(_Name, _Function) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Register a trigger to fire when a connection drops.
%% @end
%% -----------------------------------------------------------------------------
on_down(_Name, _Function, _Opts) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Register a trigger to fire when a connection opens.
%% @end
%% -----------------------------------------------------------------------------
on_up(_Name, _Function) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Register a trigger to fire when a connection opens.
%% @end
%% -----------------------------------------------------------------------------
on_up(_Name, _Function, _Opts) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Update membership.
%% @end
%% -----------------------------------------------------------------------------
update_members(Members) ->
    gen_server:call(?MODULE, {update_members, Members}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Send message to a remote peer service manager.
%% @end
%% -----------------------------------------------------------------------------
send_message(Name, Message) ->
    Cmd = {send_message, Name, Message},
    gen_server:call(?MODULE, Cmd, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    Term :: partisan_remote_ref:p() | partisan_remote_ref:n() | pid(),
    MEssage :: partisan:message()) -> ok.

cast_message(Term, Message) ->
    FullMessage = {'$gen_cast', Message},
    forward_message(Term, FullMessage, #{}).


%% -----------------------------------------------------------------------------
%% @doc Cast a message to a remote gen_server.
%% @end
%% -----------------------------------------------------------------------------
cast_message(Node, ServerRef, Message) ->
    cast_message(Node, ServerRef, Message, #{}).


%% -----------------------------------------------------------------------------
%% @doc Cast a message to a remote gen_server.
%% @end
%% -----------------------------------------------------------------------------
cast_message(Node, ServerRef, Message, Options) ->
    FullMessage = {'$gen_cast', Message},
    forward_message(Node, ServerRef, FullMessage, Options).


%% -----------------------------------------------------------------------------
%% @doc Gensym support for forwarding.
%% @end
%% -----------------------------------------------------------------------------
forward_message(Term, Message) ->
    forward_message(Term, Message, #{}).


%% -----------------------------------------------------------------------------
%% @doc Gensym support for forwarding.
%% @end
%% -----------------------------------------------------------------------------
forward_message(Pid, Message, Opts) when is_pid(Pid) ->
    forward_message(partisan:node(), Pid, Message, Opts);

forward_message(RemoteRef, Message, Opts) ->
    partisan_remote_ref:is_pid(RemoteRef)
        orelse partisan_remote_ref:is_name(RemoteRef)
        orelse error(badarg),

    Node = partisan_remote_ref:node(RemoteRef),
    Target = partisan_remote_ref:target(RemoteRef),

    forward_message(Node, Target, Message, Opts).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
forward_message(Node, ServerRef, Message, Opts) when is_list(Opts) ->
    forward_message(Node, ServerRef, Message, maps:from_list(Opts));

forward_message(Node, ServerRef, Message, Opts) when is_map(Opts) ->
    ?LOG_TRACE(#{
        description => "About to send message",
        node => partisan:node(),
        process => ServerRef,
        message => Message
    }),

    %% We ignore channel -> Why?
    FullMessage = {forward_message, Node, ServerRef, Message, Opts},

    %% Attempt to fast-path through the memoized connection cache.
    case partisan_peer_connections:dispatch(FullMessage) of
        ok ->
            ok;
        {error, _} ->
            gen_server:call(?MODULE, FullMessage, infinity)
    end.


%% -----------------------------------------------------------------------------
%% @doc Receive message from a remote manager.
%% @end
%% -----------------------------------------------------------------------------
receive_message(Peer, Channel, {forward_message, ServerRef, Msg} = Cmd) ->
    case partisan_config:get(disable_fast_receive, true) of
        true ->
            gen_server:call(
                ?MODULE, {receive_message, Peer, Channel, Cmd}, infinity
            );
        false ->
            partisan_peer_service_manager:process_forward(ServerRef, Msg)
    end;

receive_message(Peer, Channel, Msg) ->
    ?LOG_TRACE(#{
        description => "Manager received message from peer",
        peer_node => Peer,
        channel => Channel,
        message => Msg
    }),

    Result = gen_server:call(
        ?MODULE, {receive_message, Peer, Channel, Msg}, infinity
    ),

    ?LOG_TRACE(#{
        description => "Processed message from peer",
        peer_node => Peer,
        channel => Channel,
        message => Msg
    }),

    Result.


%% -----------------------------------------------------------------------------
%% @doc Attempt to join a remote node.
%% @end
%% -----------------------------------------------------------------------------
join(Node) ->
    gen_server:call(?MODULE, {join, Node}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Attempt to join a remote node.
%% @end
%% -----------------------------------------------------------------------------
sync_join(_Node) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Leave the cluster.
%% @end
%% -----------------------------------------------------------------------------
leave() ->
    gen_server:call(?MODULE, {leave, partisan:node()}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Remove another node from the cluster.
%% @end
%% -----------------------------------------------------------------------------
leave(Node) ->
    gen_server:call(?MODULE, {leave, Node}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Reserve a slot for the particular tag.
%% @end
%% -----------------------------------------------------------------------------
reserve(Tag) ->
    gen_server:call(?MODULE, {reserve, Tag}, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec supports_capability(Arg :: atom()) -> boolean().

supports_capability(monitoring) ->
    false;

supports_capability(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc Inject a partition.
%% @end
%% -----------------------------------------------------------------------------
inject_partition(Origin, TTL) ->
    gen_server:call(?MODULE, {inject_partition, Origin, TTL}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Resolve a partition.
%% @end
%% -----------------------------------------------------------------------------
resolve_partition(Reference) ->
    gen_server:call(?MODULE, {resolve_partition, Reference}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Return partitions.
%% @end
%% -----------------------------------------------------------------------------
partitions() ->
    gen_server:call(?MODULE, partitions, infinity).



%% =============================================================================
%% DEBUGGING API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Debugging.
%% @end
%% -----------------------------------------------------------------------------
active() ->
    gen_server:call(?MODULE, active, infinity).


%% -----------------------------------------------------------------------------
%% @doc Debugging.
%% @end
%% -----------------------------------------------------------------------------
active(Tag) ->
    gen_server:call(?MODULE, {active, Tag}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Debugging.
%% @end
%% -----------------------------------------------------------------------------
passive() ->
    gen_server:call(?MODULE, passive, infinity).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



-spec init([]) -> {ok, t()} | {stop, reservation_limit_exceeded}.

init([]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    %% Trap connection process exits.
    process_flag(trap_exit, true),

    ok = partisan_peer_connections:init(),

    #{name := Name} = NodeSpec = partisan:node_spec(),

    %% Set logger metadata
    logger:set_process_metadata(#{node => Name}),

    Epoch = maybe_load_epoch_from_disk(),
    Active = sets:add_element(NodeSpec, sets:new([{version, 2}])),
    Passive = sets:new([{version, 2}]),
    SentMessageMap = maps:new(),
    RecvMessageMap = maps:new(),
    Partitions = [],

    #{
        active_max_size := ActiveMaxSize,
        active_min_size := _,
        active_rwl := _,
        passive_max_size := _,
        passive_rwl := _,
        random_promotion := _,
        random_promotion_interval := _,
        shuffle_interval := _,
        shuffle_k_active := _,
        shuffle_k_passive := _,
        xbot_enabled := _,
        xbot_interval := _
    } = Config = partisan_config:get(hyparview),

    %% Get tag, if set.
    Tag = partisan_config:get(tag, undefined),

    %% Reserved server slots.
    Reservations = partisan_config:get(reservations, []),
    Reserved = maps:from_list([{T, undefined} || T <- Reservations]),

    %% Verify we don't have too many reservations.
    case length(Reservations) > ActiveMaxSize of
        true ->
            {stop, reservation_limit_exceeded};
        false ->
            State = #state{
                name = Name,
                node_spec = NodeSpec,
                config = Config,
                active = Active,
                passive = Passive,
                reserved = Reserved,
                tag = Tag,
                out_links = [],
                epoch = Epoch + 1,
                sent_message_map = SentMessageMap,
                recv_message_map = RecvMessageMap,
                partitions = Partitions
            },

            %% Schedule periodic maintenance of the passive view.
            schedule_passive_view_maintenance(State),

            %% Schedule periodic execution of xbot algorithm (optimization)
            %% when it is enabled
            schedule_xbot_execution(State),

            %% Schedule tree peers refresh.
            schedule_tree_refresh(State),

            %% Schedule periodic random promotion when it is enabled.
            schedule_random_promotion(State),

            {ok, State}
    end.


-spec handle_call(call(), {pid(), term()}, t()) ->
    {reply, term(), t()}.

handle_call(partitions, _From, State) ->
    {reply, {ok, State#state.partitions}, State};

handle_call({leave, _Node}, _From, State) ->
    {reply, error, State};

handle_call({join, #{name := _Name} = Node}, _From, State) ->
    gen_server:cast(?MODULE, {join, Node}),
    {reply, ok, State};

handle_call({update_members, Members}, _, #state{} = State0) ->
    State = handle_update_members(Members, State0),
    {reply, ok, State};

handle_call({resolve_partition, Reference}, _From, State) ->
    Partitions = handle_partition_resolution(Reference, State),
    {reply, ok, State#state{partitions = Partitions}};

handle_call({inject_partition, Origin, TTL}, _From, State) ->
    Myself = State#state.node_spec,
    Reference = make_ref(),

    ?LOG_DEBUG(#{
        description => "Injecting partition",
        origin => Origin,
        node_spec => Myself,
        ttl => TTL
    }),

    case Origin of
        Myself ->
            Partitions = handle_partition_injection(
                Reference, Origin, TTL, State
            ),
            {reply, {ok, Reference}, State#state{partitions = Partitions}};
        _ ->
            Result = do_send_message(
                Origin,
                {inject_partition, Reference, Origin, TTL}
            ),

            case Result of
                {error, Error} ->
                    {reply, {error, Error}, State};
                ok ->
                    {reply, {ok, Reference}, State}
            end
    end;

handle_call({reserve, Tag}, _From, State) ->
    Reserved0 = State#state.reserved,
    ActiveMaxSize = config_get(active_max_size, State),
    Present = maps:keys(Reserved0),

    case length(Present) < ActiveMaxSize of
        true ->
            Reserved = case lists:member(Tag, Present) of
                true ->
                    Reserved0;
                false ->
                    maps:put(Tag, undefined, Reserved0)
            end,
            {reply, ok, State#state{reserved = Reserved}};

        false ->
            {reply, {error, no_available_slots}, State}
    end;

handle_call(active, _From, State) ->
    {reply, {ok, State#state.active}, State};

handle_call({active, Tag}, _From, State) ->
    Result = case maps:find(Tag, State#state.reserved) of
        {ok, #{name := Peer}} ->
            {ok, Peer};
        {ok, undefined} ->
            {ok, undefined};
        error ->
            error
    end,
    {reply, Result, State};

handle_call(passive, _From, State) ->
    {reply, {ok, State#state.passive}, State};

handle_call({send_message, Name, Msg}, _From, State) ->
    Result = do_send_message(Name, Msg),
    {reply, Result, State};

handle_call({forward_message, Name, ServerRef, Msg, Opts}, _From, State) ->
    Partitions = State#state.partitions,
    IsPartitioned = lists:any(
        fun({_, #{name := N}}) ->
            case N of
                Name ->
                    true;
                _ ->
                  false
            end
        end,
        Partitions
    ),
    case IsPartitioned of
        true ->
            {reply, {error, partitioned}, State};
        false ->
            Result = do_send_message(
                Name,
                {forward_message, ServerRef, Msg},
                Opts
            ),
            {reply, Result, State}
    end;

handle_call({receive_message, _, _, _} = Cmd, _From, State) ->
    %% This is important, we immediately cast the message to ourselves to
    %% unblock the calling process (partisan_peer_service_server who manages
    %% the socket).
    %% TODO: We should consider rewriting receive_message/2 to use
    %% gen_server:cast directly! Erlang guarantees the delivery
    %% order
    %% See Issue #5
    gen_server:cast(?MODULE, Cmd),
    {reply, ok, State};

handle_call(members, _From, State) ->
    Active = State#state.active,
    Members = members(Active),

    ?LOG_DEBUG(#{
        description => "Node active view",
        node_spec => State#state.node_spec,
        members => members(Active)
    }),

    Nodes = [Node || #{name := Node} <- Members],

    {reply, {ok, Nodes}, State};

handle_call(members_for_orchestration, _From, State) ->
    {reply, {ok, members(State)}, State};

handle_call(get_local_state, _From, State) ->
    Active = State#state.active,
    Epoch = State#state.epoch,
    {reply, {ok, {state, Active, Epoch}}, State};

handle_call(connections, _From, State) ->
    %% get a list of all the client connections to the various peers of the
    %% active view
    Cs = lists:map(
        fun(Peer) ->
            Pids = partisan_peer_connections:processes(Peer),
            ?LOG_DEBUG(#{
                description => "Peer connection processes",
                peer_node => Peer,
                connection_processes => Pids
            }),
            {Peer, Pids}
        end,
        peers(State)
    ),
    {reply, {ok, Cs}, State};

handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.


-spec handle_cast(cast(), t()) -> {noreply, t()}.

handle_cast({join, Peer}, State) ->
    Myself = State#state.node_spec,
    Tag = State#state.tag,
    Epoch = State#state.epoch,

    %% Trigger connection.
    ok = partisan_peer_service_manager:connect(Peer),

    ?LOG_DEBUG(#{
        description => "Sending JOIN message",
        node => Myself,
        peer_node => Peer
    }),

    %% Send the JOIN message to the peer.
    %% REVIEW we currently ignore errors, shouldn't we return them?
    _ = do_send_message(Peer, {join, Myself, Tag, Epoch}),
    {noreply, State};

handle_cast({receive_message, _Peer, Channel, Message}, State) ->
    handle_message(Message, Channel, State);

handle_cast({disconnect, Peer}, State0) ->
    Active0 = State0#state.active,

    case sets:is_element(Peer, Active0) of
        true ->
            %% If a member of the active view, remove it.
            Active = sets:del_element(Peer, Active0),
            State = add_to_passive_view(
                Peer,
                State0#state{active = Active}
            ),
            ok = disconnect(Peer),
            {noreply, State};

        false ->
            {noreply, State0}
    end;

handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),
    {noreply, State}.


-spec handle_info(term(), t()) -> {noreply, t()}.

handle_info(random_promotion, State0) ->
    Myself = State0#state.node_spec,
    Active0 = State0#state.active,
    Passive = State0#state.passive,
    Reserved0 = State0#state.reserved,
    ActiveMinSize0 = config_get(active_min_size, State0),

    Limit = has_reached_limit({active, Active0, Reserved0}, ActiveMinSize0),

    State = case Limit of
        true ->
            %% Do nothing if the active view reaches the ActiveMinSize.
            State0;
        false ->
            Peer = pick_random(Passive, [Myself]),
            promote_peer(Peer, State0)
    end,

    %% Schedule periodic random promotion.
    schedule_random_promotion(State),

    {noreply, State};

handle_info(tree_refresh, State) ->
    %% Get lazily computed outlinks.
    OutLinks = retrieve_outlinks(State#state.name),

    %% Reschedule.
    schedule_tree_refresh(State),

    {noreply, State#state{out_links = OutLinks}};

handle_info(passive_view_maintenance, State0) ->
    %% The passive view is maintained using a cyclic strategy. Periodically,
    %% each node perform a shuffle operation with one of its peers at random.
    %% The purpose of the shuffle operation is to update the passive views of
    %% the nodes involved in the exchange. The node p that initiates the
    %% exchange creates an exchange list with the following contents:
    %% - p’s own identifier (node_spec()),
    %% - ka nodes from its active view (shuffle_k_active), and
    %% - kp nodes from its passive view (shuffle_k_passive)
    %% (where ka and kp are protocol parameters).
    Myself = State0#state.node_spec,
    Active = State0#state.active,

    Exchange = select_peers_for_exchange(State0),

    %% Select random member of the active list to send the shuffle message to
    State = case pick_random(Active, [Myself]) of
        undefined ->
            State0;
        Peer ->
            %% Trigger connection.
            ok = partisan_peer_service_manager:connect(Peer),

            %% Forward shuffle request.
            ARWL = config_get(active_rwl, State0),
            do_send_message(Peer, {shuffle, Exchange, ARWL, Myself}),

            State0
    end,

    %% Reschedule.
    schedule_passive_view_maintenance(State),

    {noreply, State};

% handle optimization using xbot algorithm
handle_info(xbot_execution, #state{} = State) ->

    Active = State#state.active,
    Passive = State#state.passive,
    Reserved = State#state.reserved,
    ActiveMaxSize = config_get(active_max_size, State),

	% check if active view is full
	case is_full({active, Active, Reserved}, ActiveMaxSize) of
		% if full, check for candidates and try to optimize
		true ->
			Candidates = pick_random(Passive, 2),
			send_optimization_messages(members(Active), Candidates, State);
		% in other case, do nothing
		false -> ok
	end,

	%In any case, schedule periodic xbot execution algorithm (optimization)
	ok = schedule_xbot_execution(State),
	{noreply, State};

handle_info({'EXIT', Pid, Reason}, State0) when is_pid(Pid) ->
    ?LOG_DEBUG(#{
        description => "Active view connection process died.",
        process => Pid,
        reason => Reason
    }),

    Myself = State0#state.node_spec,
    Active0 = State0#state.active,
    Passive0 = State0#state.passive,

    %% Prune active connections from map.
    try partisan_peer_connections:prune(Pid) of
        {Info, _Connections} ->
            Peer = partisan_peer_connections:node_spec(Info),
            %% If it was in the passive view and our connection attempt failed,
            %% remove from the passive view altogether.
            Passive = case is_in_passive_view(Peer, Passive0) of
                true ->
                    remove_from_passive_view(Peer, Passive0);
                false ->
                    Passive0
            end,

            %% If it was in the active view and our connection attempt failed,
            %% remove from the active view altogether.
            {Active, RemovedFromActive} =
                case is_in_active_view(Peer, Active0) of
                    true ->
                        {remove_from_active_view(Peer, Active0), true};
                    false ->
                        {Active0, false}
                end,

            State = case RemovedFromActive of
                true ->
                    RandomPeer = pick_random(Passive, [Myself]),
                    promote_peer(
                        RandomPeer, State0#state{active=Active, passive=Passive}
                    );
                false ->
                    State0#state{active=Active, passive=Passive}
            end,

            ?LOG_DEBUG(#{
                description => "Active view",
                node_spec => Myself,
                active_view => members(State)
            }),

            {noreply, State}
    catch
        error:badarg ->
            {noreply, State0}
    end;

handle_info(
    {connected, Peer, _Channel, _Tag, _PeerEpoch, _RemoteState}, State) ->
    ?LOG_DEBUG(#{
        description => "Node is now connected",
        peer_node => Peer
    }),

    {noreply, State};

handle_info(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled info event", event => Event}),
    {noreply, State}.


-spec terminate(term(), t()) -> term().

terminate(_Reason, _State) ->
    Fun =
        fun(_Info, Connections) ->
            lists:foreach(
              fun(Connection) ->
                    Pid = partisan_peer_connections:pid(Connection),
                    catch gen_server:stop(Pid, normal, infinity),
                    ok
              end,
              Connections
            )
         end,
    ok = partisan_peer_connections:foreach(Fun).


-spec code_change(term() | {down, term()}, t(), term()) ->
    {ok, t()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
handle_message({resolve_partition, Reference}, _, State) ->
    Partitions = handle_partition_resolution(Reference, State),
    {noreply, State#state{partitions = Partitions}};

handle_message({inject_partition, Reference, Origin, TTL}, _, State) ->
    Partitions = handle_partition_injection(Reference, Origin, TTL, State),
    {noreply, State#state{partitions = Partitions}};

handle_message(
    {join, Peer, PeerTag, PeerEpoch},
    _Channel,
   #state{node_spec=Myself0,
          active=Active0,
          tag=Tag0,
          sent_message_map=SentMessageMap0,
          recv_message_map=RecvMessageMap0}=State0) ->
    ?LOG_DEBUG(#{
        description => "Node is now connected",
        node_spec => Myself0,
        peer_node => Peer,
        peer_epoch => PeerEpoch
    }),

    IsAddable = is_addable(PeerEpoch, Peer, SentMessageMap0),
    NotInActiveView = not sets:is_element(Peer, Active0),
    State = case IsAddable andalso NotInActiveView of
        true ->
            ?LOG_DEBUG(#{
                description => "Adding peer node to the active view",
                peer_node => Peer
            }),
            %% Establish connections.
            ok = partisan_peer_service_manager:connect(Peer),
            Connected = partisan_peer_connections:is_connected(Peer),
            case Connected of
                true ->
                    %% only find the peer connection will add the peer to the
                    %% active
                    %% Add to active view.
                    State1 = add_to_active_view(Peer, PeerTag, State0),
                    LastDisconnectId = get_current_id(Peer, RecvMessageMap0),
                    %% Send the NEIGHBOR message to origin, that will update
                    %% it's view.
                    do_send_message(
                        Peer,
                        {neighbor, Myself0, Tag0, LastDisconnectId, Peer}
                    ),

                    %% Random walk for forward join.
                    %% Since we might have dropped peers from the active view
                    %% when adding this one we need to use the most up to date
                    %% active view, and that's the one that's currently in the
                    %% state also disregard the the new joiner node
                    Peers =
                        (members(State1) -- [Myself0]) -- [Peer],

                    ok = lists:foreach(
                        fun(P) ->
                            %% Establish connections.
                            ok = partisan_peer_service_manager:connect(P),

                            ?LOG_DEBUG(#{
                                description =>
                                    "Forwarding join of to active view peer",
                                from => Peer,
                                to => P
                            }),

                            ARWL = config_get(active_rwl, State1),

                            Message = {
                                forward_join,
                                Peer,
                                PeerTag,
                                PeerEpoch,
                                ARWL,
                                Myself0
                            },

                            do_send_message(P, Message),
                            ok
                        end,
                        Peers
                    ),

                    ?LOG_DEBUG(
                        fun([S]) ->
                            #{
                                description => "Active view",
                                node_spec => Myself0,
                                active_view => members(S)
                            }
                        end,
                        [State1]
                    ),

                    %% Notify with event.
                    notify(State1),
                    State1;

                false ->
                    State0
            end;

        false ->
            ?LOG_DEBUG(#{
                description => "Peer node will not be added to the active view",
                peer_node => Peer
            }),
            State0
    end,

    {noreply, State};

handle_message({neighbor, Peer, PeerTag, DisconnectId, _Sender},
               _Channel,
               #state{node_spec=Myself0,
                      sent_message_map=SentMessageMap0}=State0) ->
    ?LOG_DEBUG(#{
        description => "Node received the NEIGHBOR message from peer",
        node_spec => Myself0,
        peer_node => Peer,
        peer_tag =>  PeerTag
    }),

    State =
        case is_addable(DisconnectId, Peer, SentMessageMap0) of
            true ->
                %% Establish connections.
                ok = partisan_peer_service_manager:connect(Peer),

                case partisan_peer_connections:is_connected(Peer) of
                    true ->
                        %% Add node into the active view.
                        State1 = add_to_active_view(
                            Peer, PeerTag, State0
                        ),
                        ?LOG_DEBUG(#{
                            description => "Active view",
                            node_spec => Myself0,
                            active_view => members(State1)
                        }),
                        State1;
                    false ->
                        State0
                end;
            false ->
                State0
        end,

    %% Notify with event.
    notify(State),

    {noreply, State};

handle_message({forward_join, Peer, PeerTag, PeerEpoch, TTL, Sender},
               _Channel,
               #state{node_spec=Myself0,
                      active=Active0,
                      tag=Tag0,
                      sent_message_map=SentMessageMap0,
                      recv_message_map=RecvMessageMap0}=State0) ->
    %% When a node p receives a forward_join, it performs the following steps
    %% in sequence:
    %% i) If the time to live is equal to zero or if the number of
    %% nodes in p’s active view is equal to one, it will add the new node to
    %% its active view. This step is performed even if a random node must be
    %% dropped from the active view. In the later case, the node being ejected
    %% from the active view receives a disconnect notification.
    %% ii) If the time to live is equal to PRWL, p will insert the new node
    %% into its passive view.
    %% iii) The time to live field is decremented. iv) If, at this point,
    %% n has not been inserted in p’s active view, p will forward the request
    %% to a random node in its active view (different from the one from which
    %% the request was received).

    ?LOG_DEBUG("
        Node ~p received the FORWARD_JOIN message from ~p about ~p",
        [Myself0, Sender, Peer]
    ),

    ActiveViewSize = sets:size(Active0),

    State = case TTL =:= 0 orelse ActiveViewSize =:= 1 of
        true ->
            ?LOG_DEBUG(
                "FORWARD_JOIN: ttl(~p) expired or only one peer in "
                "active view (~p), "
                "adding ~p tagged ~p to active view",
                [TTL, ActiveViewSize, Peer, PeerTag]
            ),

            IsAddable0 = is_addable(PeerEpoch, Peer, SentMessageMap0),
            NotInActiveView0 = not sets:is_element(Peer, Active0),

            case IsAddable0 andalso NotInActiveView0 of
                true ->
                    %% Establish connections.
                    ok = partisan_peer_service_manager:connect(Peer),

                    case partisan_peer_connections:is_connected(Peer) of
                        true ->
                            %% Add to our active view.
                            State1 = add_to_active_view(Peer, PeerTag, State0),

                            LastDisconnectId = get_current_id(
                                Peer, RecvMessageMap0
                            ),
                            %% Send neighbor message to origin, that will
                            %% update it's view.
                            Message = {
                                neighbor,
                                Myself0, Tag0, LastDisconnectId, Peer
                            },

                            do_send_message(Peer, Message),

                            ?LOG_DEBUG(#{
                                description => "Active view",
                                node_spec => Myself0,
                                active_view => members(State1)
                            }),

                            State1;

                        false ->
                            State0
                    end;

                false ->
                    ?LOG_DEBUG(
                        "Peer node ~p will not be added to the active view",
                        [Peer]
                    ),
                    State0
            end;

        false ->
            %% If we run out of peers before we hit the PRWL, that's
            %% fine, because exchanges between peers will eventually
            %% repair the passive view during shuffles.
            PRWL = config_get(passive_rwl, State0),

            State2 = case TTL =:= PRWL of
                true ->
                    ?LOG_DEBUG(
                        "FORWARD_JOIN: Passive walk ttl expired, "
                        "adding ~p to the passive view",
                        [Peer]
                    ),
                    add_to_passive_view(Peer, State0);

                false ->
                    State0
             end,

            %% Don't forward the join to the sender, ourself, or the joining
            %% peer.
            case pick_random(Active0, [Sender, Myself0, Peer]) of
                undefined ->
                    IsAddable1 = is_addable(PeerEpoch, Peer, SentMessageMap0),
                    NotInActiveView1 = not sets:is_element(Peer, Active0),

                    case IsAddable1 andalso NotInActiveView1 of
                        true ->
                            ?LOG_DEBUG(
                                "FORWARD_JOIN: No node for forward, "
                                "adding ~p to active view",
                                [Peer]
                            ),
                            %% Establish connections.
                            ok = partisan_peer_service_manager:connect(Peer),

                            case partisan_peer_connections:is_connected(Peer) of
                                true ->
                                    %% Add to our active view.
                                    State3 = add_to_active_view(
                                        Peer, PeerTag, State2
                                    ),
                                    LastDisconnectId = get_current_id(
                                        Peer, RecvMessageMap0
                                    ),
                                    %% Send neighbor message to origin, that
                                    %% will update it's view.
                                    Message = {
                                        neighbor,
                                        Myself0,
                                        Tag0,
                                        LastDisconnectId,
                                        Peer
                                    },
                                    do_send_message(Peer, Message),

                                    ?LOG_DEBUG(#{
                                        description => "Active view",
                                        node_spec => Myself0,
                                        active_view => members(State3)
                                    }),

                                    State3;

                                false ->
                                    State0
                            end;

                        false ->
                            ?LOG_DEBUG(
                                "Peer node ~p will not be added to the "
                                "active view",
                                [Peer]
                            ),
                            State2
                    end;

                Random ->
                    %% Establish any new connections.
                    ok = partisan_peer_service_manager:connect(Random),

                    ?LOG_DEBUG("FORWARD_JOIN: forwarding to ~p", [Random]),

                    Message = {forward_join,
                        Peer,
                        PeerTag,
                        PeerEpoch,
                        TTL - 1,
                        Myself0
                    },

                    %% Forward join.
                    do_send_message(Random, Message),

                    State2
            end
    end,

    %% Notify with event.
    notify(State),
    {noreply, State};

handle_message({disconnect, Peer, DisconnectId},
               _Channel,
               #state{node_spec=Myself0,
                      active=Active0,
                      passive=Passive,
                      recv_message_map=RecvMessageMap0}=State0) ->
    ?LOG_DEBUG("Node ~p received the DISCONNECT message from ~p with ~p",
               [Myself0, Peer, DisconnectId]),

    case is_valid_disconnect(Peer, DisconnectId, RecvMessageMap0) of
        false ->
            %% Ignore the older disconnect message.
            {noreply, State0};
        true ->
            %% Remove from active
            Active = sets:del_element(Peer, Active0),

            ?LOG_DEBUG(#{
                description => "Active view",
                node_spec => Myself0,
                active_view => members(Active)
            }),

            %% Add to passive view.
            State1 = add_to_passive_view(Peer,
                                         State0#state{active=Active}),

            %% Update the AckMessageMap.
            RecvMessageMap = maps:put(Peer, DisconnectId, RecvMessageMap0),

            %% Trigger disconnection.
            ok = disconnect(Peer),

            State =
                case sets:size(Active) == 1 of
                    true ->
                        %% the peer that disconnected us just got moved to the
                        %% passive view, exclude it when selecting a new one to
                        %% move back into the active view
                        RandomPeer = pick_random(Passive, [Myself0, Peer]),
                        ?LOG_DEBUG(
                            "Node ~p is isolated, moving random peer ~p "
                            "from passive to active view",
                            [RandomPeer, Myself0]
                        ),
                        promote_peer(RandomPeer,
                            State1#state{recv_message_map=RecvMessageMap});
                    false ->
                        State1#state{recv_message_map=RecvMessageMap}
                end,

            {noreply, State}
    end;

handle_message(
    {neighbor_request, Peer, Priority, PeerTag, DisconnectId, Exchange},
    _Channel,
    #state{} = State0) ->

    Myself0 = State0#state.node_spec,
    Tag0 = State0#state.tag,
    SentMessageMap0 = State0#state.sent_message_map,
    RecvMessageMap0 = State0#state.recv_message_map,

    ?LOG_DEBUG(
        "Node ~p received the NEIGHBOR_REQUEST message from ~p with ~p",
        [Myself0, Peer, DisconnectId]
    ),

    %% Establish connections.
    ok = partisan_peer_service_manager:connect(Peer),

    Exchange_Ack = select_peers_for_exchange(State0),

    State2 =
        case neighbor_acceptable(Priority, PeerTag, State0) of
            true ->
                case is_addable(DisconnectId, Peer, SentMessageMap0) of
                    true ->
                        Connected = partisan_peer_connections:is_connected(
                            Peer
                        ),
                        case Connected of
                            true ->
                                ?LOG_DEBUG(
                                    "Node ~p accepted neighbor peer ~p",
                                    [Myself0, Peer]
                                ),
                                LastDisconnectId =
                                    get_current_id(Peer, RecvMessageMap0),
                                %% Reply to acknowledge the neighbor was
                                %% accepted.
                                do_send_message(
                                  Peer,
                                  {
                                    neighbor_accepted,
                                    Myself0,
                                    Tag0,
                                    LastDisconnectId,
                                    Exchange_Ack
                                    }
                                ),

                                State1 = add_to_active_view(
                                    Peer, PeerTag, State0
                                ),
                                ?LOG_DEBUG(#{
                                    description => "Active view",
                                    node_spec => Myself0,
                                    active_view => members(State1)
                                }),

                                State1;
                            false ->
                                %% the connections does not change, the peer
                                %% can not be connected
                                State0
                        end;
                    false ->
                        ?LOG_DEBUG(
                            "Node ~p rejected neighbor peer ~p",
                            [Myself0, Peer]
                        ),

                        %% Reply to acknowledge the neighbor was rejected.
                        do_send_message(
                            Peer, {neighbor_rejected, Myself0, Exchange_Ack}
                        ),

                        State0
                end;
            false ->
                ?LOG_DEBUG(
                    "Node ~p rejected neighbor peer ~p",
                    [Myself0, Peer]
                ),
                %% Reply to acknowledge the neighbor was rejected.
                do_send_message(Peer, {neighbor_rejected, Myself0}),
                State0
        end,

    State = merge_exchange(Exchange, State2),

    %% Notify with event.
    notify(State),

    {noreply, State};

handle_message({neighbor_rejected, Peer, Exchange},
               _Channel,
               #state{node_spec=Myself0} = State0) ->
    ?LOG_DEBUG("Node ~p received the NEIGHBOR_REJECTED message from ~p",
               [Myself0, Peer]),

    %% Trigger disconnection.
    ok = disconnect(Peer),

    State = merge_exchange(Exchange, State0),

    {noreply, State};

handle_message({neighbor_accepted, Peer, PeerTag, DisconnectId, Exchange},
               _Channel,
               #state{node_spec=Myself0,
                      sent_message_map=SentMessageMap0} = State0) ->
    ?LOG_DEBUG(
        "Node ~p received the NEIGHBOR_ACCEPTED message from ~p with ~p",
        [Myself0, Peer, DisconnectId]
    ),

    State1 = case is_addable(DisconnectId, Peer, SentMessageMap0) of
                 true ->
                     %% Add node into the active view.
                     add_to_active_view(Peer, PeerTag, State0);
                 false ->
                     State0
             end,

    State = merge_exchange(Exchange, State1),

    %% Notify with event.
    notify(State),

    {noreply, State};

handle_message({shuffle_reply, Exchange, _Sender}, _Channel, State0) ->
    State = merge_exchange(Exchange, State0),
    {noreply, State};

handle_message({shuffle, Exchange, TTL, Sender},
               _Channel,
               #state{node_spec=Myself,
                      active=Active0,
                      passive=Passive0}=State0) ->
    ?LOG_DEBUG(
        "Node ~p received the SHUFFLE message from ~p",
        [Myself, Sender]
    ),
    %% Forward to random member of the active view.
    State = case TTL > 0 andalso sets:size(Active0) > 1 of
        true ->
            State1 = case pick_random(Active0, [Sender, Myself]) of
                         undefined ->
                             State0;
                         Random ->
                             %% Trigger connection.
                             ok = partisan_peer_service_manager:connect(Random),

                             %% Forward shuffle until random walk complete.
                             do_send_message(
                                 Random,
                                {shuffle, Exchange, TTL - 1, Myself}
                            ),

                             State0
                     end,

            State1;
        false ->
            %% Randomly select nodes from the passive view and respond.
            ResponseExchange = shuffle(members(Passive0), length(Exchange)),

            %% Trigger connection.
            ok = partisan_peer_service_manager:connect(Sender),

            do_send_message(
                Sender,
                {shuffle_reply, ResponseExchange, Myself}
            ),

            State2 = merge_exchange(Exchange, State0),
            State2
    end,
    {noreply, State};

handle_message({relay_message, NodeSpec, Message, TTL}, Channel, #state{} = State) ->
    ?LOG_TRACE(
        "Node ~p received tree relay to ~p", [partisan:node(), NodeSpec]
    ),

    OutLinks = State#state.out_links,

    ActiveMembers = [P || #{name := P} <- members(State)],

    Opts = #{
        out_links => OutLinks,
        channel => Channel
    },

    case lists:member(NodeSpec, ActiveMembers) of
        true ->
            do_send_message(NodeSpec, Message, Opts#{transitive => true});
        false ->
            case TTL of
                0 ->
                    %% No longer forward.
                    ?LOG_DEBUG(
                        "TTL expired, dropping message for node ~p: ~p",
                        [NodeSpec, Message]
                    ),
                    ok;
                _ ->
                    do_tree_forward(NodeSpec, Message, Opts, TTL),
                    ok
            end
    end,

    {noreply, State};

handle_message({forward_message, ServerRef, Message}, _Channel, State) ->
    partisan_peer_service_manager:process_forward(ServerRef, Message),
    {noreply, State};

handle_message(
    {optimization_reply, true, _, Initiator, Candidate, undefined},
    _Channel,
    State) ->
	#{name := MyName} = Initiator,
	#{name := CandidateName} = Candidate,
	?LOG_DEBUG(
        "XBOT: Received optimization reply message at Node ~p from ~p",
        [MyName, CandidateName]
    ),
	%% Revise this behaviour, when candidate accepts immediately because it has
    %% availability in his active view
	%% what to do with old node?? we cannot disconnect from it because maybe it
    %% will be isolated
	%Check = is_in_active_view(OldNode, Active),
	%if Check ->
		%remove_from_active_view(OldNode, Active),
		%add_to_passive_view(OldNode, State)
	%	do_disconnect(OldNode, State)
	%end,
	%promote_peer(Candidate, State),
	_ = send_join(Candidate, State),

	?LOG_DEBUG(
        "XBOT: Finished optimization round started by Node ~p ",
        [MyName]
    ),
	{noreply, State};

handle_message(
    {optimization_reply, true, OldNode, Initiator, Candidate, _},
    _Channel,
    #state{} = State) ->
    Active = State#state.active,
	#{name := InitiatorName} = Initiator,
	#{name := CandidateName} = Candidate,

    ?LOG_DEBUG(
        "XBOT: Received optimization reply message at Node ~p from ~p",
        [InitiatorName, CandidateName]
    ),

	case is_in_active_view(OldNode, Active) of
        true ->
		  %remove_from_active_view(OldNode, Active);
		  do_disconnect(OldNode, State);
		false ->
            ok
	end,

	%% promote_peer(Candidate, State),
	_ = send_join(Candidate, State),

    ?LOG_DEBUG(
        "XBOT: Finished optimization round started by Node ~p ",
        [InitiatorName]
    ),
	{noreply, State};

handle_message({optimization_reply, false, _, _, _, _}, _Channel, State) ->
	{noreply, State};

handle_message(
    {optimization, _, OldNode, Initiator, Candidate, undefined},
    _Channel,
    #state{} = State) ->
    Active = State#state.active,
    Reserved = State#state.reserved,
    ActiveMaxSize = config_get(active_max_size, State),
	#{name := CandidateName} = Candidate,
	#{name := InitiatorName} = Initiator,
	?LOG_DEBUG(
        "XBOT: Received optimization message at Node ~p from ~p",
        [CandidateName, InitiatorName]
    ),

	Check = is_full({active, Active, Reserved}, ActiveMaxSize),
	if not Check ->
			%add_to_active_view(Candidate, MyTag, State),
			_ = send_join(Initiator, State),

			ok = partisan_peer_service_manager:connect(Initiator),

            Message = {
                optimization_reply,
                true,
                OldNode,
                Initiator,
                Candidate,
                undefined
            },

			_ = do_send_message(Initiator, Message),

			?LOG_DEBUG(
                "XBOT: Sending optimization reply message to Node ~p from ~p", [InitiatorName, CandidateName]
            );

		true ->
			DisconnectNode = select_disconnect_node(sets:to_list(Active)),
			#{name := DisconnectName} = DisconnectNode,

			ok = partisan_peer_service_manager:connect(DisconnectNode),

            Message = {
                replace,
                undefined,
                OldNode,
                Initiator,
                Candidate,
                DisconnectNode
            },
			_ = do_send_message(DisconnectNode, Message),
			?LOG_DEBUG(
                "XBOT: Sending replace message to Node ~p from ~p", [DisconnectName, CandidateName]
            )
	end,
	{noreply, State};

handle_message(
    {replace_reply, true, OldNode, Initiator, Candidate, DisconnectNode},
    _Channel,
    #state{} = State) ->
	#{name := InitiatorName} = Initiator,
	#{name := DisconnectName} = DisconnectNode,
	#{name := CandidateName} = Candidate,
	?LOG_DEBUG("XBOT: Received replace reply message at Node ~p from ~p", [CandidateName, DisconnectName]),
	%remove_from_active_view(DisconnectNode, Active),
	do_disconnect(DisconnectNode, State),
	%add_to_active_view(Initiator, MyTag, State),
	_ = send_join(Initiator, State),
	ok = partisan_peer_service_manager:connect(Initiator),
	_ = do_send_message(Initiator,{optimization_reply, true, OldNode, Initiator, Candidate, DisconnectNode}),
	?LOG_DEBUG("XBOT: Sending optimization reply to Node ~p from ~p", [InitiatorName, CandidateName]),
	{noreply, State};

handle_message(
    {replace_reply, false, OldNode, Initiator, Candidate, DisconnectNode},_Channel,
    #state{} = State) ->
	#{name := InitiatorName} = Initiator,
	#{name := DisconnectName} = DisconnectNode,
	#{name := CandidateName} = Candidate,
	?LOG_DEBUG("XBOT: Received replace reply message at Node ~p from ~p", [CandidateName, DisconnectName]),
	ok = partisan_peer_service_manager:connect(Initiator),
	_ = do_send_message(Initiator,{optimization_reply, false, OldNode, Initiator, Candidate, DisconnectNode}),
	?LOG_DEBUG("XBOT: Sending optimization reply to Node ~p from ~p", [InitiatorName, CandidateName]),
	{noreply, State};

handle_message(
    {replace, _, OldNode, Initiator, Candidate, DisconnectNode},
    _Channel,
    #state{} = State) ->
	#{name := DisconnectName} = DisconnectNode,
	#{name := CandidateName} = Candidate,
	#{name := OldName} = OldNode,
	?LOG_DEBUG("XBOT: Received replace message at Node ~p from ~p", [DisconnectName, CandidateName]),
	Check = is_better(?HYPARVIEW_XBOT_ORACLE, OldNode, Candidate),
	if not Check ->
			ok = partisan_peer_service_manager:connect(Candidate),
			_ = do_send_message(Candidate,{replace_reply, false, OldNode, Initiator, Candidate, DisconnectNode}),
			?LOG_DEBUG("XBOT: Sending replace reply to Node ~p from ~p", [CandidateName, DisconnectName]);
		true ->
			ok = partisan_peer_service_manager:connect(OldNode),
			_ = do_send_message(OldNode,{switch, undefined, OldNode, Initiator, Candidate, DisconnectNode}),
			?LOG_DEBUG("XBOT: Sending switch to Node ~p from ~p", [OldName, DisconnectName])
	end,
	{noreply, State};

handle_message(
    {switch_reply, true, OldNode, Initiator, Candidate, DisconnectNode},
    _Channel,
    #state{} = State) ->
	#{name := DisconnectName} = DisconnectNode,
	#{name := CandidateName} = Candidate,
	#{name := OldName} = OldNode,
	?LOG_DEBUG("XBOT: Received switch reply message at Node ~p from ~p", [DisconnectName, OldName]),
	%remove_from_active_view(Candidate, Active),
	do_disconnect(Candidate, State),
	%add_to_active_view(OldNode, MyTag, State),
	_ = send_join(OldNode, State),
	ok = partisan_peer_service_manager:connect(Candidate),
	_ = do_send_message(Candidate,{replace_reply, true, OldNode, Initiator, Candidate, DisconnectNode}),
	?LOG_DEBUG("XBOT: Sending replace reply to Node ~p from ~p", [CandidateName, DisconnectName]),
	{noreply, State};

handle_message(
    {switch_reply, false, OldNode, Initiator, Candidate, DisconnectNode},
    _Channel,
    #state{} = State) ->
	#{name := DisconnectName} = DisconnectNode,
	#{name := CandidateName} = Candidate,
	#{name := OldName} = OldNode,
	?LOG_DEBUG("XBOT: Received switch reply message at Node ~p from ~p", [DisconnectName, OldName]),
	ok = partisan_peer_service_manager:connect(Candidate),
	_ = do_send_message(Candidate, {replace_reply, false, OldNode, Initiator, Candidate, DisconnectNode}),
	?LOG_DEBUG("XBOT: Sending replace reply to Node ~p from ~p", [CandidateName, DisconnectName]),
	{noreply, State};

handle_message(
    {switch, _, OldNode, Initiator, Candidate, DisconnectNode},
    _Channel,
	#state{active = Active} = State) ->
	#{name := DisconnectName} = DisconnectNode,
	#{name := OldName} = OldNode,
	?LOG_DEBUG("XBOT: Received switch message at Node ~p from ~p", [OldName, DisconnectName]),
	Check = is_in_active_view(Initiator, Active),
	if Check ->
			%remove_from_active_view(Initiator, Active),
			do_disconnect(Initiator, State),
			%add_to_active_view(DisconnectNode, MyTag, State),
			_ = send_join(DisconnectNode, State),
			ok = partisan_peer_service_manager:connect(DisconnectNode),
			_ = do_send_message(DisconnectNode, {switch_reply, true, OldNode, Initiator, Candidate, DisconnectNode}),
			?LOG_DEBUG("XBOT: Sending switch reply to Node ~p from ~p", [DisconnectName, OldName]);
		true ->
			ok = partisan_peer_service_manager:connect(DisconnectNode),
			_ = do_send_message(DisconnectNode, {switch_reply, false, OldNode, Initiator, Candidate, DisconnectNode}),
			?LOG_DEBUG("XBOT: Sending switch reply to Node ~p from ~p", [DisconnectName, OldName])
	end,
	{noreply, State}.

%% @private
send_join(Peer, #state{node_spec=Myself0, tag=Tag0, epoch=Epoch0}) ->
    %% Trigger connection.
    ok = partisan_peer_service_manager:connect(Peer),

    ?LOG_DEBUG("Node ~p sends the JOIN message to ~p", [Myself0, Peer]),

    %% Send the JOIN message to the peer.
    do_send_message(Peer, {join, Myself0, Tag0, Epoch0}).


%% @private
zero_epoch() ->
    Epoch = 0,
    persist_epoch(Epoch),
    Epoch.


%% @private
data_root() ->
    case application:get_env(partisan, partisan_data_dir) of
        {ok, PRoot} ->
            filename:join(PRoot, "peer_service");
        undefined ->
            undefined
    end.


%% @private
write_state_to_disk(Epoch) ->
    case data_root() of
        undefined ->
            ok;
        Dir ->
            File = filename:join(Dir, "cluster_state"),
            ok = filelib:ensure_dir(File),
            ok = file:write_file(File, term_to_binary(Epoch))
    end.


%% @private
delete_state_from_disk() ->
    case data_root() of
        undefined ->
            ok;
        Dir ->
            File = filename:join(Dir, "cluster_state"),
            ok = filelib:ensure_dir(File),
            case file:delete(File) of
                ok ->
                    ?LOG_DEBUG(#{
                        description => "Leaving cluster, removed cluster_state"
                    });
                {error, Reason} ->
                    ?LOG_DEBUG(
                        "Unable to remove cluster_state for reason ~p",
                        [Reason]
                    )
            end
    end.


%% @private
maybe_load_epoch_from_disk() ->
    case data_root() of
        undefined ->
            zero_epoch();
        Dir ->
            case filelib:is_regular(filename:join(Dir, "cluster_state")) of
                true ->
                    {ok, Bin} = file:read_file(filename:join(Dir, "cluster_state")),
                    binary_to_term(Bin);
                false ->
                    zero_epoch()
            end
    end.


%% @private
persist_epoch(Epoch) ->
    write_state_to_disk(Epoch).


%% @private
members(#state{active = Set}) ->
    members(Set);

members(Set) ->
    sets:to_list(Set).


%% @private
peers(#state{active = Set, node_spec = NodeSpec}) ->
    sets:to_list(sets:del_element(NodeSpec, Set)).


%% @private
-spec disconnect(Node :: partisan:node_spec()) -> ok.

disconnect(Node) ->
    try partisan_peer_connections:prune(Node) of
        {_Info, Connections} ->
            [
                begin
                    Pid = partisan_peer_connections:pid(Connection),
                    ?LOG_DEBUG(
                        "disconnecting node ~p by stopping connection pid ~p",
                        [Node, Pid]
                    ),
                    unlink(Pid),
                    _ = catch gen_server:stop(Pid)
                end
                || Connection <- Connections
            ],
            ok
    catch
        error:badarg ->
            ok
    end.


%% @private
-spec do_send_message(
    Node :: atom() | partisan:node_spec(),
    Message :: partisan:message()) ->
    ok | {error, disconnected} | {error, not_yet_connected} | {error, term()}.

do_send_message(Node, Message) ->
    do_send_message(Node, Message, #{}).


%% @private
-spec do_send_message(
    Node :: atom() | partisan:node_spec(),
    Message :: partisan:message(),
    Options :: map()) ->
    ok | {error, disconnected} | {error, not_yet_connected} | {error, term()}.

do_send_message(Node, Message, Options) when is_atom(Node) ->
    %% TODO Shouldn't broadcast default to true?
    %% otherwise we will only forward to
    %% nodes in the active view that are connected.
    %% Also why do we have 2 options
    Broadcast = partisan_config:get(broadcast, false),
    Transitive = maps:get(transitive, Options, false),

    case partisan_peer_connections:dispatch_pid(Node) of
        {ok, Pid} ->
            %% We have a connection to the destination Node.
            try
                gen_server:call(Pid, {send_message, Message})
            catch
                Class:EReason ->
                    ?LOG_DEBUG(
                        "failed to send a message to ~p due to ~p:~p",
                        [Node, Class, EReason]
                    ),
                    {error, EReason}
            end;

        {error, Reason} ->
            case Reason of
                not_yet_connected ->
                    ?LOG_DEBUG(#{
                        description =>
                            "Node not yet connected to peer node "
                            "when sending message.",
                        message => Message,
                        peer_node => Node,
                        options => #{
                            broadcast => Broadcast,
                            transitive => Transitive
                        }
                    });
                disconnected ->
                    ?LOG_DEBUG(#{
                        description =>
                            "Node disconnected to peer node "
                            "when sending message.",
                        message => Message,
                        peer_node => Node,
                        options => #{
                            broadcast => Broadcast,
                            transitive => Transitive
                        }
                    })
            end,

            %% TODO use retransmission and acks
            case {Broadcast, Transitive} of
                {true, true} ->
                    TTL = partisan_config:get(relay_ttl, ?RELAY_TTL),
                    do_tree_forward(Node, Message, Options, TTL);

                {_, _} ->
                    {error, Reason}
            end
    end;

do_send_message(#{name := Node}, Message, Options) ->
    do_send_message(Node, Message, Options).


%% @private
pick_random(View, Omit) ->
    List = members(View) -- lists:flatten([Omit]),

    %% Catch exceptions where there may not be enough members.
    try
        Index = rand:uniform(length(List)),
        lists:nth(Index, List)
    catch
        _:_ ->
            undefined
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Returns a list of `K' values extracted randomly from `L'.
%% @end
%% -----------------------------------------------------------------------------
shuffle(L, K) when is_list(L) ->
    %% We use maps instead of lists:sort/1 which is faster for longer lists
    %% and uses less memory (erts_debug:size/1).
    maps:values(
        maps:from_list(
            lists:sublist([{rand:uniform(), N} || N <- L], K)
        )
    ).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
select_peers_for_exchange(#state{} = State) ->
    Myself = State#state.node_spec,
    Active = State#state.active,
    Passive = State#state.passive,
    KActive = config_get(shuffle_k_active, State),
    KPassive = config_get(shuffle_k_passive, State),

    L = [Myself | shuffle(members(Active), KActive)]
        ++ shuffle(members(Passive), KPassive),
    lists:usort(L).


%% @doc Add to the active view.
%%
%% However, interesting race condition here: if the passive random walk
%% timer exceeded and the node was added to the passive view, we might
%% also have the active random walk timer exceed *after* because of a
%% network delay; if so, we have to remove this element from the passive
%% view, otherwise it will exist in both places.
%%
add_to_active_view(#{name := Name}=Peer, Tag,
                   #state{active=Active0,
                          node_spec=Myself,
                          passive=Passive0,
                          reserved=Reserved0}=State0) ->

    ActiveMaxSize = config_get(active_max_size, State0),
    IsNotMyself = not (Name =:= partisan:node()),
    NotInActiveView = not sets:is_element(Peer, Active0),

    case IsNotMyself andalso NotInActiveView of
        true ->
            %% See above for more information.
            Passive = remove_from_passive_view(Peer, Passive0),
            State1 = State0#state{passive = Passive},

            IsFull = is_full({active, Active0, Reserved0}, ActiveMaxSize),
            State2 = case IsFull of
                true ->
                    drop_random_element_from_active_view(State1);
                false ->
                    State1
            end,

            ?LOG_DEBUG(
                "Node ~p adds ~p to active view with tag ~p",
                [Myself, Peer, Tag]
            ),

            %% Add to the active view.
            Active = sets:add_element(Peer, State2#state.active),

            %% Fill reserved slot if necessary.
            Reserved = case maps:find(Tag, Reserved0) of
                {ok, undefined} ->
                    ?LOG_DEBUG(#{
                        description => "Node added to reserved slot!"
                    }),
                    maps:put(Tag, Peer, Reserved0);

                {ok, _} ->
                    %% Slot already filled, treat this as a normal peer.
                    ?LOG_DEBUG(#{
                        description =>
                            "Node added to active view, "
                            "but reserved slot already full!"
                    }),
                    Reserved0;

                error ->
                    ?LOG_DEBUG("Tag is not reserved: ~p ~p", [Tag, Reserved0]),
                    Reserved0
            end,

            State = State2#state{
                active = Active,
                passive = Passive,
                reserved = Reserved
            },

            persist_epoch(State#state.epoch),

            State;

        false ->
            State0
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Add to the passive view.
%% @end
%% -----------------------------------------------------------------------------
add_to_passive_view(#{name := Name} = Peer, #state{} = State0) ->

    Myself = State0#state.node_spec,
    Active0 = State0#state.active,
    Passive0 = State0#state.passive,

    IsNotMyself = not (Name =:= partisan:node()),

    NotInActiveView = not sets:is_element(Peer, Active0),
    NotInPassiveView = not sets:is_element(Peer, Passive0),

    Allowed = IsNotMyself andalso NotInActiveView andalso NotInPassiveView,

    Passive = case Allowed of
        true ->
            PassiveMaxSize = config_get(passive_max_size, State0),

            Passive1 = case is_full({passive, Passive0}, PassiveMaxSize) of
                true ->
                    Random = pick_random(Passive0, [Myself]),
                    sets:del_element(Random, Passive0);
                false ->
                    Passive0
            end,
            sets:add_element(Peer, Passive1);
        false ->
            Passive0
    end,

    State = State0#state{passive = Passive},
    persist_epoch(State#state.epoch),
    State.


%% @private
is_full({active, Active, Reserved}, MaxSize) ->
    %% Find the slots that are reserved, but not filled.
    Open = maps:fold(
        fun
            (Key, undefined, Acc) ->
                [Key | Acc];
            (_, _, Acc) ->
                Acc
        end,
        [],
        Reserved
    ),
    sets:size(Active) + length(Open) >= MaxSize;

is_full({passive, Passive}, MaxSize) ->
    sets:size(Passive) >= MaxSize.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Process of removing a random element from the active view.
%% @end
%% -----------------------------------------------------------------------------
drop_random_element_from_active_view(
        #state{node_spec=Myself0,
               active=Active0,
               reserved=Reserved0,
               epoch=Epoch0,
               sent_message_map=SentMessageMap0}=State0) ->
    ReservedPeers = maps:fold(fun(_K, V, Acc) -> [V | Acc] end,
                              [],
                              Reserved0),
    %% Select random peer, but omit the peers in reserved slots and omit
    %% ourself from the active view.
    case pick_random(Active0, [Myself0, ReservedPeers]) of
        undefined ->
            State0;
        Peer ->
            ?LOG_DEBUG("Removing and disconnecting peer: ~p", [Peer]),

            %% Remove from the active view.
            Active = sets:del_element(Peer, Active0),

            %% Add to the passive view.
            State = add_to_passive_view(Peer, State0#state{active = Active}),

            %% Trigger connection.
            ok = partisan_peer_service_manager:connect(Peer),

            %% Get next disconnect id for the peer.
            NextId = get_next_id(Peer, Epoch0, SentMessageMap0),
            %% Update the SentMessageMap.
            SentMessageMap = maps:put(Peer, NextId, SentMessageMap0),

            %% Let peer know we are disconnecting them.
            do_send_message(Peer, {disconnect, Myself0, NextId}),

            %% Trigger disconnection.
            ok = disconnect(Peer),

            ?LOG_DEBUG(
                fun([A]) ->
                    #{
                        description => "Active view",
                        node_spec => Myself0,
                        active_view => members(A)
                    }
                end,
                [Active]
            ),

            State#state{sent_message_map = SentMessageMap}
    end.


%% @private
remove_from_passive_view(Peer, Passive) ->
    sets:del_element(Peer, Passive).


%% @private
is_in_passive_view(Peer, Passive) ->
    sets:is_element(Peer, Passive).


%% @private
remove_from_active_view(Peer, Active) ->
    sets:del_element(Peer, Active).


%% @private
is_in_active_view(Peer, Active) ->
    sets:is_element(Peer, Active).


%% @private
neighbor_acceptable(high, _, _) ->
    %% Always true.
    true;

neighbor_acceptable(_, Tag, #state{} = State) ->
    Reserved = State#state.reserved,

    case reserved_slot_available(Tag, Reserved) of
        true ->
            %% Always take.
            true;
        _ ->
            %% Otherwise, only if we have a slot available.
            Active = State#state.active,
            ActiveMaxSize = config_get(active_max_size, State),
            not is_full({active, Active, Reserved}, ActiveMaxSize)
    end.


%% @private
merge_exchange(Exchange, #state{} = State) ->
    %% Remove ourself and active set members from the exchange.
    Myself = State#state.node_spec,
    Active = State#state.active,
    ToAdd = lists:usort(Exchange -- ([Myself] ++ members(Active))),

    %% Add to passive view.
    lists:foldl(fun(X, P) -> add_to_passive_view(X, P) end, State, ToAdd).


%% @private
handle_update_members(Members, State) ->
    merge_exchange(Members, State).


%% @private
notify(#state{active = Active}) ->
    _ = catch partisan_peer_service_events:update(Active),
    ok.


%% @private
reserved_slot_available(Tag, Reserved) ->
    case maps:find(Tag, Reserved) of
        {ok, undefined} ->
            true;
        _ ->
            false
    end.


%% %% @private
%%remove_from_reserved(Peer, Reserved) ->
%%    maps:fold(fun(K, V, Acc) ->
%%                      case V of
%%                          Peer ->
%%                              Acc;
%%                          _ ->
%%                              maps:put(K, V, Acc)
%%                      end
%%              end, maps:new(), Reserved).


%% @private
get_current_id(Peer, MessageMap) ->
    case maps:find(Peer, MessageMap) of
        {ok, Id} ->
            Id;
        error ->
            %% Default value for the messageId:
            %% {First start, No disconnect}
            {1, 0}
    end.


%% @private
get_next_id(Peer, MyEpoch, SentMessageMap) ->
    case maps:find(Peer, SentMessageMap) of
        {ok, {MyEpoch, Cnt}} ->
            {MyEpoch, Cnt + 1};
        error ->
            {MyEpoch, 1}
    end.


%% @private
is_valid_disconnect(Peer, {IdEpoch, IdCnt}, AckMessageMap) ->
    case maps:find(Peer, AckMessageMap) of
        error ->
            true;
        {ok, {Epoch, Cnt}} ->
            case IdEpoch > Epoch of
                true ->
                    true;
                false ->
                    IdCnt > Cnt
            end
    end.


%% @private
is_addable({IdEpoch, IdCnt}, Peer, SentMessageMap) ->
    case maps:find(Peer, SentMessageMap) of
        error ->
            true;
        {ok, {Epoch, Cnt}} ->
            case IdEpoch > Epoch of
                true ->
                    true;
                false when IdEpoch == Epoch ->
                    IdCnt >= Cnt;
                false ->
                    false
            end
    end;

is_addable(PeerEpoch, Peer, SentMessageMap) ->
    case maps:find(Peer, SentMessageMap) of
        error ->
            true;
        {ok, {Epoch, _Cnt}} ->
            PeerEpoch >= Epoch
    end.


%% @private
promote_peer(undefined, State) ->
    State;

promote_peer(Peer, #state{} = State) ->
    Myself = State#state.node_spec,
    Tag = State#state.tag,
    RecvMessageMap0 = State#state.recv_message_map,

    ?LOG_DEBUG("Node ~p sends the NEIGHBOR_REQUEST to ~p", [Myself, Peer]),

    Exchange = select_peers_for_exchange(State),

    %% Trigger connection.
    ok = partisan_peer_service_manager:connect(Peer),

    LastDisconnectId = get_current_id(Peer, RecvMessageMap0),

    do_send_message(
        Peer,
        {neighbor_request, Myself, high, Tag, LastDisconnectId, Exchange}
    ),

    State.


%% @private
has_reached_limit({active, Active, Reserved}, LimitActiveSize) ->
    %% Find the slots that are reserved, but not filled.
    Open = maps:fold(
        fun(Key, Value, Acc) ->
            case Value of
                undefined ->
                    [Key | Acc];
                _ ->
                    Acc
            end
         end,
         [],
         Reserved
    ),
    sets:size(Active) + length(Open) >= LimitActiveSize.


%% @private
propagate_partition_injection(Ref, Origin, TTL, Peer) ->
    ?LOG_DEBUG("Forwarding partition request to: ~p", [Peer]),

    do_send_message(Peer, {inject_partition, Ref, Origin, TTL}).


%% @private
propagate_partition_resolution(Reference, Peer) ->
    ?LOG_DEBUG("Forwarding partition request to: ~p", [Peer]),

    do_send_message(Peer, {resolve_partition, Reference}).


%% @private
handle_partition_injection(Reference, _Origin, TTL, #state{} = State) ->
    Myself = State#state.node_spec,
    Members = members(State#state.active),
    Partitions0 = State#state.partitions,

    %% If the TTL hasn't expired, re-forward the partition injection
    %% request.
    case TTL > 0 of
        true ->
            [
                propagate_partition_injection(Reference, Myself, TTL - 1, Peer)
                || Peer <- Members
            ];
        false ->
            ok
    end,

    %% Update partition table marking all immediate neighbors as
    %% partitioned.
    Partitions0 ++ lists:map(
        fun(Peer) ->
            {Reference, Peer}
        end,
        Members
    ).


%% @private
handle_partition_resolution(Reference, #state{} = State) ->
    Members = members(State#state.active),
    Partitions0 = State#state.partitions,

    %% Remove partitions.
    Partitions =
        lists:foldl(
            fun({Ref, Peer}, Acc) ->
                case Reference of
                    Ref ->
                        Acc;
                    _ ->
                        Acc ++ [{Ref, Peer}]
                end
            end,
            [],
            Partitions0
        ),

    %% If the list hasn't changed, then don't further propagate
    %% the message.
    case Partitions == Partitions0 of
        true ->
            ok;
        false ->
            [
                propagate_partition_resolution(Reference, Peer)
                || Peer <- Members
            ]
    end,

    Partitions.


%% @private
do_tree_forward(Node, Message, Options, TTL) ->
    MyNode = partisan:node(),

    ?LOG_TRACE(
        "Attempting to forward message ~p from ~p to ~p.",
        [Message, MyNode, Node]
    ),

    %% Preempt with user-supplied outlinks.
    OutLinks = case maps:get(out_links, Options, undefined) of
        undefined ->
            try retrieve_outlinks(MyNode) of
                Value ->
                    Value
            catch
                _:Reason ->
                    ?LOG_INFO(#{
                        description => "Outlinks retrieval failed",
                        reason => Reason
                    }),
                    []
            end;
        OL ->
            OL -- [MyNode]
    end,

    %% Send messages, but don't attempt to forward again if we aren't
    %% connected.
    _ = lists:foreach(
        fun(Relay) ->
            ?LOG_TRACE(
                "Forwarding relay message ~p to node ~p "
                "for node ~p from node ~p",
                [Message, Relay, Node, MyNode]
            ),

            RelayMessage = {relay_message, Node, Message, TTL - 1},

            do_send_message(
                Relay,
                RelayMessage,
                maps:without([transitive], Options)
            )
        end,
        OutLinks
    ),
    ok.


%% @private
retrieve_outlinks(Root) ->
    ?LOG_TRACE(#{description => "About to retrieve outlinks..."}),

    Result = partisan_plumtree_broadcast:debug_get_peers(Root, Root, 1000),

    OutLinks =
        try Result of
            {EagerPeers, _LazyPeers} ->
                ordsets:to_list(EagerPeers)
        catch
            _:_ ->
                ?LOG_INFO(#{
                    description => "Request to get outlinks timed out..."
                }),
                []
        end,

    ?LOG_TRACE("Finished getting outlinks: ~p", [OutLinks]),

    OutLinks -- [Root].



%% =============================================================================
%% PRIVATE CONFIG
%% =============================================================================



%% @private
config_get(Key, #state{config = C}) ->
    maps:get(Key, C).



%% =============================================================================
%% PRIVATE VIEW MAINTENANCE SCHEDULING
%% =============================================================================


%% @private
schedule_tree_refresh(_State) ->
    case partisan_config:get(broadcast, false) of
        true ->
            Time = partisan_config:get(tree_refresh, 1000),
            erlang:send_after(Time, ?MODULE, tree_refresh);
        false ->
            ok
    end.


%% @private
schedule_passive_view_maintenance(State) ->
    Time = config_get(shuffle_interval, State),
    erlang:send_after(Time, ?MODULE, passive_view_maintenance).


%% @private
schedule_random_promotion(#state{config = #{random_promotion := true} = C}) ->
    Time = maps:get(random_promotion_interval, C),
    erlang:send_after(Time, ?MODULE, random_promotion);

schedule_random_promotion(_) ->
    ok.



%% =============================================================================
%% %% PRIVATE VIEW MAINTENANCE: X-BOT OPTIMIZATION
%% =============================================================================



%% @private
schedule_xbot_execution(#state{config = #{xbot_enabled := true} = C}) ->
    Time = maps:get(xbot_interval, C),
    erlang:send_after(Time, ?MODULE, xbot_execution);

schedule_xbot_execution(_) ->
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Send optimization messages when apply
%% @end
%% -----------------------------------------------------------------------------
send_optimization_messages(_, [], _) ->
    ok;

send_optimization_messages(Active, L, State) ->
    % check each first candidate against every node in the active view
    _ = [process_candidate(Active, X, State) || X <- L],
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc check if a candidate is valid and send message to try optimization
%% we only send an optimization messages for one node in active view, once we
%% have sent it we stop searching possibilities
%% @end
%% -----------------------------------------------------------------------------
process_candidate([], _, _) ->
    ok;

process_candidate([H|T], Candidate, #state{} = State) ->
    #{name := MyName} = Myself = State#state.node_spec,
    #{name := CandidateName} = Candidate,

    case is_better(?HYPARVIEW_XBOT_ORACLE, Candidate, H) of
        true ->
            ok = partisan_peer_service_manager:connect(Candidate),

            %% if candidate is better that first node in active view,
            %% send optimization message
            Msg = {optimization, undefined, H, Myself, Candidate, undefined},
            _ = do_send_message(Candidate, Msg),

            ?LOG_DEBUG(
                "XBOT: Optimization message sent to Node ~p from ~p",
                [CandidateName, MyName]
            );
       false ->
            process_candidate(T, Candidate, State)
    end.




%% -----------------------------------------------------------------------------
%% @private
%% @doc Determine if New node is better than Old node based on ping (latency)
%% @end
%% -----------------------------------------------------------------------------
is_better(latency, #{name := NewNodeName}, #{name := OldNodeName}) ->
    is_better_node_by_latency(timer:tc(net_adm, ping, [NewNodeName]), timer:tc(net_adm, ping, [OldNodeName]));

is_better(_, _, _) ->
    true.

%% @private
is_better_node_by_latency({_, pang}, {_, _}) ->
    %% if we do not get ping response from new node
    false;
is_better_node_by_latency({_, pong}, {_, pang}) ->
    %% if we cannot get response from old node but we got response from new (this should never happen, in general)
    true;
is_better_node_by_latency({NewTime, pong}, {OldTime, pong}) ->
    ?LOG_DEBUG("XBOT: Checking is better - OldTime ~p - NewTime ~p", [OldTime, NewTime]),
    %% otherwise check lower ping response
    (OldTime-NewTime) > 0.

%% @private
select_disconnect_node([H | T]) ->
    select_worst_in_active_view(T, H).

%% @private
select_worst_in_active_view([], Worst) ->
    Worst;
select_worst_in_active_view([H | T], Worst) ->
    Check = is_better(?HYPARVIEW_XBOT_ORACLE, H, Worst),
    if Check -> select_worst_in_active_view(T, Worst);
        true -> select_worst_in_active_view(T, H)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Send a disconnect message to a peer
%% @end
%% -----------------------------------------------------------------------------
do_disconnect(Peer, #state{active=Active0}=State0) ->
    case sets:is_element(Peer, Active0) of
        true ->
            %% If a member of the active view, remove it.
            Active = sets:del_element(Peer, Active0),
            State = add_to_passive_view(Peer, State0#state{active=Active}),
            ok = disconnect(Peer),
            {noreply, State};
        false ->
            {noreply, State0}
    end.





