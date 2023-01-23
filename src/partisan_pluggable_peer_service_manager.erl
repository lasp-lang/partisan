%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Christopher Meiklejohn.  All Rights Reserved.
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

%% -----------------------------------------------------------------------------
%% @doc This module realises the {@link partisan_peer_service_manager}
%% behaviour implementing a full-mesh topology.
%%
%% == Characteristics ==
%% <ul>
%% <li>Uses TCP/IP.</li>
%% <li>All nodes communicate and maintain connections with all other nodes.</li>
%% <li>Nodes periodically send heartbeat messages. The service considers a node
%% "failed" when it misses X heartbeats.</li>
%% <li>Point-to-point messaging with a single network hop.</li>
%% <li>Eventually consistent membership maintained in a CRDT and replicated
%% using gossip.</li>
%% <li>Scalability limited to hundres of nodes (60-200 nodes).</li>
%% </ul>
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_pluggable_peer_service_manager).

-behaviour(gen_server).
-behaviour(partisan_peer_service_manager).

-include("partisan_logger.hrl").
-include("partisan.hrl").


-define(SET_FROM_LIST(L), sets:from_list(L, [{version, 2}])).


-type on_change_fun()  ::  partisan_peer_service_manager:on_change_fun().
-type from()                    ::  {pid(), atom()}.
-type interposition_arg()       ::  {receive_message, node(), any()}.
-type interposition_fun()       ::  fun(
                                        (interposition_arg()) ->
                                            interposition_arg()
                                    ).
-type pre_post_interposition_fun()       ::  fun(
                                        (interposition_arg()) -> ok
                                    ).



-record(state, {
    actor                       ::  partisan:actor(),
    distance_metrics            ::  map(),
    vclock                      ::  term(),
    pending                     ::  [partisan:node_spec()],
    membership                  ::  [partisan:node_spec()],
    down_functions              ::  #{'_' | node() => on_change_fun()},
    up_functions                ::  #{'_' | node() => on_change_fun()},
    out_links                   ::  [term()],
    pre_interposition_funs      ::  #{any() => pre_post_interposition_fun()},
    interposition_funs          ::  #{any() => interposition_fun()},
    post_interposition_funs     ::  #{any() => pre_post_interposition_fun()},
    sync_joins                  ::  [{partisan:node_spec(), from()}],
    membership_strategy         ::  atom(),
    membership_strategy_state   ::  term()
}).

-type t() :: #state{}.


%% API
-export([member/1]).

%% PARTISAN_PEER_SERVICE_MANAGER CALLBACKS
-export([add_interposition_fun/2]).
-export([add_post_interposition_fun/2]).
-export([add_pre_interposition_fun/2]).
-export([cast_message/2]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([decode/1]).
-export([forward_message/2]).
-export([forward_message/3]).
-export([forward_message/4]).
-export([get_interposition_funs/0]).
-export([get_local_state/0]).
-export([get_pre_interposition_funs/0]).
-export([inject_partition/2]).
-export([join/1]).
-export([leave/0]).
-export([leave/1]).
-export([members/0]).
-export([members_for_orchestration/0]).
-export([on_down/2]).
-export([on_up/2]).
-export([partitions/0]).
-export([receive_message/2]).
-export([remove_interposition_fun/1]).
-export([remove_post_interposition_fun/1]).
-export([remove_pre_interposition_fun/1]).
-export([reserve/1]).
-export([resolve_partition/1]).
-export([send_message/2]).
-export([start_link/0]).
-export([supports_capability/1]).
-export([sync_join/1]).
-export([update_members/1]).


%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Returns true if node `Node' is a member in the membership list.
%% Otherwise returns `false'.
%% @end
%% -----------------------------------------------------------------------------
member(#{name := Node}) ->
    member(Node);

member(Node) when is_atom(Node) ->
    gen_server:call(?MODULE, {member, Node}, infinity).



%% =============================================================================
%% PARTISAN_PEER_SERVICE_MANAGER CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Same as start_link([]).
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
%% @doc Update membership.
%% @end
%% -----------------------------------------------------------------------------
update_members(NodeSpecs) ->
    gen_server:call(?MODULE, {update_members, NodeSpecs}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Return local node's view of cluster membership.
%% @end
%% -----------------------------------------------------------------------------
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection close for a given node.
%% `Function' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_down(#{name := Node}, Function) ->
    on_down(Node, Function);

on_down(any, Function) ->
    on_down('_', Function);

on_down(Node, Function)
when is_atom(Node) andalso (
    is_function(Function, 0) orelse is_function(Function, 1)
) ->
    gen_server:call(?MODULE, {on_down, Node, Function}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection open for a given node.
%% `Function' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_up(#{name := Node}, Function) ->
    on_up(Node, Function);

on_up(any, Function) ->
    on_up('_', Function);

on_up(Node, Function)
when is_atom(Node) andalso (
    is_function(Function, 0) orelse
    is_function(Function, 1) orelse
    is_function(Function, 2)
) ->
    gen_server:call(?MODULE, {on_up, Node, Function}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Attempt to join a remote node.
%% @end
%% -----------------------------------------------------------------------------
join(#{name := _} = NodeSpec) ->
    gen_server:call(?MODULE, {join, NodeSpec}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Attempt to join a remote node.
%% @end
%% -----------------------------------------------------------------------------
sync_join(#{name := _} = NodeSpec) ->
    gen_server:call(?MODULE, {sync_join, NodeSpec}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Leave the cluster.
%% @end
%% -----------------------------------------------------------------------------
leave() ->
    gen_server:call(?MODULE, {leave, partisan:node_spec()}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Remove another node from the cluster.
%% @end
%% -----------------------------------------------------------------------------
leave(#{name := _} = NodeSpec) ->
    gen_server:call(?MODULE, {leave, NodeSpec}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Send message to a remote peer service manager.
%% @end
%% -----------------------------------------------------------------------------
send_message(Node, Message) ->
    Cmd = {send_message, Node, ?DEFAULT_CHANNEL, Message},
    gen_server:call(?MODULE, Cmd, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    Term :: partisan_remote_ref:p() | partisan_remote_ref:n() | pid(),
    Message :: partisan:message()) -> ok.

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
    %% If attempting to forward to the local node or using disterl, bypass.
    ProcessFwd =
        Node =:= partisan:node()
        orelse partisan_config:get(connect_disterl, false),

    case ProcessFwd of
        true ->
            partisan_peer_service_manager:process_forward(ServerRef, Message);
        false ->
            Channel = maps:get(channel, Opts, ?DEFAULT_CHANNEL),

            %% Attempt to get the partition key, if possible.
            PartitionKey = maps:get(
                partition_key, Opts, ?DEFAULT_PARTITION_KEY
            ),

            %% Use a clock provided by the sender,
            %% otherwise, use a generated one.
            Clock = maps:get(clock, Opts, undefined),

            %% Should ack?
            ShouldAck = maps:get(ack, Opts, false),

            %% Use causality?
            CausalLabel =
                maps:get(causal_label, Opts, undefined),

            %% Get forwarding options and combine with message
            %% specific options.
            ForwardOptions = maps:merge(Opts, forward_opts()),

            %% Use configuration to disable fast forwarding.
            DisableFastForward =
                partisan_config:get(disable_fast_forward, false),

            %% Should use fast forwarding?
            %%
            %% Conditions:
            %% - not labeled for causal delivery
            %% - message does not need acknowledgements
            %% - fastforward is not disabled
            FastForward =
                not (CausalLabel =/= undefined)
                andalso not ShouldAck
                andalso not DisableFastForward,

            BinaryPadding = partisan_config:get(binary_padding, false),

            PaddedMessage = case BinaryPadding of
                true ->
                    Term = partisan_config:get(
                        binary_padding_term, undefined
                    ),
                    {'$partisan_padded', Term, Message};
                false ->
                    Message

            end,

            Cmd = {
                forward_message,
                Node,
                Channel,
                Clock,
                PartitionKey,
                ServerRef,
                PaddedMessage,
                ForwardOptions
            },

            case FastForward of
                true ->
                    %% Attempt to fast-path by accessing the connection
                    %% directly
                    case partisan_peer_connections:dispatch(Cmd) of
                        ok ->
                            ok;
                        {error, _} ->
                            gen_server:call(?MODULE, Cmd, infinity)
                    end;
                false ->
                    gen_server:call(?MODULE, Cmd, infinity)
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc Receive message from a remote manager.
%% @end
%% -----------------------------------------------------------------------------
receive_message(
    Node,
    {forward_message, _SrcNode, _Clock, _ServerRef, _Message} = Cmd) ->
    %% Process the message and generate the acknowledgement.
    gen_server:call(?MODULE, {receive_message, Node, Cmd}, infinity);

receive_message(
    Node,
    {forward_message, ServerRef, {'$partisan_padded', _Padding, Message}}) ->
    receive_message(Node, {forward_message, ServerRef, Message});

receive_message(
    _,
    {forward_message, _ServerRef, {causal, Label, _, _, _, _, _} = Message}) ->
    partisan_causality_backend:receive_message(Label, Message);

receive_message(Node, {forward_message, ServerRef, Message} = Cmd) ->
    case partisan_config:get(disable_fast_receive, false) of
        true ->
            gen_server:call(?MODULE, {receive_message, Node, Cmd}, infinity);
        false ->
            partisan_peer_service_manager:process_forward(ServerRef, Message)
    end;

receive_message(Node, Message) ->
    gen_server:call(?MODULE, {receive_message, Node, Message}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Decode state.
%% @end
%% -----------------------------------------------------------------------------
decode(Membership) ->
    Membership.

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
    true;

supports_capability(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
add_pre_interposition_fun(Name, PreInterpositionFun) ->
    gen_server:call(
        ?MODULE,
        {add_pre_interposition_fun, Name, PreInterpositionFun},
        infinity
    ).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
remove_pre_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_pre_interposition_fun, Name}, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
add_interposition_fun(Name, InterpositionFun) ->
    gen_server:call(
        ?MODULE, {add_interposition_fun, Name, InterpositionFun}, infinity
    ).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
remove_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_interposition_fun, Name}, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
get_interposition_funs() ->
    gen_server:call(?MODULE, get_interposition_funs, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
get_pre_interposition_funs() ->
    gen_server:call(?MODULE, get_pre_interposition_funs, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
add_post_interposition_fun(Name, PostInterpositionFun) ->
    gen_server:call(
        ?MODULE,
        {add_post_interposition_fun, Name, PostInterpositionFun},
        infinity
    ).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
remove_post_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_post_interposition_fun, Name}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Inject a partition.
%% @end
%% -----------------------------------------------------------------------------
inject_partition(_Origin, _TTL) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Resolve a partition.
%% @end
%% -----------------------------------------------------------------------------
resolve_partition(_Reference) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Return partitions.
%% @end
%% -----------------------------------------------------------------------------
partitions() ->
    {error, not_implemented}.



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



-spec init([]) -> {ok, t()}.

init([]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    case partisan_config:get(binary_padding, false) of
        true ->
            %% Use 64-byte binary to force shared heap usage to cut down on copying.
            BinaryPaddingTerm = rand_bits(512),
            partisan_config:set(binary_padding_term, BinaryPaddingTerm);
        _ ->
            undefined
    end,

    %% Process connection exits.
    process_flag(trap_exit, true),

    %% Schedule periodic.
    schedule_periodic(),

    %% Schedule instrumentation.
    schedule_instrumentation(),

    %% Schedule distance metric.
    schedule_distance(),

    %% Schedule periodic connections.
    schedule_connections(),

    %% Schedule periodic retransmissionj.
    schedule_retransmit(),

    %% Schedule tree peers refresh.
    schedule_tree_refresh(),

    Actor = gen_actor(),
    VClock = partisan_vclock:fresh(),

    %% We init the connections table and we become owners, if we crash the
    %% table will be destroyed.
    ok = partisan_peer_connections:init(),

    MStrategy = partisan_config:get(membership_strategy),

    {ok, Membership, MStrategyState} = MStrategy:init(Actor),


    {ok, #state{
        actor = Actor,
        pending = [],
        vclock = VClock,
        pre_interposition_funs =  #{},
        interposition_funs =  #{},
        post_interposition_funs =  #{},
        distance_metrics = #{},
        sync_joins = [],
        up_functions = #{},
        down_functions = #{},
        out_links = [],
        membership = Membership,
        membership_strategy = MStrategy,
        membership_strategy_state = MStrategyState
    }}.


-spec handle_call(term(), {pid(), term()}, t()) ->
    {reply, term(), t()}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call(
    {on_up, Name, Function},
    _From,
    #state{up_functions = UpFunctions0} = State) ->
    UpFunctions = partisan_util:maps_append(Name, Function, UpFunctions0),
    {reply, ok, State#state{up_functions=UpFunctions}};

handle_call(
    {on_down, Name, Function},
    _From,
    #state{down_functions = DownFunctions0} = State) ->
    DownFunctions = partisan_util:maps_append(Name, Function, DownFunctions0),
    {reply, ok, State#state{down_functions=DownFunctions}};

handle_call(
    {add_pre_interposition_fun, Name, PreInterpositionFun},
    _From,
    #state{pre_interposition_funs = PreInterpositionFuns0} = State) ->
    PreInterpositionFuns = maps:put(Name, PreInterpositionFun, PreInterpositionFuns0),
    {reply, ok, State#state{pre_interposition_funs=PreInterpositionFuns}};

handle_call(
    {remove_pre_interposition_fun, Name},
    _From,
    #state{pre_interposition_funs = PreInterpositionFuns0} = State) ->
    PreInterpositionFuns = maps:remove(Name, PreInterpositionFuns0),
    {reply, ok, State#state{pre_interposition_funs=PreInterpositionFuns}};

handle_call(
    {add_interposition_fun, Name, InterpositionFun},
    _From,
    #state{interposition_funs = InterpositionFuns0} = State) ->
    InterpositionFuns = maps:put(Name, InterpositionFun, InterpositionFuns0),
    {reply, ok, State#state{interposition_funs=InterpositionFuns}};

handle_call(
    {remove_interposition_fun, Name},
    _From,
    #state{interposition_funs=InterpositionFuns0} = State) ->
    InterpositionFuns = maps:remove(Name, InterpositionFuns0),
    {reply, ok, State#state{interposition_funs=InterpositionFuns}};

handle_call(get_interposition_funs, _From, #state{interposition_funs=InterpositionFuns}=State) ->
    {reply, {ok, InterpositionFuns}, State};

handle_call(get_pre_interposition_funs, _From, #state{pre_interposition_funs=PreInterpositionFuns}=State) ->
    {reply, {ok, PreInterpositionFuns}, State};

handle_call({add_post_interposition_fun, Name, PostInterpositionFun}, _From, #state{post_interposition_funs=PostInterpositionFuns0}=State) ->
    PostInterpositionFuns = maps:put(Name, PostInterpositionFun, PostInterpositionFuns0),
    {reply, ok, State#state{post_interposition_funs=PostInterpositionFuns}};

handle_call({remove_post_interposition_fun, Name}, _From, #state{post_interposition_funs=PostInterpositionFuns0}=State) ->
    PostInterpositionFuns = maps:remove(Name, PostInterpositionFuns0),
    {reply, ok, State#state{post_interposition_funs=PostInterpositionFuns}};

handle_call({update_members, Members}, _From, #state{} = State0) ->
    %% For compatibility with external membership services.
    MState = State0#state.membership_strategy_state,

    {Joiners, Leavers} = partisan_membership_set:compare(Members, MState),

    %% Issue leaves.
    State1 = lists:foldl(
        fun(NodeSpec , S) -> internal_leave(NodeSpec, S) end,
        State0,
        Leavers
    ),
    %% Issue joins.
    State2 = lists:foldl(
        fun(NodeSpec, S) -> internal_join(NodeSpec, undefined, S) end,
        State1,
        Joiners
    ),

    %% Compute current pending list.
    Pending1 = lists:filter(
        fun(NodeSpec) -> lists:member(NodeSpec, State0#state.membership) end,
        State2#state.pending
    ),

    State = State2#state{pending = Pending1},

    %% Finally schedule the removal of connections
    %% We do this async because internal_leave will schedule the sending of
    %% membership update messages
    LeavingNodes = [Node || #{name := Node} <- Leavers],
    gen_server:cast(?MODULE, {kill_connections, LeavingNodes}),

    {reply, ok, State};

handle_call({leave, #{name := Node} = NodeSpec}, From, State0) ->
    %% Perform leave.
    State = internal_leave(NodeSpec, State0),

    case Node == partisan:node() of
        true ->
            gen_server:reply(From, ok),

            %% We need to stop (to cleanup all connections and state) or do it
            %% manually. However, we cannot do it straight away as
            %% internal_leave/2 has send some async messages we need to process
            %% (casts to ourself) so we cast ourselves a shutdown message.
            gen_server:cast(?MODULE, stop),
            {noreply, State};

        false ->
            gen_server:cast(?MODULE, {kill_connections, [NodeSpec]}),
            {reply, ok, State}
    end;

handle_call({join, #{name := Node} = NodeSpec}, _From, State0) ->
    case Node == partisan:node() of
        true ->
            %% Ignoring self join.
            {reply, ok, State0};

        false ->
            %% Perform join.
            State = internal_join(NodeSpec, undefined, State0),

            %% Return.
            {reply, ok, State}
    end;

handle_call({sync_join, #{name := Node} = NodeSpec}, From, State0) ->
    ?LOG_DEBUG(#{
        description => "Starting synchronous join with peer",
        node => partisan:node(),
        peer => NodeSpec
    }),

    case Node == partisan:node() of
        true ->
            %% Ignoring self join.
            {reply, ok, State0};

        false ->
            %% Perform join.
            State = internal_join(NodeSpec, From, State0),
            {reply, ok, State}
    end;

handle_call({send_message, Node, Channel, Message}, _From, #state{} = State) ->

    schedule_self_message_delivery(
        Node,
        Message,
        Channel,
        ?DEFAULT_PARTITION_KEY,
        State#state.pre_interposition_funs
    ),
    {reply, ok, State};

handle_call(
    {forward_message,
        Node, Channel, Clock, PartitionKey, ServerRef, OriginalMessage, Options
    },
    From,
    #state{} = State) ->

    %% Run all interposition functions.
    DeliveryFun = fun() ->
        %% Fire pre-interposition functions.
        PreFoldFun = fun(_Node, Fun, ok) ->
            Fun({forward_message, Node, OriginalMessage}),
            ok
        end,
        maps:fold(PreFoldFun, ok, State#state.pre_interposition_funs),

        %% Once pre-interposition returns, then schedule for delivery.
        Msg = {
            forward_message,
            From,
            Node,
            Channel,
            Clock,
            PartitionKey,
            ServerRef,
            OriginalMessage,
            Options
        },
        gen_server:cast(?MODULE, Msg)
    end,

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver, preserving serial order of messages.
            DeliveryFun();
        true ->
            %% Allow the system to proceed, and the message will be delivered once pre-interposition is done.
            spawn_link(DeliveryFun)
    end,

    {noreply, State};

handle_call({receive_message, Node, OriginalMessage}, From, #state{} = State) ->
    #state{pre_interposition_funs = PreInterpositionFuns} = State,
    %% Run all interposition functions.
    DeliveryFun = fun() ->
        %% Fire pre-interposition functions.
        PreFoldFun = fun(_Name, PreInterpositionFun, ok) ->
            PreInterpositionFun({receive_message, Node, OriginalMessage}),
            ok
        end,
        maps:fold(PreFoldFun, ok, PreInterpositionFuns),

        %% Once pre-interposition returns, then schedule for delivery.
        gen_server:cast(?MODULE, {receive_message, From, Node, OriginalMessage})
    end,

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver, preserving serial order of messages.
            DeliveryFun();
        true ->
            %% Allow the system to proceed, and the message will be delivered once pre-interposition is done.
            spawn_link(DeliveryFun)
    end,

    {noreply, State};

handle_call(members_for_orchestration, _From, #state{} = State) ->
    {reply, {ok, State#state.membership}, State};

handle_call(members, _From, #state{} = State) ->
    Members = [Node || #{name := Node} <- State#state.membership],
    {reply, {ok, Members}, State};

handle_call({member, Node}, _From, #state{} = State) ->
    IsMember = lists:any(
        fun(#{name := X}) -> X =:= Node end,
        State#state.membership
    ),
    {reply, IsMember, State};

handle_call(get_local_state, _From, #state{} = State) ->
    {reply, {ok, State#state.membership_strategy_state}, State};

handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.


%% @private
-spec handle_cast(term(), t()) -> {noreply, t()}.

handle_cast(stop, State) ->
    %% We send ourselves this message when we left the cluster
    %% We stop to cleanup, supervisor will start us again, this will kill all
    %% connections.
    {stop, normal, State};
    % {noreply, State};

handle_cast({kill_connections, Nodes}, State) ->
    ok = kill_connections(Nodes, State),
    {noreply, State};

handle_cast({receive_message, From, Node, OriginalMessage}, #state{} = State) ->
    %% Filter messages using interposition functions.
    FoldFun = fun(_InterpositionName, Fun, M) ->
        Fun({receive_message, Node, M})
    end,
    Message = maps:fold(
        FoldFun, OriginalMessage, State#state.interposition_funs
    ),

    %% Fire post-interposition functions.
    PostFoldFun = fun(_Name, Fun, ok) ->
        Fun(
            {receive_message, Node, OriginalMessage},
            {receive_message, Node, Message}
        ),
        ok
    end,
    maps:fold(PostFoldFun, ok, State#state.post_interposition_funs),

    case Message of
        undefined ->
            gen_server:reply(From, ok),
            {noreply, State};

        {'$delay', NewMessage} ->
            ?LOG_DEBUG(
                "Delaying receive_message due to interposition result: ~p",
                [NewMessage]
            ),
            gen_server:cast(?MODULE, {receive_message, From, Node, NewMessage}),
            {noreply, State};

        _ ->
            handle_message(Message, From, State)
    end;

handle_cast(
    {forward_message, From, Node, Channel, Clock, PartitionKey, ServerRef, OriginalMsg, Options},
    State) ->

    #state{
        interposition_funs = InterpositionFuns,
        pre_interposition_funs = PreInterpositionFuns,
        post_interposition_funs = PostInterpositionFuns,
        vclock = VClock0
    } = State,

    %% Filter messages using interposition functions.
    FoldFun = fun(_InterpositionName, InterpositionFun, M) ->
        InterpositionFun({forward_message, Node, M})
    end,
    Message = maps:fold(FoldFun, OriginalMsg, InterpositionFuns),

    %% Increment the clock.
    VClock = partisan_vclock:increment(partisan:node(), VClock0),

    %% Are we using causality?
    CausalLabel = maps:get(causal_label, Options, undefined),

    %% Use local information for message unless it's a causal message.
    {MessageClock, FullMessage} = case CausalLabel of
        undefined ->
            %% Generate a message clock or use the provided clock.
            LocalClock = case Clock of
                undefined ->
                    {undefined, VClock};
                Clock ->
                    Clock
            end,

            {LocalClock, Message};
        CausalLabel ->
            case Clock of
                undefined ->
                    %% First time through.
                    %% We don't have a clock yet,
                    %% get one using the causality backend.
                    {ok, LocalClock0, CausalMessage} =
                        partisan_causality_backend:emit(
                            CausalLabel, Node, ServerRef, Message
                        ),

                    %% Wrap the clock with a scope.
                    %% TODO: Maybe do this wrapping inside of the causality backend.
                    LocalClock = {CausalLabel, LocalClock0},

                    %% Return clock and wrapped message.
                    {LocalClock, CausalMessage};

                _ ->
                    %% Retransmission.
                    %% Get the clock and message we used last time.
                    {ok, LocalClock, CausalMessage} =
                        partisan_causality_backend:reemit(CausalLabel, Clock),

                    %% Return clock and wrapped message.
                    {LocalClock, CausalMessage}
            end
    end,

    case Message of
        undefined ->
            %% Store for reliability, if necessary.
            case maps:get(ack, Options, false) of
                false ->
                    ok;
                true ->
                    %% Acknowledgements.
                    case maps:get(retransmission, Options, false) of
                        false ->
                            RescheduleableMessage = {
                                forward_message,
                                From,
                                Node,
                                Channel,
                                MessageClock,
                                PartitionKey,
                                ServerRef,
                                OriginalMsg,
                                Options
                            },
                            partisan_acknowledgement_backend:store(
                                MessageClock, RescheduleableMessage
                            );
                        true ->
                            ok
                    end
            end,

            %% Fire post-interposition functions.
            PostFoldFun = fun(_Name, PostInterpositionFun, ok) ->
                PostInterpositionFun(
                    {forward_message, Node, OriginalMsg},
                    {forward_message, Node, FullMessage}
                ),
                ok
            end,
            maps:fold(PostFoldFun, ok, PostInterpositionFuns),

            ?LOG_DEBUG(
                "~p: Message ~p after send interposition is: ~p",
                [partisan:node(), OriginalMsg, FullMessage]
            ),

            case From of
                undefined ->
                    ok;
                _ ->
                    gen_server:reply(From, ok)
            end,

            {noreply, State#state{vclock = VClock}};

        {'$delay', NewMessage} ->
            ?LOG_DEBUG(
                "Delaying receive_message due to interposition result: ~p",
                [NewMessage]
            ),
            gen_server:cast(
                ?MODULE,
                {
                    forward_message,
                    From,
                    Node,
                    Channel,
                    Clock,
                    PartitionKey,
                    ServerRef,
                    NewMessage,
                    Options
                }
            ),
            {noreply, State};

        _ ->
            %% Store for reliability, if necessary.
            Result = case maps:get(ack, Options, false) of
                false ->
                    %% Tracing.
                    WrappedMessage = {forward_message, ServerRef, FullMessage},
                    WrappedOriginalMessage =
                        {forward_message, ServerRef, OriginalMsg},

                    %% Fire post-interposition functions -- trace after wrapping!
                    PostFoldFun = fun(_Name, PostInterpositionFun, ok) ->
                        PostInterpositionFun(
                            {forward_message, Node, WrappedOriginalMessage},
                            {forward_message, Node, WrappedMessage}
                        ),
                        ok
                    end,
                    maps:fold(PostFoldFun, ok, PostInterpositionFuns),

                    %% Send message along.
                    do_send_message(
                        Node,
                        Channel,
                        PartitionKey,
                        WrappedMessage,
                        Options,
                        PreInterpositionFuns
                    );
                true ->
                    %% Tracing.
                    WrappedOriginalMessage = {forward_message, partisan:node(), MessageClock, ServerRef, OriginalMsg},
                    WrappedMessage = {forward_message, partisan:node(), MessageClock, ServerRef, FullMessage},

                    ?LOG_DEBUG(
                        "should acknowledge message: ~p", [WrappedMessage]
                    ),

                    %% Fire post-interposition functions -- trace after wrapping!
                    PostFoldFun = fun(_Name, PostInterpositionFun, ok) ->
                        PostInterpositionFun(
                            {forward_message, Node, WrappedOriginalMessage},
                            {forward_message, Node, WrappedMessage}
                        ),
                        ok
                    end,
                    maps:fold(PostFoldFun, ok, PostInterpositionFuns),

                    ?LOG_DEBUG(
                        "~p: Sending message ~p with clock: ~p",
                        [partisan:node(), Message, MessageClock]
                    ),
                    ?LOG_DEBUG(
                        "~p: Message after send interposition is: ~p",
                        [partisan:node(), Message]
                    ),

                    %% Acknowledgements.
                    case maps:get(retransmission, Options, false) of
                        false ->
                            RescheduleableMessage = {
                                forward_message,
                                From,
                                Node,
                                Channel,
                                MessageClock,
                                PartitionKey,
                                ServerRef,
                                OriginalMsg,
                                Options
                            },
                            partisan_acknowledgement_backend:store(
                                MessageClock, RescheduleableMessage
                            );
                        true ->
                            ok
                    end,

                    %% Send message along.
                    do_send_message(
                        Node,
                        Channel,
                        PartitionKey,
                        WrappedMessage,
                        Options,
                        PreInterpositionFuns
                    )
            end,

            case From of
                undefined ->
                    ok;
                _ ->
                    gen_server:reply(From, Result)
            end,

            {noreply, State#state{vclock = VClock}}
    end;

handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),
    {noreply, State}.

%% @private
-spec handle_info(term(), t()) -> {noreply, t()}.
handle_info(tree_refresh, #state{} = State) ->
    %% Get lazily computed outlinks.
    OutLinks = retrieve_outlinks(),

    %% Reschedule.
    schedule_tree_refresh(),

    {noreply, State#state{out_links = OutLinks}};

handle_info(distance, #state{} = State0) ->
    %% Establish any new connections.
    State = establish_connections(State0),

    %% Record time.
    Time = erlang:timestamp(),

    %% Send distance requests.
    SrcNode = partisan:node_spec(),
    PreInterpositionFuns = State#state.pre_interposition_funs,

    ok = lists:foreach(
        fun(DestTo) ->
            schedule_self_message_delivery(
                DestTo,
                {ping, SrcNode, DestTo, Time},
                ?MEMBERSHIP_CHANNEL,
                ?DEFAULT_PARTITION_KEY,
                PreInterpositionFuns
            )
        end,
        State#state.membership
    ),

    schedule_distance(),

    {noreply, State};

handle_info(instrumentation, State) ->
    MessageQueueLen = process_info(self(), message_queue_len),
    ?LOG_DEBUG("message_queue_len: ~p", [MessageQueueLen]),
    schedule_instrumentation(),
    {noreply, State};

handle_info(periodic, #state{membership_strategy=MStrategy,
                             membership_strategy_state=MStrategyState0,
                             pre_interposition_funs=PreInterpositionFuns
                             }=State0) ->
    {ok, Membership, OutgoingMessages, MStrategyState} =
        MStrategy:periodic(MStrategyState0),

    %% Send outgoing messages.
    ok = lists:foreach(
        fun({Node, Message}) ->
            schedule_self_message_delivery(
                Node,
                Message,
                ?MEMBERSHIP_CHANNEL,
                ?DEFAULT_PARTITION_KEY,
                PreInterpositionFuns
            )
        end,
        OutgoingMessages
    ),

    State1 = State0#state{
        membership = Membership,
        membership_strategy_state = MStrategyState
    },

    %% Establish any new connections.
    State = establish_connections(State1),

    schedule_periodic(),

    {noreply, State};

handle_info(retransmit, #state{} = State) ->
    PreInterpositionFuns = State#state.pre_interposition_funs,

    RetransmitFun = fun({_, {forward_message, From, Node, Channel, Clock, PartitionKey, ServerRef, Message, Options}}) ->
        ?LOG_DEBUG(
            "~p no acknowledgement yet, "
            "restranmitting message ~p with clock ~p to ~p",
            [partisan:node(), Message, Clock, Node]
        ),

        %% Fire pre-interposition functions.
        PreFoldFun = fun(_Name, PreInterpositionFun, ok) ->
            ?LOG_DEBUG(
                "firing preinterposition fun for original message: ~p",
                [Message]
            ),
            PreInterpositionFun({forward_message, Node, Message}),
            ok
        end,
        maps:fold(PreFoldFun, ok, PreInterpositionFuns),

        %% Schedule message for redelivery.
        RetryOptions = Options#{retransmission => true},

        gen_server:cast(
            ?MODULE,
            {
                forward_message,
                From,
                Node,
                Channel,
                Clock,
                PartitionKey,
                ServerRef,
                Message,
                RetryOptions
            }
        )
    end,

    {ok, Outstanding} = partisan_acknowledgement_backend:outstanding(),

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver,
            %% preserving serial order of messages.
            lists:foreach(RetransmitFun, Outstanding);
        true ->
            %% Allow the system to proceed, and the message will be delivered
            %% once pre-interposition is done.
            lists:foreach(
                fun(OutstandingMessage) ->
                    spawn_link(fun() ->
                        RetransmitFun(OutstandingMessage)
                    end)
                end,
                Outstanding
            )
    end,

    %% Reschedule retransmission.
    schedule_retransmit(),

    {noreply, State};


handle_info(connections, #state{} = State0) ->
    State1 = establish_connections(State0),

    SyncJoins0 = State1#state.sync_joins,

    %% Advance sync_join's if we have enough open connections to remote host.
    SyncJoins = lists:foldl(
        fun({#{name := Node} = NodeSpec, FromPid}, Joins) ->
            case partisan:is_fully_connected(NodeSpec) of
                true ->
                    ?LOG_DEBUG("Node ~p is now fully connected.", [Node]),

                    gen_server:reply(FromPid, ok),

                    Joins;

                false ->
                    Joins ++ [{NodeSpec, FromPid}]
            end
        end,
        [],
        SyncJoins0
    ),

    State = State1#state{sync_joins = SyncJoins},

    schedule_connections(),

    {noreply, State};

handle_info({'EXIT', Pid, Reason}, #state{} = State0) ->
    ?LOG_DEBUG(#{
        description => "Connection closed",
        reason => Reason
    }),

    %% A connection has closed, prune it from the connections table
    try partisan_peer_connections:prune(Pid) of
        {Info, [_Connection]} ->
            State =
                case partisan_peer_connections:connection_count(Info) of
                    0 ->
                        %% This was the last connection so the node is down.
                        NodeSpec = partisan_peer_connections:node_spec(Info),
                        %% We notify all subscribers.
                        ok = down(NodeSpec, State0),
                        %% If still a member we add it to pending, so that we
                        %% can compute the on_up signal
                        maybe_append_pending(NodeSpec, State0);
                    _ ->
                        State0
                end,

            {noreply, State}

    catch
        error:badarg ->
            %% Weird, connection pid did not exist
            {noreply, State0}
    end;

handle_info(
    {connected, NodeSpec, _Channel, _Tag, RemoteState}, #state{} = State0) ->
    #state{
        pending = Pending0,
        sync_joins = SyncJoins0,
        membership = Membership0,
        membership_strategy = MStrategy,
        membership_strategy_state = MStrategyState0,
        pre_interposition_funs = PreInterpositionFuns
    } = State0,

    ?LOG_DEBUG(#{
        description => "Node connected!",
        node => NodeSpec,
        pending => Pending0,
        membership => State0#state.membership
    }),

    State = case lists:member(NodeSpec, Pending0) of
        true ->
            %% Move out of pending.
            Pending = Pending0 -- [NodeSpec],

            %% Update membership by joining with remote membership.
            {ok, Membership, OutgoingMessages, MStrategyState} =
                MStrategy:join(
                    MStrategyState0, NodeSpec, RemoteState
                ),

            %% Gossip the new membership.
            lists:foreach(
                fun({Node, Message}) ->
                    schedule_self_message_delivery(
                        Node,
                        Message,
                        ?MEMBERSHIP_CHANNEL,
                        ?DEFAULT_PARTITION_KEY,
                        PreInterpositionFuns
                    )
                end,
                OutgoingMessages
            ),

            %% Notify event handlers
            ok = case Membership == Membership0 of
                true ->
                    ok;
                false ->
                    partisan_peer_service_events:update(Membership)
            end,

            %% notify subscribers
            up(NodeSpec, State0),

            State0#state{
                pending = Pending,
                membership = Membership,
                membership_strategy_state = MStrategyState
            };

        false ->
            State0
    end,

    %% Notify for sync join.
    SyncJoins = case lists:keyfind(NodeSpec, 1, SyncJoins0) of
        {PeerNodeSpec, FromPid} ->
            case partisan:is_fully_connected(PeerNodeSpec) of
                true ->
                    gen_server:reply(FromPid, ok),
                    lists:keydelete(FromPid, 2, SyncJoins0);
                false ->
                    SyncJoins0
            end;
        false ->
            SyncJoins0
    end,

    {noreply, State#state{sync_joins = SyncJoins}};

handle_info(Msg, State) ->
    handle_message(Msg, undefined, State).


%% @private
-spec terminate(term(), t()) -> term().

terminate(_Reason, #state{}) ->
    Fun = fun(_NodeInfo, Connections) ->
        lists:foreach(
            fun(C) ->
                Pid = partisan_peer_connections:pid(C),
                catch gen_server:stop(Pid, normal, infinity),
                ok
            end,
            Connections
        )
    end,
    ok = partisan_peer_connections:foreach(Fun).


%% @private
-spec code_change(term() | {down, term()}, t(), term()) ->
    {ok, t()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
gen_actor() ->
    Node = atom_to_list(partisan:node()),
    Unique = erlang:unique_integer([positive]),
    TS = integer_to_list(Unique),
    Term = Node ++ TS,
    crypto:hash(sha, Term).


%% @private
without_me(Members) ->
    Node = partisan:node(),
    lists:filter(fun(#{name := X}) -> X =/= Node end, Members).


%% -----------------------------------------------------------------------------
%% @private
%% @doc Establish any new connections and prunes no longer valid nodes.
%% @end
%% -----------------------------------------------------------------------------
-spec establish_connections(t()) -> t().

establish_connections(State) ->
    Pending = State#state.pending,
    Membership = State#state.membership,

    %% Compute list of nodes that should be connected.
    Nodes = without_me(Membership ++ Pending),

    %% Reconnect disconnected members and members waiting to join.
    LoL = lists:foldl(
        fun(Node, Acc) ->
            %% TODO this should be a fold that will return the invalid NodeSpes
            %% (nodes that have an invalid IP address because we already have a
            %% connection to NodeSpec.node on another IP address)
            %% We then remove those NodeSpes from the membership set and update
            %% ourselves without the need for sending any leave/join gossip.
            {ok, L} =
                partisan_peer_service_manager:connect(Node, #{prune => true}),
            [L | Acc]
        end,
        [],
        Nodes
    ),
    prune(lists:append(LoL), State).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
prune([], State) ->
    State;

prune(L, State) ->
    Mod = State#state.membership_strategy,
    MState0 = State#state.membership_strategy_state,
    {ok, MList, MState} = Mod:prune(L, MState0),
    State#state{membership = MList, membership_strategy_state = MState}.


%% @private
kill_connections(Nodes, State) ->
    Fun = fun(Node) ->
        ok = down(Node, State)
    end,
    partisan_peer_service_manager:disconnect(Nodes, Fun).


%% @private
handle_message({ping, SrcNode, DestNode, SrcTime}, From, #state{} = State0) ->

    %% Establish any new connections.
    State = establish_connections(State0),

    PreInterpositionFuns = State#state.pre_interposition_funs,

    %% Send ping response.
    schedule_self_message_delivery(
        SrcNode,
        {pong, SrcNode, DestNode, SrcTime},
        ?MEMBERSHIP_CHANNEL,
        ?DEFAULT_PARTITION_KEY,
        PreInterpositionFuns
    ),

    optional_gen_server_reply(From, ok),

    {noreply, State};

handle_message({pong, SrcNode, DestNode, SrcTime}, From, #state{} = State) ->

    %% Compute difference.
    DistanceMetrics0 = State#state.distance_metrics,
    ArrivalTime = erlang:timestamp(),
    Difference = timer:now_diff(ArrivalTime, SrcTime),

    ?LOG_TRACE(
        "Updating distance metric for node ~p => ~p communication: ~p",
        [SrcNode, DestNode, Difference]
    ),

    %% Update differences.
    DistanceMetrics = maps:put(DestNode, Difference, DistanceMetrics0),

    %% Store in pdict.
    put(distance_metrics, DistanceMetrics),

    optional_gen_server_reply(From, ok),

    {noreply, State#state{distance_metrics=DistanceMetrics}};

handle_message({membership_strategy, ProtocolMsg}, From, #state{} = State0) ->

    #state{
        membership = Membership0,
        membership_strategy = MStrategy,
        membership_strategy_state = MStrategyState0,
        pre_interposition_funs = PreInterpositionFuns
    } = State0,

    %% Process the protocol message.
    {ok, Membership, OutgoingMessages, MStrategyState} =
        MStrategy:handle_message(MStrategyState0, ProtocolMsg),


    %% Update users of the peer service.
    case Membership == Membership0 of
        true ->
            ok;
        false ->
            partisan_peer_service_events:update(Membership)
    end,

    %% Send outgoing messages.
    lists:foreach(
        fun({Node, Message}) ->
            schedule_self_message_delivery(
                Node,
                Message,
                ?MEMBERSHIP_CHANNEL,
                ?DEFAULT_PARTITION_KEY,
                PreInterpositionFuns
            )
        end,
        OutgoingMessages
    ),

    State1 = State0#state{
        membership = Membership,
        membership_strategy_state = MStrategyState
    },

    {Pending, LeavingNodes} = pending_leavers(State1),
    State2 = State1#state{pending = Pending},

    %% Establish any new connections.
    State = establish_connections(State2),

    gen_server:cast(?MODULE, {kill_connections, LeavingNodes}),

    case lists:member(partisan:node_spec(), Membership) of
        false ->
            ?LOG_INFO(#{
                description => "Shutting down: membership doesn't contain us",
                reason => "We've been removed from the cluster."
            }),

            ?LOG_DEBUG(#{
                membership => Membership
            }),

            %% Shutdown if we've been removed from the cluster.
            {stop, normal, State};

        true ->
            optional_gen_server_reply(From, ok),
            {noreply, State}
    end;

%% Causal and acknowledged messages.
handle_message({forward_message, SrcNode, MessageClock, ServerRef, {causal, Label, _, _, _, _, _} = Message},
               From,
               #state{pre_interposition_funs=PreInterpositionFuns}=State) ->
    %% Send message acknowledgement.
    send_acknowledgement(SrcNode, MessageClock, PreInterpositionFuns),

    case partisan_causality_backend:is_causal_message(Message) of
        true ->
            partisan_causality_backend:receive_message(Label, Message);
        false ->
            %% Attempt message delivery.
            partisan_peer_service_manager:process_forward(ServerRef, Message)
    end,

    optional_gen_server_reply(From, ok),

    {noreply, State};

%% Acknowledged messages.
handle_message({forward_message, SrcNode, MessageClock, ServerRef, Message},
               From,
               #state{pre_interposition_funs=PreInterpositionFuns}=State) ->
    %% Send message acknowledgement.
    send_acknowledgement(SrcNode, MessageClock, PreInterpositionFuns),

    partisan_peer_service_manager:process_forward(ServerRef, Message),

    optional_gen_server_reply(From, ok),

    {noreply, State};

%% Causal messages.
handle_message(
    {forward_message, ServerRef, {causal, Label, _, _, _, _, _} = Message},
               From,
               State) ->
    case partisan_causality_backend:is_causal_message(Message) of
        true ->
            partisan_causality_backend:receive_message(Label, Message);
        false ->
            %% Attempt message delivery.
            partisan_peer_service_manager:process_forward(ServerRef, Message)
    end,

    optional_gen_server_reply(From, ok),

    {noreply, State};

%% Best-effort messages.
%% TODO: Maybe remove me.
handle_message({forward_message, ServerRef, Message},
               From,
               State) ->
    partisan_peer_service_manager:process_forward(ServerRef, Message),
    optional_gen_server_reply(From, ok),
    {noreply, State};

handle_message({ack, MessageClock},
               From,
               State) ->
    partisan_acknowledgement_backend:ack(MessageClock),
    optional_gen_server_reply(From, ok),
    {noreply, State};

handle_message(Message,
               _From,
               State) ->
    ?LOG_WARNING(#{description => "Unhandled message", message => Message}),
    {noreply, State}.


%% @private
schedule_distance() ->
    case partisan_config:get(distance_enabled, false) of
        true ->
            DistanceInterval = partisan_config:get(distance_interval, 10000),
            erlang:send_after(DistanceInterval, ?MODULE, distance);
        false ->
            ok
    end.


%% @private
schedule_instrumentation() ->
    case partisan_config:get(instrumentation, false) of
        true ->
            erlang:send_after(1000, ?MODULE, instrumentation);
        _ ->
            ok
    end.


%% @private
schedule_periodic() ->
    case partisan_config:get(periodic_enabled, false) of
        true ->
            Time = partisan_config:get(periodic_interval, ?PERIODIC_INTERVAL),
            erlang:send_after(Time, ?MODULE, periodic);
        false ->
            ok
    end.


%% @private
schedule_retransmit() ->
    Time = partisan_config:get(retransmit_interval, 1000),
    erlang:send_after(Time, ?MODULE, retransmit).


%% @private
schedule_connections() ->
    Time = partisan_config:get(connection_interval, 1000),
    erlang:send_after(Time, ?MODULE, connections).


%% @private
do_send_message(
    Node, Channel, PartitionKey, Message, Options, PreInterpositionFuns) ->
    %% Find a connection for the remote node, if we have one.
    Res = partisan_peer_connections:dispatch_pid(
        Node, Channel, PartitionKey
    ),

    case Res of
        {ok, Pid} ->
            gen_server:cast(Pid, {send_message, Message});

        {error, Reason} ->
            %% We were connected, but we're not anymore, or never connected
            case partisan_config:get(broadcast, false) of
                true ->
                    case maps:get(transitive, Options, false) of
                        true ->
                            ?LOG_DEBUG(
                                "Performing tree forward from node ~p to node ~p and message: ~p",
                                [partisan:node(), Node, Message]
                            ),
                            TTL = partisan_config:get(relay_ttl, ?RELAY_TTL),
                            do_tree_forward(
                                Node,
                                Channel,
                                PartitionKey,
                                Message,
                                Options,
                                TTL,
                                PreInterpositionFuns
                            );
                        false ->
                            ok
                    end;
                false ->
                    case Reason of
                        disconnected ->
                            ?LOG_TRACE(
                                "Node ~p was connected, but is now disconnected!",
                                [Node]
                            ),
                            {error, disconnected};
                        not_yet_connected ->
                            ?LOG_TRACE(
                                "Node ~p not yet connected!",
                                [Node]
                            ),
                            {error, not_yet_connected}
                    end
            end
    end.


%% @private
up(NodeOrSpec, State) ->
    apply_funs(NodeOrSpec, State#state.up_functions).


%% @private
down(NodeOrSpec, State) ->
    apply_funs(NodeOrSpec, State#state.down_functions).


%% @private
apply_funs(Node, Mapping) when is_atom(Node) ->
    ?LOG_DEBUG(#{
        description => "Node status change notification",
        node => Node,
        funs => Mapping
    }),

    Funs = lists:append(
        %% Notify functions matching the wildcard '_'
        maps:get('_', Mapping, []),
        %% Notify functions matching Node
        maps:get(Node, Mapping, [])
    ),

    _ = [
        begin
            case erlang:fun_info(F, arity) of
                {arity, 0} -> catch F();
                {arity, 1} -> catch F(Node)
            end
        end || F <- Funs
    ],

    ok;

apply_funs(#{name := Node}, Mapping) ->
    apply_funs(Node, Mapping).


%% @private
pending_leavers(#state{} = State) ->
    Members = ?SET_FROM_LIST(State#state.membership),
    Pending0 = ?SET_FROM_LIST(State#state.pending),
    Connected = ?SET_FROM_LIST(partisan_peer_connections:node_specs()),

    %% Connected nodes that are no longer members
    Leavers = sets:to_list(sets:subtract(Connected, Members)),

    %% Disconnected nodes that are members
    Pending = sets:to_list(
        sets:subtract(sets:union(Members, Pending0), Connected)
    ),

    {Pending, Leavers}.



%% @private
internal_leave(#{name := Name} = Node, State0) ->

    #state{
        membership_strategy = MStrategy,
        membership_strategy_state = MStrategyState0,
        pre_interposition_funs = PreInterpositionFuns
    } = State0,

    ?LOG_DEBUG(#{
        description => "Processing leave",
        leaving_node => Name
    }),

    {ok, Membership, OutgoingMessages, MStrategyState} =
        MStrategy:leave(MStrategyState0, Node),


    ?LOG_DEBUG(#{
        description => "Processing leave",
        leaving_node => Name,
        outgoing_messages => OutgoingMessages,
        new_membership => Membership
    }),

    State1 = State0#state{
        membership = Membership,
        membership_strategy_state = MStrategyState
    },

    %% Establish any new connections.
    %% This will also prune no longer valid node_specs, setting the new
    %% membership in State
    State = establish_connections(State1),

    %% Transmit outgoing messages.
    lists:foreach(
        fun
            ({#{name := Peername}, Message}) ->
                schedule_self_message_delivery(
                    Peername,
                    Message,
                    ?MEMBERSHIP_CHANNEL,
                    ?DEFAULT_PARTITION_KEY,
                    PreInterpositionFuns
                )
        end,
        OutgoingMessages
    ),

    partisan_peer_service_events:update(State#state.membership),

    State.


%% @private
internal_join(#{name := Node} = NodeSpec, From, #state{} = State0) ->

    #state{
        pending = Pending0,
        sync_joins = SyncJoins0
    } = State0,

    case partisan_config:get(connect_disterl, false) of
        true ->
            %% Maintain disterl connection for control messages.
            _ = net_kernel:connect_node(Node);
        false ->
            ok
    end,

    %% Add to list of pending connections.
    Pending = Pending0 ++ [NodeSpec],

    %% Sleep before connecting, to avoid a rush on connections.
    avoid_rush(),

    %% Add to sync joins list.
    SyncJoins = case From of
        undefined ->
            SyncJoins0;
        _ ->
            SyncJoins0 ++ [{NodeSpec, From}]
    end,

    State = State0#state{
        pending = Pending,
        sync_joins = SyncJoins
    },

    %% Establish any new connections.
    establish_connections(State).


%% @private
rand_bits(BitLen) ->
    Bytes = (BitLen + 7) div 8,
    <<Result:BitLen/bits, _/bits>> = crypto:strong_rand_bytes(Bytes),
    Result.


%% @private
avoid_rush() ->
    %% Sleep before connecting, to avoid a rush on connections.
    Jitter = partisan_config:get(connection_jitter, ?CONNECTION_JITTER),
    case partisan_config:get(jitter, false) of
        true ->
            timer:sleep(rand:uniform(Jitter));
        false ->
            timer:sleep(Jitter)
    end.

%% @private
do_tree_forward(
    Node, Channel, PartitionKey, Message, Options, TTL, PreInterpositionFuns) ->
    MyNode = partisan:node(),

    ?LOG_TRACE(
        "Attempting to forward message ~p from ~p to ~p.",
        [Message, MyNode, Node]
    ),

    %% Preempt with user-supplied outlinks.
    UserOutLinks = maps:get(out_links, Options, undefined),

    OutLinks = case UserOutLinks of
        undefined ->
            try retrieve_outlinks() of
                Value ->
                    Value
            catch
                _:Reason ->
                    ?LOG_ERROR(#{
                        description => "Outlinks retrieval failed",
                        reason => Reason
                    }),
                    []
            end;
        OL ->
            OL -- [MyNode]
    end,

    %% Send messages, but don't attempt to forward again, if we aren't connected.
    lists:foreach(fun(N) ->
        ?LOG_TRACE(
            "Forwarding relay message ~p to node ~p for node ~p from node ~p",
            [Message, N, Node, partisan:node()]
        ),

        RelayMessage = {relay_message, Node, Message, TTL - 1},
        schedule_self_message_delivery(
            N,
            RelayMessage,
            Channel,
            PartitionKey,
            maps:without([transitive], Options),
            PreInterpositionFuns
        )
    end, OutLinks),
    ok.

%% @private
retrieve_outlinks() ->
    ?LOG_TRACE(#{description => "About to retrieve outlinks..."}),

    Root = partisan:node(),
    Result = partisan_plumtree_broadcast:debug_get_peers(Root, Root, 1000),

    OutLinks = try Result of
        {EagerPeers, _LazyPeers} ->
            ordsets:to_list(EagerPeers)
    catch
        _:_ ->
            ?LOG_INFO(#{description => "Request to get outlinks timed out..."}),
            []
    end,

    ?LOG_TRACE("Finished getting outlinks: ~p", [OutLinks]),

    OutLinks -- [Root].


%% @private
schedule_tree_refresh() ->
    case partisan_config:get(broadcast, false) of
        true ->
            Period = partisan_config:get(tree_refresh, 1000),
            erlang:send_after(Period, ?MODULE, tree_refresh);
        false ->
            ok
    end.

%% @private
schedule_self_message_delivery(
    Node, Message, Channel, PartitionKey, PreInterpositionFuns) ->
    schedule_self_message_delivery(
        Node, Message, Channel, PartitionKey, #{}, PreInterpositionFuns
    ).

%% @private
schedule_self_message_delivery(
    Node, Message, Channel, PartitionKey, Options, PreInterpositionFuns) ->
    %% Run all interposition functions.
    DeliveryFun = fun() ->
        %% Fire pre-interposition functions.
        PreFoldFun = fun(_Name, PreInterpositionFun, ok) ->
            ?LOG_DEBUG(
                "firing forward_message preinterposition fun for message: ~p",
                [Message]
            ),
            PreInterpositionFun({forward_message, Node, Message}),
            ok
        end,

        maps:fold(PreFoldFun, ok, PreInterpositionFuns),

        %% Once pre-interposition returns, then schedule for delivery.
        gen_server:cast(?MODULE, {
            forward_message,
            undefined,
            Node,
            Channel,
            undefined,
            PartitionKey,
            ?MODULE,
            Message,
            Options
        })
    end,

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver, preserving serial order of messages.
            DeliveryFun();
        true ->
            %% Allow the system to proceed, and the message will be delivered once pre-interposition is done.
            spawn_link(DeliveryFun)
    end,

    ok.

%% @private
send_acknowledgement(Node, MessageClock, PreInterpositionFuns) ->
    %% Generate message.
    Message = {ack, MessageClock},

    %% Send on the default channel.
    schedule_self_message_delivery(
        Node,
        Message,
        ?DEFAULT_CHANNEL,
        ?DEFAULT_PARTITION_KEY,
        PreInterpositionFuns
    ).

%% @private
optional_gen_server_reply(From, Response) ->
    case From of
        undefined ->
            ok;
        _ ->
            gen_server:reply(From, Response)
    end.


%% @private
forward_opts() ->
    case partisan_config:get(forward_options, #{}) of
        List when is_list(List) ->
            maps:from_list(List);
        Map when is_map(Map) ->
            Map
    end.


%% @private
maybe_append_pending(NodeSpec, #state{} = State) ->
    Pending0 = State#state.pending,
    Membership = State#state.membership,

    Pending = case lists:member(NodeSpec, Membership) of
        true ->
            Pending0 ++ [NodeSpec];
        false ->
            Pending0
    end,
    State#state{pending = Pending}.


