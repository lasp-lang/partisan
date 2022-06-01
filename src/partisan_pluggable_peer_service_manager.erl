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

-module(partisan_pluggable_peer_service_manager).

-behaviour(gen_server).
-behaviour(partisan_peer_service_manager).

-include("partisan_logger.hrl").

%% partisan_peer_service_manager callbacks
-export([start_link/0,
         members/0,
         member/1,
         members_for_orchestration/0,
         myself/0,
         get_local_state/0,
         join/1,
         sync_join/1,
         leave/0,
         leave/1,
         update_members/1,
         on_down/2,
         on_up/2,
         send_message/2,
         cast_message/3,
         forward_message/2,
         forward_message/3,
         cast_message/4,
         forward_message/4,
         cast_message/5,
         forward_message/5,
         receive_message/2,
         decode/1,
         reserve/1,
         partitions/0,
         add_pre_interposition_fun/2,
         remove_pre_interposition_fun/1,
         add_interposition_fun/2,
         remove_interposition_fun/1,
         get_interposition_funs/0,
         get_pre_interposition_funs/0,
         add_post_interposition_fun/2,
         remove_post_interposition_fun/1,
         inject_partition/2,
         resolve_partition/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("partisan.hrl").


-define(SET_FROM_LIST(L), sets:from_list(L, [{version, 2}])).

-type from()                    ::  {pid(), atom()}.
-type on_change_function()      ::  fun(() -> ok) | fun((node()) -> ok).
-type interposition_arg()       ::  {receive_message, node(), any()}.
-type interposition_fun()       ::  fun(
                                        (interposition_arg()) ->
                                            interposition_arg()
                                    ).
-type pre_post_interposition_fun()       ::  fun(
                                        (interposition_arg()) -> ok
                                    ).

-record(state, {
    actor                       ::  actor(),
    distance_metrics            ::  map(),
    vclock                      ::  term(),
    pending                     ::  [node_spec()],
    membership                  ::  [node_spec()],
    down_functions              ::  #{'_' | node() => on_change_function()},
    up_functions                ::  #{'_' | node() => on_change_function()},
    out_links                   ::  [term()],
    pre_interposition_funs      ::  #{any() => pre_post_interposition_fun()},
    interposition_funs          ::  #{any() => interposition_fun()},
    post_interposition_funs     ::  #{any() => pre_post_interposition_fun()},
    sync_joins                  ::  [{node_spec(), from()}],
    membership_strategy         ::  atom(),
    membership_strategy_state   ::  term()
}).

-type state_t() :: #state{}.

%%%===================================================================
%%% partisan_peer_service_manager callbacks
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Returns true if node `Node' is a member in the membership list.
%% Otherwise returns `false'.
%% @end
member(#{name := Node}) ->
    member(Node);

member(Node) when is_atom(Node) ->
    gen_server:call(?MODULE, {member, Node}, infinity).


%% @doc Return membership list.
members() ->
    gen_server:call(?MODULE, members, infinity).

%% @doc Return membership list.
members_for_orchestration() ->
    gen_server:call(?MODULE, members_for_orchestration, infinity).


%% @doc Return partisan:node_spec().
myself() ->
    partisan:node_spec().

%% @doc Update membership.
update_members(Nodes) ->
    gen_server:call(?MODULE, {update_members, Nodes}, infinity).

%% @doc Return local node's view of cluster membership.
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).

%% @doc Trigger function on connection close for a given node.
%% `Function' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
on_down(#{name := Name}, Function) ->
    on_down(Name, Function);

on_down(any, Function) ->
    on_down('_', Function);

on_down(Name, Function)
when is_atom(Name) andalso (
    is_function(Function, 0) orelse is_function(Function, 1)
) ->
    gen_server:call(?MODULE, {on_down, Name, Function}, infinity).

%% @doc Trigger function on connection open for a given node.
%% `Function' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
on_up(#{name := Name}, Function) ->
    on_up(Name, Function);

on_up(any, Function) ->
    on_up('_', Function);

on_up(Name, Function)
when is_atom(Name) andalso (
    is_function(Function, 0) orelse
    is_function(Function, 1) orelse
    is_function(Function, 2)
) ->
    gen_server:call(?MODULE, {on_up, Name, Function}, infinity).

%% @doc Send message to a remote manager.
send_message(Name, Message) ->
    gen_server:call(?MODULE, {send_message, Name, ?DEFAULT_CHANNEL, Message}, infinity).

%% @doc Cast a message to a remote gen_server.
cast_message(Name, ServerRef, Message) ->
    cast_message(Name, ?DEFAULT_CHANNEL, ServerRef, Message).

%% @doc Cast a message to a remote gen_server.
cast_message(Name, Channel, ServerRef, Message) ->
    FullMessage = {'$gen_cast', Message},
    forward_message(Name, Channel, ServerRef, FullMessage),
    ok.

%% @doc Cast a message to a remote gen_server.
cast_message(Name, Channel, ServerRef, Message, Options) ->
    FullMessage = {'$gen_cast', Message},
    forward_message(Name, Channel, ServerRef, FullMessage, Options),
    ok.

%% @doc Gensym support for forwarding.
forward_message(Pid, Message) when is_pid(Pid) ->
    forward_message(partisan:node(), ?DEFAULT_CHANNEL, Pid, Message);

forward_message({partisan_remote_reference, Node, ServerRef}, Message) ->
    forward_message(Node, ?DEFAULT_CHANNEL, ServerRef, Message).

%% @doc Forward message to registered process on the remote side.
forward_message(Node, ServerRef, Message) ->
    forward_message(Node, ?DEFAULT_CHANNEL, ServerRef, Message).

%% @doc Forward message to registered process on the remote side.
forward_message(Node, Channel, ServerRef, Message) ->
    forward_message(Node, Channel, ServerRef, Message, #{}).

%% @doc Forward message to registered process on the remote side.
forward_message(Node, Channel, ServerRef, Message, Options)
when is_list(Options) ->
    forward_message(Node, Channel, ServerRef, Message, maps:from_list(Options));

forward_message(Node, Channel, ServerRef, Message, Options)
when is_map(Options) ->

    %% If attempting to forward to the local node, bypass.
    case partisan:node() of
        Node ->
            partisan_util:process_forward(ServerRef, Message),
            ok;
        _ ->
            case partisan_config:get(connect_disterl, false) of
                true ->
                    partisan_util:process_forward(ServerRef, Message),
                    ok;
                false ->
                    %% Attempt to get the partition key, if possible.
                    PartitionKey = maps:get(
                        partition_key, Options, ?DEFAULT_PARTITION_KEY
                    ),

                    %% Use a clock provided by the sender, otherwise, use a generated one.
                    Clock = maps:get(clock, Options, undefined),

                    %% Should ack?
                    ShouldAck = maps:get(ack, Options, false),

                    %% Use causality?
                    CausalLabel =
                        maps:get(causal_label, Options, undefined),

                    %% Get forwarding options and combine with message specific options.
                    ForwardOptions = maps:merge(Options, forward_options()),

                    %% Use configuration to disable fast forwarding.
                    DisableFastForward =
                        partisan_config:get(disable_fast_forward, false),

                    %% Should use fast forwarding?
                    %%
                    %% Conditions:
                    %% - not labeled for causal delivery
                    %% - message does not need acknowledgements
                    %%
                    FastForward = not (CausalLabel =/= undefined)
                                andalso not ShouldAck
                                andalso not DisableFastForward,

                    %% TODO Unless we need this properties to change in runtime,
                    %% we should store most of these options in the gen_server
                    %% state on init
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

                    FullMessage = {
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
                            %% Attempt to fast-path by accesing the conneciton
                            %% directly
                            case partisan_peer_connections:dispatch(FullMessage) of
                                ok ->
                                    ok;
                                {error, _} ->
                                    gen_server:call(?MODULE, FullMessage, infinity)
                            end;
                        false ->
                            gen_server:call(?MODULE, FullMessage, infinity)
                    end
            end
    end.

%% @doc Receive message from a remote manager.
receive_message(Peer, {forward_message, _SourceNode, _MessageClock, _ServerRef, _Message} = FullMessage) ->
    %% Process the message and generate the acknowledgement.
    gen_server:call(?MODULE, {receive_message, Peer, FullMessage}, infinity);

receive_message(Peer, {forward_message, ServerRef, {'$partisan_padded', _Padding, Message}}) ->
    receive_message(Peer, {forward_message, ServerRef, Message});

receive_message(_Peer, {forward_message, _ServerRef, {causal, Label, _, _, _, _, _} = Message}) ->
    partisan_causality_backend:receive_message(Label, Message);

receive_message(Peer, {forward_message, ServerRef, Message} = FullMessage) ->
    case partisan_config:get(disable_fast_receive, false) of
        true ->
            gen_server:call(?MODULE, {receive_message, Peer, FullMessage}, infinity);
        false ->
            partisan_util:process_forward(ServerRef, Message)
    end;

receive_message(Peer, Message) ->
    gen_server:call(?MODULE, {receive_message, Peer, Message}, infinity).


%% @doc Attempt to join a remote node.
sync_join(Node) ->
    gen_server:call(?MODULE, {sync_join, Node}, infinity).

%% @doc Attempt to join a remote node.
join(Node) ->
    gen_server:call(?MODULE, {join, Node}, infinity).

%% @doc Leave the cluster.
leave() ->
    gen_server:call(
        ?MODULE, {leave, partisan:node_spec()}, infinity
    ).

%% @doc Remove another node from the cluster.
leave(Node) ->
    gen_server:call(?MODULE, {leave, Node}, infinity).

%% @doc Decode state.
decode(Membership) ->
    Membership.

%% @doc Reserve a slot for the particular tag.
reserve(Tag) ->
    gen_server:call(?MODULE, {reserve, Tag}, infinity).

%% @doc
add_pre_interposition_fun(Name, PreInterpositionFun) ->
    gen_server:call(?MODULE, {add_pre_interposition_fun, Name, PreInterpositionFun}, infinity).

%% @doc
remove_pre_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_pre_interposition_fun, Name}, infinity).

%% @doc
add_interposition_fun(Name, InterpositionFun) ->
    gen_server:call(?MODULE, {add_interposition_fun, Name, InterpositionFun}, infinity).

%% @doc
remove_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_interposition_fun, Name}, infinity).

%% @doc
get_interposition_funs() ->
    gen_server:call(?MODULE, get_interposition_funs, infinity).

%% @doc
get_pre_interposition_funs() ->
    gen_server:call(?MODULE, get_pre_interposition_funs, infinity).

%% @doc
add_post_interposition_fun(Name, PostInterpositionFun) ->
    gen_server:call(?MODULE, {add_post_interposition_fun, Name, PostInterpositionFun}, infinity).

%% @doc
remove_post_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_post_interposition_fun, Name}, infinity).

%% @doc Inject a partition.
inject_partition(_Origin, _TTL) ->
    {error, not_implemented}.

%% @doc Resolve a partition.
resolve_partition(_Reference) ->
    {error, not_implemented}.

%% @doc Return partitions.
partitions() ->
    {error, not_implemented}.



%%%===================================================================
%%% gen_server callbacks
%%%===================================================================



%% @private
-spec init([]) -> {ok, state_t()}.

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


%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call({on_up, Name, Function},
            _From,
            #state{up_functions=UpFunctions0}=State) ->
    UpFunctions = partisan_util:maps_append(Name, Function, UpFunctions0),
    {reply, ok, State#state{up_functions=UpFunctions}};

handle_call({on_down, Name, Function},
            _From,
            #state{down_functions=DownFunctions0}=State) ->
    DownFunctions = partisan_util:maps_append(Name, Function, DownFunctions0),
    {reply, ok, State#state{down_functions=DownFunctions}};

handle_call({add_pre_interposition_fun, Name, PreInterpositionFun}, _From, #state{pre_interposition_funs=PreInterpositionFuns0}=State) ->
    PreInterpositionFuns = maps:put(Name, PreInterpositionFun, PreInterpositionFuns0),
    {reply, ok, State#state{pre_interposition_funs=PreInterpositionFuns}};

handle_call({remove_pre_interposition_fun, Name}, _From, #state{pre_interposition_funs=PreInterpositionFuns0}=State) ->
    PreInterpositionFuns = maps:remove(Name, PreInterpositionFuns0),
    {reply, ok, State#state{pre_interposition_funs=PreInterpositionFuns}};

handle_call({add_interposition_fun, Name, InterpositionFun}, _From, #state{interposition_funs=InterpositionFuns0}=State) ->
    InterpositionFuns = maps:put(Name, InterpositionFun, InterpositionFuns0),
    {reply, ok, State#state{interposition_funs=InterpositionFuns}};

handle_call({remove_interposition_fun, Name}, _From, #state{interposition_funs=InterpositionFuns0}=State) ->
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

%% For compatibility with external membership services.
handle_call({update_members, Nodes}, _From, #state{} = State) ->
    Membership = State#state.membership,

    %% Get the current membership.
    CurrentMembership = [N || #{name := N} <- Membership],

    %% need to support Nodes as a list of maps or atoms
    %% TODO: require each node to be a map
    NodesNames = lists:map(
        fun
            (#{name := N}) ->
                N;
            (N) when is_atom(N) ->
                N
        end,
        Nodes
    ),

    %% Compute leaving list.
    LeavingNodes = lists:filter(
        fun(N) -> not lists:member(N, NodesNames) end,
        CurrentMembership
    ),

    %% Issue leaves.
    State1 = lists:foldl(
        fun(N, S) ->
            case N of
                Map when is_map(Map) ->
                    internal_leave(Map, S);
                N when is_atom(N) ->
                    %% find map based on name
                    [Map] = lists:filter(
                        fun(M) -> maps:get(name, M) == N end,
                        Membership
                    ),
                    internal_leave(Map, S)
            end
        end,
        State,
        LeavingNodes
    ),

    %% Compute joining list.
    JoiningNodes = lists:filter(
        fun
            (#{name := N}) ->
                not lists:member(N, CurrentMembership);
            (N) when is_atom(N) ->
                not lists:member(N, CurrentMembership)
        end,
        Nodes
    ),

    %% Issue joins.
    State2=#state{pending=Pending} = lists:foldl(
        fun(N, S) -> internal_join(N, undefined, S) end,
        State1,
        JoiningNodes
    ),

    %% Compute current pending list.
    Pending1 = lists:filter(
        fun(#{name := N}) -> lists:member(N, NodesNames) end,
        Pending
    ),

    %% Finally schedule the removal of connections
    %% We do this async because internal_leave will schedule the sending of
    %% membership update messages
    gen_server:cast(?MODULE, {kill_connections, LeavingNodes}),

    {reply, ok, State2#state{pending=Pending1}};

handle_call({leave, #{name := Name} = Node}, From, State0) ->
    %% Perform leave.
    State = internal_leave(Node, State0),

    case partisan:node() of
        Name ->
            gen_server:reply(From, ok),

            %% We need to stop (to cleanup all connections and state) or do it
            %% manually. However, we cannot do it straight away as
            %% internal_leave/2 has send some async messages we need to process
            %% (casts to ourself) so we cast ourselves a shutdown message.
            gen_server:cast(?MODULE, stop),
            {noreply, State};
        _ ->
            gen_server:cast(?MODULE, {kill_connections, [Node]}),
            {reply, ok, State}
    end;

handle_call({join, #{name := Name} = Node}, _From, State0) ->
    case partisan:node() of
        Name ->
            %% Ignoring self join.
            {reply, ok, State0};
        _ ->
            %% Perform join.
            State = internal_join(Node, undefined, State0),

            %% Return.
            {reply, ok, State}
    end;

handle_call({sync_join, #{name := Name} = Node}, From, State0) ->
    ?LOG_DEBUG(#{
        description => "Starting synchronous join with peer",
        node => partisan:node(),
        peer => Node
    }),

    case partisan:node() of
        Name ->
            %% Ignoring self join.
            {reply, ok, State0};
        _ ->
            %% Perform join.
            State = internal_join(Node, From, State0),
            {noreply, State}
    end;

handle_call({send_message, Name, Channel, Message}, _From, #state{} = State) ->

    schedule_self_message_delivery(
        Name,
        Message,
        Channel,
        ?DEFAULT_PARTITION_KEY,
        State#state.pre_interposition_funs
    ),
    {reply, ok, State};

handle_call(
    {forward_message,
        Name, Channel, Clock, PartitionKey, ServerRef, OriginalMessage, Options
    },
    From,
    #state{} = State) ->

    %% Run all interposition functions.
    DeliveryFun = fun() ->
        %% Fire pre-interposition functions.
        PreFoldFun = fun(_Name, Fun, ok) ->
            Fun({forward_message, Name, OriginalMessage}),
            ok
        end,
        maps:fold(PreFoldFun, ok, State#state.pre_interposition_funs),

        %% Once pre-interposition returns, then schedule for delivery.
        Msg = {
            forward_message,
            From,
            Name,
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

handle_call({receive_message, Peer, OriginalMessage},
            From,
            #state{pre_interposition_funs=PreInterpositionFuns}=State) ->

    %% Run all interposition functions.
    DeliveryFun = fun() ->
        %% Fire pre-interposition functions.
        PreFoldFun = fun(_Name, PreInterpositionFun, ok) ->
            PreInterpositionFun({receive_message, Peer, OriginalMessage}),
            ok
        end,
        maps:fold(PreFoldFun, ok, PreInterpositionFuns),

        %% Once pre-interposition returns, then schedule for delivery.
        gen_server:cast(?MODULE, {receive_message, From, Peer, OriginalMessage})
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
    Members = [P || #{name := P} <- State#state.membership],
    {reply, {ok, Members}, State};

handle_call({member, Node}, _From, #state{} = State) ->
    IsMember = lists:any(
        fun(#{name := Name}) -> Name =:= Node end,
        State#state.membership
    ),
    {reply, IsMember, State};

handle_call(get_local_state, _From, #state{} = State) ->
    {reply, {ok, State#state.membership_strategy_state}, State};

handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.

handle_cast(stop, State) ->
    %% We send ourselves this message when we left the cluster
    %% We stop to cleanup, supervisor will start us again, this will kill all
    %% connections.
    {stop, normal, State};
    % {noreply, State};

handle_cast({kill_connections, Nodes}, State) ->
    ok = kill_connections(Nodes, State),
    {noreply, State};

handle_cast({receive_message, From, Peer, OriginalMessage}, #state{} = State) ->
    %% Filter messages using interposition functions.
    FoldFun = fun(_InterpositionName, Fun, M) ->
        Fun({receive_message, Peer, M})
    end,
    Message = maps:fold(
        FoldFun, OriginalMessage, State#state.interposition_funs
    ),

    %% Fire post-interposition functions.
    PostFoldFun = fun(_Name, Fun, ok) ->
        Fun(
            {receive_message, Peer, OriginalMessage},
            {receive_message, Peer, Message}
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
            gen_server:cast(?MODULE, {receive_message, From, Peer, NewMessage}),
            {noreply, State};

        _ ->
            handle_message(Message, From, State)
    end;

handle_cast({forward_message, From, Name, Channel, Clock, PartitionKey, ServerRef, OriginalMessage, Options},
            #state{interposition_funs=InterpositionFuns,
                   pre_interposition_funs=PreInterpositionFuns,
                   post_interposition_funs=PostInterpositionFuns,
                   vclock=VClock0}=State) ->
    %% Filter messages using interposition functions.
    FoldFun = fun(_InterpositionName, InterpositionFun, M) ->
        InterpositionResult = InterpositionFun({forward_message, Name, M}),
        InterpositionResult
    end,
    Message = maps:fold(FoldFun, OriginalMessage, InterpositionFuns),

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
                %% First time through.
                undefined ->
                    %% We don't have a clock yet, get one using the causality backend.
                    {ok, LocalClock0, CausalMessage} = partisan_causality_backend:emit(CausalLabel, Name, ServerRef, Message),

                    %% Wrap the clock wih a scope.
                    %% TODO: Maybe do this wrapping inside of the causality backend.
                    LocalClock = {CausalLabel, LocalClock0},

                    %% Return clock and wrapped message.
                    {LocalClock, CausalMessage};
                %% Retransmission.
                _ ->
                    %% Get the clock and message we used last time.
                    {ok, LocalClock, CausalMessage} = partisan_causality_backend:reemit(CausalLabel, Clock),

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
                            RescheduleableMessage = {forward_message, From, Name, Channel, MessageClock, PartitionKey, ServerRef, OriginalMessage, Options},
                            partisan_acknowledgement_backend:store(MessageClock, RescheduleableMessage);
                        true ->
                            ok
                    end
            end,

            %% Fire post-interposition functions.
            PostFoldFun = fun(_Name, PostInterpositionFun, ok) ->
                PostInterpositionFun({forward_message, Name, OriginalMessage}, {forward_message, Name, FullMessage}),
                ok
            end,
            maps:fold(PostFoldFun, ok, PostInterpositionFuns),

            ?LOG_DEBUG(
                "~p: Message ~p after send interposition is: ~p",
                [partisan:node(), OriginalMessage, FullMessage]
            ),

            case From of
                undefined ->
                    ok;
                _ ->
                    gen_server:reply(From, ok)
            end,

            {noreply, State#state{vclock=VClock}};
        {'$delay', NewMessage} ->
            ?LOG_DEBUG(
                "Delaying receive_message due to interposition result: ~p",
                [NewMessage]
            ),
            gen_server:cast(?MODULE, {forward_message, From, Name, Channel, Clock, PartitionKey, ServerRef, NewMessage, Options}),
            {noreply, State};
        _ ->
            %% Store for reliability, if necessary.
            Result = case maps:get(ack, Options, false) of
                false ->
                    %% Tracing.
                    WrappedMessage = {forward_message, ServerRef, FullMessage},
                    WrappedOriginalMessage = {forward_message, ServerRef, OriginalMessage},

                    %% Fire post-interposition functions -- trace after wrapping!
                    PostFoldFun = fun(_Name, PostInterpositionFun, ok) ->
                        PostInterpositionFun({forward_message, Name, WrappedOriginalMessage}, {forward_message, Name, WrappedMessage}),
                        ok
                    end,
                    maps:fold(PostFoldFun, ok, PostInterpositionFuns),

                    %% Send message along.
                    do_send_message(Name,
                                    Channel,
                                    PartitionKey,
                                    WrappedMessage,
                                    Options,
                                    PreInterpositionFuns);
                true ->
                    %% Tracing.
                    WrappedOriginalMessage = {forward_message, partisan:node(), MessageClock, ServerRef, OriginalMessage},
                    WrappedMessage = {forward_message, partisan:node(), MessageClock, ServerRef, FullMessage},

                    ?LOG_DEBUG(
                        "should acknowledge message: ~p", [WrappedMessage]
                    ),

                    %% Fire post-interposition functions -- trace after wrapping!
                    PostFoldFun = fun(_Name, PostInterpositionFun, ok) ->
                        PostInterpositionFun({forward_message, Name, WrappedOriginalMessage}, {forward_message, Name, WrappedMessage}),
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
                            RescheduleableMessage = {forward_message, From, Name, Channel, MessageClock, PartitionKey, ServerRef, OriginalMessage, Options},
                            partisan_acknowledgement_backend:store(MessageClock, RescheduleableMessage);
                        true ->
                            ok
                    end,

                    %% Send message along.
                    do_send_message(Name,
                                    Channel,
                                    PartitionKey,
                                    WrappedMessage,
                                    Options,
                                    PreInterpositionFuns)
            end,

            case From of
                undefined ->
                    ok;
                _ ->
                    gen_server:reply(From, Result)
            end,

            {noreply, State#state{vclock=VClock}}
    end;
handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),
    {noreply, State}.

%% @private
-spec handle_info(term(), state_t()) -> {noreply, state_t()}.
handle_info(tree_refresh, #state{}=State) ->
    %% Get lazily computed outlinks.
    OutLinks = retrieve_outlinks(),

    %% Reschedule.
    schedule_tree_refresh(),

    {noreply, State#state{out_links=OutLinks}};

handle_info(distance, #state{pending=Pending,
                             membership=Membership,
                             pre_interposition_funs=PreInterpositionFuns}=State) ->
    %% Establish any new connections.
    ok = establish_connections(Pending, Membership),

    %% Record time.
    SourceTime = erlang:timestamp(),

    %% Send distance requests.
    SourceNode = partisan:node_spec(),

    lists:foreach(fun(Peer) ->
        schedule_self_message_delivery(
            Peer,
            {ping, SourceNode, Peer, SourceTime},
            ?MEMBERSHIP_PROTOCOL_CHANNEL,
            ?DEFAULT_PARTITION_KEY,
            PreInterpositionFuns
        )
    end, Membership),

    schedule_distance(),

    {noreply, State};

handle_info(instrumentation, State) ->
    MessageQueueLen = process_info(self(), message_queue_len),
    ?LOG_DEBUG("message_queue_len: ~p", [MessageQueueLen]),
    schedule_instrumentation(),
    {noreply, State};

handle_info(periodic, #state{pending=Pending,
                             membership_strategy=MStrategy,
                             membership_strategy_state=MStrategyState0,
                             pre_interposition_funs=PreInterpositionFuns
                             }=State) ->
    {ok, Membership, OutgoingMessages, MStrategyState} = MStrategy:periodic(MStrategyState0),

    %% Establish any new connections.
    ok = establish_connections(Pending, Membership),

    %% Send outgoing messages.
    lists:foreach(fun({Peer, Message}) ->
        schedule_self_message_delivery(
            Peer,
            Message,
            ?MEMBERSHIP_PROTOCOL_CHANNEL,
            ?DEFAULT_PARTITION_KEY,
            PreInterpositionFuns
        )
    end, OutgoingMessages),

    schedule_periodic(),

    {noreply, State#state{membership=Membership,
                          membership_strategy_state=MStrategyState
                          }};

handle_info(retransmit, #state{pre_interposition_funs=PreInterpositionFuns}=State) ->
    RetransmitFun = fun({_, {forward_message, From, Name, Channel, Clock, PartitionKey, ServerRef, Message, Options}}) ->
        Mynode = partisan:node(),
        ?LOG_DEBUG(
            "~p no acknowledgement yet, restranmitting message ~p with clock ~p to ~p",
            [Mynode, Message, Clock, Name]
        ),

        %% Fire pre-interposition functions.
        PreFoldFun = fun(_Name, PreInterpositionFun, ok) ->
            ?LOG_DEBUG(
                "firing preinterposition fun for original message: ~p",
                [Message]
            ),
            PreInterpositionFun({forward_message, Name, Message}),
            ok
        end,
        maps:fold(PreFoldFun, ok, PreInterpositionFuns),

        %% Schedule message for redelivery.
        RetryOptions = Options#{retransmission => true},

        gen_server:cast(?MODULE, {forward_message, From, Name, Channel, Clock, PartitionKey, ServerRef, Message, RetryOptions})
    end,

    {ok, Outstanding} = partisan_acknowledgement_backend:outstanding(),

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver, preserving serial order of messages.
            lists:foreach(RetransmitFun, Outstanding);
        true ->
            %% Allow the system to proceed, and the message will be delivered once pre-interposition is done.
            lists:foreach(fun(OutstandingMessage) ->
                spawn_link(fun() ->
                    RetransmitFun(OutstandingMessage)
                end)
            end, Outstanding)
    end,

    %% Reschedule retransmission.
    schedule_retransmit(),

    {noreply, State};

handle_info(connections, #state{} = State) ->
    #state{
        pending = Pending,
        membership = Membership,
        sync_joins = SyncJoins0
    } = State,

    %% Establish any new connections.
    ok = establish_connections(Pending, Membership),

    %% Advance sync_join's if we have enough open connections to remote host.
    SyncJoins = lists:foldl(
        fun({NodeSpec, FromPid}, Joins) ->
            case partisan:is_fully_connected(NodeSpec) of
                true ->
                    ?LOG_DEBUG("Node ~p is now fully connected.", [NodeSpec]),

                    gen_server:reply(FromPid, ok),

                    Joins;

                false ->
                    Joins ++ [{NodeSpec, FromPid}]
            end
        end,
        [],
        SyncJoins0
    ),

    schedule_connections(),

    {noreply, State#state{pending = Pending, sync_joins = SyncJoins}};

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
                fun({Peer, Message}) ->
                    schedule_self_message_delivery(
                        Peer,
                        Message,
                        ?MEMBERSHIP_PROTOCOL_CHANNEL,
                        ?DEFAULT_PARTITION_KEY,
                        PreInterpositionFuns
                    )
                end,
                OutgoingMessages
            ),

            %% notify subscribers
            up(NodeSpec, State0),

            ok = case Membership of
                Membership0 ->
                    ok;
                _ ->
                    partisan_peer_service_events:update(Membership)
            end,

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
        {NodeSpec, FromPid} ->
            case partisan:is_fully_connected(NodeSpec) of
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
-spec terminate(term(), state_t()) -> term().

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
-spec code_change(term() | {down, term()}, state_t(), term()) ->
    {ok, state_t()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%%%===================================================================
%%% Internal functions
%%%===================================================================



%% @private
gen_actor() ->
    Node = atom_to_list(partisan:node()),
    Unique = erlang:unique_integer([positive]),
    TS = integer_to_list(Unique),
    Term = Node ++ TS,
    crypto:hash(sha, Term).


%% @private
without_me(Members) ->
    MyNode = partisan:node(),
    lists:filter(fun(#{name := Name}) -> Name =/= MyNode end, Members).


%% @private
%% @doc Establish any new connections.
%% @end
establish_connections(Pending, Membership) ->
    %% Compute list of nodes that should be connected.
    Peers = without_me(Membership ++ Pending),

    %% Reconnect disconnected members and members waiting to join.
    ok = lists:foreach(
        fun(Peer) -> partisan_util:maybe_connect(Peer) end,
        Peers
    ).



%% @private
kill_connections(Nodes, State) ->
    MyNode = partisan:node(),
    _ = [
        begin
            ok = partisan_peer_connections:erase(Node),
            ok = down(Node, State)
        end || Node <- Nodes, Node =/= MyNode
    ],
    ok.


%% @private
handle_message(
    {ping, SourceNode, DestinationNode, SourceTime}, From, #state{} = State) ->

    #state{
        pending = Pending,
        membership = Membership,
        pre_interposition_funs = PreInterpositionFuns
    } = State,

    %% Establish any new connections.
    ok = establish_connections(Pending, Membership),

    %% Send ping response.
    schedule_self_message_delivery(
        SourceNode,
        {pong, SourceNode, DestinationNode, SourceTime},
        ?MEMBERSHIP_PROTOCOL_CHANNEL,
        ?DEFAULT_PARTITION_KEY,
        PreInterpositionFuns
    ),

    optional_gen_server_reply(From, ok),

    {noreply, State};

handle_message({pong, SourceNode, DestinationNode, SourceTime},
               From,
               #state{distance_metrics=DistanceMetrics0}=State) ->
    %% Compute difference.
    ArrivalTime = erlang:timestamp(),
    Difference = timer:now_diff(ArrivalTime, SourceTime),

    ?LOG_TRACE(
        "Updating distance metric for node ~p => ~p communication: ~p",
        [SourceNode, DestinationNode, Difference]
    ),

    %% Update differences.
    DistanceMetrics = maps:put(DestinationNode, Difference, DistanceMetrics0),

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
    case Membership of
        Membership0 ->
            ok;
        _ ->
            partisan_peer_service_events:update(Membership)
    end,

    %% Send outgoing messages.
    lists:foreach(
        fun({Peer, Message}) ->
            schedule_self_message_delivery(
                Peer,
                Message,
                ?MEMBERSHIP_PROTOCOL_CHANNEL,
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

    %% Establish any new connections.
    ok = establish_connections(Pending, Membership),

    State = State1#state{pending = Pending},

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
handle_message({forward_message, SourceNode, MessageClock, ServerRef, {causal, Label, _, _, _, _, _} = Message},
               From,
               #state{pre_interposition_funs=PreInterpositionFuns}=State) ->
    %% Send message acknowledgement.
    send_acknowledgement(SourceNode, MessageClock, PreInterpositionFuns),

    case partisan_causality_backend:is_causal_message(Message) of
        true ->
            partisan_causality_backend:receive_message(Label, Message);
        false ->
            %% Attempt message delivery.
            partisan_util:process_forward(ServerRef, Message)
    end,

    optional_gen_server_reply(From, ok),

    {noreply, State};

%% Acknowledged messages.
handle_message({forward_message, SourceNode, MessageClock, ServerRef, Message},
               From,
               #state{pre_interposition_funs=PreInterpositionFuns}=State) ->
    %% Send message acknowledgement.
    send_acknowledgement(SourceNode, MessageClock, PreInterpositionFuns),

    partisan_util:process_forward(ServerRef, Message),

    optional_gen_server_reply(From, ok),

    {noreply, State};

%% Causal messages.
handle_message({forward_message, ServerRef, {causal, Label, _, _, _, _, _} = Message},
               From,
               State) ->
    case partisan_causality_backend:is_causal_message(Message) of
        true ->
            partisan_causality_backend:receive_message(Label, Message);
        false ->
            %% Attempt message delivery.
            partisan_util:process_forward(ServerRef, Message)
    end,

    optional_gen_server_reply(From, ok),

    {noreply, State};

%% Best-effort messages.
%% TODO: Maybe remove me.
handle_message({forward_message, ServerRef, Message},
               From,
               State) ->
    partisan_util:process_forward(ServerRef, Message),
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
            PeriodicInterval = partisan_config:get(periodic_interval, ?PERIODIC_INTERVAL),
            erlang:send_after(PeriodicInterval, ?MODULE, periodic);
        false ->
            ok
    end.

%% @private
schedule_retransmit() ->
    RetransmitInterval = partisan_config:get(retransmit_interval, 1000),
    erlang:send_after(RetransmitInterval, ?MODULE, retransmit).

%% @private
schedule_connections() ->
    ConnectionInterval = partisan_config:get(connection_interval, 1000),
    erlang:send_after(ConnectionInterval, ?MODULE, connections).

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
internal_leave(#{name := Name} = Node, State) ->

    #state{
        pending = Pending,
        membership_strategy = MStrategy,
        membership_strategy_state = MStrategyState0,
        pre_interposition_funs = PreInterpositionFuns
    } = State,

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

    %% Establish any new connections.
    ok = establish_connections(Pending, Membership),

    %% Transmit outgoing messages.
    lists:foreach(
        fun
            ({#{name := Peername}, Message}) ->
                schedule_self_message_delivery(
                    Peername,
                    Message,
                    ?MEMBERSHIP_PROTOCOL_CHANNEL,
                    ?DEFAULT_PARTITION_KEY,
                    PreInterpositionFuns
                )
        end,
        OutgoingMessages
    ),

    partisan_peer_service_events:update(Membership),

    case partisan_config:get(connect_disterl, false) of
        true ->
            %% call the net_kernel:disconnect(Node) function to leave
            %% erlang network explicitly
            net_kernel:disconnect(Name);
        false ->
            ok
    end,

    State#state{
        membership = Membership,
        membership_strategy_state = MStrategyState
    }.


%% @private
internal_join(#{name := Node} = NodeSpec, From, #state{} = State) ->

    #state{
        pending = Pending0,
        membership = Membership,
        sync_joins = SyncJoins0
    } = State,

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

    %% Establish any new connections.
    ok = establish_connections(Pending, Membership),

    State#state{
        pending = Pending,
        sync_joins = SyncJoins
    }.


%% @private
rand_bits(Bits) ->
        Bytes = (Bits + 7) div 8,
        <<Result:Bits/bits, _/bits>> = crypto:strong_rand_bytes(Bytes),
        Result.

%% @private
avoid_rush() ->
    %% Sleep before connecting, to avoid a rush on connections.
    ConnectionJitter = partisan_config:get(connection_jitter, ?CONNECTION_JITTER),
    case partisan_config:get(jitter, false) of
        true ->
            timer:sleep(rand:uniform(ConnectionJitter));
        false ->
            timer:sleep(ConnectionJitter)
    end.

%% @private
do_tree_forward(Node, Channel, PartitionKey, Message, Options, TTL, PreInterpositionFuns) ->
    ?LOG_TRACE(
        "Attempting to forward message ~p from ~p to ~p.",
        [Message, partisan:node(), Node]
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
            OL -- [partisan:node()]
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

    OutLinks = try partisan_plumtree_broadcast:debug_get_peers(partisan:node(), Root, 1000) of
        {EagerPeers, _LazyPeers} ->
            ordsets:to_list(EagerPeers)
    catch
        _:_ ->
            ?LOG_INFO(#{description => "Request to get outlinks timed out..."}),
            []
    end,

    ?LOG_TRACE("Finished getting outlinks: ~p", [OutLinks]),

    OutLinks -- [partisan:node()].

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
    Name, Message, Channel, PartitionKey, PreInterpositionFuns) ->
    schedule_self_message_delivery(
        Name, Message, Channel, PartitionKey, #{}, PreInterpositionFuns
    ).

%% @private
schedule_self_message_delivery(
    Name, Message, Channel, PartitionKey, Options, PreInterpositionFuns) ->
    %% Run all interposition functions.
    DeliveryFun = fun() ->
        %% Fire pre-interposition functions.
        PreFoldFun = fun(_Name, PreInterpositionFun, ok) ->
            ?LOG_DEBUG(
                "firing forward_message preinterposition fun for message: ~p",
                [Message]
            ),
            PreInterpositionFun({forward_message, Name, Message}),
            ok
        end,

        maps:fold(PreFoldFun, ok, PreInterpositionFuns),

        %% Once pre-interposition returns, then schedule for delivery.
        gen_server:cast(?MODULE, {
            forward_message,
            undefined,
            Name,
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
send_acknowledgement(Name, MessageClock, PreInterpositionFuns) ->
    %% Generate message.
    Message = {ack, MessageClock},

    %% Send on the default channel.
    schedule_self_message_delivery(
        Name,
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


forward_options() ->
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


