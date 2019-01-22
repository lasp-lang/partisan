%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Helium Systems, Inc.  All Rights Reserved.
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

-module(partisan_pluggable_peer_service_manager).

-behaviour(gen_server).
-behaviour(partisan_peer_service_manager).

%% partisan_peer_service_manager callbacks
-export([start_link/0,
         members/0,
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

%% debug callbacks
-export([connections/0]).

-include("partisan.hrl").

-define(SET, state_orset).

-type pending() :: [node_spec()].
-type from() :: {pid(), atom()}.

-record(state, {actor :: actor(),
                distance_metrics :: dict:dict(),
                vclock :: term(),
                pending :: pending(),
                membership :: list(),
                down_functions :: dict:dict(),
                up_functions :: dict:dict(),
                out_links :: [term()],
                pre_interposition_funs :: dict:dict(),
                interposition_funs :: dict:dict(),
                post_interposition_funs :: dict:dict(),
                sync_joins :: [{node_spec(), from()}],
                connections :: partisan_peer_service_connections:t(),
                membership_strategy :: atom(),
                membership_strategy_state :: term()}).

-type state_t() :: #state{}.

%%%===================================================================
%%% partisan_peer_service_manager callbacks
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return membership list.
members() ->
    gen_server:call(?MODULE, members, infinity).

%% @doc Return membership list.
members_for_orchestration() ->
    gen_server:call(?MODULE, members_for_orchestration, infinity).

%% @doc Return connections list.
connections() ->
    gen_server:call(?MODULE, connections, infinity).

%% @doc Return myself.
myself() ->
    partisan_peer_service_manager:myself().

%% @doc Update membership.
update_members(Nodes) ->
    gen_server:call(?MODULE, {update_members, Nodes}, infinity).

%% @doc Return local node's view of cluster membership.
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).

%% @doc Trigger function on connection close for a given node.
on_down(#{name := Name}, Function) ->
    on_down(Name, Function);
on_down(Name, Function) when is_atom(Name) ->
    gen_server:call(?MODULE, {on_down, Name, Function}, infinity).

%% @doc Trigger function on connection open for a given node.
on_up(#{name := Name}, Function) ->
    on_up(Name, Function);
on_up(Name, Function) when is_atom(Name) ->
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
forward_message({partisan_remote_reference, Name, ServerRef}, Message) ->
    forward_message(Name, ?DEFAULT_CHANNEL, ServerRef, Message).

%% @doc Forward message to registered process on the remote side.
forward_message(Name, ServerRef, Message) ->
    forward_message(Name, ?DEFAULT_CHANNEL, ServerRef, Message).

%% @doc Forward message to registered process on the remote side.
forward_message(Name, Channel, ServerRef, Message) ->
    forward_message(Name, Channel, ServerRef, Message, []).

%% @doc Forward message to registered process on the remote side.
forward_message(Name, Channel, ServerRef, Message, Options) ->
    lager:info("Entering forward_message at ~p for message: ~p ~p", [node(), Name, Message]),

    %% Attempt to get the partition key, if possible.
    PartitionKey = proplists:get_value(partition_key, Options, ?DEFAULT_PARTITION_KEY),

    %% Use a clock provided by the sender, otherwise, use a generated one.
    Clock = proplists:get_value(clock, Options, undefined),

    %% Should ack?
    ShouldAck = proplists:get_value(ack, Options, false),

    %% Use causality?
    CausalLabel = proplists:get_value(causal_label, Options, undefined),

    %% Use configuration to disable fast forwarding.
    DisableFastForward = partisan_config:get(disable_fast_forward, false),

    %% Should use fast forwarding?
    %%
    %% Conditions:
    %% - not labeled for causal delivery
    %% - message does not need acknowledgements
    %%
    FastForward = not (CausalLabel =/= undefined) andalso 
                  not ShouldAck andalso 
                  not DisableFastForward,

    %% If attempting to forward to the local node, bypass.
    case partisan_peer_service_manager:mynode() of
        Name ->
            partisan_util:process_forward(ServerRef, Message),
            ok;
        _ ->
            Disterl = partisan_config:get(disterl, false),
            case Disterl of
                true ->
                    partisan_util:process_forward(ServerRef, Message),
                    ok;
                false ->
                    FullMessage = case partisan_config:get(binary_padding, false) of
                        true ->
                            BinaryPadding = partisan_config:get(binary_padding_term, undefined),
                            {forward_message, Name, Channel, Clock, PartitionKey, ServerRef, {'$partisan_padded', BinaryPadding, Message}, Options};
                        false ->
                            {forward_message, Name, Channel, Clock, PartitionKey, ServerRef, Message, Options}
                    end,

                    case FastForward of
                        true ->
                            %% Attempt to fast-path through the memoized connection cache.
                            case partisan_connection_cache:dispatch(FullMessage) of
                                ok ->
                                    ok;
                                {error, trap} ->
                                    gen_server:call(?MODULE, FullMessage, infinity)
                            end;
                        false ->
                            gen_server:call(?MODULE, FullMessage, infinity)
                    end                
            end
    end.

%% @doc Receive message from a remote manager.
receive_message(_Peer, {forward_message, _SourceNode, _MessageClock, _ServerRef, _Message} = FullMessage) ->
    %% Process the message and generate the acknowledgement.
    gen_server:call(?MODULE, {receive_message, FullMessage}, infinity);
receive_message(Peer, {forward_message, ServerRef, {'$partisan_padded', _Padding, Message}}) ->
    receive_message(Peer, {forward_message, ServerRef, Message});
receive_message(_Peer, {forward_message, _ServerRef, {causal, Label, _, _, _, _, _} = Message}) ->
    partisan_causality_backend:receive_message(Label, Message);
receive_message(Peer, {forward_message, ServerRef, Message} = FullMessage) ->
    % lager:info("in mesage receive at node ~p for peer ~p", [node(), Peer]),

    case partisan_config:get(disable_fast_receive, true) of
        true ->
            % lager:info("in mesage receive at node ~p for peer ~p FAST RECEIVE DISABLE", [node(), Peer]),
            gen_server:call(?MODULE, {receive_message, Peer, FullMessage}, infinity);
        false ->
            % lager:info("in mesage receive at node ~p for peer ~p FAST RECEIVE NOT DISABLE", [node(), Peer]),
            partisan_util:process_forward(ServerRef, Message)
    end;
receive_message(_Peer, Message) ->
    gen_server:call(?MODULE, {receive_message, Message}, infinity).

%% @doc Attempt to join a remote node.
sync_join(Node) ->
    gen_server:call(?MODULE, {sync_join, Node}, infinity).

%% @doc Attempt to join a remote node.
join(Node) ->
    gen_server:call(?MODULE, {join, Node}, infinity).

%% @doc Leave the cluster.
leave() ->
    gen_server:call(?MODULE, {leave, partisan_peer_service_manager:mynode()}, infinity).

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
    Connections = partisan_peer_service_connections:new(),
    MembershipStrategy = partisan_config:get(membership_strategy),
    {ok, Membership, MembershipStrategyState} = MembershipStrategy:init(Actor),

    DistanceMetrics = dict:new(),

    {ok, #state{actor=Actor,
                pending=[],
                vclock=VClock,
                pre_interposition_funs=dict:new(),
                interposition_funs=dict:new(),
                post_interposition_funs=dict:new(),
                connections=Connections,
                distance_metrics=DistanceMetrics,
                sync_joins=[],
                up_functions=dict:new(),
                down_functions=dict:new(),
                out_links=[],
                membership=Membership,
                membership_strategy=MembershipStrategy,
                membership_strategy_state=MembershipStrategyState}}.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call({on_up, Name, Function},
            _From,
            #state{up_functions=UpFunctions0}=State) ->
    UpFunctions = dict:append(Name, Function, UpFunctions0),
    {reply, ok, State#state{up_functions=UpFunctions}};

handle_call({on_down, Name, Function},
            _From,
            #state{down_functions=DownFunctions0}=State) ->
    DownFunctions = dict:append(Name, Function, DownFunctions0),
    {reply, ok, State#state{down_functions=DownFunctions}};

handle_call({add_pre_interposition_fun, Name, PreInterpositionFun}, _From, #state{pre_interposition_funs=PreInterpositionFuns0}=State) ->
    PreInterpositionFuns = dict:store(Name, PreInterpositionFun, PreInterpositionFuns0),
    {reply, ok, State#state{pre_interposition_funs=PreInterpositionFuns}};

handle_call({remove_pre_interposition_fun, Name}, _From, #state{pre_interposition_funs=PreInterpositionFuns0}=State) ->
    PreInterpositionFuns = dict:erase(Name, PreInterpositionFuns0),
    {reply, ok, State#state{pre_interposition_funs=PreInterpositionFuns}};

handle_call({add_interposition_fun, Name, InterpositionFun}, _From, #state{interposition_funs=InterpositionFuns0}=State) ->
    InterpositionFuns = dict:store(Name, InterpositionFun, InterpositionFuns0),
    {reply, ok, State#state{interposition_funs=InterpositionFuns}};

handle_call({remove_interposition_fun, Name}, _From, #state{interposition_funs=InterpositionFuns0}=State) ->
    InterpositionFuns = dict:erase(Name, InterpositionFuns0),
    {reply, ok, State#state{interposition_funs=InterpositionFuns}};

handle_call({add_post_interposition_fun, Name, PostInterpositionFun}, _From, #state{post_interposition_funs=PostInterpositionFuns0}=State) ->
    PostInterpositionFuns = dict:store(Name, PostInterpositionFun, PostInterpositionFuns0),
    {reply, ok, State#state{post_interposition_funs=PostInterpositionFuns}};

handle_call({remove_post_interposition_fun, Name}, _From, #state{post_interposition_funs=PostInterpositionFuns0}=State) ->
    PostInterpositionFuns = dict:erase(Name, PostInterpositionFuns0),
    {reply, ok, State#state{post_interposition_funs=PostInterpositionFuns}};

%% For compatibility with external membership services.
handle_call({update_members, Nodes}, 
            _From, 
            #state{membership=Membership}=State) ->
    % lager:debug("Updating membership with: ~p", [Nodes]),

    %% Get the current membership.
    CurrentMembership = [N || #{name := N} <- Membership],
    % lager:debug("CurrentMembership: ~p", [CurrentMembership]),

    %% need to support Nodes as a list of maps or atoms
    %% TODO: require each node to be a map
    NodesNames = lists:map(fun(#{name := N}) ->
                                   N;
                              (N) when is_atom(N) ->
                                   N
                           end, Nodes),

    %% Compute leaving list.
    LeavingNodes = lists:filter(fun(N) ->
                                        not lists:member(N, NodesNames)
                                end, CurrentMembership),
    % lager:debug("LeavingNodes: ~p", [LeavingNodes]),

    %% Issue leaves.
    State1 = lists:foldl(fun(N, S) ->
                                 internal_leave(N, S)
                         end, State, LeavingNodes),

    %% Compute joining list.
    JoiningNodes = lists:filter(fun(#{name := N}) ->
                                        not lists:member(N, CurrentMembership);
                                   (N) when is_atom(N) ->
                                        not lists:member(N, CurrentMembership)
                                end, Nodes),
    % lager:debug("JoiningNodes: ~p", [JoiningNodes]),

    %% Issue joins.
    State2=#state{pending=Pending} = lists:foldl(fun(N, S) ->
                                                         internal_join(N, undefined, S)
                                                 end, State1, JoiningNodes),

    %% Compute current pending list.
    Pending1 = lists:filter(fun(#{name := N}) ->
                                    lists:member(N, NodesNames)
                            end, Pending),

    {reply, ok, State2#state{pending=Pending1}};

handle_call({leave, #{name := Name} = Node}, 
            From, 
            State0) ->
    %% Perform leave.
    State = internal_leave(Node, State0),

    case partisan_peer_service_manager:mynode() of
        Name ->
            gen_server:reply(From, ok),

            {stop, normal, State};
        _ ->
            {reply, ok, State}
    end;

handle_call({join, #{name := Name} = Node},
            _From,
            State0) ->
    case partisan_peer_service_manager:mynode() of
        Name ->
            %% Ignoring self join.
            {reply, ok, State0};
        _ ->
            %% Perform join.
            State = internal_join(Node, undefined, State0),

            %% Return.
            {reply, ok, State}
    end;

handle_call({sync_join, #{name := Name} = Node},
            From,
            State0) ->
    lager:debug("Starting synchronous join to ~p from ~p", [Node, partisan_peer_service_manager:mynode()]),

    case partisan_peer_service_manager:mynode() of
        Name ->
            %% Ignoring self join.
            {reply, ok, State0};
        _ ->
            %% Perform join.
            State = internal_join(Node, From, State0),

            %% Return.
            {noreply, State}
    end;

handle_call({send_message, Name, Channel, Message}, _From,
            #state{connections=Connections}=State) ->
    Result = do_send_message(Name, 
                             Channel, 
                             ?DEFAULT_PARTITION_KEY, 
                             Message, 
                             Connections),
    {reply, Result, State};

handle_call({forward_message, Name, Channel, Clock, PartitionKey, ServerRef, OriginalMessage, Options}, 
            From, 
            #state{pre_interposition_funs=PreInterpositionFuns}=State) ->
    %% Run all interposition functions.
    DeliveryFun = fun() ->
        %% Fire pre-interposition functions.
        PreFoldFun = fun(_Name, PreInterpositionFun, ok) ->
            lager:info("firing preinterposition fun for original message: ~p", [OriginalMessage]),
            PreInterpositionFun({forward_message, Name, OriginalMessage}),
            ok
        end,
        dict:fold(PreFoldFun, ok, PreInterpositionFuns),

        %% Once pre-interposition returns, then schedule for delivery.
        gen_server:cast(?MODULE, {forward_message, From, Name, Channel, Clock, PartitionKey, ServerRef, OriginalMessage, Options})
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
            lager:info("firing preinterposition fun for original message: ~p", [OriginalMessage]),
            PreInterpositionFun({receive_message, Peer, OriginalMessage}),
            ok
        end,
        dict:fold(PreFoldFun, ok, PreInterpositionFuns),

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

handle_call({receive_message, Message}, From, State) ->
    handle_message(Message, From, State);

handle_call(members_for_orchestration, _From, #state{membership=Membership}=State) ->
    {reply, {ok, Membership}, State};

handle_call(members, _From, #state{membership=Membership}=State) ->
    Members = [P || #{name := P} <- Membership],
    {reply, {ok, Members}, State};

handle_call(connections, _From, #state{connections=Connections}=State) ->
    {reply, {ok, Connections}, State};

handle_call(get_local_state, _From, #state{membership_strategy_state=MembershipStrategyState}=State) ->
    {reply, {ok, MembershipStrategyState}, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.
handle_cast({receive_message, From, Peer, OriginalMessage}, 
            #state{interposition_funs=InterpositionFuns,
                   post_interposition_funs=PostInterpositionFuns} = State) ->
    lager:info("~p: Inside the receive interposition, message from ~p at node ~p", [node(), Peer, node()]),
    lager:info("~p: Count of interposition funs: ~p", [node(), dict:size(InterpositionFuns)]),

    %% Filter messages using interposition functions.
    FoldFun = fun(_Name, InterpositionFun, M) ->
        InterpositionFun({receive_message, Peer, M})
    end,
    Message = dict:fold(FoldFun, OriginalMessage, InterpositionFuns),

    %% Fire post-interposition functions.
    PostFoldFun = fun(_Name, PostInterpositionFun, ok) ->
        PostInterpositionFun({receive_message, Peer, OriginalMessage}, {receive_message, Peer, Message}),
        ok
    end,
    dict:fold(PostFoldFun, ok, PostInterpositionFuns),

    lager:info("~p: Message after receive interposition is: ~p", [node(), Message]),

    case Message of
        undefined ->
            gen_server:reply(From, ok),
            {noreply, State};
        _ ->
            handle_message(Message, From, State)
    end;

handle_cast({forward_message, From, Name, Channel, Clock, PartitionKey, ServerRef, OriginalMessage, Options},
            #state{interposition_funs=InterpositionFuns, 
                   post_interposition_funs=PostInterpositionFuns, 
                   connections=Connections, 
                   vclock=VClock0}=State) ->
    lager:info("~p: Inside the send interposition, message ~p from ~p at node ~p", [node(), OriginalMessage, Name, node()]),
    lager:info("~p: Count of interposition funs: ~p", [node(), dict:size(InterpositionFuns)]),

    %% Filter messages using interposition functions.
    FoldFun = fun(_Name, InterpositionFun, M) ->
        InterpositionFun({forward_message, Name, M})
    end,
    Message = dict:fold(FoldFun, OriginalMessage, InterpositionFuns),

    case Message of
        undefined ->
            %% Fire post-interposition functions.
            PostFoldFunUndefined = fun(_Name, PostInterpositionFun, ok) ->
                PostInterpositionFun({forward_message, Name, OriginalMessage}, {forward_message, Name, Message}),
                ok
            end,
            dict:fold(PostFoldFunUndefined, ok, PostInterpositionFuns),

            lager:info("~p: Message after send interposition is: ~p", [node(), Message]),

            gen_server:reply(From, ok),

            {noreply, State};
        Message ->
            %% Increment the clock.
            VClock = partisan_vclock:increment(myself(), VClock0),

            %% Are we using causality?
            CausalLabel = proplists:get_value(causal_label, Options, undefined),

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

                    {LocalClock,
                        {forward_message, Name, Channel, Clock, PartitionKey, ServerRef, Message, Options}};
                CausalLabel ->
                    %% Get causality assignment and message buffer.
                    {ok, LocalClock0, CausalMessage} = partisan_causality_backend:emit(CausalLabel, Name, ServerRef, Message),

                    %% Wrap the clock wih a scope.
                    %% TODO: Maybe do this wrapping inside of the causality backend.
                    LocalClock = {CausalLabel, LocalClock0},

                    {LocalClock,
                        {forward_message, Name, Channel, LocalClock, PartitionKey, ServerRef, CausalMessage, Options}}
            end,

            %% Store for reliability, if necessary.
            Result = case proplists:get_value(ack, Options, false) of
                false ->
                    %% Tracing.
                    WrappedMessage =  {forward_message, ServerRef, Message},
                    WrappedOriginalMessage =  {forward_message, ServerRef, OriginalMessage},

                    %% Fire post-interposition functions -- trace after wrapping!
                    PostFoldFun = fun(_Name, PostInterpositionFun, ok) ->
                        PostInterpositionFun({forward_message, Name, WrappedOriginalMessage}, {forward_message, Name, WrappedMessage}),
                        ok
                    end,
                    dict:fold(PostFoldFun, ok, PostInterpositionFuns),

                    lager:info("~p: Message after send interposition is: ~p", [node(), Message]),

                    %% Send message along.
                    do_send_message(Name,
                                    Channel,
                                    PartitionKey,
                                    WrappedMessage,
                                    Connections);
                true ->
                    %% Tracing.
                    WrappedOriginalMessage = {forward_message, myself(), MessageClock, ServerRef, OriginalMessage},
                    WrappedMessage = {forward_message, myself(), MessageClock, ServerRef, Message},

                    %% Fire post-interposition functions -- trace after wrapping!
                    PostFoldFun = fun(_Name, PostInterpositionFun, ok) ->
                        PostInterpositionFun({forward_message, Name, WrappedOriginalMessage}, {forward_message, Name, WrappedMessage}),
                        ok
                    end,
                    dict:fold(PostFoldFun, ok, PostInterpositionFuns),

                    lager:info("~p: Message after send interposition is: ~p", [node(), Message]),

                    %% Acknowledgements.
                    partisan_acknowledgement_backend:store(MessageClock, FullMessage),

                    %% Send message along.
                    do_send_message(Name,
                                    Channel,
                                    PartitionKey,
                                    WrappedMessage,
                                    Connections)
            end,

            gen_server:reply(From, Result),

            {noreply, State#state{vclock=VClock}}
    end;
handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
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
                             connections=Connections0}=State) ->
    %% Establish any new connections.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),

    %% Record time.
    SourceTime = erlang:timestamp(),

    %% Send distance requests.
    SourceNode = partisan_peer_service_manager:myself(),

    lists:foreach(fun(Peer) ->
                do_send_message(Peer,
                                ?MEMBERSHIP_PROTOCOL_CHANNEL,
                                ?DEFAULT_PARTITION_KEY,
                                {ping, SourceNode, Peer, SourceTime},
                                Connections)
        end, Membership),

    schedule_distance(),

    {noreply, State#state{connections=Connections}};

handle_info(periodic, #state{pending=Pending,
                             membership_strategy=MembershipStrategy,
                             membership_strategy_state=MembershipStrategyState0,
                             connections=Connections0}=State) ->
    {ok, Membership, OutgoingMessages, MembershipStrategyState} = MembershipStrategy:periodic(MembershipStrategyState0),

    %% Establish any new connections.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),

    %% Send outgoing messages.
    lists:foreach(fun({Peer, Message}) ->
                do_send_message(Peer,
                                ?MEMBERSHIP_PROTOCOL_CHANNEL,
                                ?DEFAULT_PARTITION_KEY,
                                Message,
                                Connections)
        end, OutgoingMessages),

    schedule_periodic(),

    {noreply, State#state{membership=Membership,
                          membership_strategy_state=MembershipStrategyState,
                          connections=Connections}};

handle_info(retransmit, #state{connections=Connections}=State) ->
    RetransmitFun = fun({_, {forward_message, Name, Channel, _Clock, PartitionKey, ServerRef, Message, _Options}}) ->
        do_send_message(Name,
                        Channel,
                        PartitionKey,
                        {forward_message, ServerRef, Message},
                        Connections)
    end,
    {ok, Outstanding} = partisan_acknowledgement_backend:outstanding(),
    lists:foreach(RetransmitFun, Outstanding),
    schedule_retransmit(),
    {noreply, State};

handle_info(connections, #state{pending=Pending,
                                membership=Membership,
                                sync_joins=SyncJoins0, 
                                connections=Connections0}=State) ->
    %% Trigger connection.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),

    %% Advance sync_join's if we have enough open connections to remote host.
    SyncJoins = lists:foldl(fun({Node, FromPid}, Joins) ->
            case fully_connected(Node, Connections) of
                true ->
                    lager:debug("Node ~p is now fully connected.", [Node]),
                    gen_server:reply(FromPid, ok),
                    Joins;
                _ ->
                    Joins ++ [{Node, FromPid}]
            end
        end, [], SyncJoins0),

    schedule_connections(),

    {noreply, State#state{pending=Pending,
                          sync_joins=SyncJoins,
                          connections=Connections}};

handle_info({'EXIT', From, _Reason}, #state{connections=Connections0}=State) ->
    %% invoke the down callback on each matching entry
    partisan_peer_service_connections:foreach(
          fun(Node, Pids) ->
                case lists:keymember(From, 3, Pids) andalso length(Pids) =:= 1 of
                    true ->
                        down(Node, State);
                    false ->
                        ok
                end
          end, Connections0),
    %% and then prune all node entries containing the exit pid
    {_, Connections} = partisan_peer_service_connections:prune(From, Connections0),
    {noreply, State#state{connections=Connections}};

handle_info({connected, Node, _Tag, RemoteState},
               #state{pending=Pending0,
                      sync_joins=SyncJoins0,
                      connections=Connections,
                      membership_strategy=MembershipStrategy,
                      membership_strategy_state=MembershipStrategyState0}=State0) ->
    lager:debug("Node ~p connected!", [Node]),

    State = case lists:member(Node, Pending0) of
        true ->
            %% Move out of pending.
            Pending = Pending0 -- [Node],

            %% Update membership by joining with remote membership.
            {ok, Membership, OutgoingMessages, MembershipStrategyState} = MembershipStrategy:join(MembershipStrategyState0, Node, RemoteState),

            %% Gossip the new membership.
            lists:foreach(fun({Peer, Message}) ->
                        do_send_message(Peer,
                                        ?MEMBERSHIP_PROTOCOL_CHANNEL,
                                        ?DEFAULT_PARTITION_KEY,
                                        Message,
                                        Connections)
                end, OutgoingMessages),

            %% Announce to the peer service.
            partisan_peer_service_events:update(Membership),

            %% Send up notifications.
            partisan_peer_service_connections:foreach(
                fun(N, Pids) ->
                        case length(Pids -- Pending) =:= 1 of
                            true ->
                                up(N, State0);
                            false ->
                                ok
                        end
                end, Connections),

            %% Return.
            State0#state{pending=Pending,
                         membership=Membership,
                         membership_strategy_state=MembershipStrategyState};
        false ->
            State0
    end,

    %% Notify for sync join.
    SyncJoins = case lists:keyfind(Node, 1, SyncJoins0) of
        {Node, FromPid} ->
            case fully_connected(Node, Connections) of
                true ->
                    gen_server:reply(FromPid, ok),
                    lists:keydelete(FromPid, 2, SyncJoins0);
                _ ->
                    SyncJoins0
            end;
        false ->
            SyncJoins0
    end,

    {noreply, State#state{sync_joins=SyncJoins}};

handle_info(Msg, State) ->
    lager:warning("Unhandled info messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), state_t()) -> term().
terminate(_Reason, #state{connections=Connections}=_State) ->
    Fun =
        fun(_K, Pids) ->
            lists:foreach(
              fun(Pid) ->
                 try
                     gen_server:stop(Pid, normal, infinity)
                 catch
                     _:_ ->
                         ok
                 end
              end, Pids)
         end,
    partisan_peer_service_connections:foreach(Fun, Connections),
    ok.

%% @private
-spec code_change(term() | {down, term()}, state_t(), term()) -> {ok, state_t()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
gen_actor() ->
    Node = atom_to_list(partisan_peer_service_manager:mynode()),
    Unique = erlang:unique_integer([positive]),
    TS = integer_to_list(Unique),
    Term = Node ++ TS,
    crypto:hash(sha, Term).

%% @private
without_me(Members) ->
    lists:filter(fun(#{name := Name}) ->
                         case partisan_peer_service_manager:mynode() of
                             Name ->
                                 false;
                             _ ->
                                 true
                         end
                 end, Members).

%% @private
establish_connections(Pending,
                      Membership,
                      Connections0) ->
    %% Compute list of nodes that should be connected.
    Peers = without_me(Membership ++ Pending),

    %% Reconnect disconnected members and members waiting to join.
    Connections = lists:foldl(fun(Peer, Cs) ->
                                partisan_util:maybe_connect(Peer, Cs)
                              end, Connections0, without_me(Peers)),

    %% Return the updated list of connections.
    Connections.

%% @private
handle_message({ping, SourceNode, DestinationNode, SourceTime},
               From,
               #state{pending=Pending,
                      connections=Connections0,
                      membership=Membership}=State) ->
    %% Establish any new connections.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),

    %% Send ping response.
    do_send_message(SourceNode,
                    ?MEMBERSHIP_PROTOCOL_CHANNEL,
                    ?DEFAULT_PARTITION_KEY,
                    {pong, SourceNode, DestinationNode, SourceTime},
                    Connections),

    gen_server:reply(From, ok),

    {noreply, State#state{connections=Connections}};
handle_message({pong, SourceNode, DestinationNode, SourceTime},
               From,
               #state{distance_metrics=DistanceMetrics0}=State) ->
    %% Compute difference.
    ArrivalTime = erlang:timestamp(),
    Difference = timer:now_diff(ArrivalTime, SourceTime),
    
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("Updating distance metric for node ~p => ~p communication: ~p", [SourceNode, DestinationNode, Difference]);
        false ->
            ok
    end,

    %% Update differences.
    DistanceMetrics = dict:store(DestinationNode, Difference, DistanceMetrics0),

    %% Store in pdict.
    put(distance_metrics, DistanceMetrics),

    gen_server:reply(From, ok),

    {noreply, State#state{distance_metrics=DistanceMetrics}};
handle_message({protocol, ProtocolMessage},
               From,
               #state{pending=Pending,
                      connections=Connections0,
                      membership=Membership0,
                      membership_strategy=MembershipStrategy,
                      membership_strategy_state=MembershipStrategyState0}=State) ->
    %% Process the protocol message.
    {ok, Membership, OutgoingMessages, MembershipStrategyState} = MembershipStrategy:handle_message(MembershipStrategyState0, ProtocolMessage),

    %% Establish any new connections.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),

    %% Update users of the peer service.
    case Membership of
        Membership0 ->
            ok;
        _ ->
            partisan_peer_service_events:update(Membership)
    end,

    %% Send outgoing messages.
    lists:foreach(fun({Peer, Message}) ->
                do_send_message(Peer,
                                ?MEMBERSHIP_PROTOCOL_CHANNEL,
                                ?DEFAULT_PARTITION_KEY,
                                Message,
                                Connections)
        end, OutgoingMessages),

    case lists:member(partisan_peer_service_manager:myself(), Membership) of
        false ->
            lager:info("Shutting down: membership doesn't contain us. ~p not in ~p", [partisan_peer_service_manager:myself(), Membership]),
            %% Shutdown if we've been removed from the cluster.
            {stop, normal, State#state{membership=Membership,
                                       connections=Connections,
                                       membership_strategy_state=MembershipStrategyState}};
        true ->
            gen_server:reply(From, ok),

            {noreply, State#state{membership=Membership,
                                  connections=Connections,
                                  membership_strategy_state=MembershipStrategyState}}
    end;

%% Causal and acknowledged messages.
handle_message({forward_message, SourceNode, MessageClock, ServerRef, {causal, Label, _, _, _, _, _} = Message},
               From,
               #state{connections=Connections}=State) ->
    %% Try to acknowledge.
    do_send_message(SourceNode,
                    ?DEFAULT_CHANNEL,
                    ?DEFAULT_PARTITION_KEY,
                    {ack, MessageClock},
                    Connections),

    case partisan_causality_backend:is_causal_message(Message) of
        true ->
            partisan_causality_backend:receive_message(Label, Message);
        false ->
            %% Attempt message delivery.
            partisan_util:process_forward(ServerRef, Message)
    end,

    gen_server:reply(From, ok),

    {noreply, State};

%% Acknowledged messages.
handle_message({forward_message, SourceNode, MessageClock, ServerRef, Message},
               From,
               #state{connections=Connections}=State) ->
    %% Try to acknowledge.
    do_send_message(SourceNode,
                    ?DEFAULT_CHANNEL,
                    ?DEFAULT_PARTITION_KEY,
                    {ack, MessageClock},
                    Connections),

    partisan_util:process_forward(ServerRef, Message),

    gen_server:reply(From, ok),

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

    gen_server:reply(From, ok),

    {noreply, State};

%% Best-effort messages.
handle_message({forward_message, ServerRef, Message}, 
               From,
               State) ->
    partisan_util:process_forward(ServerRef, Message),
    gen_server:reply(From, ok),
    {noreply, State};

handle_message({ack, MessageClock}, 
               From,
               State) ->
    partisan_acknowledgement_backend:ack(MessageClock),
    gen_server:reply(From, ok),
    {noreply, State}.

%% @private
schedule_distance() ->
    DistanceInterval = partisan_config:get(distance_interval, 10000),
    erlang:send_after(DistanceInterval, ?MODULE, distance).

%% @private
schedule_periodic() ->
    PeriodicInterval = partisan_config:get(periodic_interval, ?PERIODIC_INTERVAL),
    erlang:send_after(PeriodicInterval, ?MODULE, periodic).

%% @private
schedule_retransmit() ->
    RetransmitInterval = partisan_config:get(retransmit_interval, 1000),
    erlang:send_after(RetransmitInterval, ?MODULE, retransmit).

%% @private
schedule_connections() ->
    ConnectionInterval = partisan_config:get(connection_interval, 1000),
    erlang:send_after(ConnectionInterval, ?MODULE, connections).

%% @private
-spec do_send_message(Node :: atom() | node_spec(),
                      Channel :: channel(),
                      PartitionKey :: term(),
                      Message :: term(),
                      Connections :: partisan_peer_service_connections:t(),
                      Options :: [{atom(), term()}]) -> ok.
do_send_message(Node, Channel, PartitionKey, Message, Connections) ->
    ForwardOptions = partisan_config:get(forward_options, []),
    do_send_message(Node, Channel, PartitionKey, Message, Connections, ForwardOptions).

%% @private
-spec do_send_message(Node :: atom() | node_spec(),
                      Channel :: channel(),
                      PartitionKey :: term(),
                      Message :: term(),
                      Connections :: partisan_peer_service_connections:t()) -> ok.
do_send_message(Node, Channel, PartitionKey, Message, Connections, Options) ->
    %% Find a connection for the remote node, if we have one.
    case partisan_peer_service_connections:find(Node, Connections) of
        {ok, []} ->
            %% Tracing.
            case partisan_config:get(tracing, ?TRACING) of 
                true ->
                    lager:info("Node ~p was connected, but is now disconnected!", [Node]);
                false ->
                    ok
            end,

            %% We were connected, but we're not anymore.
            case partisan_config:get(broadcast, false) of
                true ->
                    case proplists:get_value(transitive, Options, false) of
                        true ->
                            lager:info("Performing tree forward from node ~p to node ~p and message: ~p", [node(), Node, Message]),
                            TTL = partisan_config:get(relay_ttl, ?RELAY_TTL),
                            do_tree_forward(Node, Channel, PartitionKey, Message, Connections, Options, TTL);
                        false ->
                            ok
                    end;
                false ->
                    %% Node was connected but is now disconnected.
                    {error, disconnected}
            end;
        {ok, Entries} ->
            Pid = partisan_util:dispatch_pid(PartitionKey, Channel, Entries),
            gen_server:cast(Pid, {send_message, Message});
        {error, not_found} ->
            %% Tracing.
            case partisan_config:get(tracing, ?TRACING) of 
                true ->
                    lager:info("Node ~p is not directly connected.", [Node]);
                false ->
                    ok
            end,

            %% We were connected, but we're not anymore.
            case partisan_config:get(broadcast, false) of
                true ->
                    case proplists:get_value(transitive, Options, false) of
                        true ->
                            lager:info("Performing tree forward from node ~p to node ~p and message: ~p", [node(), Node, Message]),
                            TTL = partisan_config:get(relay_ttl, ?RELAY_TTL),
                            do_tree_forward(Node, Channel, PartitionKey, Message, Connections, Options, TTL);
                        false ->
                            ok
                    end;
                false ->
                    %% Node has not been connected yet.
                    {error, not_yet_connected}
            end
    end.

%% @private
up(#{name := Name}, State) ->
    up(Name, State);
up(Name, #state{up_functions=UpFunctions}) ->
    case dict:find(Name, UpFunctions) of
        error ->
            ok;
        {ok, Functions} ->
            [Function() || Function <- Functions],
            ok
    end.

%% @private
down(#{name := Name}, State) ->
    down(Name, State);
down(Name, #state{down_functions=DownFunctions}) ->
    case dict:find(Name, DownFunctions) of
        error ->
            ok;
        {ok, Functions} ->
            [Function() || Function <- Functions],
            ok
    end.

%% @private
internal_leave(#{name := Name} = Node,
               #state{pending=Pending,
                      connections=Connections0,
                      membership_strategy=MembershipStrategy,
                      membership_strategy_state=MembershipStrategyState0}=State) ->
    lager:debug("Leaving node ~p at node ~p", [Node, partisan_peer_service_manager:mynode()]),

    {ok, Membership, OutgoingMessages, MembershipStrategyState} = MembershipStrategy:leave(MembershipStrategyState0, Node),

    %% Establish any new connections.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),

    %% Transmit outgoing messages.
    lists:foreach(fun({Peer, Message}) ->
                do_send_message(Peer,
                                ?MEMBERSHIP_PROTOCOL_CHANNEL,
                                ?DEFAULT_PARTITION_KEY,
                                Message,
                                Connections)
        end, OutgoingMessages),

    case partisan_config:get(connect_disterl) of 
        true ->
            %% call the net_kernel:disconnect(Node) function to leave erlang network explicitly
            net_kernel:disconnect(Name);
        false ->
            ok
    end,

    State#state{membership=Membership,
                membership_strategy_state=MembershipStrategyState}.

%% @private
internal_join(#{name := Name} = Node,
              From,
              #state{pending=Pending0,
                     membership=Membership,
                     sync_joins=SyncJoins0,
                     connections=Connections0}=State) ->
    case partisan_config:get(connect_disterl) of 
        true ->
            %% Maintain disterl connection for control messages.
            _ = net_kernel:connect_node(Name);
        false ->
            ok
    end,

    %% Add to list of pending connections.
    Pending = [Node|Pending0],

    %% Sleep before connecting, to avoid a rush on connections.
    avoid_rush(),

    %% Add to sync joins list.
    SyncJoins = case From of 
        undefined ->
            SyncJoins0;
        _ ->
            SyncJoins0 ++ [{Node, From}]
    end,

    %% Trigger connection.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),

    State#state{pending=Pending, 
                sync_joins=SyncJoins, 
                connections=Connections}.

%% @private
fully_connected(Node, Connections) ->
    Parallelism = maps:get(parallelism, Node, ?PARALLELISM),

    Channels = case maps:get(channels, Node, [?DEFAULT_CHANNEL]) of
        [] ->
            [?DEFAULT_CHANNEL];
        undefined ->
            [?DEFAULT_CHANNEL];
        Other ->
            lists:usort(Other ++ [?DEFAULT_CHANNEL])
    end,

    case partisan_peer_service_connections:find(Node, Connections) of
        {ok, Conns} ->
            Open = length(Conns),
            Required = length(Channels) * Parallelism,
            Open =:= Required;
        _ ->
            false
    end.

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
do_tree_forward(Node, Channel, PartitionKey, Message, Connections, Options, TTL) ->
    case partisan_config:get(tracing, ?TRACING) of
        true ->
            lager:info("Attempting to forward message ~p from ~p to ~p.", 
                    [Message, partisan_peer_service_manager:mynode(), Node]);
        false ->
            ok
    end,

    %% Preempt with user-supplied outlinks.
    UserOutLinks = proplists:get_value(out_links, Options, undefined),

    OutLinks = case UserOutLinks of
        undefined ->
            try retrieve_outlinks() of
                Value ->
                    Value
            catch
                _:Reason ->
                    lager:error("Outlinks retrieval failed, reason: ~p", [Reason]),
                    []
            end;
        OL ->
            OL -- [node()]
    end,

    %% Send messages, but don't attempt to forward again, if we aren't connected.
    lists:foreach(fun(N) ->
        case partisan_config:get(tracing, ?TRACING) of
            true ->
                lager:info("Forwarding relay message ~p to node ~p for node ~p from node ~p", 
                        [Message, N, Node, partisan_peer_service_manager:mynode()]);
            false ->
                ok
        end,

        RelayMessage = {relay_message, Node, Message, TTL - 1},
        do_send_message(N, Channel, PartitionKey, RelayMessage, Connections, proplists:delete(transitive, Options))
        end, OutLinks),
    ok.

%% @private
retrieve_outlinks() ->
    case partisan_config:get(tracing, ?TRACING) of
        true ->
            lager:info("About to retrieve outlinks...");
        false ->
            ok
    end,

    Root = partisan_peer_service_manager:mynode(),

    OutLinks = try partisan_plumtree_broadcast:debug_get_peers(partisan_peer_service_manager:mynode(), Root, 1000) of
        {EagerPeers, _LazyPeers} ->
            ordsets:to_list(EagerPeers)
    catch
        _:_ ->
            lager:info("Request to get outlinks timed out..."),
            []
    end,

    case partisan_config:get(tracing, ?TRACING) of
        true ->
            lager:info("Finished getting outlinks: ~p", [OutLinks]);
        false ->
            ok
    end,

    OutLinks -- [node()].

%% @private
schedule_tree_refresh() ->
    case partisan_config:get(broadcast, false) of
        true ->
            Period = partisan_config:get(tree_refresh, 1000),
            erlang:send_after(Period, ?MODULE, tree_refresh);
        false ->
            ok
    end.