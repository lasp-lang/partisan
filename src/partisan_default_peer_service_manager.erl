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

-module(partisan_default_peer_service_manager).

-behaviour(gen_server).
-behaviour(partisan_peer_service_manager).

%% partisan_peer_service_manager callbacks
-export([start_link/0,
         members/0,
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
         add_interposition_fun/2,
         remove_interposition_fun/1,
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
-type membership() :: ?SET:state_orset().
-type from() :: {pid(), atom()}.

-record(state, {actor :: actor(),
                vclock :: term(),
                pending :: pending(),
                down_functions :: dict:dict(),
                up_functions :: dict:dict(),
                membership :: membership(),
                interposition_funs :: dict:dict(),
                sync_joins :: [{node_spec(), from()}],
                connections :: partisan_peer_service_connections:t()}).

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
decode(State) ->
    sets:to_list(?SET:query(State)).

%% @doc Reserve a slot for the particular tag.
reserve(Tag) ->
    gen_server:call(?MODULE, {reserve, Tag}, infinity).

%% @doc
add_interposition_fun(Name, InterpositionFun) ->
    gen_server:call(?MODULE, {add_interposition_fun, Name, InterpositionFun}, infinity).

%% @doc
remove_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_interposition_fun, Name}, infinity).

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
    %% Seed the process at initialization.
    rand:seed(exsplus, {erlang:phash2([partisan_peer_service_manager:mynode()]),
                        erlang:monotonic_time(),
                        erlang:unique_integer()}),

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

    %% Schedule periodic gossip.
    schedule_gossip(),

    %% Schedule periodic connections.
    schedule_connections(),

    %% Schedule periodic retransmissionj.
    schedule_retransmit(),

    Actor = gen_actor(),
    VClock = partisan_vclock:fresh(),
    Membership = maybe_load_state_from_disk(Actor),
    Connections = partisan_peer_service_connections:new(),

    {ok, #state{actor=Actor,
                pending=[],
                vclock=VClock,
                membership=Membership,
                interposition_funs=dict:new(),
                connections=Connections,
                sync_joins=[],
                up_functions=dict:new(),
                down_functions=dict:new()}}.

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

handle_call({add_interposition_fun, Name, InterpositionFun}, _From, #state{interposition_funs=InterpositionFuns0}=State) ->
    InterpositionFuns = dict:store(Name, InterpositionFun, InterpositionFuns0),
    {reply, ok, State#state{interposition_funs=InterpositionFuns}};

handle_call({remove_interposition_fun, Name}, _From, #state{interposition_funs=InterpositionFuns0}=State) ->
    InterpositionFuns = dict:erase(Name, InterpositionFuns0),
    {reply, ok, State#state{interposition_funs=InterpositionFuns}};

handle_call({update_members, Nodes}, _From, #state{membership=Membership}=State) ->
    % lager:debug("Updating membership with: ~p", [Nodes]),

    %% Get the current membership.
    CurrentMembership = [N || #{name := N} <- sets:to_list(?SET:query(Membership))],
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
                                                         internal_join(N, S)
                                                 end, State1, JoiningNodes),

    %% Compute current pending list.
    Pending1 = lists:filter(fun(#{name := N}) ->
                                    lists:member(N, NodesNames)
                            end, Pending),

    {reply, ok, State2#state{pending=Pending1}};

handle_call({leave, Node}, From, #state{actor=Actor}=State0) ->
    %% Perform leave.
    State = internal_leave(Node, State0),

    case partisan_peer_service_manager:mynode() of
        Node ->
            gen_server:reply(From, ok),

            %% Reset membership, normal terminate on the gen_server:
            %% this will close all connections, restart the gen_server,
            %% and reboot with empty state, so the node will be isolated.
            EmptyMembership = empty_membership(Actor),
            persist_state(EmptyMembership),

            {stop, normal, State#state{membership=EmptyMembership}};
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
            State = internal_join(Node, State0),

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
            State = sync_internal_join(Node, From, State0),

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
            _From,
            #state{interposition_funs=InterpositionFuns, connections=Connections, vclock=VClock0}=State) ->
    %% Determine if message should be allowed to pass.
    FoldFun = fun(_Name, InterpositionFun, M) ->
        InterpositionFun({forward_message, Name, M})
    end,
    Message = dict:fold(FoldFun, OriginalMessage, InterpositionFuns),

    case Message of
        undefined ->
            {reply, ok, State};
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
                    %% Send message along.
                    do_send_message(Name,
                                    Channel,
                                    PartitionKey,
                                    {forward_message, ServerRef, Message},
                                    Connections);
                true ->
                    partisan_reliability_backend:store(MessageClock, FullMessage),

                    %% Send message along.
                    do_send_message(Name,
                                    Channel,
                                    PartitionKey,
                                    {forward_message, myself(), MessageClock, ServerRef, Message},
                                    Connections)
            end,

            {reply, Result, State#state{vclock=VClock}}
    end;

handle_call({receive_message, Peer, OriginalMessage}, _From, #state{interposition_funs=InterpositionFuns} = State) ->
    lager:info("Inside the receive interposition, message from ~p at node ~p", [Peer, node()]),
    lager:info("Count of interposition funs: ~p", [dict:size(InterpositionFuns)]),

    %% Determine if message should be allowed to pass.
    FoldFun = fun(_Name, InterpositionFun, M) ->
        InterpositionFun({receive_message, Peer, M})
    end,
    Message = dict:fold(FoldFun, OriginalMessage, InterpositionFuns),

    lager:info("Message after interposition is: ~p", [Message]),

    case Message of
        undefined ->
            {reply, ok, State};
        _ ->
            handle_message(Message, State)
    end;
handle_call({receive_message, Message}, _From, State) ->
    handle_message(Message, State);

handle_call(members, _From, #state{membership=Membership}=State) ->
    Members = [P || #{name := P} <- members(Membership)],
    {reply, {ok, Members}, State};

handle_call(connections, _From, #state{connections=Connections}=State) ->
    {reply, {ok, Connections}, State};

handle_call(get_local_state, _From, #state{membership=Membership}=State) ->
    {reply, {ok, Membership}, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), state_t()) -> {noreply, state_t()}.
handle_info(gossip, #state{pending=Pending,
                           membership=Membership,
                           connections=Connections0}=State) ->
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),
    do_gossip(Membership, Connections),
    schedule_gossip(),
    {noreply, State#state{connections=Connections}};

handle_info(retransmit, #state{connections=Connections}=State) ->
    RetransmitFun = fun({_, {forward_message, Name, Channel, _Clock, PartitionKey, ServerRef, Message, _Options}}) ->
        do_send_message(Name,
                        Channel,
                        PartitionKey,
                        {forward_message, ServerRef, Message},
                        Connections)
    end,
    {ok, Outstanding} = partisan_reliability_backend:outstanding(),
    lists:foreach(RetransmitFun, Outstanding),
    schedule_retransmit(),
    {noreply, State};

handle_info(connections, #state{pending=Pending,
                                membership=Membership,
                                connections=Connections0}=State) ->
    %% Trigger connection.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),
    schedule_connections(),
    {noreply, State#state{pending=Pending,
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
                      membership=Membership0,
                      sync_joins=SyncJoins0,
                      connections=Connections}=State) ->
    lager:debug("Node ~p connected!", [Node]),

    case lists:member(Node, Pending0) of
        true ->
            %% Move out of pending.
            Pending = Pending0 -- [Node],

            %% Update membership by joining with remote membership.
            Membership = ?SET:merge(RemoteState, Membership0),

            %% Persist state.
            persist_state(Membership),

            %% Announce to the peer service.
            partisan_peer_service_events:update(Membership),

            %% Gossip the new membership.
            do_gossip(Membership, Connections),

            %% Send up notifications.
            partisan_peer_service_connections:foreach(
                fun(N, Pids) ->
                        case length(Pids -- Pending) =:= 1 of
                            true ->
                                up(N, State);
                            false ->
                                ok
                        end
                end, Connections),

            %% Notify for sync join.
            SyncJoins = case lists:keyfind(Node, 1, SyncJoins0) of
                {Node, FromPid} ->
                    case fully_connected(Node, Connections) of
                        true ->
                            lager:debug("Node ~p is fully connected.", [Node]),
                            gen_server:reply(FromPid, ok),
                            lists:keydelete(FromPid, 2, SyncJoins0);
                        _ ->
                            SyncJoins0
                    end;
                false ->
                    SyncJoins0
            end,

            %% Return.
            {noreply, State#state{pending=Pending,
                                  sync_joins=SyncJoins,
                                  membership=Membership}};
        false ->
            {noreply, State}
    end;

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
empty_membership(Actor) ->
    {ok, LocalState} = ?SET:mutate({add, myself()}, Actor, ?SET:new()),
    persist_state(LocalState),
    LocalState.

%% @private
gen_actor() ->
    Node = atom_to_list(partisan_peer_service_manager:mynode()),
    Unique = erlang:unique_integer([positive]),
    TS = integer_to_list(Unique),
    Term = Node ++ TS,
    crypto:hash(sha, Term).

%% @private
data_root() ->
    case application:get_env(partisan, partisan_data_dir) of
        {ok, PRoot} ->
            filename:join(PRoot, "default_peer_service");
        undefined ->
            undefined
    end.

%% @private
write_state_to_disk(State) ->
    case data_root() of
        undefined ->
            ok;
        Dir ->
            File = filename:join(Dir, "cluster_state"),
            ok = filelib:ensure_dir(File),
            ok = file:write_file(File, ?SET:encode(erlang, State))
    end.

%% @private
maybe_load_state_from_disk(Actor) ->
    case data_root() of
        undefined ->
            empty_membership(Actor);
        Dir ->
            case filelib:is_regular(filename:join(Dir, "cluster_state")) of
                true ->
                    {ok, Bin} = file:read_file(filename:join(Dir, "cluster_state")),
                    ?SET:decode(erlang, Bin);
                false ->
                    empty_membership(Actor)
            end
    end.

%% @private
persist_state(State) ->
    case partisan_config:get(persist_state, true) of
        true ->
            write_state_to_disk(State);
        false ->
            ok
    end.

%% @private
members(Membership) ->
    sets:to_list(?SET:query(Membership)).

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
    Peers = without_me(members(Membership) ++ Pending),

    %% Reconnect disconnected members and members waiting to join.
    Connections = lists:foldl(fun(Peer, Cs) ->
                                partisan_util:maybe_connect(Peer, Cs)
                              end, Connections0, without_me(Peers)),

    %% Return the updated list of connections.
    Connections.

%% @private
handle_message({receive_state, #{name := From}, PeerMembership},
               #state{actor=Actor,
                      pending=Pending,
                      membership=Membership,
                      connections=Connections0}=State) ->
    case ?SET:equal(PeerMembership, Membership) of
        true ->
            %% No change.
            {reply, ok, State};
        false ->
            %% Merge data items.
            Merged = ?SET:merge(PeerMembership, Membership),

            %% Persist state.
            persist_state(Merged),

            %% Update users of the peer service.
            partisan_peer_service_events:update(Merged),

            %% Compute members.
            Members = [N || #{name := N} <- members(Merged)],

            %% Shutdown if we've been removed from the cluster.
            case lists:member(partisan_peer_service_manager:mynode(), Members) of
                true ->
                    %% Establish any new connections.
                    Connections = establish_connections(Pending,
                                                        Membership,
                                                        Connections0),

                    lager:debug("Received updated membership state: ~p from ~p", [Members, From]),

                    %% Gossip.
                    do_gossip(Merged, Connections),

                    {reply, ok, State#state{membership=Merged,
                                            connections=Connections}};
                false ->
                    lager:debug("Node ~p is no longer part of the cluster, setting empty membership.", [partisan_peer_service_manager:mynode()]),

                    %% Reset membership, normal terminate on the gen_server:
                    %% this will close all connections, restart the gen_server,
                    %% and reboot with empty state, so the node will be isolated.
                    EmptyMembership = empty_membership(Actor),
                    persist_state(EmptyMembership),

                    {stop, normal, State#state{membership=EmptyMembership}}
            end
    end;
handle_message({forward_message, SourceNode, MessageClock, ServerRef, {causal, Label, _, _, _, _, _} = Message},
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

    {reply, ok, State};
handle_message({forward_message, ServerRef, {causal, Label, _, _, _, _, _} = Message}, State) ->
    case partisan_causality_backend:is_causal_message(Message) of
        true ->
            partisan_causality_backend:receive_message(Label, Message);
        false ->
            %% Attempt message delivery.
            partisan_util:process_forward(ServerRef, Message)
    end,

    {reply, ok, State};
handle_message({forward_message, ServerRef, Message}, State) ->
    partisan_util:process_forward(ServerRef, Message),
    {reply, ok, State};
handle_message({connect, ConnectionPid, #{name := Name} = Node}, State0) ->
    MyNode = partisan_peer_service_manager:mynode(),

    lager:info("Received connection request at node ~p for node ~p from remote pid: ~p", [MyNode, Name, ConnectionPid]),

    case MyNode of
        Name ->
            %% Ignoring self join.
            {noreply, State0};
        _ ->
            %% Perform join.
            State = internal_join(Node, State0),

            %% Return.
            {noreply, State}
    end;

handle_message({ack, MessageClock}, State) ->
    partisan_reliability_backend:ack(MessageClock),
    {reply, ok, State}.

%% @private
schedule_gossip() ->
    ShouldGossip = partisan_config:get(gossip, true),

    case ShouldGossip of
        true ->
            GossipInterval = partisan_config:get(gossip_interval, 10000),
            erlang:send_after(GossipInterval, ?MODULE, gossip);
        _ ->
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
do_gossip(Membership, Connections) ->
    do_gossip(Membership, Membership, Connections).

%% @private
do_gossip(Recipients, Membership, Connections) ->
    ShouldGossip = partisan_config:get(gossip, true),

    case ShouldGossip of
        true ->
            case get_peers(Recipients) of
                [] ->
                    ok;
                AllPeers ->
                    Members = [N || #{name := N} <- members(Membership)],

                    lager:debug("Sending state with updated membership: ~p", [Members]),

                    lists:foreach(fun(Peer) ->
                                do_send_message(Peer,
                                                ?DEFAULT_CHANNEL,
                                                ?DEFAULT_PARTITION_KEY,
                                                {receive_state, myself(), Membership},
                                                Connections)
                        end, AllPeers),
                    ok
            end;
        _ ->
            ok
    end.

%% @private
get_peers(Local) ->
    Members = members(Local),
    Peers = [X || #{name := X} <- Members, X /= partisan_peer_service_manager:mynode()],
    Peers.

%% @private
-spec do_send_message(Node :: atom() | node_spec(),
                      Channel :: channel(),
                      PartitionKey :: term(),
                      Message :: term(),
                      Connections :: partisan_peer_service_connections:t()) -> ok.
do_send_message(Node, Channel, PartitionKey, Message, Connections) ->
    %% Find a connection for the remote node, if we have one.
    case partisan_peer_service_connections:find(Node, Connections) of
        {ok, []} ->
            lager:error("Node ~p was connected, but is now disconnected!", [Node]),
            %% Node was connected but is now disconnected.
            {error, disconnected};
        {ok, Entries} ->
            Pid = partisan_util:dispatch_pid(PartitionKey, Channel, Entries),
            gen_server:cast(Pid, {send_message, Message});
        {error, not_found} ->
            lager:error("Node ~p is not yet connected during send!", [Node]),
            %% Node has not been connected yet.
            {error, not_yet_connected}
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
internal_leave(Node, #state{actor=Actor,
                            connections=Connections,
                            membership=Membership0}=State) ->
    lager:debug("Leaving node ~p at node ~p", [Node, partisan_peer_service_manager:mynode()]),

    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    Membership = lists:foldl(fun(#{name := Name} = N, M0) ->
                        case Node of
                            Name ->
                                {ok, M} = ?SET:mutate({rmv, N}, Actor, M0),
                                M;
                            _ ->
                                %% call the net_kernel:disconnect(Node) function to leave erlang network explicitly
                                rpc:call(Name, net_kernel, disconnect, [Node]),
                                M0
                        end
                end, Membership0, members(Membership0)),

    %% Gossip new membership to existing members, so they remove themselves.
    do_gossip(Membership0, Membership, Connections),

    State#state{membership=Membership}.

%% @private
internal_join(Node, State) when is_atom(Node) ->
    %% Maintain disterl connection for control messages.
    _ = net_kernel:connect_node(Node),

    %% Get listen addresses.
    ListenAddrs = rpc:call(Node, partisan_config, listen_addrs, []),

    %% Get channels.
    Channels = rpc:call(Node, partisan_config, channels, []),

    %% Get parallelism.
    Parallelism = rpc:call(Node, partisan_config, parallelism, []),

    %% Perform the join.
    internal_join(#{name => Node,
                    listen_addrs => ListenAddrs,
                    channels => Channels,
                    parallelism => Parallelism}, State);
internal_join(#{name := Name} = Node,
              #state{pending=Pending0,
                     connections=Connections0,
                     membership=Membership}=State) ->
    %% Maintain disterl connection for control messages.
    _ = net_kernel:connect_node(Name),

    %% Add to list of pending connections.
    Pending = [Node|Pending0],

    %% Sleep before connecting, to avoid a rush on connections.
    avoid_rush(),

    %% Trigger connection.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),

    State#state{pending=Pending, connections=Connections}.

sync_internal_join(#{name := Name} = Node,
              From,
              #state{pending=Pending0,
                     sync_joins=SyncJoins0,
                     connections=Connections0,
                     membership=Membership}=State) ->
    %% Maintain disterl connection for control messages.
    _ = net_kernel:connect_node(Name),

    %% Add to list of pending connections.
    Pending = [Node|Pending0],

    %% Sleep before connecting, to avoid a rush on connections.
    avoid_rush(),

    %% Add to sync joins list.
    SyncJoins = SyncJoins0 ++ [{Node, From}],

    %% Trigger connection.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),

    State#state{pending=Pending, sync_joins=SyncJoins, connections=Connections}.

%% @private
fully_connected(Node, Connections) ->
    lager:debug("Checking if node ~p is fully connected.", [Node]),

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
            lager:debug("Node ~p has ~p open connections, ~p required.", [Node, Open, Required]),
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