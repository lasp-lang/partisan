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
         leave/0,
         leave/1,
         on_down/2,
         send_message/2,
         forward_message/3,
         receive_message/1,
         decode/1,
         reserve/1,
         partitions/0,
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

-define(SET, state_orset).

-type membership() :: ?SET:state_orset().

-record(state, {actor :: actor(),
                down_functions :: dict:dict(),
                membership :: membership(),
                connections :: connections()}).

-type state_t() :: #state{}.

%%%===================================================================
%%% partisan_peer_service_manager callbacks
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | error().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return membership list.
members() ->
    gen_server:call(?MODULE, members, infinity).

%% @doc Return myself.
myself() ->
    partisan_peer_service_manager:myself().

%% @doc Return local node's view of cluster membership.
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).

on_down(Name, Function) ->
    gen_server:call(?MODULE, {on_down, Name, Function}, infinity).

%% @doc Send message to a remote manager.
send_message(Name, Message) ->
    gen_server:call(?MODULE, {send_message, Name, Message}, infinity).

%% @doc Forward message to registered process on the remote side.
forward_message(Name, ServerRef, Message) ->
    gen_server:call(?MODULE, {forward_message, Name, ServerRef, Message}, infinity).

%% @doc Receive message from a remote manager.
receive_message(Message) ->
    gen_server:call(?MODULE, {receive_message, Message}, infinity).

%% @doc Attempt to join a remote node.
join(Node) ->
    gen_server:call(?MODULE, {join, Node}, infinity).

%% @doc Leave the cluster.
leave() ->
    gen_server:call(?MODULE, {leave, node()}, infinity).

%% @doc Remove another node from the cluster.
leave(Node) ->
    gen_server:call(?MODULE, {leave, Node}, infinity).

%% @doc Decode state.
decode(State) ->
    State.

%% @doc Reserve a slot for the particular tag.
reserve(Tag) ->
    gen_server:call(?MODULE, {reserve, Tag}, infinity).

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
    rand_compat:seed(erlang:phash2([node()]),
                     erlang:monotonic_time(),
                     erlang:unique_integer()),

    %% Process connection exits.
    process_flag(trap_exit, true),

    %% Schedule periodic gossip.
    schedule_gossip(),

    Actor = gen_actor(),
    Membership = maybe_load_state_from_disk(Actor),
    Connections = dict:new(),

    schedule_reconnect(),

    {ok, #state{actor=Actor,
                membership=Membership,
                connections=Connections,
                down_functions=dict:new()}}.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call({on_down, Name, Function},
            _From,
            #state{down_functions=DownFunctions0}=State) ->
    DownFunctions = dict:append(Name, Function, DownFunctions0),
    {reply, ok, State#state{down_functions=DownFunctions}};

handle_call({leave, Node}, _From,
            #state{actor=Actor,
                   membership=Membership0}=State) ->
    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    Membership = lists:foldl(fun({Name, _, _} = N, L0) ->
                        case Node of
                            Name ->
                                {ok, L} = ?SET:mutate({rmv, N}, Actor, L0),
                                L;
                            _ ->
                                L0
                        end
                end, Membership0, to_list(Membership0)),

    %% Remove state and shutdown if we are removing ourselves.
    case node() of
        Node ->
            delete_state_from_disk(),

            %% Shutdown; connections terminated on shutdown.
            {stop, normal, State#state{membership=Membership}};
        _ ->
            {reply, ok, State}
    end;

handle_call({join, {Name, _, _}=Node},
            _From,
            #state{connections=Connections0}=State) ->
    %% Attempt to join via disterl for control messages during testing.
    _ = net_kernel:connect(Name),

    %% Trigger connection.
    {Result, Connections} = maybe_connect(Node, Connections0),

    %% Return.
    {reply, Result, State#state{connections=Connections}};

handle_call({send_message, Name, Message}, _From,
            #state{connections=Connections}=State) ->
    Result = do_send_message(Name, Message, Connections),
    {reply, Result, State};

handle_call({forward_message, Name, ServerRef, Message}, _From,
            #state{connections=Connections}=State) ->
    Result = do_send_message(Name,
                             {forward_message, ServerRef, Message},
                             Connections),
    {reply, Result, State};

handle_call({receive_message, Message}, _From, State) ->
    handle_message(Message, State);

handle_call(members, _From, #state{membership=Membership,
                                   connections=Connections}=State) ->
    Members = [Name || {Name, _, _} <- membership(Membership, Connections)],
    {reply, {ok, Members}, State};

handle_call(get_local_state, _From, #state{membership=Membership}=State) ->
    {reply, {ok, Membership}, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), state_t()) -> {noreply, state_t()}.
handle_info(gossip, #state{membership=Membership,
                           connections=Connections}=State) ->
    do_gossip(Membership, Connections),
    schedule_gossip(),
    {noreply, State};

handle_info(reconnect, #state{membership=Membership,
                              connections=Connections0}=State) ->

    Connections = establish_connections(Membership, Connections0),

    schedule_reconnect(),

    {noreply, State#state{connections=Connections}};

handle_info({'EXIT', From, _Reason}, #state{membership=Membership,
                                            connections=Connections0}=State) ->

    FoldFun = fun(K, V, AccIn) ->
        case V =:= From of
            true ->
                down(K, State),
                dict:store(K, undefined, AccIn);
            false ->
                AccIn
        end
    end,
    Connections = dict:fold(FoldFun, Connections0, Connections0),

    %% Annouce to the peer service.
    ActualMembership = membership(Membership, Connections),
    partisan_peer_service_events:update(ActualMembership),

    {noreply, State#state{connections=Connections}};

handle_info({connected, _Node, _Tag, RemoteState},
               #state{membership=Membership0,
                      connections=Connections}=State) ->

    %% Update membership by joining with remote membership.
    Membership = ?SET:merge(RemoteState, Membership0),
    persist_state(Membership),

    %% Announce to the peer service.
    ActualMembership = membership(Membership, Connections),
    partisan_peer_service_events:update(ActualMembership),

    %% Return.
    {noreply, State#state{membership=Membership}};

handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), state_t()) -> term().
terminate(_Reason, #state{connections=Connections}=_State) ->
    dict:map(fun(_K, Pid) ->
                     try
                         gen_server:stop(Pid, normal, infinity)
                     catch
                         _:_ ->
                             ok
                     end
             end, Connections),
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
    Node = atom_to_list(node()),
    Unique = time_compat:unique_integer([positive]),
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
delete_state_from_disk() ->
    case data_root() of
        undefined ->
            ok;
        Dir ->
            File = filename:join(Dir, "cluster_state"),
            ok = filelib:ensure_dir(File),
            case file:delete(File) of
                ok ->
                    lager:info("Leaving cluster, removed cluster_state");
                {error, Reason} ->
                    lager:info("Unable to remove cluster_state for reason ~p", [Reason])
            end
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
    write_state_to_disk(State).

%% @private
-spec to_list(membership()) -> list(node_spec()).
to_list(Membership) ->
    sets:to_list(?SET:query(Membership)).

%% @private
establish_connections(Membership, Connections) ->
    partisan_util:establish_connections(
        to_list(Membership),
        Connections
    ).

%% @private
maybe_connect(Node, Connections) ->
    partisan_util:maybe_connect(Node, Connections).

%% @private
handle_message({receive_state, PeerMembership},
               #state{membership=Membership0,
                      connections=Connections}=State) ->
    case ?SET:equal(PeerMembership, Membership0) of
        true ->
            %% No change.
            {reply, ok, State};
        false ->
            %% Merge data items.
            Membership = ?SET:merge(PeerMembership, Membership0),

            %% Persist state.
            persist_state(Membership),

            %% Announce to the peer service.
            ActualMembership = membership(Membership, Connections),
            partisan_peer_service_events:update(ActualMembership),

            %% Shutdown if we've been removed from the cluster.
            case lists:member(myself(), ActualMembership) of
                true ->
                    {reply, ok, State#state{membership=Membership}};
                false ->
                    %% Remove state.
                    delete_state_from_disk(),

                    %% Shutdown; connections terminated on shutdown.
                    {stop, normal, State}
            end
    end;
handle_message({forward_message, ServerRef, Message}, State) ->
    gen_server:cast(ServerRef, Message),
    {reply, ok, State}.

%% @private
schedule_gossip() ->
    GossipInterval = partisan_config:get(gossip_interval, 10000),
    erlang:send_after(GossipInterval, ?MODULE, gossip).

%% @private
do_gossip(Membership, Connections) ->
    Fanout = partisan_config:get(fanout, ?FANOUT),

    ActualMembership = membership(Membership, Connections),
    lager:info("M ~p, C ~p, A ~p", [Membership, Connections, ActualMembership]),
    Peers = [Name || {Name, _, _} <- ActualMembership, Name /= node()],
    PeersFanout = random_peers(Peers, Fanout),

    lists:foreach(
        fun(Peer) ->
            do_send_message(
                Peer,
                {receive_state, Membership},
                Connections
            )
        end,
        PeersFanout
    ).

%% @private
-spec random_peers(list(node()), non_neg_integer()) ->
    list(node()).
random_peers(Peers, Fanout) ->
    Shuffled = shuffle(Peers),
    lists:sublist(Shuffled, Fanout).

%% @private
do_send_message(Name, Message, Connections) ->
    %% Find a connection for the remote node, if we have one.
    case dict:find(Name, Connections) of
        {ok, undefined} ->
            %% Node was connected but is now disconnected.
            {error, disconnected};
        {ok, Pid} ->
            gen_server:cast(Pid, {send_message, Message});
        error ->
            %% Node has not been connected yet.
            {error, not_yet_connected}
    end.

%% @reference http://stackoverflow.com/questions/8817171/shuffling-elements-in-a-list-randomly-re-arrange-list-elements/8820501#8820501
shuffle(L) ->
    [X || {_, X} <- lists:sort([{rand_compat:uniform(), N} || N <- L])].

%% @private
down(Name, #state{down_functions=DownFunctions}) ->
    case dict:find(Name, DownFunctions) of
        error ->
            ok;
        {ok, Functions} ->
            [Function() || Function <- Functions],
            ok
    end.

%% @private
-spec membership(membership(), connections()) -> list(node_spec()).
membership(Membership, Connections) ->
    lists:filter(
        fun({Name, _, _}) ->
            Connected = case dict:find(Name, Connections) of
                {ok, undefined} ->
                    false;
                {ok, _Pid} ->
                    true;
                error ->
                    false
            end,

            Connected orelse Name == node()
        end,
        to_list(Membership)
    ).

%% @private
schedule_reconnect() ->
    timer:send_after(?RECONNECT_INTERVAL, reconnect).
