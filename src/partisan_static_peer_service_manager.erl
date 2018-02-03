%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher Meiklejohn.  All Rights Reserved.
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

-module(partisan_static_peer_service_manager).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

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
         close_connections/1,
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

-type membership() :: sets:set(node_spec()).

-record(state, {membership :: membership(),
                %% peers that we were connected at least once
                %% it's only really connected, if it's entry in
                %% the connections dictionary does not map to `undefined'
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

on_down(_Name, _Function) ->
    {error, not_implemented}.

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

close_connections(IPs) ->
    gen_server:call(?MODULE, {close_connections, IPs}, infinity).

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

    Membership = maybe_load_state_from_disk(),
    Connections = dict:new(),

    %schedule_reconnect(),

    {ok, #state{membership=Membership,
                connections=Connections}}.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call({leave, _Node}, _From, State) ->
    {reply, error, State};

handle_call({join, {_, _, _}=Node}, _From,
            #state{connections=Connections0}=State) ->
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

handle_call({close_connections, IPs}, _From, #state{membership=Membership,
                                                    connections=Connections0}=State) ->

    Connections = lists:foldl(
        fun({Name, Ip, _}, AccIn) ->
            case lists:member(Ip, IPs) of
                true ->
                    %% if should close the current active connections
                    case dict:find(Name, AccIn) of
                        {ok, undefined} ->
                            AccIn;
                        {ok, Pid} ->
                            gen_server:stop(Pid),
                            dict:store(Name, undefined, AccIn);
                        error ->
                            AccIn
                    end;
                false ->
                    AccIn
            end
        end,
        Connections0,
        sets:to_list(Membership)
    ),

    %% Announce to the peer service.
    ActualMembership = membership(Membership, Connections),
    partisan_peer_service_events:update(ActualMembership),

    {reply, ok, State#state{connections=Connections}};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

handle_info(reconnect, #state{membership=Membership,
                              connections=Connections0}=State) ->

    Connections = establish_connections(Membership, Connections0),

    schedule_reconnect(),

    {noreply, State#state{connections=Connections}};

handle_info({'EXIT', From, _Reason}, #state{membership=Membership,
                                            connections=Connections0}=State) ->

    %% it's possible to receive and 'EXIT' from someone not in the
    %% connections dictionary, why?

    FoldFun = fun(K, V, AccIn) ->
        case V =:= From of
            true ->
                lager:info("EXIT received from ~p", [K]),
                AccOut = dict:store(K, undefined, AccIn),

                %% Announce to the peer service.
                ActualMembership = membership(Membership, AccOut),
                partisan_peer_service_events:update(ActualMembership),
                AccOut;

            false ->
                AccIn
        end
    end,
    Connections = dict:fold(FoldFun, Connections0, Connections0),

    {noreply, State#state{connections=Connections}};

handle_info({connected, Node, _Tag, _RemoteState},
               #state{membership=Membership0,
                      connections=Connections}=State) ->

    %% Add to our membership.
    Membership = sets:add_element(Node, Membership0),
    persist_state(Membership),

    %% Announce to the peer service.
    ActualMembership = membership(Membership, Connections),
    partisan_peer_service_events:update(ActualMembership),

    %% Compute count.
    Count = length(ActualMembership),
    lager:info("Join ACCEPTED with ~p; we have ~p members in our view.", [Node, Count]),

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
empty_membership() ->
    LocalState = sets:add_element(myself(), sets:new()),
    persist_state(LocalState),
    LocalState.

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
            ok = file:write_file(File, term_to_binary(State))
    end.

%% @private
maybe_load_state_from_disk() ->
    case data_root() of
        undefined ->
            empty_membership();
        Dir ->
            case filelib:is_regular(filename:join(Dir, "cluster_state")) of
                true ->
                    {ok, Bin} = file:read_file(filename:join(Dir, "cluster_state")),
                    {ok, State} = binary_to_term(Bin),
                    State;
                false ->
                    empty_membership()
            end
    end.

%% @private
persist_state(State) ->
    write_state_to_disk(State).

%% @private
establish_connections(Membership, Connections) ->
    partisan_util:establish_connections(
        sets:to_list(Membership),
        Connections
    ).

%% @private
maybe_connect(Node, Connections) ->
    partisan_util:maybe_connect(Node, Connections).

handle_message({forward_message, ServerRef, Message}, State) ->
    gen_server:cast(ServerRef, Message),
    {reply, ok, State}.

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

%% @private
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
        sets:to_list(Membership)
    ).

%% @private
schedule_reconnect() ->
    timer:send_after(?RECONNECT_INTERVAL, reconnect).
