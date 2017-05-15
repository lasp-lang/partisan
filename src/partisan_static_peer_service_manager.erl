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

-record(state, {myself :: node_spec(),
                pending :: pending(),
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
    sets:to_list(State).

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

    Membership = maybe_load_state_from_disk(),
    Connections = dict:new(),
    Myself = myself(),

    {ok, #state{myself=Myself,
                pending=[],
                membership=Membership,
                connections=Connections}}.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call({leave, _Node}, _From, State) ->
    {reply, error, State};

handle_call({join, {Name, _, _}=Node},
            _From,
            #state{pending=Pending0, connections=Connections0}=State) ->
    %% Attempt to join via disterl for control messages during testing.
    _ = net_kernel:connect(Name),

    %% Add to list of pending connections.
    Pending = [Node|Pending0],

    %% Trigger connection.
    {Result, Connections} = maybe_connect(Node, Connections0),

    %% Return.
    {reply, Result, State#state{pending=Pending,
                                connections=Connections}};

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

handle_call(members, _From, #state{membership=Membership}=State) ->
    Members = [P || {P, _, _} <- members(Membership)],
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

handle_info({'EXIT', From, _Reason}, #state{connections=Connections0,
                                            membership=Membership0}=State) ->
    FoldFun = fun(K, V, {ConnectionsAcc0, MembershipAcc0}=AccIn) ->
        case V =:= From of
            true ->

                % Remove from connections
                ConnectionsAcc1 = dict:store(K, undefined, ConnectionsAcc0),

                % Remove from membership
                MembershipAcc1 = sets:filter(
                    fun({Name, _, _}) ->
                        Name /= V
                    end,
                    MembershipAcc0
                ),

                {ConnectionsAcc1, MembershipAcc1};
            false ->
                AccIn
        end
    end,
    {Connections, Membership} = dict:fold(
            FoldFun,
            {Connections0, Membership0},
            Connections0
    ),
    lager:info("FROM ~p, Connections0 ~p, Connections ~p\n\n", [From, dict:to_list(Connections0), dict:to_list(Connections)]),
    lager:info("Membership0 ~p, Membership ~p\n\n", [sets:to_list(Membership0), sets:to_list(Membership)]),
    {noreply, State#state{connections=Connections,
                          membership=Membership}};

handle_info({connected, Node, _Tag, _RemoteState},
               #state{pending=Pending0,
                      membership=Membership0,
                      connections=Connections0}=State) ->
    case lists:member(Node, Pending0) of
        true ->
            %% Move out of pending.
            Pending = Pending0 -- [Node],

            %% Add to our membership.
            Membership = sets:add_element(Node, Membership0),

            %% Announce to the peer service.
            partisan_peer_service_events:update(Membership),

            %% Establish any new connections.
            Connections = establish_connections(Pending,
                                                Membership,
                                                Connections0),

            %% Compute count.
            Count = sets:size(Membership),

            lager:info("Join ACCEPTED with ~p; we have ~p members in our view.",
                       [Node, Count]),

            %% Return.
            {noreply, State#state{pending=Pending,
                                  connections=Connections,
                                  membership=Membership}};
        false ->
            {noreply, State}
    end;

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
members(Membership) ->
    sets:to_list(Membership).

%% @private
establish_connections(Pending, Membership, Connections) ->
    Members = members(Membership),
    partisan_util:establish_connections(Pending,
                                        Members,
                                        Connections).

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
