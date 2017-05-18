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

-module(partisan_client_server_peer_service_manager).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

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
-type tag() :: atom().

-record(state, {tag :: tag(),
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

    Membership = maybe_load_state_from_disk(),
    Connections = dict:new(),

    %% Get tag, if set.
    Tag = partisan_config:get(tag, undefined),

    schedule_reconnect(),

    {ok, #state{tag=Tag,
                membership=Membership,
                connections=Connections}}.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call({leave, Node}, _From,
            #state{membership=Membership0}=State) ->
    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    Membership = lists:foldl(fun({Name, _, _} = N, L0) ->
                        case Node of
                            Name ->
                                sets:del_element(N, Membership0);
                            _ ->
                                L0
                        end
                end, Membership0, decode(Membership0)),

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
                dict:store(K, undefined, AccIn);
            false ->
                AccIn
        end
    end,
    Connections = dict:fold(FoldFun, Connections0, Connections0),

    %% Announce to the peer service.
    ActualMembership = membership(Membership, Connections),
    partisan_peer_service_events:update(ActualMembership),

    {noreply, State#state{connections=Connections}};

handle_info({connected, Node, TheirTag, _RemoteState},
               #state{tag=OurTag,
                      membership=Membership0,
                      connections=Connections}=State) ->

    case accept_join_with_tag(OurTag, TheirTag) of
        true ->
            %% Add to our membership.
            Membership = sets:add_element(Node, Membership0),
            persist_state(Membership),

            %% Announce to the peer service.
            ActualMembership = membership(Membership, Connections),
            partisan_peer_service_events:update(Membership),

            %% Compute count.
            Count = length(ActualMembership),

            lager:info("Join ACCEPTED with ~p; node is ~p and we are ~p: we have ~p members in our view.",
                       [Node, TheirTag, OurTag, Count]),

            %% Return.
            {noreply, State#state{membership=Membership}};
        false ->
            lager:info("Join REFUSED with ~p; node is ~p and we are ~p",
                       [Node, TheirTag, OurTag]),
            lager:info("Keeping membership: ~p",  membership(Membership0, Connections)),

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
accept_join_with_tag(OurTag, TheirTag) ->
    case OurTag of
        server ->
            case TheirTag of
                client ->
                    true;
                server ->
                    true
            end;
        client ->
            case TheirTag of
                server ->
                    true;
                client ->
                    false
            end;
        undefined ->
            case TheirTag of
                undefined ->
                    true;
                _ ->
                    false
            end
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
