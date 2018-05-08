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
         sync_join/1,
         leave/0,
         leave/1,
         on_up/2,
         on_down/2,
         update_members/1,
         send_message/2,
         forward_message/2,
         cast_message/3,
         forward_message/3,
         cast_message/4,
         forward_message/4,
         cast_message/5,
         forward_message/5,
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

-type pending() :: [node_spec()].
-type membership() :: sets:set(node_spec()).
-type tag() :: atom().

-record(state, {myself :: node_spec(),
                tag :: tag(),
                pending :: pending(),
                membership :: membership(),
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

%% @doc Return myself.
myself() ->
    partisan_peer_service_manager:myself().

%% @doc Return local node's view of cluster membership.
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).

%% @doc Register a trigger to fire when a connection drops.
on_down(_Name, _Function) ->
    {error, not_implemented}.

%% @doc Register a trigger to fire when a connection opens.
on_up(_Name, _Function) ->
    {error, not_implemented}.

%% @doc Update membership.
update_members(_Nodes) ->
    {error, not_implemented}.

%% @doc Send message to a remote manager.
send_message(Name, Message) ->
    gen_server:call(?MODULE, {send_message, Name, Message}, infinity).

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
forward_message({partisan_gensym, Name, ServerRef}, Message) ->
    forward_message(Name, ?DEFAULT_CHANNEL, ServerRef, Message).

%% @doc Forward message to registered process on the remote side.
forward_message(Name, ServerRef, Message) ->
    forward_message(Name, ?DEFAULT_CHANNEL, ServerRef, Message).

%% @doc Forward message to registered process on the remote side.
forward_message(Name, Channel, ServerRef, Message) ->
    forward_message(Name, Channel, ServerRef, Message, []).

%% @doc Forward message to registered process on the remote side.
forward_message(Name, _Channel, ServerRef, Message, Options) ->
    gen_server:call(?MODULE, {forward_message, Name, ServerRef, Message, Options}, infinity).

%% @doc Receive message from a remote manager.
receive_message(Message) ->
    gen_server:call(?MODULE, {receive_message, Message}, infinity).

%% @doc Attempt to join a remote node.
join(Node) ->
    gen_server:call(?MODULE, {join, Node}, infinity).

%% @doc Attempt to join a remote node.
sync_join(_Node) ->
    {error, not_implemented}.

%% @doc Leave the cluster.
leave() ->
    gen_server:call(?MODULE, {leave, partisan_peer_service_manager:mynode()}, infinity).

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
    rand:seed(exsplus, {erlang:phash2([partisan_peer_service_manager:mynode()]),
                        erlang:monotonic_time(),
                        erlang:unique_integer()}),

    %% Process connection exits.
    process_flag(trap_exit, true),

    Membership = maybe_load_state_from_disk(),
    Connections = partisan_peer_service_connections:new(),
    Myself = myself(),

    %% Get tag, if set.
    Tag = partisan_config:get(tag, undefined),

    {ok, #state{tag=Tag,
                myself=Myself,
                pending=[],
                membership=Membership,
                connections=Connections}}.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call({leave, NodeName}, _From,
            #state{membership=Membership0,
                   pending = Pending,
                   connections = Connections
                  }=State) ->
    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    Membership = lists:foldl(fun(#{name := Name} = N, L0) ->
                        case NodeName of
                            Name ->
                                sets:del_element(N, L0);
                            _ ->
                                L0
                        end
                end, Membership0, decode(Membership0)),

    %% Remove state and shutdown if we are removing ourselves.
    case partisan_peer_service_manager:mynode() of
        NodeName ->
            delete_state_from_disk(),

            %% Shutdown; connections terminated on shutdown.
            {stop, normal, State#state{membership=Membership}};
        _ ->
           NewPending = lists:filter(fun(#{name := Name}) ->
                                             case Name of
                                                 NodeName ->
                                                     %% delete the same name node
                                                     false;
                                                 _ ->
                                                     true
                                             end
                                     end, Pending),
            NewConnections = partisan_util:may_disconnect(NodeName, Connections),
            {reply, ok, State#state{
                          membership = Membership,
                          pending = NewPending,
                          connections = NewConnections
                         }}
    end;

handle_call({join, #{name := Name}=Node},
            _From,
            #state{pending=Pending0, connections=Connections0}=State) ->
    %% Attempt to join via disterl for control messages during testing.
    _ = net_kernel:connect(Name),

    %% Add to list of pending connections.
    Pending = [Node|Pending0],

    %% Trigger connection.
    Connections = partisan_util:maybe_connect(Node, Connections0),

    %% Return.
    {reply, ok, State#state{pending=Pending,
                            connections=Connections}};

handle_call({send_message, Name, Message}, _From,
            #state{connections=Connections}=State) ->
    Result = do_send_message(Name, Message, Connections),
    {reply, Result, State};

handle_call({forward_message, Name, ServerRef, Message, _Options}, _From,
            #state{connections=Connections}=State) ->
    Result = do_send_message(Name,
                             {forward_message, ServerRef, Message},
                             Connections),
    {reply, Result, State};

handle_call({receive_message, Message}, _From, State) ->
    handle_message(Message, State);

handle_call(members, _From, #state{membership=Membership}=State) ->
    Members = [P || #{name := P} <- members(Membership)],
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

handle_info({'EXIT', From, _Reason}, #state{connections=Connections0}=State) ->
    {_, Connections} = partisan_peer_service_connections:prune(From, Connections0),
    {noreply, State#state{connections=Connections}};

handle_info({connected, Node, TheirTag, _RemoteState},
               #state{pending=Pending0,
                      tag=OurTag,
                      membership=Membership0,
                      connections=Connections0}=State) ->
    case lists:member(Node, Pending0) of
        true ->
            case accept_join_with_tag(OurTag, TheirTag) of
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

                    lager:info("Join ACCEPTED with ~p; node is ~p and we are ~p: we have ~p members in our view.",
                               [Node, TheirTag, OurTag, Count]),

                    %% Return.
                    {noreply, State#state{pending=Pending,
                                          connections=Connections,
                                          membership=Membership}};
                false ->
                    lager:info("Join REFUSED with ~p; node is ~p and we are ~p",
                               [Node, TheirTag, OurTag]),
                    lager:info("Keeping membership: ~p", [Membership0]),

                    {noreply, State}
            end;
        false ->
            {noreply, State}
    end;

handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), state_t()) -> term().
terminate(_Reason, #state{connections=Connections}=_State) ->
    Fun =
        fun(_K, Pids) ->
            lists:foreach(
              fun({_ListenAddr, _Channel, Pid}) ->
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
                    binary_to_term(Bin);
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
    %% Reconnect disconnected members and members waiting to join.
    Members = members(Membership),
    AllPeers = lists:filter(fun(#{name := Name}) ->
                         case partisan_peer_service_manager:mynode() of
                             Name ->
                                 false;
                             _ ->
                                 true
                         end
                 end, Members ++ Pending),
    lists:foldl(fun partisan_util:maybe_connect/2, Connections, AllPeers).

handle_message({forward_message, ServerRef, Message}, State) ->
    ServerRef ! Message,
    {reply, ok, State}.

%% @private
-spec do_send_message(Node :: atom() | node_spec(),
                      Message :: term(),
                      Connections :: partisan_peer_service_connections:t()) ->
            {error, disconnected} | {error, not_yet_connected} | {error, term()} | ok.
do_send_message(Node, Message, Connections) ->
    %% Find a connection for the remote node, if we have one.
    case partisan_peer_service_connections:find(Node, Connections) of
        {ok, []} ->
            %% Node was connected but is now disconnected.
            {error, disconnected};
        {ok, Entries} ->
            Pid = partisan_util:dispatch_pid(Entries),
            gen_server:cast(Pid, {send_message, Message});
        {error, not_found} ->
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
