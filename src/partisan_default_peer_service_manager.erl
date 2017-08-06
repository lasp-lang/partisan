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
         update_members/1,
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

%% debug callbacks
-export([connections/0]).

-include("partisan.hrl").

-define(SET, state_orset).

-type pending() :: [node_spec()].
-type membership() :: ?SET:state_orset().

-record(state, {actor :: actor(),
                pending :: pending(),
                down_functions :: dict:dict(),
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
on_down(Name, Function) ->
    gen_server:call(?MODULE, {on_down, Name, Function}, infinity).

%% @doc Send message to a remote manager.
send_message(Name, Message) ->
    gen_server:call(?MODULE, {send_message, Name, Message}, infinity).

%% @doc Forward message to registered process on the remote side.
forward_message(Name, ServerRef, Message) ->
    FullMessage = {forward_message, Name, ServerRef, Message},

    %% Attempt to fast-path through the memoized connection cache.
    case partisan_connection_cache:dispatch(FullMessage) of
        ok ->
            ok;
        {error, trap} ->
            gen_server:call(?MODULE, FullMessage, infinity)
    end.

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
    sets:to_list(?SET:query(State)).

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

    %% Schedule periodic connections.
    schedule_connections(),

    Actor = gen_actor(),
    Membership = maybe_load_state_from_disk(Actor),
    Connections = partisan_peer_service_connections:new(),

    {ok, #state{actor=Actor,
                pending=[],
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

handle_call({update_members, Nodes}, _From, #state{membership=Membership}=State) ->
    % lager:info("Updating membership with: ~p", [Nodes]),

    %% Get the current membership.
    CurrentMembership = [N || #{name := N} <- sets:to_list(?SET:query(Membership))],
    % lager:info("CurrentMembership: ~p", [CurrentMembership]),

    %% Compute leaving list.
    LeavingNodes = lists:filter(fun(N) ->
                                        not lists:member(N, Nodes)
                                end, CurrentMembership),
    % lager:info("LeavingNodes: ~p", [LeavingNodes]),

    %% Issue leaves.
    State1 = lists:foldl(fun(N, S) ->
                                 internal_leave(N, S)
                         end, State, LeavingNodes),

    %% Compute joining list.
    JoiningNodes = lists:filter(fun(N) ->
                                        not lists:member(N, CurrentMembership)
                                end, Nodes),
    % lager:info("JoiningNodes: ~p", [JoiningNodes]),

    %% Issue joins.
    State2 = lists:foldl(fun(N, S) ->
                                 internal_join(N#{parallelism => ?PARALLELISM}, S)
                         end, State1, JoiningNodes),

    {reply, ok, State2};

handle_call({leave, Node}, From, State0) ->
    %% Perform leave.
    State = internal_leave(Node, State0),

    %% Remove state and shutdown if we are removing ourselves.
    case node() of
        Node ->
            delete_state_from_disk(),

            gen_server:reply(From, ok),

            %% Shutdown; connections terminated on shutdown.
            {stop, normal, State};
        _ ->
            {reply, ok, State}
    end;

handle_call({join, #{name := _Name} = Node},
            _From,
            State0) ->
    %% Perform join.
    State = internal_join(Node, State0),

    %% Return.
    {reply, ok, State};

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
    Members = [P || #{name := P} <- members(Membership)],
    {reply, {ok, Members}, State};

handle_call(connections, _From, #state{connections=Connections}=State) ->
    {reply, {ok, Connections}, State};

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
handle_info(gossip, #state{pending=Pending,
                           membership=Membership,
                           connections=Connections0}=State) ->
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),
    do_gossip(Membership, Connections),
    schedule_gossip(),
    {noreply, State#state{connections=Connections}};

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
                case lists:keymember(From, 2, Pids) of
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
                      connections=Connections}=State) ->
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

            %% Return.
            {noreply, State#state{pending=Pending,
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
                         case node() of
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
handle_message({receive_state, PeerMembership},
               #state{pending=Pending,
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
            case lists:member(node(), Members) of
                true ->
                    %% Establish any new connections.
                    Connections = establish_connections(Pending,
                                                        Membership,
                                                        Connections0),

                    %% Gossip.
                    do_gossip(Membership, Connections),

                    {reply, ok, State#state{membership=Merged,
                                            connections=Connections}};
                false ->
                    %% Remove state.
                    delete_state_from_disk(),

                    %% Shutdown; connections terminated on shutdown.
                    {stop, normal, State#state{membership=Membership}}
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
schedule_connections() ->
    ConnectionInterval = partisan_config:get(connection_interval, 1000),
    erlang:send_after(ConnectionInterval, ?MODULE, connections).

%% @private
do_gossip(Membership, Connections) ->
    Fanout = partisan_config:get(fanout, ?FANOUT),

    case get_peers(Membership) of
        [] ->
            ok;
        AllPeers ->
            {ok, Peers} = random_peers(AllPeers, Fanout),
            lists:foreach(fun(Peer) ->
                        do_send_message(Peer,
                                        {receive_state, Membership},
                                        Connections)
                end, Peers),
            ok
    end.

%% @private
get_peers(Local) ->
    Members = members(Local),
    Peers = [X || #{name := X} <- Members, X /= node()],
    Peers.

%% @private
random_peers(Peers, Fanout) ->
    Shuffled = shuffle(Peers),
    Peer = lists:sublist(Shuffled, Fanout),
    {ok, Peer}.

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
            {_ListenAddr, Pid} = lists:nth(rand_compat:uniform(length(Entries)), Entries),
            gen_server:cast(Pid, {send_message, Message});
        {error, not_found} ->
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
internal_leave(Node, #state{actor=Actor,
                            connections=Connections,
                            membership=Membership0}=State) ->
    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    Membership = lists:foldl(fun(#{name := Name} = N, M0) ->
                        case Node of
                            Name ->
                                {ok, M} = ?SET:mutate({rmv, N}, Actor, M0),
                                M;
                            _ ->
                                M0
                        end
                end, Membership0, members(Membership0)),

    %% Gossip.
    do_gossip(Membership, Connections),

    State#state{membership=Membership}.

%% @private
internal_join(#{name := Name} = Node,
              #state{pending=Pending0,
                     connections=Connections0,
                     membership=Membership}=State) ->
    %% Maintain disterl connection for control messages.
    _ = net_kernel:connect(Name),

    %% Add to list of pending connections.
    Pending = [Node|Pending0],

    %% Trigger connection.
    Connections = establish_connections(Pending,
                                        Membership,
                                        Connections0),

    State#state{pending=Pending, connections=Connections}.
