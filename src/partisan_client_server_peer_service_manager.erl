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

-behaviour(gen_server).
-behaviour(partisan_peer_service_manager).

%% partisan_peer_service_manager callbacks
-export([start_link/0,
         members/0,
         get_local_state/0,
         join/1,
         leave/0,
         leave/1,
         send_message/2,
         forward_message/3,
         receive_message/1,
         decode/1,
         reserve/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("partisan.hrl").

-define(SET, state_orset).

-type pending() :: [node_spec()].
-type membership() :: ?SET:orswot().
-type tag() :: atom().

-record(state, {actor :: actor(),
                tag :: tag(),
                pending :: pending(),
                membership :: membership(),
                connections :: connections()}).

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

%% @doc Return local node's view of cluster membership.
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).

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
    sets:to_list(?SET:query(State)).

%% @doc Reserve a slot for the particular tag.
reserve(Tag) ->
    gen_server:call(?MODULE, {reserve, Tag}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
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

    %% Get tag, if set.
    Tag = partisan_config:get(tag, undefined),

    {ok, #state{actor=Actor,
                tag=Tag,
                pending=[],
                membership=Membership,
                connections=Connections}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call({leave, Node}, _From,
            #state{actor=Actor,
                   tag=Tag,
                   connections=Connections,
                   membership=Membership0}=State) ->
    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    Membership = lists:foldl(fun({N, _, _}, L0) ->
                        case Node of
                            N ->
                                {ok, L} = ?SET:mutate({rmv, Node}, Actor, L0),
                                L;
                            _ ->
                                L0
                        end
                end, Membership0, decode(Membership0)),

    %% Gossip.
    do_gossip(Tag, Membership, Connections),

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
            #state{pending=Pending0, connections=Connections0}=State) ->
    %% Attempt to join via disterl for control messages during testing.
    _ = net_kernel:connect(Name),

    %% Add to list of pending connections.
    Pending = [Node|Pending0],

    %% Trigger connection.
    Connections = maybe_connect(Node, Connections0),

    %% Return.
    {reply, ok, State#state{pending=Pending,
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
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(gossip, #state{pending=Pending,
                           tag=Tag,
                           membership=Membership,
                           connections=Connections0}=State) ->
    Connections = establish_connections(Pending, Membership, Connections0),
    do_gossip(Tag, Membership, Connections),
    schedule_gossip(),
    {noreply, State#state{connections=Connections}};

handle_info({'EXIT', From, _Reason}, #state{connections=Connections0}=State) ->
    FoldFun = fun(K, V, AccIn) ->
                      case V =:= From of
                          true ->
                              dict:store(K, undefined, AccIn);
                          false ->
                              AccIn
                      end
              end,
    Connections = dict:fold(FoldFun, Connections0, Connections0),
    {noreply, State#state{connections=Connections}};

handle_info({connected, Node, TheirTag, _RemoteState},
               #state{pending=Pending0,
                      actor=Actor,
                      tag=OurTag,
                      membership=Membership0,
                      connections=Connections}=State) ->
    case lists:member(Node, Pending0) of
        true ->
            case accept_join_with_tag(OurTag, TheirTag) of
                true ->
                    %% Move out of pending.
                    Pending = Pending0 -- [Node],

                    %% Add to our membership.
                    Membership = add_to_membership(Actor, Node, Membership0),

                    %% Announce to the peer service.
                    partisan_peer_service_events:update(Membership),

                    %% Gossip the new membership.
                    do_gossip(OurTag, Membership, Connections),

                    %% Return.
                    {noreply, State#state{pending=Pending,
                                          membership=Membership}};
                false ->
                    lager:info("Join refused with ~p; node is ~p and we are ~p",
                               [Node, TheirTag, OurTag]),
                    {noreply, State}
            end;
        false ->
            {noreply, State}
    end;

handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
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
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
empty_membership(Actor) ->
    Port = partisan_config:get(peer_port, ?PEER_PORT),
    IPAddress = partisan_config:get(peer_ip, ?PEER_IP),
    Self = {node(), IPAddress, Port},
    {ok, LocalState} = ?SET:mutate({add, Self}, Actor, ?SET:new()),
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
                    {ok, State} = ?SET:decode(erlang, Bin),
                    State;
                false ->
                    empty_membership(Actor)
            end
    end.

%% @private
persist_state(State) ->
    write_state_to_disk(State).

%% @private
members(Membership) ->
    sets:to_list(?SET:query(Membership)).

%% @private
establish_connections(Pending, Membership, Connections) ->
    %% Reconnect disconnected members and members waiting to join.
    Members = members(Membership),
    AllPeers = lists:keydelete(node(), 1, Members ++ Pending),
    lists:foldl(fun maybe_connect/2, Connections, AllPeers).

%% @private
%%
%% Function should enforce the invariant that all cluster members are
%% keys in the dict pointing to undefined if they are disconnected or a
%% socket pid if they are connected.
%%
maybe_connect({Name, _, _} = Node, Connections0) ->
    Connections = case dict:find(Name, Connections0) of
        %% Found in dict, and disconnected.
        {ok, undefined} ->
            case connect(Node) of
                {ok, Pid} ->
                    dict:store(Name, Pid, Connections0);
                _ ->
                    dict:store(Name, undefined, Connections0)
            end;
        %% Found in dict and connected.
        {ok, _Pid} ->
            Connections0;
        %% Not present; disconnected.
        error ->
            case connect(Node) of
                {ok, Pid} ->
                    dict:store(Name, Pid, Connections0);
                _ ->
                    dict:store(Name, undefined, Connections0)
            end
    end,
    Connections.

%% @private
connect(Node) ->
    Self = self(),
    partisan_peer_service_client:start_link(Node, Self).

%% @private
handle_message({receive_state, TheirTag, PeerMembership},
               #state{pending=Pending,
                      tag=OurTag,
                      membership=Membership,
                      connections=Connections0}=State) ->
    %% Only exchange with nodes that have the same tag: clients to
    %% clients and servers to servers.
    %%
    case TheirTag =:= OurTag of
        true ->
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
                    Members = [N || {N, _, _} <- members(Merged)],

                    %% Shutdown if we've been removed from the cluster.
                    case lists:member(node(), Members) of
                        true ->
                            %% Establish any new connections.
                            Connections = establish_connections(Pending,
                                                                Membership,
                                                                Connections0),

                            %% Gossip.
                            do_gossip(OurTag, Membership, Connections),

                            {reply, ok, State#state{membership=Merged,
                                                    connections=Connections}};
                        false ->
                            %% Remove state.
                            delete_state_from_disk(),

                            %% Shutdown; connections terminated on shutdown.
                            {stop, normal, State#state{membership=Membership}}
                    end
            end;
        false ->
            {reply, ok, State}
    end;
handle_message({forward_message, ServerRef, Message}, State) ->
    gen_server:cast(ServerRef, Message),
    {reply, ok, State}.

%% @private
schedule_gossip() ->
    GossipInterval = partisan_config:get(gossip_interval, 10000),
    erlang:send_after(GossipInterval, ?MODULE, gossip).

%% @private
do_gossip(Tag, Membership, Connections) ->
    Fanout = partisan_config:get(fanout, ?FANOUT),

    case get_peers(Membership) of
        [] ->
            ok;
        AllPeers ->
            {ok, Peers} = random_peers(AllPeers, Fanout),
            lists:foreach(fun(Peer) ->
                        do_send_message(Peer,
                                        {receive_state, Tag, Membership},
                                        Connections)
                end, Peers),
            ok
    end.

%% @private
get_peers(Local) ->
    Members = members(Local),
    Peers = [X || {X, _, _} <- Members, X /= node()],
    Peers.

%% @private
random_peers(Peers, Fanout) ->
    Shuffled = shuffle(Peers),
    Peer = lists:sublist(Shuffled, Fanout),
    {ok, Peer}.

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
accept_join_with_tag(OurTag, TheirTag) ->
    lager:info("Accepting join for: ~p ~p", [OurTag, TheirTag]),
    case OurTag of
        server ->
            case TheirTag of
                client ->
                    true;
                server ->
                    false
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
add_to_membership(Actor, Node, Membership0) ->
    {ok, Membership} = ?SET:mutate({add, Node}, Actor, Membership0),
    Membership.
