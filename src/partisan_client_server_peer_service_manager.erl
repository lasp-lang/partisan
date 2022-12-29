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

%% -----------------------------------------------------------------------------
%% @doc This module realises the {@link partisan_peer_service_manager}
%% behaviour implementing client-server topology where clients communicate with
%% a single server and servers form a full-mesh topology.
%%
%% == Characteristics ==
%% <ul>
%% <li>Uses TCP/IP.</li>
%% <li>Client nodes communicate and maintain connections with server nodes.
%% They refuse connections from other clients but refer them to a server
%% node.</li>
%% <li>Server nodes communicate and maintain connections with all other server
%% nodes.</li>
%% <li>Nodes periodically send heartbeat messages. The service considers a node
%% "failed" when it misses X heartbeats.</li>
%% <li>Point-to-point messaging through the server (server as relay).</li>
%% <li>Eventually consistent membership maintained in a CRDT and replicated
%% using gossip.</li>
%% <li>Scalability limited to hundres of nodes (60-200 nodes).</li>
%% </ul>
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_client_server_peer_service_manager).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(partisan_peer_service_manager).

-include("partisan_logger.hrl").
-include("partisan.hrl").

-record(state, {
    tag                     ::  tag(),
    pending                 ::  pending(),
    membership              ::  membership(),
    down_functions = #{}    ::  #{'_' | node() => on_change_fun()},
    up_functions = #{}      ::  #{'_' | node() => on_change_fun()}
}).


-type on_change_fun()       ::  partisan_peer_service_manager:on_change_fun().
-type pending()             ::  sets:set(partisan:node_spec()).
-type membership()          ::  sets:set(partisan:node_spec()).
-type tag()                 ::  atom().
-type state_t()             ::  #state{}.



%% partisan_peer_service_manager callbacks
-export([cast_message/2]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([decode/1]).
-export([forward_message/2]).
-export([forward_message/3]).
-export([forward_message/4]).
-export([get_local_state/0]).
-export([inject_partition/2]).
-export([join/1]).
-export([leave/0]).
-export([leave/1]).
-export([members/0]).
-export([members_for_orchestration/0]).
-export([on_down/2]).
-export([on_up/2]).
-export([partitions/0]).
-export([receive_message/2]).
-export([reserve/1]).
-export([resolve_partition/1]).
-export([send_message/2]).
-export([start_link/0]).
-export([supports_capability/1]).
-export([sync_join/1]).
-export([update_members/1]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).



%% =============================================================================
%% PARTISAN_PEER_SERVICE_MANAGER CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Same as start_link([]).
%% @end
%% -----------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% -----------------------------------------------------------------------------
%% @doc Return membership list.
%% @end
%% -----------------------------------------------------------------------------
members() ->
    gen_server:call(?MODULE, members, infinity).


%% -----------------------------------------------------------------------------
%% @doc Return membership list.
%% @end
%% -----------------------------------------------------------------------------
members_for_orchestration() ->
    gen_server:call(?MODULE, members_for_orchestration, infinity).


%% -----------------------------------------------------------------------------
%% @doc Update membership.
%% @end
%% -----------------------------------------------------------------------------
update_members(_Nodes) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Return local node's view of cluster membership.
%% @end
%% -----------------------------------------------------------------------------
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).



%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection close for a given node.
%% `Function' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_down(#{name := Node}, Function) ->
    on_down(Node, Function);

on_down(any, Function) ->
    on_down('_', Function);

on_down(Node, Function)
when is_atom(Node) andalso (
    is_function(Function, 0) orelse is_function(Function, 1)
) ->
    gen_server:call(?MODULE, {on_down, Node, Function}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection open for a given node.
%% `Function' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_up(#{name := Node}, Function) ->
    on_up(Node, Function);

on_up(any, Function) ->
    on_up('_', Function);

on_up(Node, Function)
when is_atom(Node) andalso (
    is_function(Function, 0) orelse
    is_function(Function, 1) orelse
    is_function(Function, 2)
) ->
    gen_server:call(?MODULE, {on_up, Node, Function}, infinity).




%% -----------------------------------------------------------------------------
%% @doc Send message to a remote manager.
%% @end
%% -----------------------------------------------------------------------------
send_message(Name, Message) ->
    gen_server:call(?MODULE, {send_message, Name, Message}, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    Term :: partisan_remote_ref:p() | partisan_remote_ref:n() | pid(),
    MEssage :: partisan:message()) -> ok.

cast_message(Term, Message) ->
    FullMessage = {'$gen_cast', Message},
    forward_message(Term, FullMessage, #{}).


%% -----------------------------------------------------------------------------
%% @doc Cast a message to a remote gen_server.
%% @end
%% -----------------------------------------------------------------------------
cast_message(Node, ServerRef, Message) ->
    cast_message(Node, ServerRef, Message, #{}).


%% -----------------------------------------------------------------------------
%% @doc Cast a message to a remote gen_server.
%% @end
%% -----------------------------------------------------------------------------
cast_message(Node, ServerRef, Message, Options) ->
    FullMessage = {'$gen_cast', Message},
    forward_message(Node, ServerRef, FullMessage, Options).



%% -----------------------------------------------------------------------------
%% @doc Gensym support for forwarding.
%% @end
%% -----------------------------------------------------------------------------

forward_message(Term, Message) ->
    forward_message(Term, Message, #{}).


%% -----------------------------------------------------------------------------
%% @doc Gensym support for forwarding.
%% @end
%% -----------------------------------------------------------------------------
forward_message(Pid, Message, Opts) when is_pid(Pid) ->
    forward_message(partisan:node(), Pid, Message, Opts);

forward_message(RemoteRef, Message, Opts) ->
    partisan_remote_ref:is_pid(RemoteRef)
        orelse partisan_remote_ref:is_name(RemoteRef)
        orelse error(badarg),

    Node = partisan_remote_ref:node(RemoteRef),
    Target = partisan_remote_ref:target(RemoteRef),

    forward_message(Node, Target, Message, Opts).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
forward_message(Node, ServerRef, Message, Opts) when is_list(Opts) ->
    forward_message(Node, ServerRef, Message, maps:from_list(Opts));

forward_message(Node, ServerRef, Message, Opts) when is_map(Opts) ->
    %% We ignore Opts.channel
    gen_server:call(
        ?MODULE,
        {forward_message, Node, ServerRef, Message, Opts},
        infinity
    ).


%% -----------------------------------------------------------------------------
%% @doc Receive message from a remote manager.
%% @end
%% -----------------------------------------------------------------------------
receive_message(_Peer, Message) ->
    gen_server:call(?MODULE, {receive_message, Message}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Attempt to join a remote node.
%% @end
%% -----------------------------------------------------------------------------
join(Node) ->
    gen_server:call(?MODULE, {join, Node}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Attempt to join a remote node.
%% @end
%% -----------------------------------------------------------------------------
sync_join(_Node) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Leave the cluster.
%% @end
%% -----------------------------------------------------------------------------
leave() ->
    gen_server:call(?MODULE, {leave, partisan:node_spec()}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Remove another node from the cluster.
%% @end
%% -----------------------------------------------------------------------------
leave(#{name := _} = NodeSpec) ->
    gen_server:call(?MODULE, {leave, NodeSpec}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Decode state.
%% @end
%% -----------------------------------------------------------------------------
decode(State) ->
    sets:to_list(State).


%% -----------------------------------------------------------------------------
%% @doc Reserve a slot for the particular tag.
%% @end
%% -----------------------------------------------------------------------------
reserve(Tag) ->
    gen_server:call(?MODULE, {reserve, Tag}, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec supports_capability(Arg :: atom()) -> boolean().

supports_capability(monitoring) ->
    false;

supports_capability(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc Inject a partition.
%% @end
%% -----------------------------------------------------------------------------
inject_partition(_Origin, _TTL) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Resolve a partition.
%% @end
%% -----------------------------------------------------------------------------
resolve_partition(_Reference) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Return partitions.
%% @end
%% -----------------------------------------------------------------------------
partitions() ->
    {error, not_implemented}.




%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================





-spec init([]) -> {ok, state_t()}.

init([]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    %% Process connection exits.
    process_flag(trap_exit, true),

    ok = partisan_peer_connections:init(),

    Membership = maybe_load_state_from_disk(),

    Myself = partisan:node_spec(),

    logger:set_process_metadata(#{
        node => Myself
    }),

    %% Get tag, if set.
    Tag = partisan_config:get(tag, undefined),

    State = #state{
        tag = Tag,
        pending = sets:new([{version, 2}]),
        membership = Membership
    },
    {ok, State}.


-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call(
    {on_up, Name, Function},
    _From,
    #state{up_functions = UpFunctions0} = State) ->
    UpFunctions = partisan_util:maps_append(Name, Function, UpFunctions0),
    {reply, ok, State#state{up_functions=UpFunctions}};

handle_call(
    {on_down, Name, Function},
    _From,
    #state{down_functions = DownFunctions0} = State) ->
    DownFunctions = partisan_util:maps_append(Name, Function, DownFunctions0),
    {reply, ok, State#state{down_functions=DownFunctions}};

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call({leave, #{name := LeavingNode}}, _From, #state{} = State0) ->

    Membership0 = State0#state.membership,
    Pending = State0#state.pending,

    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    Membership = sets:fold(
        fun
            (#{name := Node} = NodeSpec, Acc) when Node == LeavingNode ->
                sets:del_element(NodeSpec, Acc);

            (#{name := Node}, Acc) ->
                %% call the net_kernel:disconnect(Node) function to leave
                %% erlang network explicitly
                _ = rpc:call(LeavingNode, net_kernel, disconnect, [Node]),
                Acc
        end,
        Membership0,
        Membership0
    ),

    %% Remove state and shutdown if we are removing ourselves.
    case partisan:node() of
        LeavingNode ->
            delete_state_from_disk(),

            %% Shutdown; connections terminated on shutdown.
            State = State0#state{membership = Membership},
            {stop, normal, State};
        _ ->
            ok = partisan_peer_service_manager:disconnect(LeavingNode),

            NewPending = sets:filter(
                fun(#{name := Node}) -> Node =/= LeavingNode end,
                Pending
            ),

            State = State0#state{
                membership = Membership,
                pending = NewPending
            },
            {reply, ok, State}
    end;

handle_call({join, #{name := Node} = Spec}, _From, #state{} = State) ->
    %% Attempt to join via disterl for control messages during testing.
    case partisan_config:get(connect_disterl, false) of
        true ->
            _ = net_kernel:connect_node(Node);
        false ->
            ok
    end,

    %% Add to list of pending connections.
    Pending = sets:add_element(Spec, State#state.pending),

    %% Trigger connection.
    ok = partisan_peer_service_manager:connect(Spec),

    {reply, ok, State#state{pending = Pending}};


handle_call({send_message, Name, Message}, _From, #state{}=State) ->
    Result = do_send_message(Name, Message),
    {reply, Result, State};

handle_call(
    {forward_message, Name, ServerRef, Message, _Opts},
    _From,
    #state{} = State) ->
    Result = do_send_message(Name,{forward_message, ServerRef, Message}),
    {reply, Result, State};

handle_call({receive_message, Message}, _From, State) ->
    handle_message(Message, State);

handle_call(members, _From, #state{} = State) ->
    Members = [Node || #{name := Node} <- members(State#state.membership)],
    {reply, {ok, Members}, State};

handle_call(members_for_orchestration, _From, #state{}=State) ->
    {reply, {ok, members(State#state.membership)}, State};

handle_call(get_local_state, _From, #state{} = State) ->
    {reply, {ok, State#state.membership}, State};

handle_call(Msg, _From, State) ->
    ?LOG_WARNING(#{
        description => "Unhandled call messages",
        module => ?MODULE,
        messages => Msg
    }),
    {reply, ok, State}.



-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.

handle_cast({kill_connections, Nodes}, State) ->
    ok = kill_connections(Nodes, State),
    {noreply, State};

handle_cast(Msg, State) ->
    ?LOG_WARNING(#{
        description => "Unhandled cast messages",
        module => ?MODULE,
        messages => Msg
    }),
    {noreply, State}.


handle_info({'EXIT', From, Reason}, #state{} = State0) ->
    ?LOG_DEBUG(#{
        description => "Connection closed",
        reason => Reason
    }),

    %% A connection has closed, prune it from the connections table
    try partisan_peer_connections:prune(From) of
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
                        maybe_add_pending(NodeSpec, State0);
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
    {connected, NodeSpec, _Channel, TheirTag, _RemoteState},
    #state{} = State0) ->

    Pending0 = State0#state.pending,
    Membership0 = State0#state.membership,
    OurTag = State0#state.tag,

    case sets:is_element(NodeSpec, Pending0) of
        true ->
            case accept_join_with_tag(OurTag, TheirTag) of
                true ->
                    %% Move out of pending.
                    Pending = sets:del_element(NodeSpec, Pending0),

                    %% Add to our membership.
                    Membership = sets:add_element(NodeSpec, Membership0),

                    %% Notify event handlers
                    ok = case Membership == Membership0 of
                        true ->
                            ok;
                        false ->
                            partisan_peer_service_events:update(Membership)
                    end,

                    %% notify subscribers
                    up(NodeSpec, State0),

                    %% Establish any new connections.
                    ok = establish_connections(Pending, Membership),

                    %% Compute count.
                    Count = sets:size(Membership),

                    ?LOG_DEBUG(#{
                        description => "Join ACCEPTED.",
                        peer => NodeSpec,
                        our_tag => OurTag,
                        their_tag => TheirTag,
                        view_member_count => Count
                    }),

                    State = State0#state{
                        pending = Pending,
                        membership = Membership
                    },

                    {noreply, State};

                false ->
                    ?LOG_INFO(#{
                        description => "Join REFUSED, keeping membership.",
                        peer => NodeSpec,
                        our_tag => OurTag,
                        their_tag => TheirTag,
                        view_membership => Membership0
                    }),

                    {noreply, State0}
            end;

        false ->
            {noreply, State0}
    end;

handle_info(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled info event", event => Event}),
    {noreply, State}.


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
    ok = partisan_peer_connections:foreach(Fun),
    ok.


%% @private
-spec code_change(term() | {down, term()}, state_t(), term()) -> {ok, state_t()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
empty_membership() ->
    LocalState = sets:add_element(
        partisan:node_spec(),
        sets:new([{version, 2}])
    ),
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
                    ?LOG_INFO(#{
                        description => "Leaving cluster, removed cluster_state"
                    });
                {error, Reason} ->
                    ?LOG_INFO(#{
                        description => "Unable to remove cluster_state",
                        reason => Reason
                    })
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
-spec peers(sets:set(partisan:node_spec())) -> [partisan:node_spec()].

peers(Set) ->
    sets:to_list(without_myself(Set)).


%% @private
-spec without_myself(sets:set(partisan:node_spec())) ->
    sets:set(partisan:node_spec()).

without_myself(Set) ->
    Exclude = sets:from_list([partisan:node()]),
    sets:subtract(Set, Exclude).


%% @private
establish_connections(Pending, Membership) ->
    %% Reconnect disconnected members and members waiting to join.
    Peers = peers(sets:union(Membership, Pending)),
    lists:foreach(fun partisan_peer_service_manager:connect/1, Peers).


%% @private
kill_connections(Nodes, State) ->
    Fun = fun(Node) ->
        ok = down(Node, State)
    end,
    partisan_peer_service_manager:disconnect(Nodes, Fun).


%% @private
handle_message({forward_message, ServerRef, Message}, State) ->
    partisan_peer_service_manager:process_forward(ServerRef, Message),
    {reply, ok, State}.


%% @private
-spec do_send_message(
    Node :: atom() | partisan:node_spec(), Message :: term())->
    ok | {error, disconnected | not_yet_connected}.

do_send_message(Node, Message) ->
    %% Find a connection for the remote node, if we have one.
    case partisan_peer_connections:dispatch_pid(Node) of
        {ok, Pid} ->
            gen_server:cast(Pid, {send_message, Message});
        {error, _} = Error ->
            Error
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
maybe_add_pending(NodeSpec, #state{} = State) ->
    Pending0 = State#state.pending,
    Membership = State#state.membership,

    Pending = case sets:is_element(NodeSpec, Membership) of
        true ->
            sets:add_element(NodeSpec, Pending0);
        false ->
            Pending0
    end,
    State#state{pending = Pending}.