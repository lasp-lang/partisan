%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Helium Systems, Inc. All Rights Reserved.
%% Copyright (c) 2016 Christopher Meiklejohn. All Rights Reserved.
%% Copyright (c) 2022 Alejandro M. Ramallo. All Rights Reserved.
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

-module(partisan).

-include("partisan.hrl").

-type monitor_nodes_opt()           ::  nodedown_reason
                                        | {node_type, visible | hidden | all}.
-type monitor_process_identifier()  ::  erlang:monitor_process_identifier()
                                        | partisan_remote_ref:p()
                                        | partisan_remote_ref:n().

-type server_ref()      ::  partisan_peer_service_manager:server_ref().
-type forward_opts()    ::  partisan_peer_service_manager:forward_opts().
-type node_type()       ::  this | known | visible | connected | hidden | all.

-export_type([channel/0]).
-export_type([forward_opts/0]).
-export_type([monitor_nodes_opt/0]).
-export_type([node_type/0]).
-export_type([server_ref/0]).

-export([start/0]).
-export([stop/0]).

-export([broadcast/2]).
-export([cast_message/2]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([default_channel/0]).
-export([demonitor/1]).
-export([demonitor/2]).
-export([disconnect_node/1]).
-export([forward_message/2]).
-export([forward_message/3]).
-export([forward_message/4]).
-export([is_alive/0]).
-export([is_connected/1]).
-export([is_connected/2]).
-export([is_fully_connected/1]).
-export([is_local/1]).
-export([is_pid/1]).
-export([is_process_alive/1]).
-export([is_reference/1]).
-export([make_ref/0]).
-export([monitor/1]).
-export([monitor/2]).
-export([monitor/3]).
-export([monitor_node/2]).
-export([monitor_nodes/1]).
-export([monitor_nodes/2]).
-export([node/0]).
-export([node/1]).
-export([node_spec/0]).
-export([node_spec/1]).
-export([node_spec/2]).
-export([nodes/0]).
-export([nodes/1]).
-export([nodestring/0]).
-export([self/0]).

-compile({no_auto_import, [demonitor/2]}).
-compile({no_auto_import, [is_pid/0]}).
-compile({no_auto_import, [is_process_alive/1]}).
-compile({no_auto_import, [is_reference/0]}).
-compile({no_auto_import, [make_ref/0]}).
-compile({no_auto_import, [monitor/2]}).
-compile({no_auto_import, [monitor/3]}).
-compile({no_auto_import, [monitor_node/2]}).
-compile({no_auto_import, [node/0]}).
-compile({no_auto_import, [node/1]}).
-compile({no_auto_import, [nodes/1]}).
-compile({no_auto_import, [self/0]}).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Start the application.
%% @end
%% -----------------------------------------------------------------------------
start() ->
    application:ensure_all_started(partisan).


%% -----------------------------------------------------------------------------
%% @doc Stop the application.
%% @end
%% -----------------------------------------------------------------------------
stop() ->
    application:stop(partisan).



%% -----------------------------------------------------------------------------
%% @doc Returns a new partisan_remote_ref.
%% This is the same as calling
%% `partisan_remote_ref:from_term(erlang:make_ref())'.
%% @end
%% -----------------------------------------------------------------------------
-spec make_ref() -> partisan_remote_ref:r().

make_ref() ->
    partisan_remote_ref:from_term(erlang:make_ref()).


%% -----------------------------------------------------------------------------
%% @doc Returns the partisan encoded pid for the calling process.
%% This is the same as calling
%% `partisan_remote_ref:from_term(erlang:self())'.
%% @end
%% -----------------------------------------------------------------------------
-spec self() -> partisan_remote_ref:p().

self() ->
    partisan_remote_ref:from_term(erlang:self()).


%% -----------------------------------------------------------------------------
%% @deprecated Use monitor/2 instead.
%% @doc
%% @end
%% -----------------------------------------------------------------------------
monitor(Term) ->
    monitor(process, Term).


%% -----------------------------------------------------------------------------
%% @doc Sends a monitor request of type `Type' to the entity identified by
%% `Item'. If the monitored entity does not exist or it changes monitored
%% state, the caller of `monitor/2' is notified by a message on the following
%% format:
%% `{Tag, MonitorRef, Type, Object, Info}'
%%
%% This is the Partisan's equivalent to {@link erlang:monitor/2}.
%%
%% Failure: `notalive' if the `partisan_monitor' server is not alive.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor
    (process, monitor_process_identifier()) ->
        reference() | partisan_remote_ref:r() | no_return();
    (port, erlang:monitor_port_identifier()) ->
        reference() |  no_return();
    (time_offset, clock_service) ->
        reference() | no_return().

monitor(Type, Item) ->
    monitor(Type, Item, []).


%% -----------------------------------------------------------------------------
%% @doc Sends a monitor request of type `Type' to the entity identified by
%% `Item'. If the monitored entity does not exist or it changes monitored
%% state, the caller of `monitor/2' is notified by a message on the following
%% format:
%%
%% `{Tag, MonitorRef, Type, Object, Info}'
%%
%% This is the Partisan's equivalent to {@link erlang:monitor/2}. It differs
%% from the Erlang implementation only when monitoring a `process'. For all
%% other cases (monitoring a `port' or `time_offset') this function calls
%% `erlang:monitor/2'.
%%
%% === Monitoring a `process` ===
%% Creates monitor between the current process and another process identified
%% by Item, which can be a `pid()' (local or remote), an atom `RegisteredName'
%% or a tuple `{RegisteredName, Node}'' for a registered process, located
%% elsewhere.
%%
%% In the case of a local `pid()' or a remote `pid()'
%% A process monitor by name resolves the `RegisteredName' to `pid()' or
%% `port()' only once at the moment of monitor instantiation, later changes to
%% the name registration will not affect the existing monitor.
%%
%% Failure: `notalive' if the `partisan_monitor' server is not alive.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor
    (process, monitor_process_identifier(), [erlang:monitor_option()]) ->
        reference() | partisan_remote_ref:r() | no_return();
    (port, erlang:monitor_port_identifier(), [erlang:monitor_option()]) ->
        reference() |  no_return();
    (time_offset, clock_service, [erlang:monitor_option()]) ->
        reference() | no_return().


monitor(process, {RegisteredName, Node} = RegPid, Opts) ->
    case partisan_config:get(connect_disterl) orelse partisan:node() == Node of
        true ->
            erlang:monitor(process, RegPid, Opts);
        false ->
            Ref = partisan_remote_ref:from_term(RegisteredName, Node),
            partisan_monitor:monitor(Ref, Opts)
    end;

monitor(process, Term, Opts) when erlang:is_pid(Term) orelse is_atom(Term) ->
    erlang:monitor(process, Term, Opts);

monitor(process, RemoteRef, Opts)
when is_tuple(RemoteRef) orelse is_binary(RemoteRef) ->
    partisan_monitor:monitor(RemoteRef, Opts);

monitor(Type, Term, Opts) when Type == port orelse Type == time_offset ->
    erlang:monitor(Type, Term, Opts).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(MonitorRef :: reference() | partisan_remote_ref:r()) -> true.

demonitor(Ref) ->
    demonitor(Ref, []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(
    MonitorRef :: reference() | partisan_remote_ref:r(),
    OptionList :: partisan_monitor:demonitor_opts()) -> boolean().

demonitor(Ref, Opts) when erlang:is_reference(Ref) ->
    erlang:demonitor(Ref, Opts);

demonitor(Ref, Opts) ->
    %% partisan_monitor:demonitor will raise a badarg if Ref is not valid
    partisan_monitor:demonitor(Ref, Opts).


%% -----------------------------------------------------------------------------
%% @doc Monitor the status of the node `Node'. If Flag is true, monitoring is
%% turned on. If `Flag' is `false', monitoring is turned off.
%%
%% Making several calls to `monitor_node(Node, true)' for the same `Node' from
%% is not an error; it results in as many independent monitoring instances as
%% the number of different calling processes i.e. If a process has made two
%% calls to `monitor_node(Node, true)' and `Node' terminates, only one
%% `nodedown' message is delivered to the process (this differs from {@link
%% erlang:monitor_node/2}).
%%
%% If `Node' fails or does not exist, the message `{nodedown, Node}' is
%% delivered to the calling process. If there is no connection to Node, a
%% `nodedown' message is delivered. As a result when using a membership
%% strategy that uses a partial view, you can not monitor nodes that are not
%% members of the view.
%%
%% If `Node' is the caller's node, the function returns `false'.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_node(node() | node_spec(), boolean()) -> boolean().

monitor_node(#{name := Node}, Flag) ->
    monitor_node(Node, Flag, []);

monitor_node(Node, Flag) ->
    monitor_node(Node, Flag, []).


%% -----------------------------------------------------------------------------
%% @doc Monitor the status of the node `Node'. If Flag is true, monitoring is
%% turned on. If `Flag' is `false', monitoring is turned off.
%%
%% Making several calls to `monitor_node(Node, true)' for the same `Node' from
%% is not an error; it results in as many independent monitoring instances as
%% the number of different calling processes i.e. If a process has made two
%% calls to `monitor_node(Node, true)' and `Node' terminates, only one
%% `nodedown' message is delivered to the process (this differs from {@link
%% erlang:monitor_node/2}).
%%
%% If `Node' fails or does not exist, the message `{nodedown, Node}' is
%% delivered to the calling process. If there is no connection to Node, a
%% `nodedown' message is delivered. As a result when using a membership
%% strategy that uses a partial view, you can not monitor nodes that are not
%% members of the view.
%%
%% If `Node' is the caller's node, the function returns `false'.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_node(
    Node :: node() | node_spec(),
    Flag :: boolean(),
    Options :: [allow_passive_connect]) -> true.

monitor_node(Node, Flag, Opts) ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            erlang:monitor_node(Node, Flag, Opts);
        false ->
            %% No opts for partisan_monitor
            partisan_monitor:monitor_node(Node, Flag)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_nodes(Flag :: boolean()) -> ok | error | {error, term()}.

monitor_nodes(Flag) ->
    monitor_nodes(Flag, []).


%% -----------------------------------------------------------------------------
%% @doc The calling process subscribes or unsubscribes to node status change
%% messages. A nodeup message is delivered to all subscribing processes when a
%% new node is connected, and a nodedown message is delivered when a node is
%% disconnected.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_nodes(Flag :: boolean(), [monitor_nodes_opt()]) ->
    ok | error | {error, term()}.

monitor_nodes(Flag, Opts) ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            %% Returns ok | error | {error, term()}.
            net_kernel:monitor_nodes(Flag, Opts);
        false ->
            partisan_monitor:monitor_nodes(Flag, Opts)
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_local(Term) -> Result when
    Term :: pid() | port() | reference()
            | partisan_remote_ref:p()
            | partisan_remote_ref:r(),
    Result :: boolean().

is_local(Term) ->
    node(Term) =:= node().


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec node() -> node().

node() ->
    partisan_config:get(name).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node as a binary string.
%% @end
%% -----------------------------------------------------------------------------
-spec nodestring() -> binary().

nodestring() ->
    partisan_config:get(nodestring).


%% -----------------------------------------------------------------------------
%% @doc Returns the node where Arg originates. Arg can be a process identifier,
%% a reference, a port or a partisan remote reference.
%% @end
%% -----------------------------------------------------------------------------
-spec node(Term) -> Result when
    Term :: pid() | port() | reference()
            | partisan_remote_ref:p()
            | partisan_remote_ref:r(),
    Result :: node() | no_return().

node(Arg)
when erlang:is_pid(Arg) orelse erlang:is_reference(Arg) orelse is_port(Arg) ->
    erlang:node(Arg);

node({partisan_remote_ref, Node, _}) ->
    Node;

node(Encoded) when is_binary(Encoded) ->
    partisan_remote_ref:node(Encoded).


%% -----------------------------------------------------------------------------
%% @doc Returns a list of all nodes connected to this node through normal
%% connections (that is, hidden nodes are not listed). Same as nodes(visible).
%% @end
%% -----------------------------------------------------------------------------
-spec nodes() -> [node()].

nodes() ->
    nodes(visible).


%% -----------------------------------------------------------------------------
%% @doc Returns a list of all nodes connected to this node through normal
%% connections (that is, hidden nodes are not listed). Same as nodes(visible).
%% @end
%% -----------------------------------------------------------------------------
-spec nodes(Arg :: node_type()) -> [node()].

nodes(Arg) ->
    partisan_peer_connections:nodes(Arg).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_connected(NodeOrSpec :: node_spec() | node()) -> boolean().

is_connected(NodeOrSpec) ->
    partisan_peer_connections:is_connected(NodeOrSpec).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_connected(node_spec() | node(), channel()) -> boolean().

is_connected(NodeOrSpec, Channel) ->
    partisan_peer_connections:is_connected(NodeOrSpec, Channel).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_fully_connected(NodeOrSpec :: node_spec() | node()) -> boolean().

is_fully_connected(NodeOrSpec) ->
    partisan_peer_connections:is_fully_connected(NodeOrSpec).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec disconnect_node(Node :: node()) -> boolean() | ignored.

disconnect_node(Node) ->
    case Node == node() of
        true ->
            partisan_peer_service:leave();
        false ->
            try node_spec(Node) of
                {ok, NodeSpec} ->
                    ok = partisan_peer_service:leave(NodeSpec),
                    true;
                {error, _} ->
                    false
            catch
                _:_ ->
                    false
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns true if the local node is alive (that is, if the node can be
%% part of a distributed system), otherwise false.
%% @end
%% -----------------------------------------------------------------------------
-spec is_alive() -> boolean().

is_alive() ->
    undefined =/= whereis(?PEER_SERVICE_MANAGER).


%% -----------------------------------------------------------------------------
%% @doc Returns the node specification of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec() -> node_spec().

node_spec() ->
    Name = partisan_config:get(name),
    Parallelism = partisan_config:get(parallelism, ?PARALLELISM),
    %% Channels and ListenAddrs are sorted already
    Channels = partisan_config:get(channels, ?CHANNELS),
    ListenAddrs = partisan_config:get(listen_addrs),

    #{
        name => Name,
        listen_addrs => ListenAddrs,
        channels => Channels,
        parallelism => Parallelism
    }.


%% -----------------------------------------------------------------------------
%% @doc Return the partisan node_spec() for node named `Node'.
%%
%% This function retrieves the `node_spec()' from the remote node using RPC and
%% returns `{error, Reason}' if the RPC fails. Otherwise, assumes the node is
%% running on the same host and returns a `node_spec()' with with nodename
%% `Name' and host 'Host' and same metadata as `myself/0'.
%%
%% If configuration option `connect_disterl' is `true', the RPC will be
%% implemented using the `rpc' module. Otherwise it will use `partisan_rpc'.
%%
%% You should only use this function when distributed erlang is enabled
%% (configuration option `connect_disterl' is `true') or if the node is running
%% on the same host and you are using this for testing purposes as there is no
%% much sense in running a partisan cluster on a single host.
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec(node()) -> {ok, node_spec()} | {error, Reason :: any()}.

node_spec(Node) when is_atom(Node) ->
    node_spec(Node, #{}).


%% -----------------------------------------------------------------------------
%% @doc Return the tuple `{ok, node_spec()' for node named `Node' or the tuple
%% `{error, Reason}'.
%%
%% This function first checks If there is a partisan connection to `Node', if
%% so returns the cached specification that was used for creating the
%% connection. If no connection is present (the case for a p2p topology), then
%% it tries to use @@link partisan_rpc} to retrieve the node specification from
%% the remote node. This later alternative requires the partisan configuration
%% `forward_opts` to have `broadcast' and `transitive' enabled.
%%
%% NOTICE: At the moment partisan_rpc might not work correctly w/ a p2p
%% topology.
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec(
    Node :: list() | node(),
    Opts :: #{rpc_timeout => timeout()}) ->
    {ok, node_spec()} | {error, Reason :: any()}.

node_spec(Node, Opts) when is_list(Node) ->
    node_spec(list_to_node(Node), Opts);

node_spec(Node, Opts) when is_atom(Node), is_map(Opts) ->
    Timeout = maps:get(rpc_timeout, Opts, 5000),

    case partisan:node() of
        Node ->
            {ok, partisan:node_spec()};

        _ ->
            case is_connected(Node) of
                true ->
                    {ok, Info} = partisan_peer_connections:info(Node),
                    {ok, partisan_peer_connections:node_spec(Info)};

                false ->
                    M = ?MODULE,
                    F = node_spec,
                    A = [],
                    case partisan_rpc:call(Node, M, F, A, Timeout) of
                        #{name := Node} = Spec ->
                            {ok, Spec};
                        {badrpc, Reason} ->
                            {error, Reason}
                    end
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
default_channel() ->
    ?DEFAULT_CHANNEL.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_pid(pid() | partisan_remote_ref:p()) ->
    boolean() | no_return().

is_pid(Pid) when erlang:is_pid(Pid) ->
    true;

is_pid(RemoteRef) ->
    partisan_remote_ref:is_pid(RemoteRef).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_reference(reference() | partisan_remote_ref:r()) ->
    boolean() | no_return().

is_reference(Pid) when erlang:is_reference(Pid) ->
    true;

is_reference(RemoteRef) ->
    partisan_remote_ref:is_reference(RemoteRef).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_process_alive(pid() | partisan_remote_ref:p()) ->
    boolean() | no_return().

is_process_alive(Pid) when erlang:is_pid(Pid) ->
    erlang:is_process_alive(Pid);

is_process_alive(RemoteRef) ->
    case partisan_remote_ref:is_local(RemoteRef) of
        true ->
            erlang:is_process_alive(partisan_remote_ref:to_term(RemoteRef));
        false ->
            Node = node(RemoteRef),
            partisan_rpc:call(
                Node, ?MODULE, is_process_alive, [RemoteRef], 5000
            )
    end.


%% -----------------------------------------------------------------------------
%% @doc Cast message to a remote ref
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    ServerRef :: server_ref(),
    Msg :: message()) -> ok.

cast_message(Term, Message) ->
    (?PEER_SERVICE_MANAGER):cast_message(Term, Message).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()) -> ok.

cast_message(ServerRef, Msg, Opts) ->
    (?PEER_SERVICE_MANAGER):cast_message(ServerRef, Msg, Opts).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    Node :: node(),
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()) -> ok.

cast_message(Node, ServerRef, Message, Options) ->
    (?PEER_SERVICE_MANAGER):cast_message(Node, ServerRef, Message, Options).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(
    ServerRef :: server_ref(),
    Msg :: message()) -> ok.

forward_message(ServerRef, Message) ->
    (?PEER_SERVICE_MANAGER):forward_message(ServerRef, Message).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()) -> ok.

forward_message(ServerRef, Message, Opts) ->
    (?PEER_SERVICE_MANAGER):forward_message(ServerRef, Message, Opts).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(
    Node :: node(),
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()) -> ok.

forward_message(Node, ServerRef, Message, Opts) ->
    (?PEER_SERVICE_MANAGER):forward_message(Node, ServerRef, Message, Opts).


%% -----------------------------------------------------------------------------
%% @doc Broadcasts a message originating from this node.
%%
%% The message will be delivered to each node at least once. The `Mod' passed
%% is responsible for handling the message on remote nodes as well as providing
%% some other information both locally and and on other nodes.
%% `Mod' must be loaded on all members of the clusters and implement the
%% `partisan_plumtree_broadcast_handler' behaviour.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast(any(), module()) -> ok.

broadcast(Broadcast, Mod) ->
    partisan_plumtree_broadcast:broadcast(Broadcast, Mod).


%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
list_to_node(NodeStr) ->
    erlang:list_to_atom(lists:flatten(NodeStr)).
