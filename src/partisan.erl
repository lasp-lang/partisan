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
                                        | remote_ref(process_ref())
                                        | remote_ref(registered_name_ref()).

-export_type([monitor_nodes_opt/0]).

-export([start/0]).
-export([stop/0]).

-export([broadcast/2]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([cast_message/5]).
-export([default_channel/0]).
-export([demonitor/1]).
-export([demonitor/2]).
-export([forward_message/2]).
-export([forward_message/3]).
-export([forward_message/4]).
-export([forward_message/5]).
-export([is_connected/1]).
-export([is_connected/2]).
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
-export([send_message/2]).

-compile({no_auto_import, [demonitor/2]}).
-compile({no_auto_import, [make_ref/0]}).
-compile({no_auto_import, [monitor/2]}).
-compile({no_auto_import, [monitor_node/2]}).
-compile({no_auto_import, [node/0]}).
-compile({no_auto_import, [node/1]}).
-compile({no_auto_import, [nodes/1]}).



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
%% @doc
%% @end
%% -----------------------------------------------------------------------------
make_ref() ->
    partisan_util:ref(erlang:make_ref()).


%% -----------------------------------------------------------------------------
%% @deprecated
%% @doc
%% @end
%% -----------------------------------------------------------------------------
monitor(Term) ->
    monitor(process, Term).


%% -----------------------------------------------------------------------------
%% @doc when you attempt to monitor a partisan_remote_reference, it is not
%% guaranteed that you will receive the DOWN message. A few reasons for not
%% receiving the message are message loss, tree reconfiguration and the node
%% is no longer reachable.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor
    (process, monitor_process_identifier()) ->
        reference() | remote_ref(encoded_ref()) | no_return();
    (port, erlang:monitor_port_identifier()) ->
        reference() |  no_return();
    (time_offset, clock_service) ->
        reference() | no_return().

monitor(process, {partisan_remote_reference, _, _} = R) ->
    partisan_monitor:monitor(R, []);

monitor(Type, Term) ->
    erlang:monitor(Type, Term).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec monitor
    (process, monitor_process_identifier(), [erlang:monitor_option()]) ->
        reference() | remote_ref(encoded_ref()) | no_return();
    (port, erlang:monitor_port_identifier(), [erlang:monitor_option()]) ->
        reference() |  no_return();
    (time_offset, clock_service, [erlang:monitor_option()]) ->
        reference() | no_return().

monitor(process, {partisan_remote_reference, _, _} = R, Opts) ->
    partisan_monitor:monitor(R, Opts);

monitor(Type, Term, Opts) ->
    erlang:monitor(Type, Term, Opts).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(MonitorRef :: reference() | remote_ref(encoded_ref())) -> true.

demonitor(Ref) ->
    demonitor(Ref, []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(
    MonitorRef :: reference() | remote_ref(encoded_ref()),
    OptionList :: [erlang:monitor_option()]) -> boolean().

demonitor(MRef, Opts) when is_reference(MRef) ->
    erlang:demonitor(MRef, Opts);

demonitor(
    {partisan_remote_reference, _, {partisan_encoded_reference, _}} = R,
    Opts) ->
    partisan_monitor:demonitor(R, Opts).


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
-spec node() -> node().

node() ->
    partisan_config:get(name, erlang:node()).


%% -----------------------------------------------------------------------------
%% @doc Returns the node where Arg originates. Arg can be a process identifier,
%% a reference, a port or a partisan remote refererence.
%% @end
%% -----------------------------------------------------------------------------
node(Arg) when is_pid(Arg) orelse is_reference(Arg) orelse is_port(Arg) ->
    erlang:node(Arg);

node({partisan_remote_reference, Node, _}) ->
    Node.


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
-spec nodes(Arg :: erlang:node_type()) -> [node()].

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
%% @doc Returns the node specification of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec() -> node_spec().

node_spec() ->
    Parallelism = partisan_config:get(parallelism, ?PARALLELISM),
    Channels = partisan_config:get(channels, ?CHANNELS),
    Name = partisan_config:get(name),
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
%% If configuration option `connect_disterl' is `true', this function retrieves
%% the node_spec from the remote node using RPC and returns `{error, Reason}'
%% if the RPC fails. Otherwise, asumes the node is running on the same host and
%% return a `node_spec()' with with nodename `Name' and host 'Host' and same
%% metadata as `myself/0'.
%%
%% You should only use this function when distributed erlang is enabled
%% (configuration option `connect_disterl' is `true') or if the node is running
%% on the same host and you are using this for testing purposes as there is no
%% much sense in running a partisan cluster on a single host.
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec(node()) -> {ok, node_spec()} | {error, Reason :: any()}.

node_spec(Node) when is_atom(Node) ->
    case partisan:node() of
        Node ->
            {ok, partisan:node_spec()};

        _ ->
            Timeout = 15000,
            Result = rpc:call(
                Node,
                partisan_peer_service_manager,
                myself,
                [],
                Timeout
            ),
            case Result of
                #{name := Node} = NodeSpec ->
                    {ok, NodeSpec};
                {badrpc, Reason} ->
                    {error, Reason}
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns a peer with nodename `Name' and host 'Host' and same metadata
%% as `myself/0'.
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec(Node :: list() | node(), listen_addr() | [listen_addr()]) ->
    {ok, node_spec()} | {error, Reason :: any()}.

node_spec(Node, Endpoints) when is_list(Node) ->
    node_spec(list_to_node(Node), Endpoints);

node_spec(Node, Endpoints) when is_atom(Node) ->
    Addresses = coerce_listen_addr(Endpoints),
    %% We assume all nodes have the same parallelism and channel config
    Map = partisan:node_spec(),
    NodeSpec = Map#{name => Node, listen_addrs => Addresses},
    {ok, NodeSpec}.


default_channel() ->
    ?DEFAULT_CHANNEL.



%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec send_message(name(), message()) -> ok.

send_message(Name, Message) ->
    (?PEER_SERVICE_MANAGER):send_message(Name, Message).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(name(), pid(), message()) -> ok.

cast_message(Name, ServerRef, Message) ->
    (?PEER_SERVICE_MANAGER):cast_message(Name, ServerRef, Message).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(name(), channel(), pid(), message()) -> ok.

cast_message(Name, Channel, ServerRef, Message) ->
    (?PEER_SERVICE_MANAGER):cast_message(Name, Channel, ServerRef, Message).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(name(), channel(), pid(), message(), options()) -> ok.

cast_message(Name, Channel, ServerRef, Message, Options) ->
    (?PEER_SERVICE_MANAGER):cast_message(Name, Channel, ServerRef, Message, Options).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(remote_ref() | pid(), message()) -> ok.

forward_message(PidOrRef, Message) ->
    (?PEER_SERVICE_MANAGER):forward_message(PidOrRef, Message).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(name(), pid(), message()) -> ok.

forward_message(Name, ServerRef, Message) ->
    (?PEER_SERVICE_MANAGER):forward_message(Name, ServerRef, Message).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(name(), channel(), pid(), message()) -> ok.

forward_message(Name, Channel, ServerRef, Message) ->
    (?PEER_SERVICE_MANAGER):forward_message(Name, Channel, ServerRef, Message).



%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(name(), channel(), pid(), message(), options()) -> ok.

forward_message(Name, Channel, ServerRef, Message, Options) ->
    (?PEER_SERVICE_MANAGER):forward_message(Name, Channel, ServerRef, Message, Options).


%% -----------------------------------------------------------------------------
%% @doc Broadcasts a message originating from this node. The message will be
%% delivered to each node at least once. The `Mod' passed is responsible for
%% handling the message on remote nodes as well as providing some other
%% information both locally and and on other nodes.
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


%% @private
coerce_listen_addr({IP, Port})  ->
    [#{ip => IP, port => Port}];

coerce_listen_addr([{_, _} | _] = L) ->
    [#{ip => IP, port => Port} || {IP, Port} <- L];

coerce_listen_addr(#{ip := _, port := _} = L)  ->
    [L];

coerce_listen_addr([#{ip := _, port := _} | _] = L) ->
    L.