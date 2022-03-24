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

-module(partisan_peer_service).

-include("partisan_logger.hrl").
-include("partisan.hrl").

-define(MANAGER,
    partisan_config:get(
        partisan_peer_service_manager,
        ?DEFAULT_PEER_SERVICE_MANAGER
    )
).

-export([add_sup_callback/1]).
-export([broadcast/2]).
-export([broadcast_members/0]).
-export([broadcast_members/1]).
-export([cancel_exchanges/1]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([cast_message/5]).
-export([connections/0]).
-export([decode/1]).
-export([exchanges/0]).
-export([exchanges/1]).
-export([forward_message/3]).
-export([forward_message/4]).
-export([forward_message/5]).
-export([get_local_state/0]).
-export([inject_partition/2]).
-export([join/1]).
-export([leave/0]).
-export([leave/1]).
-export([manager/0]).
-export([members/0]).
-export([members_for_orchestration/0]).
-export([mynode/0]).
-export([myself/0]).
-export([node_spec/1]).
-export([node_spec/2]).
-export([on_down/2]).
-export([on_up/2]).
-export([partitions/0]).
-export([reserve/1]).
-export([resolve_partition/1]).
-export([send_message/2]).
-export([stop/0]).
-export([stop/1]).
-export([sync_join/1]).
-export([update_members/1]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Stop
%% @end
%% -----------------------------------------------------------------------------
stop() ->
    stop("received stop request").


%% -----------------------------------------------------------------------------
%% @doc Stop
%% @end
%% -----------------------------------------------------------------------------
stop(Reason) ->
    ?LOG_NOTICE(#{
        description => "Peer service stopping",
        reason => Reason
    }),
    init:stop().


%% -----------------------------------------------------------------------------
%% @doc Return current peer service manager for this
%% @end
%% -----------------------------------------------------------------------------
-spec manager() -> module().

manager() ->
    ?MANAGER.


%% -----------------------------------------------------------------------------
%% @doc Return the partisan node_spec() for this node.
%% @end
%% -----------------------------------------------------------------------------
-spec myself() -> node_spec().

myself() ->
    partisan_peer_service_manager:myself().


%% -----------------------------------------------------------------------------
%% @doc Return the partisan node_spec() for this node.
%% @end
%% -----------------------------------------------------------------------------
-spec mynode() -> atom().

mynode() ->
    partisan_peer_service_manager:mynode().


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
    case partisan_peer_service_manager:mynode() of
        Node ->
            {ok, partisan_peer_service_manager:myself()};

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
    Map = partisan_peer_service_manager:myself(),
    NodeSpec = Map#{name => Node, listen_addrs => Addresses},
    {ok, NodeSpec}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec join(node_spec() | node() | list) -> ok | {error, self_join | any()}.

join(#{name := Node} = NodeSpec) ->
    case partisan_peer_service_manager:mynode() of
        Node ->
            {error, self_join};
        _ ->
            (?MANAGER):join(NodeSpec)
    end;

join(Node) ->
    case node_spec(Node) of
        {ok, NodeSpec} ->
            join(NodeSpec);
        {error, _} = Error ->
            Error
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec sync_join(node_spec()) ->
    ok | {error, self_join | not_implemented | any()}.

sync_join(#{name := Node} = NodeSpec) ->
    case partisan_peer_service_manager:mynode() of
        Node ->
            {error, self_join};
        _ ->
            (?MANAGER):sync_join(NodeSpec)
    end.


%% -----------------------------------------------------------------------------
%% @doc Leave the cluster. We will not be able to re-join the cluster, we must
%% be restarted first.
%% @end
%% -----------------------------------------------------------------------------
-spec leave() -> ok.

leave() ->
    (?MANAGER):leave().


%% -----------------------------------------------------------------------------
%% @doc Remove a node from the cluster. Subsequently calling `join
%% (NodeSpec)' will not work for the removed node. The removed node must be
%% restarted first.
%% @end
%% -----------------------------------------------------------------------------
-spec leave(node_spec()) -> ok.

leave(#{name := Node} = NodeSpec) ->
    case partisan_peer_service_manager:mynode() of
        Node ->
            (?MANAGER):leave();
        _ ->
            (?MANAGER):leave(NodeSpec)
    end.


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection open for a given node.
%% `Function' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
-spec on_up(atom() | node_spec() | any | '_', function()) ->
    ok | {error, not_implemented}.

on_up(Node, Function) ->
    (?MANAGER):on_up(Node, Function).


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection close for a given node.
%% `Function' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
-spec on_down(atom() | node_spec() | any | '_', function()) ->
    ok | {error, not_implemented}.

on_down(Node, Function) ->
    (?MANAGER):on_down(Node, Function).


%% -----------------------------------------------------------------------------
%% @doc Return cluster members
%% @end
%% -----------------------------------------------------------------------------
-spec members() -> [name()].

members() ->
    (?MANAGER):members().


%% -----------------------------------------------------------------------------
%% @doc Return cluster members
%% @end
%% -----------------------------------------------------------------------------
-spec members_for_orchestration() -> [node_spec()].

members_for_orchestration() ->
    (?MANAGER):members_for_orchestration().


%% -----------------------------------------------------------------------------
%% @doc Return peer service connections
%% @end
%% -----------------------------------------------------------------------------
connections() ->
    (?MANAGER):connections().


%% -----------------------------------------------------------------------------
%% @doc Update cluster members.
%% @end
%% -----------------------------------------------------------------------------
-spec update_members([node()]) -> ok | {error, not_implemented}.

update_members(Nodes) ->
    (?MANAGER):update_members(Nodes).


%% -----------------------------------------------------------------------------
%% @doc Decode peer_service_manager state from an encoded form
%% @end
%% -----------------------------------------------------------------------------
-spec decode(term()) -> term().

decode(State) ->
    Manager = ?MANAGER,
    [P || #{name := P} <- Manager:decode(State)].


%% -----------------------------------------------------------------------------
%% @doc Reserve a slot for the particular tag.
%% @end
%% -----------------------------------------------------------------------------
-spec reserve(atom()) -> ok | {error, no_available_slots}.

reserve(Tag) ->
    (?MANAGER):reserve(Tag).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec partitions() -> {ok, partitions()} | {error, not_implemented}.

partitions() ->
    (?MANAGER):partitions().



%% -----------------------------------------------------------------------------
%% @doc Inject a partition.
%% @end
%% -----------------------------------------------------------------------------
-spec inject_partition(node_spec(), ttl()) ->
    {ok, reference()} | {error, not_implemented}.

inject_partition(Origin, TTL) ->
    (?MANAGER):inject_partition(Origin, TTL).


%% -----------------------------------------------------------------------------
%% @doc Resolve a partition.
%% @end
%% -----------------------------------------------------------------------------
-spec resolve_partition(reference()) ->
    ok | {error, not_implemented}.

resolve_partition(Reference) ->
    (?MANAGER):resolve_partition(Reference).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_local_state() -> term().

get_local_state() ->
    (?MANAGER):get_local_state().



%% -----------------------------------------------------------------------------
%% @doc Adds a supervised callback to receive peer service membership updates.
%% @end
%% -----------------------------------------------------------------------------
add_sup_callback(Function) ->
    partisan_peer_service_events:add_sup_callback(Function).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec send_message(name(), message()) -> ok.

send_message(Name, Message) ->
    (?MANAGER):send_message(Name, Message).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(name(), pid(), message()) -> ok.

cast_message(Name, ServerRef, Message) ->
    (?MANAGER):cast_message(Name, ServerRef, Message).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(name(), channel(), pid(), message()) -> ok.

cast_message(Name, Channel, ServerRef, Message) ->
    (?MANAGER):cast_message(Name, Channel, ServerRef, Message).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(name(), channel(), pid(), message(), options()) -> ok.

cast_message(Name, Channel, ServerRef, Message, Options) ->
    (?MANAGER):cast_message(Name, Channel, ServerRef, Message, Options).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(name(), pid(), message()) -> ok.

forward_message(Name, ServerRef, Message) ->
    (?MANAGER):forward_message(Name, ServerRef, Message).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(name(), channel(), pid(), message()) -> ok.

forward_message(Name, Channel, ServerRef, Message) ->
    (?MANAGER):forward_message(Name, Channel, ServerRef, Message).



%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(name(), channel(), pid(), message(), options()) -> ok.

forward_message(Name, Channel, ServerRef, Message, Options) ->
    (?MANAGER):forward_message(Name, Channel, ServerRef, Message, Options).


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


%% -----------------------------------------------------------------------------
%% @doc Returns the broadcast servers view of full cluster membership.
%% Wait indefinitely for a response is returned from the process.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_members() -> ordsets:ordset(node()).

broadcast_members() ->
    partisan_plumtree_broadcast:broadcast_members().


%% -----------------------------------------------------------------------------
%% @doc Returns the broadcast servers view of full cluster membership.
%% Waits `Timeout' ms for a response from the server.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_members(infinity | pos_integer()) -> ordsets:ordset(node()).

broadcast_members(Timeout) ->
    partisan_plumtree_broadcast:broadcast_members(Timeout).


%% -----------------------------------------------------------------------------
%% @doc return a list of exchanges, started by broadcast on thisnode, that are
%% running.
%% @end
%% -----------------------------------------------------------------------------
-spec exchanges() -> partisan_plumtree_broadcas:exchanges().

exchanges() ->
    partisan_plumtree_broadcast:exchanges().


%% -----------------------------------------------------------------------------
%% @doc returns a list of exchanges, started by broadcast on `Node', that are
%% running.
%% @end
%% -----------------------------------------------------------------------------
-spec exchanges(node()) -> partisan_plumtree_broadcast:exchanges().

exchanges(Node) ->
    partisan_plumtree_broadcast:exchanges(Node).


%% -----------------------------------------------------------------------------
%% @doc cancel exchanges started by this node.
%% @end
%% -----------------------------------------------------------------------------
-spec cancel_exchanges(partisan_plumtree_broadcast:selector()) ->
    partisan_plumtree_broadcast:exchanges().

cancel_exchanges(WhichExchanges) ->
    partisan_plumtree_broadcast:cancel_exchanges(WhichExchanges).




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

