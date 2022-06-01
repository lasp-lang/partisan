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

-export([add_sup_callback/1]).
-export([broadcast_members/0]).
-export([broadcast_members/1]).
-export([cancel_exchanges/1]).
-export([connections/0]).
-export([decode/1]).
-export([exchanges/0]).
-export([exchanges/1]).
-export([get_local_state/0]).
-export([inject_partition/2]).
-export([join/1]).
-export([leave/0]).
-export([leave/1]).
-export([manager/0]).
-export([member/1]).
-export([members/0]).
-export([members_for_orchestration/0]).
-export([on_down/2]).
-export([on_up/2]).
-export([partitions/0]).
-export([reserve/1]).
-export([resolve_partition/1]).
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
    ?PEER_SERVICE_MANAGER.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec join(node_spec() | node() | list) -> ok | {error, self_join | any()}.

join(#{name := Node} = NodeSpec) ->
    case partisan:node() of
        Node ->
            {error, self_join};
        _ ->
            (?PEER_SERVICE_MANAGER):join(NodeSpec)
    end;

join(Node) ->
    case partisan:node_spec(Node) of
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
    case partisan:node() of
        Node ->
            {error, self_join};
        _ ->
            (?PEER_SERVICE_MANAGER):sync_join(NodeSpec)
    end.


%% -----------------------------------------------------------------------------
%% @doc Leave the cluster. We will not be able to re-join the cluster, we must
%% be restarted first.
%% @end
%% -----------------------------------------------------------------------------
-spec leave() -> ok.

leave() ->
    (?PEER_SERVICE_MANAGER):leave().


%% -----------------------------------------------------------------------------
%% @doc Remove a node from the cluster. Subsequently calling `join
%% (NodeSpec)' will not work for the removed node. The removed node must be
%% restarted first.
%% @end
%% -----------------------------------------------------------------------------
-spec leave(node_spec()) -> ok.

leave(#{name := Node} = NodeSpec) ->
    case partisan:node() of
        Node ->
            (?PEER_SERVICE_MANAGER):leave();
        _ ->
            (?PEER_SERVICE_MANAGER):leave(NodeSpec)
    end.


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection open for a given node.
%% `Function' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
-spec on_up(node() | node_spec() | any | '_', function()) ->
    ok | {error, not_implemented}.

on_up(Node, Function) ->
    (?PEER_SERVICE_MANAGER):on_up(Node, Function).


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection close for a given node.
%% `Function' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
-spec on_down(node() | node_spec() | any | '_', function()) ->
    ok | {error, not_implemented}.

on_down(Node, Function) ->
    (?PEER_SERVICE_MANAGER):on_down(Node, Function).


%% -----------------------------------------------------------------------------
%% @doc Return cluster members
%% @end
%% -----------------------------------------------------------------------------
-spec member(Node :: node() | node_spec()) -> boolean().

member(Node) ->
    (?PEER_SERVICE_MANAGER):member(Node).



%% -----------------------------------------------------------------------------
%% @doc Return cluster members
%% @end
%% -----------------------------------------------------------------------------
-spec members() -> {ok, [node()]}.

members() ->
    (?PEER_SERVICE_MANAGER):members().


%% -----------------------------------------------------------------------------
%% @doc Return cluster members
%% @end
%% -----------------------------------------------------------------------------
-spec members_for_orchestration() -> [node_spec()].

members_for_orchestration() ->
    (?PEER_SERVICE_MANAGER):members_for_orchestration().


%% -----------------------------------------------------------------------------
%% @doc Return peer service connections
%% @end
%% -----------------------------------------------------------------------------
connections() ->
    {ok, partisan_peer_connections:connections()}.


%% -----------------------------------------------------------------------------
%% @doc Update cluster members.
%% @end
%% -----------------------------------------------------------------------------
-spec update_members([node()]) -> ok | {error, not_implemented}.

update_members(Nodes) ->
    (?PEER_SERVICE_MANAGER):update_members(Nodes).


%% -----------------------------------------------------------------------------
%% @doc Decode peer_service_manager state from an encoded form
%% @end
%% -----------------------------------------------------------------------------
-spec decode(term()) -> term().

decode(State) ->
    Manager = ?PEER_SERVICE_MANAGER,
    [P || #{name := P} <- Manager:decode(State)].


%% -----------------------------------------------------------------------------
%% @doc Reserve a slot for the particular tag.
%% @end
%% -----------------------------------------------------------------------------
-spec reserve(atom()) -> ok | {error, no_available_slots}.

reserve(Tag) ->
    (?PEER_SERVICE_MANAGER):reserve(Tag).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec partitions() -> {ok, partitions()} | {error, not_implemented}.

partitions() ->
    (?PEER_SERVICE_MANAGER):partitions().



%% -----------------------------------------------------------------------------
%% @doc Inject a partition.
%% @end
%% -----------------------------------------------------------------------------
-spec inject_partition(node_spec(), ttl()) ->
    {ok, reference()} | {error, not_implemented}.

inject_partition(Origin, TTL) ->
    (?PEER_SERVICE_MANAGER):inject_partition(Origin, TTL).


%% -----------------------------------------------------------------------------
%% @doc Resolve a partition.
%% @end
%% -----------------------------------------------------------------------------
-spec resolve_partition(reference()) ->
    ok | {error, not_implemented}.

resolve_partition(Reference) ->
    (?PEER_SERVICE_MANAGER):resolve_partition(Reference).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_local_state() -> term().

get_local_state() ->
    (?PEER_SERVICE_MANAGER):get_local_state().



%% -----------------------------------------------------------------------------
%% @doc Adds a supervised callback to receive peer service membership updates.
%% @end
%% -----------------------------------------------------------------------------
add_sup_callback(Function) ->
    partisan_peer_service_events:add_sup_callback(Function).




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






