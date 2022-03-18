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
-export([cast_message/3]).
-export([cast_message/4]).
-export([cast_message/5]).
-export([connections/0]).
-export([decode/1]).
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
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec(atom()) -> node_spec() | {error, Reason :: any()}.

node_spec(Node) when is_atom(Node) ->
    case partisan_peer_service_manager:mynode() of
        Node ->
            partisan_peer_service_manager:myself();

        _ ->
            case partisan_config:get(connect_disterl, false) of
                true ->
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
                            NodeSpec;
                        {badrpc, Reason} ->
                            {error, Reason}
                    end;

                false ->
                    {error, nodisterl}
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec join(list() | atom() | node_spec()) ->
    ok | {error, self_join | any()}.

join(NodeStr) when is_list(NodeStr) ->
    join(erlang:list_to_atom(lists:flatten(NodeStr)));

join(Node) when is_atom(Node) ->
    case partisan_peer_service_manager:mynode() of
        Node ->
            {error, self_join};
        _ ->
            case node_spec(Node) of
                {error, _} = Error ->
                    Error;
                NodeSpec ->
                    join(NodeSpec)
            end
    end;

join(#{name := _} = NodeSpec) ->
    (?MANAGER):join(NodeSpec).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec sync_join(list() | atom() | node_spec()) ->
    ok | {error, not_implemented | self_join | any()}.

sync_join(NodeStr) when is_list(NodeStr) ->
    sync_join(erlang:list_to_atom(lists:flatten(NodeStr)));

sync_join(Node) when is_atom(Node) ->
    case partisan_peer_service_manager:mynode() of
        Node ->
            {error, self_join};
        _ ->
            case node_spec(Node) of
                {error, _} = Error ->
                    Error;
                NodeSpec ->
                    sync_join(NodeSpec)
            end
    end;

sync_join(#{name := _} = NodeSpec) ->
    (?MANAGER):sync_join(NodeSpec).


%% -----------------------------------------------------------------------------
%% @doc Leave the cluster.
%% @end
%% -----------------------------------------------------------------------------
-spec leave() -> ok.

leave() ->
    (?MANAGER):leave().


%% -----------------------------------------------------------------------------
%% @doc Remove another node from the cluster.
%% @end
%% -----------------------------------------------------------------------------
-spec leave(node_spec()) -> ok.

leave(NodeSpec) ->
    (?MANAGER):leave(NodeSpec).


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
%% @doc Add callback.
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

