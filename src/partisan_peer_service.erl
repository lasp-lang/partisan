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

-export([join/1,
         join/2,
         join/3,
         attempt_join/1,
         sync_join/1,
         sync_join/2,
         sync_join/3,
         attempt_sync_join/1,
         leave/1,
         decode/1,
         update_members/1,
         stop/0,
         stop/1,
         members/0,
         connections/0,
         manager/0,
         add_sup_callback/1,
         cast_message/3,
         forward_message/3]).

-include("partisan.hrl").

%% @doc Return current peer service manager.
manager() ->
    partisan_config:get(partisan_peer_service_manager,
                        ?DEFAULT_PEER_SERVICE_MANAGER).

%% @doc prepare node to join a cluster
join(Node) ->
    join(Node, true).

%% @doc prepare node to join a cluster
sync_join(Node) ->
    sync_join(Node, true).

%% @doc Convert nodename to atom
join(NodeStr, Auto) when is_list(NodeStr) ->
    join(erlang:list_to_atom(lists:flatten(NodeStr)), Auto);
join(Node, Auto) when is_atom(Node) ->
    Manager = manager(),
    join(Manager:myself(name), Node, Auto);
join(Node, _Auto) ->
    attempt_join(Node).

%% @doc Convert nodename to atom
sync_join(NodeStr, Auto) when is_list(NodeStr) ->
    sync_join(erlang:list_to_atom(lists:flatten(NodeStr)), Auto);
sync_join(Node, Auto) when is_atom(Node) ->
    Manager = manager(),
    sync_join(Manager:myself(name), Node, Auto);
sync_join(Node, _Auto) ->
    attempt_sync_join(Node).

%% @doc Initiate join. Nodes cannot join themselves.
join(Node, Node, _) ->
    {error, self_join};
join(_, Node, _Auto) ->
    attempt_join(Node).

%% @doc Initiate join. Nodes cannot join themselves.
sync_join(Node, Node, _) ->
    {error, self_join};
sync_join(_, Node, _Auto) ->
    attempt_sync_join(Node).

%% @doc Return node members.
members() ->
    Manager = manager(),
    Manager:members().

%% @doc Return node connections.
connections() ->
    Manager = manager(),
    Manager:connections().

%% @doc Update cluster members.
update_members(Nodes) ->
    Manager = manager(),
    Manager:update_members(Nodes).

%% @doc Add callback.
add_sup_callback(Function) ->
    partisan_peer_service_events:add_sup_callback(Function).

%% @doc Cast message to registered process on the remote side.
cast_message(Name, ServerRef, Message) ->
    Manager = manager(),
    Manager:cast_message(Name, ServerRef, Message).

%% @doc Forward message to registered process on the remote side.
forward_message(Name, ServerRef, Message) ->
    Manager = manager(),
    Manager:forward_message(Name, ServerRef, Message).

%% @private
decode(State) ->
    Manager = manager(),
    [P || #{name := P} <- Manager:decode(State)].

%% @private
attempt_join(Node) when is_atom(Node) ->
    ListenAddrs = rpc:call(Node, partisan_config, get, [listen_addrs]),
    attempt_join(#{name => Node, listen_addrs => ListenAddrs});
attempt_join(#{name := _Name} = Node) ->
    Manager = manager(),
    Manager:join(Node).

%% @private
attempt_sync_join(Node) when is_atom(Node) ->
    ListenAddrs = rpc:call(Node, partisan_config, get, [listen_addrs]),
    attempt_sync_join(#{name => Node, listen_addrs => ListenAddrs});
attempt_sync_join(#{name := _Name} = Node) ->
    Manager = manager(),
    Manager:sync_join(Node).

%% @doc Attempt to leave the cluster.
leave(Node) ->
    Manager = manager(),
    Manager:leave(Node).

%% @doc Stop.
stop() ->
    stop("received stop request").

%% @doc Stop for a given reason.
stop(Reason) ->
    lager:notice("~p", [Reason]),
    init:stop().
