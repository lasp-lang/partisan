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
         leave/1,
         decode/1,
         stop/0,
         stop/1,
         members/0,
         manager/0,
         add_sup_callback/1,
         forward_message/3]).

-include("partisan.hrl").

%% @doc Return current peer service manager.
manager() ->
    partisan_config:get(partisan_peer_service_manager,
                        partisan_default_peer_service_manager).

%% @doc prepare node to join a cluster
join(Node) ->
    join(Node, true).

%% @doc Convert nodename to atom
join(NodeStr, Auto) when is_list(NodeStr) ->
    join(erlang:list_to_atom(lists:flatten(NodeStr)), Auto);
join(Node, Auto) when is_atom(Node) ->
    join(node(), Node, Auto);
join({_Name, _IPAddress, _Port} = Node, _Auto) ->
    attempt_join(Node).

%% @doc Initiate join. Nodes cannot join themselves.
join(Node, Node, _) ->
    {error, self_join};
join(_, Node, _Auto) ->
    attempt_join(Node).

%% @doc Return cluster members.
members() ->
    Manager = manager(),
    Manager:members().

%% @doc Add callback.
add_sup_callback(Function) ->
    partisan_peer_service_events:add_sup_callback(Function).

%% @doc Forward message to registered process on the remote side.
forward_message(Name, ServerRef, Message) ->
    Manager = manager(),
    Manager:forward_message(Name, ServerRef, Message).

%% @private
decode(State) ->
    Manager = manager(),
    [P || {P, _, _} <- Manager:decode(State)].

%% @private
attempt_join({_Name, _, _}=Node) ->
    Manager = manager(),
    Manager:join(Node).

%% @doc Attempt to leave the cluster.
leave(_Args) when is_list(_Args) ->
    Manager = manager(),
    Manager:leave().

%% @doc Stop.
stop() ->
    stop("received stop request").

%% @doc Stop for a given reason.
stop(Reason) ->
    lager:notice("~p", [Reason]),
    init:stop().
