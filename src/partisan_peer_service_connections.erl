%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher Meiklejohn.  All Rights Reserved.
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

%% @doc API for managing peer service connections
-module(partisan_peer_service_connections).

-export([
         new/0,
         find/2,
         store/3,
         prune/2,
         foreach/2]).

-include("partisan.hrl").

-type t() :: dict:dict(node_spec(), list(pid())).
-export_type([t/0]).

%% @doc Creates a new dictionary of connections.
-spec new() -> t().
new() ->
    dict:new().

%% @doc Finds connection pids in dictionary either by name or node spec.
-spec find(Node :: atom() | node_spec(),
           Connections :: t()) -> {ok, list(pid())} | {error, not_found}.
find(Name, Connections) when is_atom(Name) ->
    find_first_name(Name, dict:to_list(Connections));
find(Node, Connections) ->
    case dict:find(Node, Connections) of
        error -> {error, not_found};
        {ok, Pids} -> {ok, Pids}
    end.

%% @doc Store a connection pid
-spec store(Node :: node_spec(),
            Pid :: undefined | pid(),
            Connections :: t()) -> t().
store(Node, Pid0, Connections) ->
    Pid = case Pid0 of
            undefined -> [];
            _ -> [Pid0]
          end,
    case find(Node, Connections) of
        {error, not_found} ->
            dict:store(Node, Pid, Connections);
        _ ->
            dict:fold(fun(Node0, Pids, ConnectionsIn) when Node0 =:= Node ->
                            dict:store(Node, Pids ++ Pid, ConnectionsIn);
                         (_Node, _Pids, ConnectionsIn) -> ConnectionsIn
                      end, Connections, Connections)
    end.

%% @doc Prune all occurrences of a connection pid
%%      returns the node where the pruned pid was found
-spec prune(pid() | node_spec(),
            Connections :: t()) -> {node_spec(), t()}.
prune({_, _, _} = Node, Connections) ->
    {Node, dict:store(Node, [], Connections)};
prune(Pid, Connections) ->
    dict:fold(fun(Node, Pids, {AccNode, ConnectionsIn}) ->
                    case lists:member(Pid, Pids) of
                        true ->
                            {Node,
                             dict:store(Node, lists:subtract(Pids, [Pid]), ConnectionsIn)};
                        false ->
                            {AccNode, ConnectionsIn}
                    end
              end, {undefined, Connections}, Connections).

%% @doc Apply a function to all connection entries
-spec foreach(Fun :: fun((node_spec(), list(pid())) -> ok),
              Connections :: t()) -> ok.
foreach(Fun, Connections) ->
    dict:map(Fun, Connections),
    ok.

%% @private
-spec find_first_name(Name :: atom(),
                      ConnectionsList :: [{node_spec(), list(pid())}]) ->
            {ok, list(pid())} | {error, not_found}.
find_first_name(_Name, []) -> {error, not_found};
find_first_name(Name, [{{Name, _, _}, Pids}|_]) -> {ok, Pids};
find_first_name(Name, [_|Rest]) -> find_first_name(Name, Rest).

%%
%% Tests
%%
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

no_connections_test() ->
    Connections0 = new(),
    ?assertEqual({error, not_found}, find(node1, Connections0)).

one_connection_test() ->
    Connections0 = new(),
    Connections1 = store({node1, {127, 0, 0, 1}, 80}, self(), Connections0),
    ?assertEqual({ok, [self()]}, find({node1, {127, 0, 0, 1}, 80}, Connections1)),
    Connections2 = store({node2, {127, 0, 0, 1}, 81}, self(), Connections1),
    ?assertEqual({ok, [self()]}, find(node2, Connections2)).

several_connections_test() ->
    Connections0 = new(),
    Connections1 = store({node1, {127, 0, 0, 1}, 80}, self(), Connections0),
    Connections2 = store({node1, {127, 0, 0, 1}, 80}, self(), Connections1),
    ?assertEqual({ok, [self(), self()]}, find({node1, {127, 0, 0, 1}, 80}, Connections2)),
    ?assertEqual({ok, [self(), self()]}, find(node1, Connections2)).

prune_connections_test() ->
    Connections0 = new(),
    Connections1 = store({node1, {127, 0, 0, 1}, 80}, self(), Connections0),
    Connections2 = store({node1, {127, 0, 0, 1}, 80}, self(), Connections1),
    ?assertEqual({ok, [self(), self()]}, find({node1, {127, 0, 0, 1}, 80}, Connections2)),
    {{node1, {127, 0, 0, 1}, 80}, Connections3} = prune(self(), Connections2),
    ?assertEqual({ok, [self()]}, find({node1, {127, 0, 0, 1}, 80}, Connections3)),
    {{node1, {127, 0, 0, 1}, 80}, Connections4} = prune(self(), Connections3),
    ?assertEqual({ok, []}, find({node1, {127, 0, 0, 1}, 80}, Connections4)),
    Connections5 = store({node1, {127, 0, 0, 1}, 80}, self(), Connections4),
    Connections6 = store({node1, {127, 0, 0, 1}, 80}, self(), Connections5),
    {{node1, {127, 0, 0, 1}, 80}, Connections7} = prune(self(), Connections6),
    ?assertEqual({ok, [self()]}, find(node1, Connections7)),
    {{node1, {127, 0, 0, 1}, 80}, Connections8} = prune(self(), Connections7),
    ?assertEqual({ok, []}, find(node1, Connections8)).

add_remove_add_connection_test() ->
    Connections0 = new(),
    Connections1 = store({node1, {127, 0, 0, 1}, 80}, self(), Connections0),
    ?assertEqual({ok, [self()]}, find(node1, Connections1)),
    {{node1, {127, 0, 0, 1}, 80}, Connections2} = prune(self(), Connections1),
    ?assertEqual({ok, []}, find(node1, Connections2)),
    Connections3 = store({node1, {127, 0, 0, 1}, 81}, self(), Connections2),
    ?assertEqual({ok, [self()]}, find(node1, Connections3)),
    {{node1, {127, 0, 0, 1}, 81}, Connections4} = prune({node1, {127, 0, 0, 1}, 81}, Connections3),
    ?assertEqual({ok, []}, find(node1, Connections4)).

-endif.
