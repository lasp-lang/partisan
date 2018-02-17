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

-export([new/0,
         find/2,
         store/3,
         prune/2,
         erase/2,
         foreach/2]).

-include("partisan.hrl").

-type t() :: dict:dict(node_spec(), list(pid())).
-export_type([t/0]).

-type entries() :: [entry()].
-type entry() :: {listen_addr(), channel(), pid()}.

%% @doc Creates a new dictionary of connections.
-spec new() -> t().
new() ->
    dict:new().

%% @doc Finds connection pids in dictionary either by name or node spec.
-spec find(Node :: atom() | node_spec(),
           Connections :: t()) -> {ok, entries()} | {error, not_found}.
find(Name, Connections) when is_atom(Name) ->
    find_first_name(Name, dict:to_list(Connections));
find(Node, Connections) ->
    case dict:find(Node, Connections) of
        error ->
            {error, not_found};
        {ok, Entries} ->
            {ok, Entries}
    end.

%% @doc Store a connection pid
-spec store(Node :: node_spec(),
            Entry :: entry(),
            Connections :: t()) -> t().
store(Node, {_ListenAddr, _Channel, _Pids} = Entry, Connections) ->
    case find(Node, Connections) of
        {error, not_found} ->
            dict:store(Node, [Entry], Connections);
        _ ->
            dict:fold(fun(Node0, Entries, ConnectionsIn) when Node0 =:= Node ->
                              dict:store(Node, Entries ++ [Entry], ConnectionsIn);
                         (_Node, _Entries, ConnectionsIn) ->
                              ConnectionsIn
                      end, Connections, Connections)
    end.

%% @doc Prune all occurrences of a connection pid
%%      returns the node where the pruned pid was found
-spec prune(pid() | node_spec(),
            Connections :: t()) -> {node_spec(), t()}.
prune(#{name := _Name} = Node, Connections) ->
    {Node, dict:store(Node, [], Connections)};
prune(Pid, Connections) when is_pid(Pid) ->
    dict:fold(fun(Node, Entries, {AccNode, ConnectionsIn}) ->
                    case lists:keymember(Pid, 3, Entries) of
                        true ->
                            {Node,
                             dict:store(Node, lists:keydelete(Pid, 3, Entries), ConnectionsIn)};
                        false ->
                            {AccNode, ConnectionsIn}
                    end
              end, {undefined, Connections}, Connections).

erase(NodeName, Connections) ->
    dict:fold(fun(#{name := Name} = Node, {_ListenAddr, _Channel, Pid} = Entry, ConnectionsAcc) ->
                      case Name of
                          NodeName ->
                              try
                                  gen_server:stop(Pid, normal, infinity)
                              catch
                                  _:_ ->
                                      ok
                              end,
                              ConnectionsAcc;
                          _ ->
                              dict:store(Node, Entry, ConnectionsAcc)
                      end
              end, dict:new(), Connections).

%% @doc Apply a function to all connection entries
-spec foreach(Fun :: fun((node_spec(), list(pid())) -> ok),
              Connections :: t()) -> ok.
foreach(Fun, Connections) ->
    _ = dict:map(Fun, Connections),
    ok.

%% @private
-spec find_first_name(Name :: atom(),
                      ConnectionsList :: [{node_spec(), entries()}]) ->
            {ok, entries()} | {error, not_found}.
find_first_name(_Name, []) -> {error, not_found};
find_first_name(Name, [{#{name := Name}, Entries}|_]) -> {ok, Entries};
find_first_name(Name, [_|Rest]) -> find_first_name(Name, Rest).

%%
%% Tests
%%
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

node1() ->
    #{name => node1, listen_addrs => [node1_listen_addr()]}.

node1_listen_addr() ->
    #{ip => {127, 0, 0, 1}, port => 80}.

node2() ->
    #{name => node2, listen_addrs => [node2_listen_addr()]}.

node2_listen_addr() ->
    #{ip => {127, 0, 0, 1}, port => 81}.

node1_bind() ->
    {node1_listen_addr(), undefined, self()}.

node2_bind() ->
    {node2_listen_addr(), undefined, self()}.

no_connections_test() ->
    Connections0 = new(),
    ?assertEqual({error, not_found}, find(node1, Connections0)).

one_connection_test() ->
    Connections0 = new(),
    Connections1 = store(node1(), node1_bind(), Connections0),
    ?assertEqual({ok, [node1_bind()]}, find(node1(), Connections1)),
    Connections2 = store(node2(), node2_bind(), Connections1),
    ?assertEqual({ok, [node2_bind()]}, find(node2, Connections2)).

several_connections_test() ->
    Connections0 = new(),
    Connections1 = store(node1(), node1_bind(), Connections0),
    Connections2 = store(node1(), node1_bind(), Connections1),
    ?assertEqual({ok, [node1_bind(), node1_bind()]}, find(node1(), Connections2)),
    ?assertEqual({ok, [node1_bind(), node1_bind()]}, find(node1, Connections2)).

prune_connections_test() ->
    Connections0 = new(),
    Connections1 = store(node1(), node1_bind(), Connections0),
    Connections2 = store(node1(), node1_bind(), Connections1),
    ?assertEqual({ok, [node1_bind(), node1_bind()]}, find(node1(), Connections2)),
    {#{name := node1, listen_addrs := [#{ip := {127, 0, 0, 1}, port := 80}]},
     Connections3} = prune(self(), Connections2),
    ?assertEqual({ok, [node1_bind()]}, find(node1(), Connections3)),
    {#{name := node1, listen_addrs := [#{ip := {127, 0, 0, 1}, port := 80}]},
     Connections4} = prune(self(), Connections3),
    ?assertEqual({ok, []}, find(node1(), Connections4)),
    Connections5 = store(node1(), node1_bind(), Connections4),
    Connections6 = store(node1(), node1_bind(), Connections5),
    {#{name := node1, listen_addrs := [#{ip := {127, 0, 0, 1}, port := 80}]},
     Connections7} = prune(self(), Connections6),
    ?assertEqual({ok, [node1_bind()]}, find(node1, Connections7)),
    {#{name := node1, listen_addrs := [#{ip := {127, 0, 0, 1}, port := 80}]},
     Connections8} = prune(self(), Connections7),
    ?assertEqual({ok, []}, find(node1, Connections8)).

add_remove_add_connection_test() ->
    Connections0 = new(),
    Connections1 = store(node1(), node1_bind(), Connections0),
    ?assertEqual({ok, [node1_bind()]}, find(node1, Connections1)),
    {#{name := node1, listen_addrs := [#{ip := {127, 0, 0, 1}, port := 80}]},
     Connections2} = prune(self(), Connections1),
    ?assertEqual({ok, []}, find(node1, Connections2)),
    Connections3 = store(node1(), node1_bind(), Connections2),
    ?assertEqual({ok, [node1_bind()]}, find(node1, Connections3)),
    {#{name := node1, listen_addrs := [#{ip := {127, 0, 0, 1}, port := 80}]},
     Connections4} = prune(node1(), Connections3),
    ?assertEqual({ok, []}, find(node1, Connections4)).

-endif.
