%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(partisan_util).

-include("partisan.hrl").

-export([build_tree/3,
         establish_connections/2,
         maybe_connect/2]).

%% @doc Convert a list of elements into an N-ary tree. This conversion
%%      works by treating the list as an array-based tree where, for
%%      example in a binary 2-ary tree, a node at index i has children
%%      2i and 2i+1. The conversion also supports a "cycles" mode where
%%      the array is logically wrapped around to ensure leaf nodes also
%%      have children by giving them backedges to other elements.

-spec build_tree(N :: integer(), Nodes :: [term()], Opts :: [term()])
                -> orddict:orddict().
build_tree(N, Nodes, Opts) ->
    case lists:member(cycles, Opts) of
        true ->
            Expand = lists:flatten(lists:duplicate(N+1, Nodes));
        false ->
            Expand = Nodes
    end,
    {Tree, _} =
        lists:foldl(fun(Elm, {Result, Worklist}) ->
                            Len = erlang:min(N, length(Worklist)),
                            {Children, Rest} = lists:split(Len, Worklist),
                            NewResult = [{Elm, Children} | Result],
                            {NewResult, Rest}
                    end, {[], tl(Expand)}, Nodes),
    orddict:from_list(Tree).

%% @doc Reconnect disconnected members.
%%      Members waiting to join are already in the connections dictionary.
-spec establish_connections([node_spec()], connections()) -> connections().
establish_connections(Membership, Connections) ->
    lists:foldl(
        fun(Peer, AccIn) ->
            {_Result, AccOut} = maybe_connect(Peer, AccIn),
            AccOut
        end,
        Connections,
        Membership
    ).

%% @doc Function should enforce the invariant that all cluster
%%      members are keys in the dict pointing to undefined if they
%%      are disconnected or a socket pid if they are connected.
%%
%%      - In the end, the node name will be in the dictionary
%%      (even if undefined).
-spec maybe_connect(node_spec(), connections()) -> {ok | error(), connections()}.
maybe_connect({Name, _, _} = Node, Connections0) ->
    ShouldConnect0 = case dict:find(Name, Connections0) of
        %% Found in dict, and disconnected.
        {ok, undefined} ->
            lager:info("Node ~p is not connected; initiating.", [Node]),
            true;
        %% Found in dict and connected.
        {ok, _Pid} ->
            lager:info("Node ~p is already connect.", [Node]),
            false;
        %% Not present; disconnected.
        error ->
            lager:info("Node ~p never was connected; initiating.", [Node]),
            true
    end,

    ShouldConnect = ShouldConnect0 andalso Name /= node(),
    lager:info("SC0 ~p, SC ~p, Name ~p, node() ~p\n\n", [ShouldConnect0, ShouldConnect, Name, node()]),


    case ShouldConnect of
        true ->
            case connect(Node) of
                {ok, Pid} ->
                    lager:info("Node ~p connected.", [Node]),
                    Result = ok,
                    Connections1 = dict:store(Name,
                                              Pid,
                                              Connections0),
                    {Result, Connections1};
                _ ->
                    lager:info("Node ~p failed connection.", [Node]),
                    Result = {error, undefined},
                    Connections1 = dict:store(Name,
                                              undefined,
                                              Connections0),
                    {Result, Connections1}
            end;
        false ->
            {ok, Connections0}
    end.

%% @private
connect(Node) ->
    Self = self(),
    partisan_peer_service_client:start_link(Node, Self).
