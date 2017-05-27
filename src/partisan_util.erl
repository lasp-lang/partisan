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
         maybe_connect/2, maybe_connect/3]).

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

%% @doc Create a new connection to a node and return a new dictionary
%%      with the associated connect pid
%%
%%      Function should enforce the invariant that all cluster members are
%%      keys in the dict pointing to empty list if they are disconnected or a
%%      socket pid if they are connected.
%%
-spec maybe_connect(Node :: node_spec(),
                    Connections :: partisan_peer_service_connections:t()) ->
            partisan_peer_service_connections:t().
maybe_connect(Node, Connections0) ->
    maybe_connect(Node, Connections0, dict:new()).

-spec maybe_connect(Node :: node_spec(),
                    Connections :: partisan_peer_service_connections:t(),
                    MemberParallelism :: dict:dict()) ->
            partisan_peer_service_connections:t().
maybe_connect(Node, Connections, MemberParallelism) ->
    %% Compute desired parallelism.
    Parallelism  = case dict:find(Node, MemberParallelism) of
        {ok, V} ->
            %% We learned about a node via an explicit join.
            V;
        error ->
            %% We learned about a node through a state merge; assume the
            %% default unless later specified.
            partisan_config:get(parallelism, ?PARALLELISM)
    end,

    %% Initiate connections.
    case partisan_peer_service_connections:find(Node, Connections) of
        %% Found disconnected.
        {ok, []} ->
            lager:info("Node ~p is not connected; initiating.", [Node]),
            case connect(Node) of
                {ok, Pid} ->
                    lager:info("Node ~p connected.", [Node]),
                    partisan_peer_service_connections:store(Node, Pid, Connections);
                Error ->
                    lager:info("Node ~p failed connection: ~p.", [Node, Error]),
                    partisan_peer_service_connections:store(Node, undefined, Connections)
            end;
        %% Found and connected.
        {ok, Pids} ->
            case length(Pids) < Parallelism andalso Parallelism =/= undefined of
                true ->
                    lager:info("(~p of ~p) Connecting node ~p.",
                               [length(Pids), Parallelism, Node]),

                    case connect(Node) of
                        {ok, Pid} ->
                            lager:info("Node connected with ~p", [Pid]),
                            partisan_peer_service_connections:store(Node, Pid, Connections);
                        Error ->
                            lager:info("Node failed connect with ~p", [Error]),
                            partisan_peer_service_connections:store(Node, undefined, Connections)
                    end;
                false ->
                    Connections
            end;
        %% Not present; disconnected.
        {error, not_found} ->
            case connect(Node) of
                {ok, Pid} ->
                    lager:info("Node ~p connected.", [Node]),
                    partisan_peer_service_connections:store(Node, Pid, Connections);
                {error, normal} ->
                    lager:info("Node ~p isn't online just yet.", [Node]),
                    partisan_peer_service_connections:store(Node, undefined, Connections);
                Error ->
                    lager:info("Node ~p failed connection: ~p.", [Node, Error]),
                    partisan_peer_service_connections:store(Node, undefined, Connections)
            end
    end.

%% @private
-spec connect(Node :: node_spec()) -> {ok, pid()} | ignore | {error, term()}.
connect(Node) ->
    Self = self(),
    partisan_peer_service_client:start_link(Node, Self).

