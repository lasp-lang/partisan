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
         dispatch_pid/1,
         dispatch_pid/2,
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
    Expand = case lists:member(cycles, Opts) of
        true ->
            lists:flatten(lists:duplicate(N+1, Nodes));
        false ->
            Nodes
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
maybe_connect(#{name := _Name, listen_addrs := ListenAddrs} = Node, Connections0) ->
    FoldFun = fun(ListenAddr, Connections) ->
                      maybe_connect_listen_addr(Node, ListenAddr, Connections)
              end,
    lists:foldl(FoldFun, Connections0, ListenAddrs).

%% @private
maybe_connect_listen_addr(Node, ListenAddr, Connections0) ->
    Parallelism = maps:get(parallelism, Node, ?PARALLELISM),

    %% Always have a default, unlabeled channel.
    Channels = case maps:get(channels, Node, [?DEFAULT_CHANNEL]) of
        [] ->
            [?DEFAULT_CHANNEL];
        undefined ->
            [?DEFAULT_CHANNEL];
        Other ->
            lists:usort(Other ++ [?DEFAULT_CHANNEL])
    end,

    %% Initiate connections.
    Connections = case partisan_peer_service_connections:find(Node, Connections0) of
        %% Found disconnected.
        {ok, []} ->
            lager:debug("Node ~p is not connected; initiating.", [Node]),
            case connect(Node, ListenAddr, ?DEFAULT_CHANNEL) of
                {ok, Pid} ->
                    lager:debug("Node ~p connected, pid: ~p", [Node, Pid]),
                    partisan_peer_service_connections:store(Node, {ListenAddr, ?DEFAULT_CHANNEL, Pid}, Connections0);
                Error ->
                    lager:debug("Node ~p failed connection: ~p.", [Node, Error]),
                    Connections0
            end;
        %% Found and connected.
        {ok, Pids} ->
            lists:foldl(fun(Channel, ChannelConnections) ->
                maybe_initiate_parallel_connections(ChannelConnections, Channel, Node, ListenAddr, Parallelism, Pids)
            end, Connections0, Channels);
        %% Not present; disconnected.
        {error, not_found} ->
            case connect(Node, ListenAddr, ?DEFAULT_CHANNEL) of
                {ok, Pid} ->
                    lager:debug("Node ~p connected, pid: ~p", [Node, Pid]),
                    partisan_peer_service_connections:store(Node, {ListenAddr, ?DEFAULT_CHANNEL, Pid}, Connections0);
                {error, normal} ->
                    lager:debug("Node ~p isn't online just yet.", [Node]),
                    Connections0;
                Error ->
                    lager:debug("Node ~p failed connection: ~p.", [Node, Error]),
                    Connections0
            end
    end,

    %% Memoize connections.
    partisan_connection_cache:update(Connections),

    Connections.

%% @private
-spec connect(Node :: node_spec(), listen_addr(), channel()) -> {ok, pid()} | ignore | {error, term()}.
connect(Node, ListenAddr, Channel) ->
    Self = self(),
    partisan_peer_service_client:start_link(Node, ListenAddr, Channel, Self).

%% @doc Return a pid to use for message dispatch.
dispatch_pid(Entries) ->
    dispatch_pid(undefined, Entries).

%% @doc Return a pid to use for message dispatch for a given channel.
dispatch_pid(Channel, Entries) ->
    %% Entries for channel.
    ChannelEntries = lists:filter(fun({_, C, _}) ->
        case C of
            Channel ->
                true;
            _ ->
                false
        end
    end, Entries),

    %% Fall back to unlabeled channels.
    DispatchEntries = case length(ChannelEntries) of
        0 ->
            Entries;
        _ ->
            ChannelEntries
    end,

    %% Randomly select one.
    {_ListenAddr, _Channel, Pid} = lists:nth(rand:uniform(length(DispatchEntries)), DispatchEntries),

    Pid.

%% @private
maybe_initiate_parallel_connections(Connections0, Channel, Node, ListenAddr, Parallelism, Pids) ->
    FilteredPids = lists:filter(fun({A, C, _}) ->
                            case A of
                                ListenAddr ->
                                    case C of
                                        Channel ->
                                            true;
                                        _ ->
                                            false
                                    end;
                                _ ->
                                    false
                            end
                    end, Pids),
    case length(FilteredPids) < Parallelism andalso Parallelism =/= undefined of
        true ->
            lager:debug("(~p of ~p connected for channel ~p) Connecting node ~p.",
                        [length(FilteredPids), Parallelism, Channel, Node]),

            case connect(Node, ListenAddr, Channel) of
                {ok, Pid} ->
                    lager:debug("Node ~p connected, pid: ~p", [Node, Pid]),
                    partisan_peer_service_connections:store(Node, {ListenAddr, Channel, Pid}, Connections0);
                Error ->
                    lager:error("Node failed connect with ~p", [Error]),
                    Connections0
            end;
        false ->
            Connections0
    end.
