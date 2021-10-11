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

-include("partisan_logger.hrl").
-include("partisan.hrl").

-export([build_tree/3,
         dispatch_pid/1,
         dispatch_pid/2,
         dispatch_pid/3,
         maybe_connect/2,
         may_disconnect/2,
         process_forward/2,
         term_to_iolist/1,
         gensym/1,
         pid/0,
         pid/1,
         ref/1,
         registered_name/1]).

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

may_disconnect(NodeName, Connections) ->
    partisan_peer_service_connections:erase(NodeName, Connections).

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
            ?LOG_DEBUG("Node ~p is not connected; initiating.", [Node]),
            case connect(Node, ListenAddr, ?DEFAULT_CHANNEL) of
                {ok, Pid} ->
                    ?LOG_DEBUG("Node ~p connected, pid: ~p", [Node, Pid]),
                    partisan_peer_service_connections:store(Node, {ListenAddr, ?DEFAULT_CHANNEL, Pid}, Connections0);
                Error ->
                    ?LOG_DEBUG("Node ~p failed connection: ~p.", [Node, Error]),
                    Connections0
            end;
        %% Found and connected.
        {ok, Entries} ->
            lists:foldl(fun(Channel, ChannelConnections) ->
                maybe_initiate_parallel_connections(ChannelConnections, Channel, Node, ListenAddr, Parallelism, Entries)
            end, Connections0, Channels);
        %% Not present; disconnected.
        {error, not_found} ->
            case connect(Node, ListenAddr, ?DEFAULT_CHANNEL) of
                {ok, Pid} ->
                    ?LOG_DEBUG("Node ~p connected, pid: ~p", [Node, Pid]),
                    partisan_peer_service_connections:store(Node, {ListenAddr, ?DEFAULT_CHANNEL, Pid}, Connections0);
                {error, normal} ->
                    ?LOG_DEBUG("Node ~p isn't online just yet.", [Node]),
                    Connections0;
                Error ->
                    ?LOG_DEBUG("Node ~p failed connection: ~p.", [Node, Error]),
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

%% @doc Return a pid to use for message dispatch.
dispatch_pid(Channel, Entries) ->
    dispatch_pid(undefined, Channel, Entries).

%% @doc Return a pid to use for message dispatch for a given channel.
dispatch_pid(PartitionKey, Channel, Entries) ->
    UndefinedEntries = lists:filter(fun({_, C, _}) ->
        case C of
            undefined ->
                true;
            _ ->
                false
        end
    end, Entries),

    DispatchEntries = case Channel of
        undefined ->
            UndefinedEntries;
        _ ->
            %% Entries for channel.
            ChannelEntries = lists:filter(fun({_, C, _}) ->
                case C of
                    {monotonic, Channel} ->
                        true;
                    Channel ->
                        true;
                    _ ->
                        false
                end
            end, Entries),

            %% Fall back to unlabeled channels.
            case ChannelEntries of
                [] ->
                    UndefinedEntries;
                _ ->
                    ChannelEntries
            end
    end,

    %% Get the number of elements in the list.
    NumEntries = length(DispatchEntries),

    %% Depending on whether or not a hash key has been provided, use it for routing.
    EntriesIndex = case PartitionKey of
        undefined ->
            rand:uniform(NumEntries);
        PartitionKey when is_integer(PartitionKey) ->
            PartitionKey rem NumEntries + 1
    end,

    %% Select that entry from the list.
    {_ListenAddr, _Channel, Pid} = lists:nth(EntriesIndex, DispatchEntries),

    %% Return pid of connection process.
    Pid.

%% @private
maybe_initiate_parallel_connections(Connections0, Channel, Node, ListenAddr, Parallelism, Entries) ->
    FilteredEntries = lists:filter(fun({A, C, _}) ->
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
                    end, Entries),

    Len = length(FilteredEntries),

    case Len < Parallelism andalso Parallelism =/= undefined of
        true ->
            ?LOG_DEBUG(
                "~p of ~p connected for channel ~p) Connecting node ~p.",
                [Len, Parallelism, Channel, Node]
            ),

            case connect(Node, ListenAddr, Channel) of
                {ok, Pid} ->
                    ?LOG_DEBUG(
                        "Node ~p connected, pid: ~p", [Node, Pid]
                    ),
                    partisan_peer_service_connections:store(
                        Node, {ListenAddr, Channel, Pid}, Connections0
                    );
                Error ->
                    ?LOG_ERROR(#{
                        description => "Node failed connect",
                        error => Error,
                        connections => Connections0
                    })
            end;
        false ->
            Connections0
    end.

term_to_iolist(Term) ->
    [131, term_to_iolist_(Term)].

term_to_iolist_([]) ->
    106;
term_to_iolist_({}) ->
    [104, 0];
term_to_iolist_(T) when is_atom(T) ->
    L = atom_to_list(T),
    Len = length(L),
    %% TODO utf-8 atoms
    case Len > 256 of
        false ->
            [115, Len, L];
        true->
            [100, <<Len:16/integer-big>>, L]
    end;
term_to_iolist_(T) when is_binary(T) ->
    Len = byte_size(T),
    [109, <<Len:32/integer-big>>, T];
term_to_iolist_(T) when is_tuple(T) ->
    Len = tuple_size(T),
    case Len > 255 of
        false ->
            [104, Len, [term_to_iolist_(E) || E <- tuple_to_list(T)]];
        true ->
            [104, <<Len:32/integer-big>>, [term_to_iolist_(E) || E <- tuple_to_list(T)]]
    end;
term_to_iolist_(T) when is_list(T) ->
    %% TODO improper lists
    Len = length(T),
    case Len < 64436 andalso lists:all(fun(E) when is_integer(E), E >= 0, E < 256 ->
                                                true;
                                            (_) ->
                                                 false
                                        end, T) of
        true ->
            [107, <<Len:16/integer-big>>, T];
        false ->
            [108, <<Len:32/integer-big>>, [[term_to_iolist_(E) || E <- T]], 106]
    end;
term_to_iolist_(T) when is_reference(T) ->
    case partisan_config:get(ref_encoding, true) of
        false ->
            <<131, Rest/binary>> = term_to_binary(T),
            Rest;
        true ->
            <<131, Rest/binary>> = term_to_binary(ref(T)),
            Rest
    end;
term_to_iolist_(T) when is_pid(T) ->
    case partisan_config:get(pid_encoding, true) of
        false ->
            <<131, Rest/binary>> = term_to_binary(T),
            Rest;
        true ->
            <<131, Rest/binary>> = term_to_binary(pid(T)),
            Rest
    end;
term_to_iolist_(T) ->
    %% fallback clause
    <<131, Rest/binary>> = term_to_binary(T),
    Rest.

gensym(Name) when is_atom(Name) ->
    {partisan_registered_name_reference, atom_to_list(Name)};
gensym(Pid) when is_pid(Pid) ->
    {partisan_process_reference, pid_to_list(Pid)};
gensym(Ref) when is_reference(Ref) ->
    % {partisan_encoded_reference, binary_to_list(term_to_binary(Ref))}.
    {partisan_encoded_reference, 1}.

ref(Ref) ->
    GenSym = gensym(Ref),
    Node = partisan_peer_service_manager:mynode(),
    {partisan_remote_reference, Node, GenSym}.

pid() ->
    pid(self()).

pid(Pid) ->
    Node = node(Pid),

    case partisan_peer_service_manager:mynode() of
        Node ->
            %% This is super dangerous.
            case partisan_config:get(register_pid_for_encoding, false) of
                true ->
                    Unique = erlang:unique_integer([monotonic, positive]),

                    Name = case process_info(Pid, registered_name) of
                        {registered_name, OldName} ->
                            ?LOG_DEBUG("unregistering pid: ~p with name: ~p", [Pid, OldName]),

                            %% TODO: Race condition on unregister/register.
                            unregister(OldName),
                            atom_to_list(OldName);
                        [] ->
                            "partisan_registered_name_" ++ integer_to_list(Unique)
                    end,

                    ?LOG_DEBUG("registering pid: ~p as name: ~p at node: ~p", [Pid, Name, Node]),
                    true = erlang:register(list_to_atom(Name), Pid),
                    {partisan_remote_reference, Node, {partisan_registered_name_reference, Name}};
                false ->
                    {partisan_remote_reference, Node, {partisan_process_reference, pid_to_list(Pid)}}
            end;
        _ ->
            %% This is even mmore super dangerous.
            case partisan_config:get(register_pid_for_encoding, false) of
                true ->
                    Unique = erlang:unique_integer([monotonic, positive]),

                    Name = "partisan_registered_name_" ++ integer_to_list(Unique),
                    [_, B, C] = split(pid_to_list(Pid), "."),
                    RewrittenProcessIdentifier = "<0." ++ B ++ "." ++ C,

                    RegisterFun = fun() ->
                        RewrittenPid = list_to_pid(RewrittenProcessIdentifier),

                        NewName = case process_info(RewrittenPid, registered_name) of
                            {registered_name, OldName} ->
                                ?LOG_DEBUG("unregistering pid: ~p with name: ~p", [Pid, OldName]),

                                %% TODO: Race condition on unregister/register.
                                unregister(OldName),
                                atom_to_list(OldName);
                            [] ->
                                Name
                        end,

                        erlang:register(list_to_atom(NewName), RewrittenPid)
                    end,
                    ?LOG_DEBUG("registering pid: ~p as name: ~p at node: ~p", [Pid, Name, Node]),
                    %% TODO: Race here unless we wait.
                    _ = rpc:call(Node, erlang, spawn, [RegisterFun]),
                    {partisan_remote_reference, Node, {partisan_registered_name_reference, Name}};
                false ->
                    [_, B, C] = split(pid_to_list(Pid), "."),
                    RewrittenProcessIdentifier = "<0." ++ B ++ "." ++ C,
                    {partisan_remote_reference, Node, {partisan_process_reference, RewrittenProcessIdentifier}}
            end
    end.

registered_name(Name) ->
    GenSym = gensym(Name),
    Node = partisan_peer_service_manager:mynode(),
    {partisan_remote_reference, Node, GenSym}.

process_forward(ServerRef, Message) ->
    ?LOG_DEBUG(
        "node ~p recieved message ~p for ~p", [node(), Message, ServerRef]
    ),

    Node = partisan_peer_service_manager:mynode(),

    try
        case ServerRef of
            {partisan_remote_reference, _, {partisan_registered_name_reference, RegisteredName}} ->
                Name = list_to_atom(RegisteredName),
                Name ! Message;
            {partisan_remote_reference, OtherNode, {partisan_process_reference, ProcessIdentifier}} ->

                case split(ProcessIdentifier, ".") of
                    ["<0",_B,_C] ->
                        Pid = list_to_pid(ProcessIdentifier),

                        ?LOG_TRACE("pid reference is: ~p", [Pid]),
                        ?LOG_TRACE_IF(
                            is_process_alive(Pid),
                            "Process ~p is NOT ALIVE for message: ~p", [ServerRef, Message]
                        ),

                        Pid ! Message;
                    [_,B,C] ->
                        %% Remote written pid from Distributed Erlang.
                        case OtherNode =:= Node of
                            true ->
                                RewrittenProcessIdentifier = "<0." ++ B ++ "." ++ C,
                                ?LOG_DEBUG(
                                    "rewritten process reference is: ~p", [RewrittenProcessIdentifier]
                                ),
                                Pid = list_to_pid(RewrittenProcessIdentifier),
                                ?LOG_DEBUG("pid reference is: ~p", [Pid]),
                                Pid ! Message;
                            false ->
                                ?LOG_DEBUG(
                                    "unknown destination, dropping message for process reference: ~p, other_node: ~p, node: ~p, message: ~p",
                                    [ProcessIdentifier, OtherNode, Node, Message]
                                ),
                                ok
                        end;
                    _ ->
                        ?LOG_DEBUG("couldn't deserialize unknown process reference: ~p", [ProcessIdentifier]),
                        ok
                end;
            {partisan_registered_name_reference, RegisteredName} ->
                Pid = list_to_atom(RegisteredName),
                Pid ! Message;
            {partisan_process_reference, ProcessIdentifier} ->
                Pid = list_to_pid(ProcessIdentifier),
                Pid ! Message;
            {global, Name} ->
                Pid = global:whereis_name(Name),
                Pid ! Message;
            {via, Module, Name} ->
                Pid =  Module:whereis_name(Name),
                Pid ! Message;
            _ ->
                ServerRef ! Message,

                Trace =
                    (is_pid(ServerRef) andalso not is_process_alive(ServerRef))
                        orelse whereis(ServerRef) == undefined
                        orelse not is_process_alive(whereis(ServerRef)),

                ?LOG_TRACE_IF(
                    Trace, "Process ~p is NOT ALIVE.", [ServerRef]
                ),

                ok
        end
    catch
        _:Error ->
            ?LOG_DEBUG("Error forwarding message ~p to process ~p: ~p", [Message, ServerRef, Error])
    end.

split(Subject0, Pattern0) ->
    Subject = list_to_binary(Subject0),
    Pattern = list_to_binary(Pattern0),
    Results0 = binary:split(Subject, Pattern, [global]),
    lists:map(fun(X) -> binary_to_list(X) end, Results0).
