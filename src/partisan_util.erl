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

-type connect_opts() :: #{prune => boolean()}.

-export([build_tree/3]).
-export([maps_append/3]).
-export([may_disconnect/1]).
-export([maybe_connect/1]).
-export([maybe_connect/2]).
-export([term_to_iolist/1]).

-compile({no_auto_import, [node/1]}).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Convert a list of elements into an N-ary tree. This conversion
%%      works by treating the list as an array-based tree where, for
%%      example in a binary 2-ary tree, a node at index i has children
%%      2i and 2i+1. The conversion also supports a "cycles" mode where
%%      the array is logically wrapped around to ensure leaf nodes also
%%      have children by giving them backedges to other elements.
%% @end
%% -----------------------------------------------------------------------------
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


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
maps_append(Key, Value, Map) ->
    maps:update_with(
        Key,
        fun
            ([]) ->
                [Value];
            (L) when is_list(L) ->
                [Value | L];
            (X) ->
                [Value, X]
        end,
        [Value],
        Map
    ).


%% -----------------------------------------------------------------------------
%% @doc Tries to create a new connection to a node if required. If succesfull
%% it store the new connection record in partisan_peer_connections.
%% @end
%% -----------------------------------------------------------------------------
-spec maybe_connect(Node :: node_spec()) -> ok.

maybe_connect(NodeSpec) ->
    maybe_connect(NodeSpec, #{prune => false}).


%% -----------------------------------------------------------------------------
%% @doc Create a new connection to a node storing a new connection record in
%% partisan_peer_connections.
%%
%% Returns the tuple `{ok, L :: [node_spec()]}' where list L is the list of all
%% invalid nodes specifications.
%%
%% Aa specification is invalid if there is another specification for the same
%% node for which we already have succesful connections. An invalid
%% specification will exist when a node has crashed (without leaving the
%% cluster) and later on returned with a different IP address i.e. a normal
%% situation on cloud orchestration platforms. In this case the membership set
%% ({@link partisan_membership_set}) will have two node specifications for the
%% same node (with differing values for the `listen_addrs' property).
%% @end
%% -----------------------------------------------------------------------------
-spec maybe_connect(Node :: node_spec(), connect_opts()) ->
    ok | {ok, StaleSpecs :: [node_spec()]}.

maybe_connect(#{listen_addrs := ListenAddrs} = NodeSpec, #{prune := true}) ->
    ToPrune = lists:foldl(
        fun(ListenAddr, Acc) ->
            maybe_connect(NodeSpec, ListenAddr, Acc)
        end,
        [],
        ListenAddrs
    ),
    {ok, ToPrune};

maybe_connect(#{listen_addrs := ListenAddrs} = NodeSpec, #{prune := false}) ->
    ok = lists:foreach(
        fun(ListenAddr) ->
            maybe_connect(NodeSpec, ListenAddr, ok)
        end,
        ListenAddrs
    ).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
may_disconnect(NodeName) ->
    partisan_peer_connections:erase(NodeName).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
term_to_iolist(Term) ->
    [131, term_to_iolist_(Term)].




%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
get_channels(NodeSpec) ->
    %% Always have a default, unlabeled channel.
    case maps:get(channels, NodeSpec, [?DEFAULT_CHANNEL]) of
        undefined ->
            [?DEFAULT_CHANNEL];
        [] ->
            [?DEFAULT_CHANNEL];
        Other ->
            lists:usort(Other ++ [?DEFAULT_CHANNEL])
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
maybe_connect(#{name := Node} = NodeSpec, ListenAddr, Acc) ->
    Parallelism = maps:get(parallelism, NodeSpec, ?PARALLELISM),
    Channels = get_channels(NodeSpec),

    %% We check count using Node and not NodeSpec cause it is much faster and
    %% we are only interested in knowing if we have at least one connection
    %% even if this was a stale NodeSpec.
    %% See the section Stale Specifications in partisan_membership_set.
    case partisan_peer_connections:connection_count(Node) of
        0 ->
            %% Found disconnected or not found
            ?LOG_DEBUG("Node ~p is not connected; initiating.", [NodeSpec]),

            case connect(NodeSpec, ListenAddr, ?DEFAULT_CHANNEL) of
                {ok, Pid} ->
                    ?LOG_DEBUG("Node ~p connected, pid: ~p", [NodeSpec, Pid]),
                    ok = partisan_peer_connections:store(
                        NodeSpec, Pid, ?DEFAULT_CHANNEL, ListenAddr
                    ),
                    Acc;
                ignore ->
                    ?LOG_DEBUG(#{
                        description => "Node failed connection.",
                        node_spec => NodeSpec,
                        reason => ignore
                    }),
                    Acc;
                {error, normal} ->
                    ?LOG_DEBUG(#{
                        description => "Node isn't online just yet.",
                        node_spec => NodeSpec
                    }),
                    Acc;
                {error, Reason} ->
                    ?LOG_DEBUG(#{
                        description => "Node failed connection.",
                        node_spec => NodeSpec,
                        reason => Reason
                    }),
                    Acc
            end;
        _ ->
            %% Found Node and connected
            maybe_connect(Channels, NodeSpec, ListenAddr, Parallelism, Acc)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc This function is called by maybe_connect/3 only when the Node in
%% NodeSpec has at least one active connection.
%% @end
%% -----------------------------------------------------------------------------
maybe_connect(_, _, _, undefined, Acc) ->
    %% No parallel connections
    Acc;

maybe_connect([Ch|T], NodeSpec, ListenAddr, Parallelism, Acc) ->
    %% There is at least one connection for Node.
    case partisan_peer_connections:connection_count(NodeSpec, Ch) of
        Count when Count < Parallelism ->
            ?LOG_DEBUG(
                "~p of ~p connected for channel ~p) Connecting node ~p.",
                [Count, Parallelism, Ch, NodeSpec]
            ),

            % Count might be == 0, but we try to connect anyway and we deal
            % with that case below.
            case connect(NodeSpec, ListenAddr, Ch) of
                {ok, Pid} ->
                    ?LOG_DEBUG(
                        "Node ~p connected, pid: ~p", [NodeSpec, Pid]
                    ),
                    ok = partisan_peer_connections:store(
                        NodeSpec, Pid, Ch, ListenAddr
                    ),
                    Acc;

                {error, Reason} when Count == 0 ->
                    %% The connection we have must have been created using a
                    %% different node_spec() for Node. Since we have a
                    %% connection we need to assume NodeSpec might be stale (a
                    %% previous version of the spec for example if a Node
                    %% crashed and come back again with a (i) different set of
                    %% ListenAddrs, or (ii) different values for channel and/or
                    %% parallellism).
                    %% maybe_stale/6 will try to determine is this is an
                    %% instance of case (i). At the moment we cannot deal with
                    %% instances of case (ii).
                    maybe_stale(NodeSpec, Ch, ListenAddr, Acc, Count, Reason);

                Error ->
                    %% We have some connections to this ListenAddr already
                    ?LOG_ERROR(#{
                        description => "Node failed to connect",
                        error => Error,
                        node_spec => NodeSpec,
                        listen_address => ListenAddr,
                        channel => Ch
                    }),
                    Acc
            end;

        Count when Count == Parallelism ->
            Acc
    end,

    maybe_connect(T, NodeSpec, ListenAddr, Parallelism, Acc);

maybe_connect([], _, _, _, Acc) ->
    Acc.


%% @private
maybe_stale(NodeSpec, Channel, ListenAddr, Acc, 0, Reason) ->
    Node = maps:get(name, NodeSpec),
    %% TODO check is we are already connected using connection_count
    %% (Node, Channel). If so, then this ListenAddr is not longer valid
    %% we need to accumulate this NodeSpec and return to the caller of
    %% maybe_connect

    %% Do we have a connection to the node for this channel on this addr?
    %% If so, check the connected spec with this one, cause this might be
    %% invalid.
    %% If not, then we cannot rule out the NodeSpec as valid.
    ListenAddrCount =
        partisan_peer_connections:connection_count(Node, Channel, ListenAddr),


    case ListenAddrCount > 0 of
        true ->
            ListenAddrs = maps:get(listen_addrs, NodeSpec),

            %% We got connections for Node, so we fetch the associated Info
            %% which contains the node_spec
            {ok, Info} = partisan_peer_connections:info(Node),

            %% We are trying to determine if NodeSpec might be a previous
            %% instance.
            case partisan_peer_connections:node_spec(Info) of
                Connected when Connected == NodeSpec ->
                    %% It is the same node_spec, so we are just having problems
                    %% openning more connections at the time being.
                    ?LOG_DEBUG(#{
                        description => "Node failed to connect",
                        reason => Reason,
                        node_spec => NodeSpec,
                        listen_address => ListenAddr,
                        channel => Channel
                    }),
                    Acc;
                #{listen_addrs := L} when L == ListenAddrs ->
                    %% The specs differ on channels or parallelism
                    ?LOG_DEBUG(#{
                        description => "Node failed to connect",
                        reason => Reason,
                        node_spec => NodeSpec,
                        listen_address => ListenAddr,
                        channel => Channel
                    }),
                    Acc;

                Connected ->
                    %% Listen addresses differ!
                    %% TODO use info and connections timestamps
                    case partisan_peer_connections:is_connected(NodeSpec) of
                        true ->
                            %% Ummmm....we got some connections, so we keep it
                            ?LOG_DEBUG(#{
                                description => "Node failed to connect",
                                reason => Reason,
                                node_spec => NodeSpec,
                                listen_address => ListenAddr,
                                channel => Channel
                            }),
                            Acc;
                        false ->
                            %% IP has changed
                            %% We add it to the invalid list
                            ?LOG_INFO(#{
                                description => "Flagging node specification to be pruned",
                                reason => duplicate,
                                node_spec => NodeSpec,
                                active => Connected
                            }),
                            [NodeSpec|Acc]
                    end
            end;

        false ->
            Acc
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec connect(Node :: node_spec(), listen_addr(), channel()) ->
    {ok, pid()} | ignore | {error, any()}.

connect(NodeSpec, Address, Channel) ->
    partisan_peer_service_client:start_link(NodeSpec, Address, Channel, self()).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
term_to_iolist_([]) ->
    106;

term_to_iolist_({}) ->
    [104, 0];

term_to_iolist_(T) when is_atom(T) ->
    L = atom_to_list(T),
    Len = length(L),
    %% TODO this function uses a DEPRECATED FORMAT!
    %% https://www.erlang.org/doc/apps/erts/erl_ext_dist.html#map_ext
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

term_to_iolist_(T) when is_map(T) ->
    Len = maps:size(T),
    [116, <<Len:32/integer-big>>, [[term_to_iolist_(K), term_to_iolist_(V)] || {K, V} <- maps:to_list(T)]];
term_to_iolist_(T) when is_reference(T) ->
    case partisan_config:get(ref_encoding, true) of
        false ->
            <<131, Rest/binary>> = term_to_binary(T),
            Rest;
        true ->
            <<131, Rest/binary>> = term_to_binary(
                partisan_remote_ref:from_term(T)
            ),
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




%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec pid(pid()) -> partisan_remote_ref:p().

pid(Pid) ->
    Node = erlang:node(Pid),

    case Node == partisan:node() of
        true ->
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
                    {partisan_remote_ref, Node, {encoded_name, Name}};
                false ->
                    {partisan_remote_ref, Node, {encoded_pid, pid_to_list(Pid)}}
            end;
        false ->
            %% This is even mmore super dangerous.
            case partisan_config:get(register_pid_for_encoding, false) of
                true ->
                    Unique = erlang:unique_integer([monotonic, positive]),

                    Name = "partisan_registered_name_" ++ integer_to_list(Unique),
                    [_, B, C] = string:split(pid_to_list(Pid), ".", all),
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
                    {partisan_remote_ref, Node, {encoded_name, Name}};
                false ->
                    [_, B, C] = string:split(pid_to_list(Pid), ".", all),
                    RewrittenProcessIdentifier = "<0." ++ B ++ "." ++ C,
                    {partisan_remote_ref, Node, {encoded_pid, RewrittenProcessIdentifier}}
            end
    end.




