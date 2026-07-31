%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Christopher Meiklejohn. All Rights Reserved.
%% Copyright (c) 2022 Alejandro M. Ramallo. All Rights Reserved.
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

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_peer_connections).

-include("partisan.hrl").
-include("partisan_util.hrl").
-include("partisan_logger.hrl").

-define(SELECT(Arg),
    try
        ets:select(?MODULE, Arg)
    catch
        error:badarg ->
            []
    end
).

-define(SELECT_COUNT(Arg),
    try
        ets:select_count(?MODULE, Arg)
    catch
        error:badarg ->
            0
    end
).

-define(NOT_GROUND_ERROR(Args),
    erlang:error(
        badarg,
        Args,
        [
            {error_info, #{
                cause => #{
                    1 =>
                        "term is not ground (term is a pattern containing "
                        "one or more wildcards or variables)."
                }
            }}
        ]
    )
).

%% We store two records (partisan_peer_info and partisan_peer_connection) on the
%% same table, using the first field as a key on keypos 2.
%% Since both keys differ on type (node() and pid() respectively) this does not
%% affect lookup performance.
%% maybe_var(T) type is used for match patterns in match specifications.
%% Invariants:
%% - For every Node there might be 0 or 1 partisan_peer_info object.
%% - For every partisan_peer_info there might be 0 or more
%% partisan_peer_connection objects.
%% - For every partisan_peer_connection for Node there is 1 partisan_peer_info
%% object
-record(partisan_peer_info, {
    node :: maybe_var(node()),
    node_spec :: maybe_var(partisan:node_spec()),
    connection_count = 0 :: maybe_var(non_neg_integer()),
    timestamp :: maybe_var(non_neg_integer())
}).

-record(partisan_peer_connection, {
    pid :: maybe_var(pid()),
    node :: maybe_var(node()),
    channel :: maybe_var(partisan:channel()),
    listen_addr ::
        maybe_var(partisan:listen_addr())
        | listen_addr_spec(),
    timestamp :: maybe_var(non_neg_integer())
}).

-type maybe_var(T) :: T | var().
-type var() :: '_' | '$1' | '$2' | '$3'.
-type info() :: #partisan_peer_info{}.
-type connection() :: #partisan_peer_connection{}.
-type connections() :: [connection()].
-type listen_addr_spec() :: #{ip := var(), port := var()}.

-export_type([connection/0]).
-export_type([info/0]).
-export_type([connections/0]).

-export([channel/1]).
-export([count/0]).
-export([count/1]).
-export([count/2]).
-export([count/3]).
-export([connections/0]).
-export([connections/1]).
-export([connections/2]).
-export([connections/3]).
-export([cast_encoded/3]).
-export([dispatch/1]).
-export([dispatch_many/4]).
-export([dispatch_pid/1]).
-export([dispatch_pid/2]).
-export([dispatch_pid/3]).
-export([erase/1]).
-export([fold/2]).
-export([foreach/1]).
-export([foreach/2]).
-export([info/1]).
-export([init/0]).
-export([is_connected/1]).
-export([is_connected/2]).
-export([is_fully_connected/1]).
-export([kill_all/0]).
-export([listen_addr/1]).
-export([node/1]).
-export([node_spec/1]).
-export([node_specs/0]).
-export([nodes/0]).
-export([pid/1]).
-export([processes/1]).
-export([processes/2]).
-export([prune/1]).
-export([prune/2]).
-export([store/4]).
-export([timestamp/1]).

-compile({no_auto_import, [nodes/1]}).
-compile({no_auto_import, [erase/1]}).
-compile({no_auto_import, [pid/1]}).

%% =============================================================================
%% API
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc Creates a new connections table. The owner of the table is the calling
%% process and the table is protected so only the owner can write to it.
%% @end
%% -----------------------------------------------------------------------------
-spec init() -> ok.

init() ->
    case ets:info(?MODULE, name) of
        ?MODULE ->
            %% Already initialised
            ok;
        undefined ->
            %% Make sure both record keys are in the same key position,
            %% otherwise crash.
            Pos = #partisan_peer_info.node,
            Pos = #partisan_peer_connection.pid,

            ?MODULE = ets:new(?MODULE, [
                named_table,
                protected,
                ordered_set,
                {keypos, Pos},
                %% We enable both as we have concurrent reads and writes,
                %% although writes will only happen when a connection is
                %% started/stopped for a peer node.
                {read_concurrency, true},
                {write_concurrency, true},
                %% This is redundant for ordered_set with write_concurrency
                %% enabled, but we like it to be explicit
                {decentralized_counters, true}
            ]),

            ok
    end.

%% -----------------------------------------------------------------------------
%% @doc Returns a list of all nodes connected to this node through normal
%% connections (that is, hidden nodes are not listed).
%% @end
%% -----------------------------------------------------------------------------
-spec nodes() -> [node()].

nodes() ->
    %% We project the first element (node) of each record where the
    %% third element (connection_count counter) is greater than zero.
    MatchHead = #partisan_peer_info{
        node = '$1',
        node_spec = '_',
        connection_count = '$2',
        timestamp = '_'
    },
    MS = [{MatchHead, [{'>', '$2', 0}], ['$1']}],
    ?SELECT(MS).

%% -----------------------------------------------------------------------------
%% @doc Returns a list of all nodes specifications connected to this node.
%% @end
%% -----------------------------------------------------------------------------
node_specs() ->
    %% We project the second element (node_spec) of each record where the
    %% third element (connection connection_count) is greater than zero.
    MatchHead = #partisan_peer_info{
        node = '_',
        node_spec = '$1',
        connection_count = '$2',
        timestamp = '_'
    },
    MS = [{MatchHead, [{'>', '$2', 0}], ['$1']}],
    ?SELECT(MS).

%% -----------------------------------------------------------------------------
%% @doc Returns true is this node is connected to `NodeOrName'.
%% If `Node' is this node, returns `true'.
%% @end
%% -----------------------------------------------------------------------------
-spec is_connected(NodeOrSpec :: partisan:node_spec() | node()) ->
    boolean().

is_connected(Node) when is_atom(Node) ->
    Node =:= partisan:node() orelse count(Node) > 0;
is_connected(#{name := _} = NodeSpec) ->
    is_connected(NodeSpec, '_').

%% -----------------------------------------------------------------------------
%% @doc Returns true is this node is connected to `NodeOrName'.
%% If `Node' is this node, returns `true'.
%% @end
%% -----------------------------------------------------------------------------
-spec is_connected(
    NodeOrSpec :: partisan:node_spec() | node(),
    Channels :: maybe_var(partisan:channel() | [partisan:channel()])
) ->
    boolean() | no_return().

is_connected(Node, Channels) when is_atom(Node) ->
    Node =:= partisan:node() orelse count(Node, Channels) > 0;
is_connected(#{name := Node} = Spec, Channels) ->
    Node =:= partisan:node() orelse count(Spec, Channels) > 0.

%% -----------------------------------------------------------------------------
%% @doc Returns true is this node has all the requested connections
%% (`parallelism' configuration parameter) for all the configured channels with
%% node `NodeOrSpec'.
%% @end
%% -----------------------------------------------------------------------------
-spec is_fully_connected(Peer :: partisan:node_spec() | node()) -> boolean().

is_fully_connected(Node) when is_atom(Node) ->
    case partisan:node() of
        Node ->
            %% We are fully connected with ourselves
            true;
        _ ->
            case info(Node) of
                {ok, #partisan_peer_info{
                    node_spec = Spec,
                    connection_count = Count
                }} ->
                    is_fully_connected(Spec, Count);
                error ->
                    false
            end
    end;
is_fully_connected(#{name := Node, channels := Channels} = NodeSpec) when
    is_atom(Node) andalso is_map(Channels)
->
    is_fully_connected(NodeSpec, count(Node));
is_fully_connected(#{name := Node}) ->
    is_fully_connected(Node).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec count() -> non_neg_integer().

count() ->
    %% Global static match spec
    Key = {?MODULE, count},
    MS =
        case persistent_term:get(Key, undefined) of
            undefined ->
                Value = match_spec('_', '_', '_', count),
                _ = persistent_term:put(Key, Value),
                Value;
            Value ->
                Value
        end,

    ?SELECT_COUNT(MS).

%% -----------------------------------------------------------------------------
%% @doc Returns the number of connections for node `Node'.
%%
%% When passed a `partisan:node_spec()' as `Arg' it is equivalent to calling
%% {@link count/2} with a wildcard as a second argument i.e. '_'.
%% However, when passed a `node()` as `Arg' is uses the more efficient `ets`
%% `lookup_element' operation.
%% @end
%% -----------------------------------------------------------------------------
-spec count(Arg :: partisan:node_spec() | node() | info()) ->
    non_neg_integer().

count(Arg) when is_atom(Arg) ->
    %% An optimisation that is faster than count(Arg, '_'),
    %% as connection_count/1 is more often called than connection_count/2.
    try
        Pos = #partisan_peer_info.connection_count,
        ets:lookup_element(?MODULE, Arg, Pos)
    catch
        error:badarg ->
            0
    end;
count(Arg) when is_map(Arg) ->
    count(Arg, '_');
count(#partisan_peer_info{connection_count = Val}) when
    is_integer(Val)
->
    Val;
count(#partisan_peer_info{} = T) ->
    ?NOT_GROUND_ERROR([T]).

%% -----------------------------------------------------------------------------
%% @doc Returns the nbr of connections for node `Node' and channel `Channel'.
%% @end
%% -----------------------------------------------------------------------------
-spec count(
    NodeOrSpec :: maybe_var(partisan:node_spec() | node()),
    Channels :: maybe_var(partisan:channel() | [partisan:channel()])
) ->
    non_neg_integer() | no_return().

count(Node, Channels) ->
    MS = match_spec(Node, Channels, '_', count),
    try
        ets:select_count(?MODULE, MS)
    catch
        error:badarg:Stacktrace ->
            case ets:info(id) == undefined of
                true ->
                    %% Tab doesn't exist
                    0;
                false ->
                    %% We have a bug in our match spec
                    error(error, badarg, Stacktrace)
            end
    end.

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec count(
    Node :: maybe_var(node() | partisan:node_spec()),
    Channels :: maybe_var(partisan:channel() | [partisan:channel()]),
    ListenAddr :: partisan:listen_addr()
) -> Count :: non_neg_integer().

count(Node, Channels, ListenAddr) ->
    MS = match_spec(Node, Channels, ListenAddr, count),
    ?SELECT_COUNT(MS).

%% -----------------------------------------------------------------------------
%% @doc Finds connection for a node.
%% @end
%% -----------------------------------------------------------------------------
-spec connections() -> connections().

connections() ->
    connections('_', '_').

%% -----------------------------------------------------------------------------
%% @doc Finds connection for a node.
%% @end
%% -----------------------------------------------------------------------------
-spec connections(NodeOrSpec :: atom() | partisan:node_spec()) -> connections().

connections(NodeOrSpec) ->
    connections(NodeOrSpec, '_').

%% -----------------------------------------------------------------------------
%% @doc Finds connection for a node and channel.
%% @end
%% -----------------------------------------------------------------------------
-spec connections(
    NodeOrSpec :: maybe_var(atom() | partisan:node_spec()),
    Channels :: maybe_var(partisan:channel() | [partisan:channel()])
) ->
    connections() | no_return().

connections(Node, Channels) ->
    MS = match_spec(Node, Channels, '_', select),
    ?SELECT(MS).

%% -----------------------------------------------------------------------------
%% @doc Finds connection for a node and channel.
%% @end
%% -----------------------------------------------------------------------------
-spec connections(
    NodeOrSpec :: maybe_var(atom() | partisan:node_spec()),
    Channels :: maybe_var(partisan:channel() | [partisan:channel()]),
    ListenAddr :: partisan:listen_addr()
) ->
    connections() | no_return().

connections(Node, Channels, ListenAddr) ->
    MS = match_spec(Node, Channels, ListenAddr, select),
    ?SELECT(MS).

%% -----------------------------------------------------------------------------
%% @doc Returns the pids for all the active connection for a node.
%% @end
%% -----------------------------------------------------------------------------
-spec processes(NodeOrSpec :: atom() | partisan:node_spec()) -> [pid()].

processes(NodeOrSpec) ->
    processes(NodeOrSpec, '_').

%% -----------------------------------------------------------------------------
%% @doc Returns the pids for all the active connection for a node and channel.
%% @end
%% -----------------------------------------------------------------------------
-spec processes(
    NodeOrSpec :: node() | partisan:node_spec(),
    Channel :: maybe_var(partisan:channel())
) -> [pid()].

processes(#{name := Node}, Channel) ->
    processes(Node, Channel);
processes(Node, Channel) when is_atom(Node), is_atom(Channel) ->
    MatchHead = #partisan_peer_connection{
        pid = '$1',
        node = Node,
        channel = Channel,
        listen_addr = '_',
        timestamp = '_'
    },
    MS = [{MatchHead, [], ['$1']}],
    ?SELECT(MS).

%% -----------------------------------------------------------------------------
%% @doc Returns a tuple `{ok, Value}', where `Value' is an instance of
%% `info()' associated with `Node', or `error' if no info is associated with
%% `Node'.
%% @end
%% -----------------------------------------------------------------------------
-spec info(NodeOrSpec :: partisan:node_spec() | node()) -> {ok, info()} | error.

info(Node) when is_atom(Node) ->
    %% An optimisation that is faster than is_connected(Node, '_'),
    %% as is_connected/1 is more often called than is_connected/2.
    try
        case ets:lookup(?MODULE, Node) of
            [#partisan_peer_info{} = I] ->
                {ok, I};
            [] ->
                error
        end
    catch
        error:badarg ->
            error
    end;
info(#{name := Node}) ->
    info(Node).

%% -----------------------------------------------------------------------------
%% @doc Returns the channel name of the connection
%% @end
%% -----------------------------------------------------------------------------
-spec channel(connection()) -> partisan:channel().

channel(#partisan_peer_connection{channel = Val}) when is_atom(Val) ->
    Val;
channel(#partisan_peer_connection{} = T) ->
    ?NOT_GROUND_ERROR([T]).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec pid(connection()) -> pid().

pid(#partisan_peer_connection{pid = Val}) when is_pid(Val) ->
    Val;
pid(#partisan_peer_connection{} = T) ->
    ?NOT_GROUND_ERROR([T]).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec listen_addr(connection()) -> partisan:listen_addr() | no_return().

listen_addr(
    #partisan_peer_connection{listen_addr = #{ip := IP, port := Port} = Val}
) when ?IS_IP(IP), is_integer(Port) ->
    Val;
listen_addr(#partisan_peer_connection{} = T) ->
    ?NOT_GROUND_ERROR([T]).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node(info() | connection()) -> node() | no_return().

node(#partisan_peer_info{node = Val}) when is_atom(Val) ->
    Val;
node(#partisan_peer_info{} = T) ->
    ?NOT_GROUND_ERROR([T]);
node(#partisan_peer_connection{node = Val}) when is_atom(Val) ->
    Val;
node(#partisan_peer_connection{} = T) ->
    ?NOT_GROUND_ERROR([T]).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec(info() | connection()) -> partisan:node_spec() | no_return().

node_spec(#partisan_peer_info{node_spec = Val}) when is_map(Val) ->
    Val;
node_spec(#partisan_peer_info{} = T) ->
    ?NOT_GROUND_ERROR([T]);
node_spec(#partisan_peer_connection{node = Val}) when is_atom(Val) ->
    case info(Val) of
        {ok, Info} ->
            node_spec(Info);
        error ->
            error(badarg)
    end;
node_spec(#partisan_peer_connection{} = T) ->
    ?NOT_GROUND_ERROR([T]).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec timestamp(info() | connection()) -> non_neg_integer() | no_return().

timestamp(#partisan_peer_info{timestamp = Val}) when is_integer(Val) ->
    Val;
timestamp(#partisan_peer_info{} = T) ->
    ?NOT_GROUND_ERROR([T]);
timestamp(#partisan_peer_connection{timestamp = Val}) when is_integer(Val) ->
    Val;
timestamp(#partisan_peer_connection{} = T) ->
    ?NOT_GROUND_ERROR([T]).

%% -----------------------------------------------------------------------------
%% @doc Store a connection
%% @end
%% -----------------------------------------------------------------------------
-spec store(
    Node :: partisan:node_spec(),
    Pid :: pid(),
    Channel :: partisan:channel(),
    LitenAddr :: partisan:listen_addr()
) -> ok | no_return().

store(
    #{name := Node} = Spec,
    Pid,
    Channel,
    #{ip := IP, port := Port} = ListenAddr
) when
    is_pid(Pid) andalso
        is_atom(Channel) andalso Channel =/= '_' andalso
        ?IS_IP(IP) andalso is_integer(Port) andalso Port >= 0
->
    %% We insert separately as we have N connections per node.
    Conn = #partisan_peer_connection{
        pid = Pid,
        node = Node,
        channel = Channel,
        listen_addr = ListenAddr,
        timestamp = erlang:system_time(nanosecond)
    },

    try ets:insert_new(?MODULE, Conn) of
        true ->
            incr_counter(Spec),
            ok = telemetry_connection_up(Node, Channel, ListenAddr);
        false ->
            {ok, Info} = info(Node),
            InfoSpec = node_spec(Info),
            Count = count(Node),

            case Count == 0 of
                true ->
                    ?LOG_DEBUG(#{
                        description =>
                            "A new connection was made using a node "
                            "specification instance that differs from the "
                            "existing specification for node. "
                            "Replacing the existing specification with "
                            "the new one as no existing connections exist.",
                        node_spec => InfoSpec,
                        connection => #{
                            pid => Pid,
                            node_spec => Spec
                        }
                    }),
                    ets:insert(?MODULE, Conn),
                    incr_counter(Spec),
                    ok = telemetry_connection_up(Node, Channel, ListenAddr);
                false ->
                    ?LOG_WARNING(#{
                        description =>
                            "A new connection was made using a node "
                            "specification instance that differs from the "
                            "existing specification for node. "
                            "Keeping the existing specification in the info "
                            "record as connections exist.",
                        node_spec => InfoSpec,
                        connection_count => Count,
                        connection => #{
                            pid => Pid,
                            node_spec => Spec
                        }
                    }),
                    ok
            end
    catch
        error:badarg ->
            error(notalive)
    end.

%% -----------------------------------------------------------------------------
%% @doc Prune all occurrences of a connection pid returns the node where the
%% pruned pid was found. Equivalent to `prune(Arg, undefined)'.
%% @end
%% -----------------------------------------------------------------------------
-spec prune(pid() | node() | partisan:node_spec()) ->
    {info(), connections()} | no_return().

prune(Arg) ->
    prune(Arg, undefined).

%% -----------------------------------------------------------------------------
%% @doc Same as `prune/1' but allows passing a `Reason' (e.g. the connection
%% process' exit reason) that is added to the metadata of the resulting
%% `[partisan, connection, down]' telemetry event.
%% @end
%% -----------------------------------------------------------------------------
-spec prune(pid() | node() | partisan:node_spec(), Reason :: term()) ->
    {info(), connections()} | no_return().

prune(Node, Reason) when is_atom(Node) ->
    MatchHead = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '_',
        listen_addr = '_',
        timestamp = '_'
    },

    %% Remove all connections
    Connections =
        %% There is no select_take in ets, so we select and then delete.
        try ets:select(?MODULE, [{MatchHead, [], ['$_']}]) of
            [] ->
                [];
            L ->
                _ = ets:select_delete(?MODULE, [{MatchHead, [], [true]}]),
                L
        catch
            error:badarg ->
                error(notalive)
        end,

    ok = telemetry_connection_down(Connections, Reason),

    %% We finally remove info and return it as part of the result
    case ets:take(?MODULE, Node) of
        [#partisan_peer_info{} = I] ->
            {I, Connections};
        [] ->
            error(badarg)
    end;
prune(Pid, Reason) when is_pid(Pid) ->
    %% Remove matching connection
    try ets:take(?MODULE, Pid) of
        [#partisan_peer_connection{node = Node}] = L ->
            %% We decrease the connection count
            ok = decr_counter(Node),
            ok = telemetry_connection_down(L, Reason),
            {ok, #partisan_peer_info{} = I} = info(Node),
            {I, L};
        [] ->
            error(badarg)
    catch
        error:badarg ->
            error(notalive)
    end;
prune(#{name := Node}, Reason) ->
    prune(Node, Reason).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec erase(pid() | node() | partisan:node_spec()) -> ok.

erase(Pid) when is_pid(Pid) ->
    _ = prune(Pid),
    ok;
erase(Node) when is_atom(Node) ->
    MatchHead = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '_',
        listen_addr = '_',
        timestamp = '_'
    },
    MS = [{MatchHead, [], [true]}],

    %% Remove all connections
    _ = catch ets:select_delete(?MODULE, MS),

    %% Remove info
    _ = catch ets:delete(?MODULE, Node),

    ok;
erase(#{name := Node}) ->
    erase(Node).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec kill_all() -> ok.

kill_all() ->
    Fun = fun(_NodeInfo, Connections) ->
        lists:foreach(
            fun(#partisan_peer_connection{} = C) ->
                Pid = pid(C),
                catch gen_server:stop(Pid, normal, infinity),
                ok
            end,
            Connections
        )
    end,
    ok = foreach(Fun).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec kill(node()) -> ok.

kill(Node) ->
    Fun = fun(_NodeInfo, Connections) ->
        lists:foreach(
            fun(#partisan_peer_connection{} = C) ->
                Pid = pid(C),
                catch gen_server:stop(Pid, normal, infinity),
                ok
            end,
            Connections
        )
    end,
    ok = foreach(Fun, Node).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec fold(
    Fun :: fun(
        (partisan:node_spec(), connections(), Acc1 :: any()) -> Acc2 :: any()
    ),
    AccIn :: any()
) -> AccOut :: any().

fold(Fun, Acc) ->
    MatchHead = #partisan_peer_info{
        node = '_',
        node_spec = '_',
        connection_count = '$1',
        timestamp = '_'
    },
    MS = [{MatchHead, [{'>', '$1', 0}], ['$_']}],

    case ets:select(?MODULE, MS) of
        [] ->
            ok;
        L ->
            %% We assume we have at most a few hundreds of connections.
            %% An optimisation will be to use batches (limit + continuations).
            _ = lists:foldl(
                fun(
                    #partisan_peer_info{node = Node, node_spec = Spec}, IAcc
                ) when
                    is_atom(Node), is_map(Spec)
                ->
                    Connections = connections(Node),
                    Fun(Spec, Connections, IAcc)
                end,
                Acc,
                L
            )
    end.

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec foreach(Fun :: fun((info(), connections()) -> ok)) -> ok.

foreach(Fun) ->
    MatchHead = #partisan_peer_info{
        node = '_',
        node_spec = '_',
        connection_count = '$1',
        timestamp = '_'
    },
    MS = [{MatchHead, [{'>', '$1', 0}], ['$_']}],

    case ets:select(?MODULE, MS) of
        [] ->
            ok;
        L ->
            %% We assume we have at most a few hundreds connections max.
            %% An optimisation will be to use batches (limit + continuations).
            %% E.g. a 100 node full-mesh cluster with 4 channels and
            %% parallelism of 1 will have 800 connections
            %% (400 outbound, 400 inbound).
            _ = lists:foreach(
                fun(#partisan_peer_info{node = Node} = Info) ->
                    Connections = connections(Node),
                    catch Fun(Info, Connections),
                    ok
                end,
                L
            )
    end.

foreach(Fun, Node) ->
    MatchHead = #partisan_peer_info{
        node = Node,
        node_spec = '_',
        connection_count = '$1',
        timestamp = '_'
    },
    MS = [{MatchHead, [{'>', '$1', 0}], ['$_']}],

    case ets:select(?MODULE, MS) of
        [] ->
            ok;
        L ->
            %% We assume we have at most a few hundreds connections max.
            %% An optimisation will be to use batches (limit + continuations).
            %% E.g. a 100 node full-mesh cluster with 4 channels and
            %% parallelism of 1 will have 800 connections
            %% (400 outbound, 400 inbound).
            _ = lists:foreach(
                fun(#partisan_peer_info{node = N} = Info) ->
                    Connections = connections(N),
                    catch Fun(Info, Connections),
                    ok
                end,
                L
            )
    end.

%% -----------------------------------------------------------------------------
%% @doc Return a pid to use for message dispatch.
%% @end
%% -----------------------------------------------------------------------------
-spec dispatch_pid(node() | partisan:node_spec()) ->
    {ok, pid()} | {error, disconnected | not_yet_connected | notalive}.

dispatch_pid(Node) ->
    DefaultChannel = ?DEFAULT_CHANNEL,
    dispatch_pid(Node, DefaultChannel).

%% -----------------------------------------------------------------------------
%% @doc Return a pid to use for message dispatch.
%% @end
%% -----------------------------------------------------------------------------
-spec dispatch_pid(
    Node :: node() | partisan:node_spec(),
    Channel :: partisan:channel()
) ->
    {ok, pid()}
    | {error, disconnected | not_yet_connected | notalive}
    | no_return().

dispatch_pid(Node, Channel) ->
    dispatch_pid(Node, Channel, undefined).

%% -----------------------------------------------------------------------------
%% @doc Return a `{ok, Pid}' where `Pid' is the connection pid to use for
%% message dispatch.
%% If channel `Channel' is disconnected it falls back to a default channel
%% connection if one exists.
%% If no connections exist returns `{error, disconnected}'.
%% @end
%% -----------------------------------------------------------------------------
-spec dispatch_pid(
    Node :: node() | partisan:node_spec(),
    Channel :: partisan:channel(),
    PartitionKey :: optional(any())
) ->
    {ok, pid()}
    | {error, disconnected | not_yet_connected | notalive}
    | no_return().

dispatch_pid(Node, Channel, PartitionKey) when
    is_atom(Node), is_atom(Channel)
->
    Connections =
        case connections(Node, Channel) of
            [] when Channel =/= ?DEFAULT_CHANNEL ->
                case partisan_config:get(channel_fallback, true) of
                    true ->
                        %% Fallback to default channel
                        connections(Node, ?DEFAULT_CHANNEL);
                    false ->
                        []
                end;
            L ->
                L
        end,
    do_dispatch_pid(Connections, PartitionKey, Node);
dispatch_pid(#{name := Node}, Channel, PartitionKey) ->
    dispatch_pid(Node, Channel, PartitionKey).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec dispatch(any()) -> ok | {error, disconnected | not_yet_connected}.

dispatch({forward_message, Node, ServerRef, Message, Opts}) when
    is_map(Opts)
->
    Channel = maps:get(channel, Opts, ?DEFAULT_CHANNEL),
    do_dispatch(Node, ServerRef, Message, Channel, undefined);
dispatch({forward_message, Node, _Clock, PartKey, ServerRef, Msg, Opts}) when
    is_map(Opts)
->
    Channel = maps:get(channel, Opts, ?DEFAULT_CHANNEL),
    do_dispatch(Node, ServerRef, Msg, Channel, PartKey).

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
is_fully_connected(#{channels := Channels}, Count) ->
    Expected = lists:sum([N || #{parallelism := N} <- maps:values(Channels)]),
    Expected =:= Count.

%% -----------------------------------------------------------------------------
%% @private
%% @doc We conditionally insert a new info record incrementing its connection
%% count.
%% @end
%% -----------------------------------------------------------------------------
incr_counter(#{name := Node} = Spec) ->
    Ops = [{#partisan_peer_info.connection_count, 1}],
    Default = #partisan_peer_info{
        node = Node,
        node_spec = Spec,
        timestamp = erlang:system_time(nanosecond)
    },
    _ = ets:update_counter(?MODULE, Node, Ops, Default),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
decr_counter(Node) ->
    Ops = [{#partisan_peer_info.connection_count, -1}],
    _ = ets:update_counter(?MODULE, Node, Ops),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc Emits `[partisan, connection, up]' whenever a connection is added to
%% the table, followed by the `[partisan, channel, connections]' gauge.
%% @end
%% -----------------------------------------------------------------------------
telemetry_connection_up(Node, Channel, ListenAddr) ->
    partisan_telemetry:count(
        [partisan, connection, up],
        #{peer_node => Node, channel => Channel, listen_addr => ListenAddr}
    ),
    telemetry_channel_connections(Node, Channel).

%% -----------------------------------------------------------------------------
%% @private
%% @doc Emits a gauge of the current connection count for `Node'/`Channel'
%% against the channel's configured `parallelism', so a consumer can detect a
%% channel that is running under its target connection count (e.g. after
%% churn). `target' is `undefined' if `Channel' is not currently configured.
%% @end
%% -----------------------------------------------------------------------------
telemetry_channel_connections(Node, Channel) ->
    Target =
        try
            #{parallelism := N} = partisan_config:channel_opts(Channel),
            N
        catch
            error:badarg ->
                undefined
        end,

    partisan_telemetry:execute(
        [partisan, channel, connections],
        #{size => count(Node, Channel), target => Target},
        #{peer_node => Node, channel => Channel}
    ).

%% -----------------------------------------------------------------------------
%% @private
%% @doc Emits `[partisan, connection, down]' for every connection removed by a
%% `prune/2' call.
%% @end
%% -----------------------------------------------------------------------------
telemetry_connection_down(Connections, Reason) when is_list(Connections) ->
    lists:foreach(
        fun(#partisan_peer_connection{node = Node, channel = Channel}) ->
            partisan_telemetry:count(
                [partisan, connection, down],
                #{peer_node => Node, channel => Channel, reason => Reason}
            ),
            telemetry_channel_connections(Node, Channel)
        end,
        Connections
    ).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
match_spec(Node, Channel, ListenAddr, Mode) when
    is_tuple(Channel) orelse (is_atom(Channel) andalso Channel =/= '_')
->
    match_spec(Node, [Channel], ListenAddr, Mode);
match_spec(Node, Channels, ListenAddr, select) when
    is_list(Channels); Channels =:= '_'
->
    do_match_spec(Node, Channels, ListenAddr, ['$_']);
match_spec(Node, Channels, ListenAddr, count) when
    is_list(Channels); Channels =:= '_'
->
    do_match_spec(Node, Channels, ListenAddr, [true]).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
do_match_spec(Node, '_', '_', Return) when is_atom(Node) ->
    Pattern = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '_',
        listen_addr = '_',
        timestamp = '_'
    },
    [{Pattern, [], Return}];
do_match_spec(Node, Channels, '_', Return) when
    is_atom(Node), is_list(Channels)
->
    Pattern = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '$1',
        listen_addr = '_',
        timestamp = '_'
    },

    [
        {Pattern, [{'==', '$1', Channel}], Return}
     || Channel <- Channels,
        is_atom(Channel) andalso Channel =/= '_'
    ];
do_match_spec(#{name := Node} = Spec, '_', '_', Return) ->
    ListenAddrs = maps:get(listen_addrs, Spec),

    Pattern = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '_',
        listen_addr = #{ip => '$1', port => '$2'},
        timestamp = '_'
    },

    [
        {
            Pattern,
            [{'andalso', {'==', '$1', {IP}}, {'==', '$2', Port}}],
            Return
        }
     || #{ip := IP, port := Port} <- ListenAddrs
    ];
do_match_spec(#{name := Node} = Spec, Channels, '_', Return) when
    is_list(Channels)
->
    ListenAddrs = maps:get(listen_addrs, Spec),

    Pattern = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '$3',
        listen_addr = #{ip => '$1', port => '$2'},
        timestamp = '_'
    },

    [
        {
            Pattern,
            [
                {'andalso', {'==', '$1', {IP}}, {'==', '$2', Port},
                    {'==', '$3', Channel}}
            ],
            Return
        }
     || Channel <- Channels,
        #{ip := IP, port := Port} <- ListenAddrs,
        is_atom(Channel) andalso Channel =/= '_'
    ];
do_match_spec(#{name := Node}, Channels, ListenAddr, Return) when
    is_list(Channels)
->
    %% We extract the node as channel and listenaddr override those in spec
    do_match_spec(Node, Channels, ListenAddr, Return);
do_match_spec(Node, Channels, #{ip := IP, port := Port}, Return) when
    is_list(Channels)
->
    Pattern = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '$3',
        listen_addr = #{ip => '$1', port => '$2'},
        timestamp = '_'
    },

    [
        {
            Pattern,
            [
                {'andalso', {'==', '$1', {IP}}, {'==', '$2', Port},
                    {'==', '$3', Channel}}
            ],
            Return
        }
     || Channel <- Channels,
        is_atom(Channel) andalso Channel =/= '_'
    ].

%% @private
do_dispatch_pid([], _, Node) ->
    MatchHead = #partisan_peer_info{
        node = Node,
        node_spec = '_',
        connection_count = '_',
        timestamp = '_'
    },
    MS = [{MatchHead, [], [true]}],

    try ets:select_count(?MODULE, MS) == 1 of
        true ->
            {error, disconnected};
        false ->
            {error, not_yet_connected}
    catch
        error:badarg ->
            {error, notalive}
    end;
do_dispatch_pid(Connections, PartitionKey, _) ->
    %% Get the number of elements in the list.
    NumEntries = length(Connections),

    %% Depending on whether or not a hash key has been provided, use it for
    %% routing.
    Index =
        case PartitionKey of
            undefined ->
                rand:uniform(NumEntries);
            PartitionKey when is_integer(PartitionKey) ->
                PartitionKey rem NumEntries + 1
        end,

    %% Select that entry from the list.
    Connection = lists:nth(Index, Connections),

    %% Return pid of connection process.
    {ok, Connection#partisan_peer_connection.pid}.

%% @private
do_dispatch(Node, ServerRef, Message, Channel, PartitionKey) when
    is_atom(Node)
->
    case dispatch_pid(Node, Channel, PartitionKey) of
        {ok, Pid} ->
            case partisan_config:get(tracing, ?TRACING) of
                true ->
                    case is_process_alive(Pid) of
                        true ->
                            ?LOG_TRACE(#{
                                description => "Dispatching message",
                                message => Message,
                                to => Pid
                            }),
                            ok;
                        false ->
                            ?LOG_TRACE(#{
                                description =>
                                    "Dispatching message, process is NOT ALIVE",
                                message => Message,
                                to => Pid
                            })
                    end;
                false ->
                    ok
            end,

            %% Encode here, in the *calling* process, rather than in the
            %% connection process. Serialisation (and compression, when the
            %% channel enables it) is CPU work that would otherwise be
            %% serialised per connection — one encoder per peer at
            %% `parallelism => 1' — on the critical path of every message to
            %% that peer. Encoding here also keeps the term out of the
            %% connection's mailbox, since refc binaries are not copied.
            Data = encode_for_channel(
                {forward_message, ServerRef, Message}, Channel
            ),
            cast_encoded(Pid, Data, Channel);
        {error, _} = Error ->
            Error
    end;
do_dispatch(#{name := Node}, ServerRef, Message, Channel, PartitionKey) ->
    do_dispatch(Node, ServerRef, Message, Channel, PartitionKey).

%% -----------------------------------------------------------------------------
%% @doc Sends the *same* message to several peers, encoding it once.
%%
%% A fan-out (plumtree's eager push to its peer set) otherwise encodes an
%% identical payload once per peer, because each peer's send is an independent
%% `forward_message/4' call. The wire term is byte-identical for every peer —
%% the destination is the group's registered name, which is the same atom on
%% every node — so one encoding serves all of them.
%%
%% Returns the peers this function did **not** handle. The caller must send to
%% those by the ordinary per-peer path. A peer is deferred when it needs
%% routing this function deliberately does not reimplement:
%%
%% <ul>
%% <li>the local node, which is a direct delivery rather than a send;</li>
%% <li>a peer reachable over disterl while `connect_disterl' is set, which
%% `forward_message/4' short-circuits with `erlang:send/3';</li>
%% <li>a peer with no connection on `Channel', which has its own
%% not-yet-connected / disconnected handling;</li>
%% <li>every peer, when `disable_fast_forward' is set — that option exists to
%% force traffic through the peer service manager.</li>
%% </ul>
%%
%% `Message' must already be in its final wire form, i.e. whatever
%% `forward_message/4' would have passed to `dispatch/1' (`$gen_cast'-wrapped
%% and padded as applicable).
%% @end
%% -----------------------------------------------------------------------------
-spec dispatch_many(
    Peers :: [node()],
    ServerRef :: partisan:server_ref(),
    Message :: any(),
    Channel :: partisan:channel()
) -> Deferred :: [node()].

dispatch_many(Peers, _ServerRef, _Message, _Channel) when
    Peers == []
->
    [];
dispatch_many(Peers, ServerRef, Message, Channel) ->
    case partisan_config:get(disable_fast_forward, false) of
        true ->
            Peers;
        false ->
            Data = encode_for_channel(
                {forward_message, ServerRef, Message}, Channel
            ),
            %% `forward_message/4' takes the partition key from the merged
            %% forward options. Callers of this function do not set one, so it
            %% comes from the global configuration — reading it here keeps
            %% sticky routing sticky instead of silently falling back to the
            %% random connection choice.
            PartitionKey = maps:get(
                partition_key,
                opts_as_map(partisan_config:get(forward_options, #{})),
                ?DEFAULT_PARTITION_KEY
            ),
            Self = partisan:node(),
            Disterl =
                case partisan_config:get(connect_disterl, false) of
                    true -> erlang:nodes();
                    false -> []
                end,
            lists:foldl(
                fun(Peer, Deferred) ->
                    case
                        Peer =/= Self andalso
                            not lists:member(Peer, Disterl)
                    of
                        false ->
                            [Peer | Deferred];
                        true ->
                            Res = dispatch_pid(Peer, Channel, PartitionKey),
                            case Res of
                                {ok, Pid} ->
                                    case cast_encoded(Pid, Data, Channel) of
                                        ok ->
                                            Deferred;
                                        {error, _} ->
                                            %% Over the high-water mark. Defer
                                            %% rather than drop: the caller's
                                            %% per-peer path reports the error
                                            %% to whoever is broadcasting.
                                            [Peer | Deferred]
                                    end;
                                {error, _} ->
                                    [Peer | Deferred]
                            end
                    end
                end,
                [],
                Peers
            )
    end.

%% @private
%% `forward_options' may be configured as a proplist or a map.
opts_as_map(L) when is_list(L) -> maps:from_list(L);
opts_as_map(M) when is_map(M) -> M.

%% @private
%% Encode `Term' the way a connection on `Channel' would have encoded it.
%% -----------------------------------------------------------------------------
%% @doc Hands already-encoded data to a connection process, subject to the
%% connection's high-water mark.
%%
%% **This is the only admission point for outbound data**, and it exists because
%% Partisan had no backpressure at all: dispatch was a bare `gen_server:cast/2'
%% into an unbounded mailbox, so a sender faster than its socket grew that
%% mailbox without limit until the node died of memory exhaustion. A cast cannot
%% fail, cannot block, and cannot tell the sender anything — which is convenient
%% right up to the point where it is fatal.
%%
%% Past the mark this returns `{error, overloaded}' and the data is **not**
%% queued. Refusing is the whole point: a bounded queue that silently discards
%% the newest message is just a lossy channel with extra steps, whereas an error
%% lets the caller retry, shed load, or fail — decisions only the caller can
%% make. `partisan:forward_message/2,3,4' already reports `{error, Reason}', so
%% this needs no further contract change.
%%
%% == Monotonic channels are exempt ==
%%
%% A `monotonic' channel already has an overload strategy, and a different one:
%% `partisan_peer_socket:send/2' *drops* a queued message when the connection has
%% any backlog and the last transmission was recent. That is correct for the
%% traffic monotonic channels carry — only the freshest value matters, so
%% discarding a superseded one loses nothing — and it is deliberately not
%% replaced here. Applying the mark to those channels would convert their silent,
%% intended drops into errors their senders have never had to handle.
%%
%% == On the cost of asking ==
%%
%% `process_info/2` for the queue length is not free, and this runs on every
%% send. It is one BIF against the alternative of an unbounded mailbox, and it is
%% measurable: `make bench BENCH_CASE=p2p_roundtrip' resolves changes of a few
%% percent (see `bench/BASELINE.md').
%% @end
%% -----------------------------------------------------------------------------
-spec cast_encoded(
    Pid :: pid(),
    Data :: iodata(),
    Channel :: partisan:channel()
) -> ok | {error, overloaded}.

cast_encoded(Pid, Data, Channel) ->
    case admit(Pid, Channel) of
        ok ->
            gen_server:cast(Pid, {send_encoded, Data});
        {error, _} = Error ->
            Error
    end.

%% @private
%% Answers whether `Pid' may be given more data.
admit(Pid, Channel) ->
    case high_watermark(Channel) of
        infinity ->
            ok;
        Max ->
            case erlang:process_info(Pid, message_queue_len) of
                {message_queue_len, Len} when Len < Max ->
                    ok;
                undefined ->
                    %% The connection process is gone. Casting to a dead pid is
                    %% harmless, and reporting `overloaded' here would be a lie;
                    %% the caller's own connection handling deals with this.
                    ok;
                {message_queue_len, Len} ->
                    ?LOG_DEBUG(#{
                        description =>
                            "Refusing to queue message, connection over "
                            "high-water mark",
                        connection => Pid,
                        channel => Channel,
                        message_queue_len => Len,
                        high_watermark => Max
                    }),
                    partisan_telemetry:execute(
                        [partisan, connection, overload],
                        #{message_queue_len => Len},
                        #{channel => Channel, high_watermark => Max}
                    ),
                    {error, overloaded}
            end
    end.

%% @private
%% `infinity' for monotonic channels — see the note on `cast_encoded/3'.
high_watermark(Channel) ->
    case partisan_config:get(connection_high_watermark, infinity) of
        infinity ->
            infinity;
        Max ->
            case partisan_config:channel_opts(Channel) of
                #{monotonic := true} -> infinity;
                _ -> Max
            end
    end.

%% The channel's options are the same map the connection process was started
%% with (`partisan_peer_service_manager' reads them from
%% `partisan_config:channels/0'), so both sides derive identical options via
%% `partisan_util:channel_encode_opts/1'.
encode_for_channel(Term, Channel) ->
    ChannelOpts = partisan_config:channel_opts(Channel),
    partisan_util:encode(Term, partisan_util:channel_encode_opts(ChannelOpts)).

%% =============================================================================
%% TESTS
%% =============================================================================

%%
%% Tests
%%
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-compile({no_auto_import, [nodes/0]}).

init_test() ->
    %% A hack to resolve node name
    partisan_config:init().

pid1() ->
    list_to_pid("<0.5001.0>").

pid2() ->
    list_to_pid("<0.5002.0>").

pid3() ->
    list_to_pid("<0.5003.0>").

pid4() ->
    list_to_pid("<0.5004.0>").

channels() ->
    #{
        undefined => #{parallelism => 1, monotonic => false},
        foo => #{parallelism => 2, monotonic => true}
    }.

spec1() ->
    #{
        name => node1,
        listen_addrs => [listen_addr1()],
        channels => channels()
    }.

spec2() ->
    #{
        name => node2,
        listen_addrs => [listen_addr2()],
        channels => channels()
    }.

listen_addr1() ->
    #{ip => {127, 0, 0, 1}, port => 80}.

listen_addr2() ->
    #{ip => {127, 0, 0, 1}, port => 81}.

idempotent_init_test() ->
    ok = init(),
    ok = init().

no_connections_test() ->
    ok = init(),
    ?assertEqual(
        [],
        nodes()
    ),
    ?assertEqual(
        false,
        is_connected(node1)
    ),
    ?assertEqual(
        false,
        is_connected(spec1())
    ),
    ?assertEqual(
        0,
        count(node1)
    ),
    ?assertEqual(
        0,
        count(spec1())
    ),
    ?assertEqual(
        0,
        count(node1, undefined)
    ),
    ?assertEqual(
        0,
        count(spec1(), undefined)
    ),
    ?assertEqual(
        error,
        info(node1)
    ),
    ?assertEqual(
        error,
        info(spec1())
    ),
    ?assertEqual(
        [],
        connections(node1)
    ),
    ?assertEqual(
        [],
        connections(spec1())
    ),
    ?assertMatch(
        {error, not_yet_connected},
        dispatch_pid(node1)
    ),
    ?assertMatch(
        {error, not_yet_connected},
        dispatch_pid(spec1())
    ),
    ?assertMatch(
        {error, not_yet_connected},
        dispatch_pid(node1, undefined)
    ),
    ?assertMatch(
        {error, not_yet_connected},
        dispatch_pid(node1, foo)
    ),
    ?assertMatch(
        {error, not_yet_connected},
        dispatch_pid(node1, unknown_channel)
    ),
    ?assertMatch(
        {error, not_yet_connected},
        dispatch_pid(node1, undefined, 100)
    ),
    ?assertError(
        badarg,
        prune(node1)
    ),
    ?assertError(
        badarg,
        prune(spec1())
    ),
    ?assertError(
        badarg,
        prune(pid1())
    ),
    ?assertEqual(
        ok,
        erase(node1)
    ),
    ?assertEqual(
        ok,
        erase(spec1())
    ).

one_connection_test() ->
    Spec1 = spec1(),
    Pid1 = pid1(),

    ok = init(),
    ok = store(Spec1, Pid1, undefined, listen_addr1()),

    ?assertEqual(
        [node1],
        nodes()
    ),

    ok = store(Spec1, Pid1, undefined, listen_addr1()),
    ?assertEqual(
        [node1],
        nodes(),
        "store/4 is idempotent"
    ),

    ?assertEqual(
        1,
        count(node1)
    ),
    ?assertEqual(
        1,
        count(Spec1)
    ),
    ?assertEqual(
        1,
        count(node1, undefined)
    ),
    ?assertEqual(
        1,
        count(Spec1, undefined)
    ),
    ?assertEqual(
        1,
        count(node1, undefined, listen_addr1())
    ),
    ?assertEqual(
        0,
        count(node1, unknown_channel)
    ),
    ?assertEqual(
        0,
        count(Spec1, unknown_channel)
    ),
    ?assertEqual(
        0,
        count(node1, unknown_channel, listen_addr1())
    ),
    ?assertEqual(
        true,
        is_connected(node1)
    ),
    ?assertEqual(
        true,
        is_connected(Spec1)
    ),

    ?assertMatch(
        {ok, #partisan_peer_info{node = node1}},
        info(node1)
    ),
    ?assertMatch(
        {ok, #partisan_peer_info{node = node1}},
        info(Spec1)
    ),
    ?assertMatch(
        [#partisan_peer_connection{pid = Pid1}],
        connections(node1)
    ),
    ?assertMatch(
        [#partisan_peer_connection{pid = Pid1}],
        connections(Spec1)
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(node1)
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(spec1())
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(node1, undefined)
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(node1, foo)
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(node1, unknown_channel)
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(node1, undefined, 100)
    ).

several_connections_test() ->
    Spec1 = spec1(),
    Pid1 = pid1(),
    Addr1 = listen_addr1(),

    Pid2 = pid2(),
    Addr2 = #{ip => {192, 168, 50, 3}, port => 81},
    Spec2 = spec2(),

    ok = init(),
    ok = store(Spec1, Pid1, undefined, Addr1),

    ?assertEqual(
        [node1],
        nodes()
    ),

    ok = store(Spec1, Pid2, foo, Addr2),
    ?assertEqual(
        [node1],
        nodes(),
        "store/4 is idempotent"
    ),

    ?assertEqual(
        true,
        is_connected(node1)
    ),
    ?assertEqual(
        true,
        is_connected(Spec1)
    ),
    ?assertEqual(
        false,
        is_connected(node2)
    ),
    ?assertEqual(
        false,
        is_connected(Spec2)
    ),
    ?assertEqual(
        2,
        count(node1)
    ),
    ?assertEqual(
        1,
        count(Spec1),
        "even though the node has 2 connections, the spec matches 1, because the second connection has a diff IP"
    ),
    ?assertEqual(
        1,
        count(node1, undefined)
    ),
    ?assertEqual(
        1,
        count(node1, foo)
    ),
    ?assertEqual(
        1,
        count(Spec1, undefined),
        "even though the node has 2 connections, the spec matches 1, because the second connection has a diff IP"
    ),
    ?assertEqual(
        0,
        count(node1, unknown_channel)
    ),
    ?assertEqual(
        0,
        count(Spec1, unknown_channel)
    ),

    ?assertEqual(
        0,
        count(node2)
    ),
    ?assertEqual(
        0,
        count(Spec2)
    ),
    ?assertEqual(
        0,
        count(node2, undefined)
    ),
    ?assertEqual(
        0,
        count(Spec2, undefined)
    ),
    ?assertEqual(
        0,
        count(node2, unknown_channel)
    ),
    ?assertEqual(
        0,
        count(Spec2, unknown_channel)
    ),

    ?assertMatch(
        {ok, #partisan_peer_info{node = node1}},
        info(node1)
    ),
    ?assertMatch(
        {ok, #partisan_peer_info{node = node1}},
        info(Spec1)
    ),
    ?assertMatch(
        [
            #partisan_peer_connection{pid = Pid1},
            #partisan_peer_connection{pid = Pid2}
        ],
        connections(node1)
    ),
    ?assertMatch(
        [
            #partisan_peer_connection{pid = Pid1}
        ],
        connections(Spec1),
        "When we match with spec we should only get 1 as the second"
    ),
    ?assertMatch(
        [],
        connections(Spec2)
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(node1)
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(spec1())
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(node1, undefined)
    ),
    ?assertMatch(
        {ok, Pid2},
        dispatch_pid(node1, foo)
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(node1, unknown_channel)
    ),
    ?assertMatch(
        {ok, Pid1},
        dispatch_pid(node1, undefined, 100)
    ),
    ?assertMatch(
        {ok, Pid2},
        dispatch_pid(node1, foo, 100)
    ).

several_nodes_undefined_test() ->
    dbg:stop(),
    Spec2 = spec2(),
    Channel = undefined,
    Pid3 = pid3(),
    ok = init(),
    ok = store(Spec2, Pid3, Channel, listen_addr2()),

    ?assertEqual(
        [node1, node2],
        nodes()
    ),

    ok = store(Spec2, Pid3, Channel, listen_addr2()),
    ?assertEqual(
        [node1, node2],
        nodes(),
        "store/4 is idempotent"
    ),

    ?assertEqual(
        1,
        count(node2)
    ),
    ?assertEqual(
        1,
        count(Spec2)
    ),
    ?assertEqual(
        1,
        count(node2, Channel)
    ),
    ?assertEqual(
        1,
        count(Spec2, Channel)
    ),
    ?assertEqual(
        1,
        count(node2, Channel, listen_addr2())
    ),
    ?assertEqual(
        0,
        count(node2, unknown_channel)
    ),
    ?assertEqual(
        0,
        count(Spec2, unknown_channel)
    ),
    ?assertEqual(
        0,
        count(node2, unknown_channel, listen_addr2())
    ),
    ?assertEqual(
        true,
        is_connected(node2)
    ),
    ?assertEqual(
        true,
        is_connected(Spec2)
    ),

    ?assertMatch(
        {ok, #partisan_peer_info{node = node2}},
        info(node2)
    ),
    ?assertMatch(
        {ok, #partisan_peer_info{node = node2}},
        info(Spec2)
    ),
    ?assertMatch(
        [#partisan_peer_connection{pid = Pid3}],
        connections(node2)
    ),
    ?assertMatch(
        [#partisan_peer_connection{pid = Pid3}],
        connections(Spec2)
    ),

    ?assertMatch(
        {ok, Pid3},
        dispatch_pid(node2)
    ),
    ?assertMatch(
        {ok, Pid3},
        dispatch_pid(Spec2)
    ),
    ?assertMatch(
        {ok, Pid3},
        dispatch_pid(node2, undefined)
    ),
    ?assertMatch(
        {ok, Pid3},
        dispatch_pid(node2, foo)
    ),
    ?assertMatch(
        {ok, Pid3},
        dispatch_pid(node2, unknown_channel)
    ),
    ?assertMatch(
        {ok, Pid3},
        dispatch_pid(node2, undefined, 100)
    ),
    ?assertMatch(
        {ok, Pid3},
        dispatch_pid(node2, foo, 100)
    ).

several_nodes_foo_test() ->
    dbg:stop(),
    Spec2 = spec2(),
    Channel = foo,
    Pid3 = pid3(),
    Pid4 = pid4(),
    ok = init(),
    ok = store(Spec2, Pid4, Channel, listen_addr2()),

    ?assertEqual(
        [node1, node2],
        nodes()
    ),

    ok = store(Spec2, Pid4, Channel, listen_addr2()),
    ?assertEqual(
        [node1, node2],
        nodes(),
        "store/4 is idempotent"
    ),

    ?assertEqual(
        2,
        count(node2)
    ),
    ?assertEqual(
        2,
        count(Spec2)
    ),
    ?assertEqual(
        1,
        count(node2, Channel)
    ),
    ?assertEqual(
        1,
        count(Spec2, Channel)
    ),
    ?assertEqual(
        1,
        count(node2, Channel, listen_addr2())
    ),
    ?assertEqual(
        0,
        count(node2, unknown_channel)
    ),
    ?assertEqual(
        0,
        count(Spec2, unknown_channel)
    ),
    ?assertEqual(
        0,
        count(node2, unknown_channel, listen_addr2())
    ),
    ?assertEqual(
        true,
        is_connected(node2)
    ),
    ?assertEqual(
        true,
        is_connected(Spec2)
    ),

    ?assertMatch(
        {ok, #partisan_peer_info{node = node2}},
        info(node2)
    ),
    ?assertMatch(
        {ok, #partisan_peer_info{node = node2}},
        info(Spec2)
    ),
    ?assertMatch(
        [
            #partisan_peer_connection{pid = Pid3},
            #partisan_peer_connection{pid = Pid4}
        ],
        connections(node2)
    ),
    ?assertMatch(
        [
            #partisan_peer_connection{pid = Pid3},
            #partisan_peer_connection{pid = Pid4}
        ],
        connections(Spec2)
    ).

erase_test() ->
    ok = store(spec1(), pid1(), undefined, listen_addr1()),
    ?assertEqual(
        ok,
        erase(node1)
    ),
    ?assertEqual(
        ok,
        erase(spec1())
    ).

prune_test() ->
    ok = store(spec1(), pid1(), undefined, listen_addr1()),
    ?assertMatch(
        {#partisan_peer_info{}, [#partisan_peer_connection{}]},
        prune(pid1())
    ),

    ?assertMatch(
        {#partisan_peer_info{}, []},
        prune(node1)
    ),

    ?assertError(
        badarg,
        prune(node1)
    ).

-endif.
