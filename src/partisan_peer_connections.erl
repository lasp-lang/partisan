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

-define(NOT_GROUND(Args),
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


%% We store two records on the same table, using the first field as a key
%% (keypos 2). Since both keys differ on type (node() and pid()) this is fine.
-record(partisan_peer_info, {
    node                    ::  maybe_var(node()),
    node_spec               ::  maybe_var(partisan:node_spec()),
    connection_count = 0    ::  maybe_var(non_neg_integer()),
    timestamp               ::  maybe_var(non_neg_integer())
}).

-record(partisan_peer_connection, {
    pid                     ::  maybe_var(pid()),
    node                    ::  maybe_var(node()),
    channel                 ::  maybe_var(partisan:channel()),
    listen_addr             ::  maybe_var(partisan:listen_addr())
                                | listen_addr_spec(),
    timestamp               ::  maybe_var(non_neg_integer())
}).


-type maybe_var(T)          ::  T | var().
-type var()                 ::  '_' | '$1' | '$2' | '$3'.
-type info()                ::  #partisan_peer_info{}.
-type connection()          ::  #partisan_peer_connection{}.
-type connections()         ::  [connection()].
-type listen_addr_spec()    :: #{ip := var(), port := var()}.

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
-export([dispatch/1]).
-export([dispatch_pid/1]).
-export([dispatch_pid/2]).
-export([dispatch_pid/3]).
-export([erase/1]).
-export([fold/2]).
-export([foreach/1]).
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
            Pos = #partisan_peer_info.node,

            ?MODULE = ets:new(?MODULE, [
                named_table,
                protected,
                ordered_set,
                {keypos, Pos},
                %% We enable both as we have concurrent reads and writes,
                %% although writes will only happen when a connection is
                %% started/stopped for a peer node.
                {read_concurrency, true},
                {write_concurrency, true}
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
    %% We select the first element (nodename) of the tuple where the
    %% second element (connection counter) is greater than zero.
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
    % We select the first element (nodename) of the tuple where the
    %% second element (connection counter) is greater than zero.
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
    Channels :: maybe_var(partisan:channel() | [partisan:channel()])) ->
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
                    node_spec = Spec, connection_count = Count
                }} ->
                    is_fully_connected(Spec, Count);
                error ->
                    false
            end

    end;

is_fully_connected(#{name := Node, channels := Channels} = NodeSpec)
when is_atom(Node) andalso is_map(Channels) ->
    is_fully_connected(NodeSpec, count(Node));

is_fully_connected(#{name := Node}) ->
    is_fully_connected(Node).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec count() -> non_neg_integer().

count() ->
    MS = match_spec('_', '_', '_', count),
    try
        ets:select_count(?MODULE, MS)
    catch
        error:badarg ->
            0
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the number of connections for node `Node'.
%%
%% When passed a `partisan:node_spec()' as `Arg' it is equivalent to calling
%% {@link connection_count/2} with a wildcard as a second argument i.e. '_'.
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

count(Arg) when is_map(Arg)->
    count(Arg, '_');

count(#partisan_peer_info{connection_count = Val})
when is_integer(Val) ->
    Val;

count(#partisan_peer_info{} = T) ->
    ?NOT_GROUND([T]).


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
    ListenAddr :: partisan:listen_addr()) -> Count :: non_neg_integer().

count(Node, Channels, ListenAddr) ->
    MS = match_spec(Node, Channels, ListenAddr, count),
    try
        ets:select_count(?MODULE, MS)
    catch
        error:badarg ->
            0
    end.


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
    Channels ::  maybe_var(partisan:channel() | [partisan:channel()])
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
    Channels ::  maybe_var(partisan:channel() | [partisan:channel()]),
    ListenAddr :: partisan:listen_addr()) ->
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
    Channel :: maybe_var(partisan:channel())) -> [pid()].

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
    ?NOT_GROUND([T]).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec pid(connection()) -> pid().

pid(#partisan_peer_connection{pid = Val}) when is_pid(Val) ->
    Val;

pid(#partisan_peer_connection{} = T) ->
    ?NOT_GROUND([T]).



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
    ?NOT_GROUND([T]).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node(info() | connection()) -> node() | no_return().

node(#partisan_peer_info{node = Val}) when is_atom(Val) ->
    Val;

node(#partisan_peer_info{} = T) ->
    ?NOT_GROUND([T]);

node(#partisan_peer_connection{node = Val}) when is_atom(Val) ->
    Val;

node(#partisan_peer_connection{} = T) ->
    ?NOT_GROUND([T]).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec(info() | connection()) -> partisan:node_spec() | no_return().

node_spec(#partisan_peer_info{node_spec = Val}) when is_map(Val) ->
    Val;

node_spec(#partisan_peer_info{} = T) ->
    ?NOT_GROUND([T]);

node_spec(#partisan_peer_connection{node = Val}) when is_atom(Val) ->
    case info(Val) of
        {ok, Info} ->
            node_spec(Info);
        error ->
            error(badarg)
    end;

node_spec(#partisan_peer_connection{} = T) ->
    ?NOT_GROUND([T]).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec timestamp(info() | connection()) -> non_neg_integer() | no_return().

timestamp(#partisan_peer_info{timestamp = Val}) when is_integer(Val) ->
    Val;

timestamp(#partisan_peer_info{} = T) ->
    ?NOT_GROUND([T]);

timestamp(#partisan_peer_connection{timestamp = Val}) when is_integer(Val) ->
    Val;

timestamp(#partisan_peer_connection{} = T) ->
    ?NOT_GROUND([T]).


%% -----------------------------------------------------------------------------
%% @doc Store a connection
%% @end
%% -----------------------------------------------------------------------------
-spec store(
    Node :: partisan:node_spec(),
    Pid :: pid(),
    Channel :: partisan:channel(),
    LitenAddr :: partisan:listen_addr()) -> ok | no_return().

store(
    #{name := Node} = Spec,
    Pid,
    Channel,
    #{ip := IP, port := Port} = ListenAddr
) when is_pid(Pid)
andalso is_atom(Channel) andalso Channel =/= '_'
andalso ?IS_IP(IP) andalso is_integer(Port) andalso Port >= 0 ->

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
            incr_counter(Spec);
        false ->
            {ok, Info} = info(Node),
            InfoSpec = node_spec(Info),
            Count = count(Node),

            case Count == 0 of
                true ->
                    ?LOG_DEBUG(#{
                        description => "A new connection was made using a node specification instance that differs from the existing specification for node. Replacing the existing specification with the new one as no existing connections exist.",
                        node_spec => InfoSpec,
                        connection => #{
                            pid => Pid,
                            node_spec => Spec
                        }
                    }),
                    ets:insert(?MODULE, Conn),
                    incr_counter(Spec);
                false ->
                    ?LOG_WARNING(#{
                        description => "A new connection was made using a node specification instance that differs from the existing specification for node. Keeping the existing specification in the info record as connections exist.",
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
%% pruned pid was found
%% @end
%% -----------------------------------------------------------------------------
-spec prune(pid() | node() | partisan:node_spec()) ->
    {info(), connections()} | no_return().

prune(Node) when is_atom(Node) ->
    MatchHead = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '_',
        listen_addr = '_',
        timestamp = '_'
    },

    %% Remove all connections
    Connections =
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

    %% Remove info and return spec
    case ets:take(?MODULE, Node) of
        [#partisan_peer_info{} = I] ->
            {I, Connections};
        [] ->
            error(badarg)
    end;

prune(Pid) when is_pid(Pid) ->
    %% Remove matching connection
    try ets:take(?MODULE, Pid) of
        [#partisan_peer_connection{node = Node}] = L ->
            %% We dexcrease the connection count
            Ops = [{#partisan_peer_info.connection_count, -1}],
            _ = ets:update_counter(?MODULE, Node, Ops),

            {ok, #partisan_peer_info{} = I} = info(Node),
            {I, L};
        [] ->
            error(badarg)
    catch
        error:badarg ->
            error(notalive)
    end;

prune(#{name := Node}) ->
    prune(Node).


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
            fun(C) ->
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
-spec fold(
    Fun :: fun((partisan:node_spec(), connections(), Acc1 :: any()) -> Acc2 :: any()),
    AccIn :: any()) -> AccOut :: any().

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
                fun(#partisan_peer_info{node = Node, node_spec = Spec}, IAcc)
                when is_atom(Node), is_map(Spec) ->
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
    Channel :: partisan:channel()) ->
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
    PartitionKey :: optional(any())) ->
    {ok, pid()}
    | {error, disconnected | not_yet_connected | notalive}
    | no_return().

dispatch_pid(Node, Channel, PartitionKey)
when is_atom(Node), is_atom(Channel) ->
    DefaultChannel = ?DEFAULT_CHANNEL,

    Connections = case connections(Node, Channel) of
        [] when Channel =/= DefaultChannel ->
            %% Fallback to default channel
            connections(Node, DefaultChannel);
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

dispatch({forward_message, Node, ServerRef, Message, Opts})
when is_map(Opts) ->
    Channel = maps:get(channel, Opts, ?DEFAULT_CHANNEL),
    do_dispatch(Node, ServerRef, Message, Channel, undefined);

dispatch({forward_message, Node, _Clock, PartKey, ServerRef, Msg, Opts})
when is_map(Opts) ->
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
match_spec(Node, Channel, ListenAddr, Mode)
when is_tuple(Channel) orelse (is_atom(Channel) andalso Channel =/= '_') ->
    match_spec(Node, [Channel], ListenAddr, Mode);

match_spec(Node, Channels, ListenAddr, select)
when is_list(Channels); Channels =:= '_' ->
    do_match_spec(Node, Channels, ListenAddr, ['$_']);

match_spec(Node, Channels, ListenAddr, count)
when is_list(Channels); Channels =:= '_' ->
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

do_match_spec(Node, Channels, '_', Return)
when is_atom(Node), is_list(Channels) ->
    Pattern = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '$1',
        listen_addr = '_',
        timestamp = '_'
    },

    [
        {Pattern, [{'==', '$1', Channel}], Return}
        ||  Channel <- Channels,
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
            [{'andalso',
                {'==', '$1', {IP}},
                {'==', '$2', Port}
            }],
            Return
        }
        ||  #{ip := IP, port := Port} <- ListenAddrs
    ];

do_match_spec(#{name := Node} = Spec, Channels, '_', Return) when is_list(Channels) ->
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
            [ {'andalso',
                {'==', '$1', {IP}},
                {'==', '$2', Port},
                {'==', '$3', Channel}
            }],
            Return
        }
        ||  Channel <- Channels,
            #{ip := IP, port := Port} <- ListenAddrs,
            is_atom(Channel) andalso Channel =/= '_'
    ];

do_match_spec(#{name := Node}, Channels, ListenAddr, Return)
when is_list(Channels) ->
    %% We extract the node as channel and listenaddr override those in spec
    do_match_spec(Node, Channels, ListenAddr, Return);

do_match_spec(Node, Channels, #{ip := IP, port := Port}, Return)
when is_list(Channels) ->

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
            [{'andalso',
                {'==', '$1', {IP}},
                {'==', '$2', Port},
                {'==', '$3', Channel}
            }],
            Return
        }
        ||  Channel <- Channels,
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
    Index = case PartitionKey of
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
do_dispatch(Node, ServerRef, Message, Channel, PartitionKey)
when is_atom(Node) ->
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
                                description => "Dispatching message, process is NOT ALIVE",
                                message => Message,
                                to => Pid
                            })
                    end;
                false ->
                    ok
            end,

            gen_server:cast(
                Pid, {send_message, {forward_message, ServerRef, Message}}
            );

        {error, _} = Error ->
            Error
    end;

do_dispatch(#{name := Node}, ServerRef, Message, Channel, PartitionKey) ->
    do_dispatch(Node, ServerRef, Message, Channel, PartitionKey).



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
    Addr2 = #{ip => {192,168,50,3}, port => 81},
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
