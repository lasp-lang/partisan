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
-module(partisan_peer_connections).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-define(SELECT(Arg),
    try
        ets:select(?MODULE, Arg)
    catch
        error:badarg ->
            []
    end
).


%% We store two records on the same table, and using the first field as a key
%% (keypos 2). Since both keys differ on type (node() and pid()) this is fine.
-record(partisan_peer_info, {
    node                    ::  maybe_var(node()),
    node_spec               ::  maybe_var(node_spec()),
    connection_count = 0    ::  maybe_var(non_neg_integer())
}).

-record(partisan_peer_connection, {
    pid                     ::  maybe_var(pid()),
    node                    ::  maybe_var(node()),
    channel                 ::  maybe_var(channel_spec()),
    listen_addr             ::  maybe_var(listen_addr())
}).

-type info()                ::  #partisan_peer_info{}.
-type connection()          ::  #partisan_peer_connection{}.
-type connections()         ::  [connection()].
%% Use when pattern matching e.g. '_', '$1'
-type maybe_var(T)          ::  T | atom().
-type channel_spec()        ::  channel() | {monotonic, channel()}.
-type maybe(T)              ::  T | undefined.

-export_type([connection/0]).
-export_type([info/0]).
-export_type([connections/0]).
-export_type([channel_spec/0]).

-export([channel/1]).
-export([connection_count/1]).
-export([connection_count/2]).
-export([connections/0]).
-export([connections/1]).
-export([connections/2]).
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
-export([listen_addr/1]).
-export([node/1]).
-export([node_spec/1]).
-export([node_specs/0]).
-export([nodes/0]).
-export([nodes/1]).
-export([pid/1]).
-export([processes/1]).
-export([processes/2]).
-export([prune/1]).
-export([store/4]).


-compile({no_auto_import, [nodes/1]}).
-compile({no_auto_import, [erase/1]}).


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
%% connections (that is, hidden nodes are not listed). Same as nodes(visible).
%% @end
%% -----------------------------------------------------------------------------
-spec nodes() -> [node()].

nodes() ->
    nodes(visible).


%% -----------------------------------------------------------------------------
%% @doc Returns a list of all nodes connected to this node with connections of
%% type `Arg'.
%% @end
%% -----------------------------------------------------------------------------
-spec nodes(Arg :: erlang:node_type()) -> [node()].

nodes(Arg) ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            erlang:nodes(Arg);

        false when Arg == visible orelse Arg == connected ->
            %% We select the first element (nodename) of the tuple where the
            %% second element (connection counter) is greater than zero.
            MatchHead = #partisan_peer_info{
                node = '$1',
                node_spec = '_',
                connection_count = '$2'
            },
            MS = [{MatchHead, [{'>', '$2', 0}], ['$1']}],
            ?SELECT(MS);

        false when Arg == known ->
            {ok, Members} = partisan_peer_service:members(),
            Members;

        false when Arg == this ->
            partisan:node();

        false when Arg == hidden ->
            []
    end.

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
        connection_count = '$2'
    },
    MS = [{MatchHead, [{'>', '$2', 0}], ['$1']}],
    ?SELECT(MS).


%% -----------------------------------------------------------------------------
%% @doc Returns true is this node is connected to `NodeOrName'.
%% If `Node' is this node, returns `true'.
%% @end
%% -----------------------------------------------------------------------------
-spec is_connected(NodeOrSpec :: node_spec() | node()) ->
    boolean().

is_connected(Node) when is_atom(Node) ->
    Node =:= partisan:node() orelse connection_count(Node) > 0;

is_connected(#{name := Node}) ->
    is_connected(Node).


%% -----------------------------------------------------------------------------
%% @doc Returns true is this node is connected to `NodeOrName'.
%% If `Node' is this node, returns `true'.
%% @end
%% -----------------------------------------------------------------------------
-spec is_connected(
    NodeOrSpec :: node_spec() | node(),
    Channel :: channel_spec() | [channel_spec()]) -> boolean() | no_return().

is_connected(Node, {monotonic, Channel}) when is_atom(Node) ->
    is_connected(Node, Channel);

is_connected(Node, Channel) when is_atom(Node) ->
    Node =:= partisan:node() orelse connection_count(Node, Channel) > 0;

is_connected(#{name := Node}, Channel) ->
    is_connected(Node, Channel).


%% -----------------------------------------------------------------------------
%% @doc Returns true is this node has all the requested connections
%% (`parallelism' configuration parameter) for all the configured channels with
%% node `NodeOrSpec'.
%% @end
%% -----------------------------------------------------------------------------
-spec is_fully_connected(Peer :: node_spec() | node()) ->
    boolean().

is_fully_connected(Node) when is_atom(Node) ->
    case partisan:node() of
        Node ->
            true;
        _ ->
            case info(Node) of
                {ok, #partisan_peer_info{} = Info} ->
                    Spec = Info#partisan_peer_info.node_spec,
                    ConnectionCount = Info#partisan_peer_info.connection_count,
                    Parallelism = maps:get(parallelism, Spec, 1),
                    ChannelCount = length(
                        lists:usort(maps:get(channels, Spec))
                    ),
                    ConnectionCount =:= Parallelism * ChannelCount;
                error ->
                    false
            end

    end;

is_fully_connected(#{name := Node, parallelism := P, channels := Channels})
when is_integer(P) andalso is_list(Channels) ->
    connection_count(Node) =:= P * length(lists:usort(Channels));

is_fully_connected(#{name := Node}) ->
    is_fully_connected(Node).


%% -----------------------------------------------------------------------------
%% @doc Returns the nbr of connections for node `Node'.
%% @end
%% -----------------------------------------------------------------------------
-spec connection_count(NodeOrSpec :: node_spec() | node() | info()) ->
    non_neg_integer().

connection_count(Node) when is_atom(Node) ->
    %% An optimisation that is faster than connection_count(Node, '_'),
    %% as connection_count/1 is more often called than connection_count/2.
    try
        Pos = #partisan_peer_info.connection_count,
        ets:lookup_element(?MODULE, Node, Pos)
    catch
        error:badarg ->
            0
    end;

connection_count(#{name := Node}) ->
    connection_count(Node);


connection_count(#partisan_peer_info{connection_count = Val}) ->
    Val.


%% -----------------------------------------------------------------------------
%% @doc Returns the nbr of connections for node `Node' and channel `Channel'.
%% @end
%% -----------------------------------------------------------------------------
-spec connection_count(
    NodeOrSpec :: node_spec() | node() | info(),
    Channel :: channel_spec() | [channel_spec()]) ->
    non_neg_integer() | no_return().

connection_count(Node, Channel) when is_atom(Channel) ->
    connection_count(Node, [Channel]);

connection_count(Node, {monotonic, Channel} = Spec) when is_atom(Channel) ->
    connection_count(Node, [Spec]);

connection_count(Node, Channels) when is_atom(Node), is_list(Channels) ->
    Pattern = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '$1',
        listen_addr = '_'
    },

    MS = [
        {Pattern, [{'==', '$1', ms_channel(Channel)}], [true]}
        ||  Channel <- Channels, validate_channel_spec(Channel)
    ],

    try
        ets:select_count(?MODULE, MS)
    catch
        error:badarg ->
            0
    end;

connection_count(#{name := Node}, Channels) ->
    connection_count(Node, Channels);

connection_count(#partisan_peer_info{node = Node}, Channels) ->
    connection_count(Node, Channels).


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
-spec connections(NodeOrSpec :: atom() | node_spec()) -> connections().

connections(NodeOrSpec) ->
    connections(NodeOrSpec, '_').


%% -----------------------------------------------------------------------------
%% @doc Finds connection for a node and channel.
%% @end
%% -----------------------------------------------------------------------------
-spec connections(
    NodeOrSpec :: maybe_var(atom() | node_spec()),
    Channel :: maybe_var(channel_spec())) ->
    connections() | no_return().

connections(Node, Channel) when is_atom(Node) ->
    true = validate_channel_spec(Channel),

    MatchHead = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = Channel,
        listen_addr = '_'
    },
    MS = [{MatchHead, [], ['$_']}],
    ?SELECT(MS);

connections(#{name := Node}, Channel) ->
    connections(Node, Channel).


%% -----------------------------------------------------------------------------
%% @doc Finds connection for a node.
%% @end
%% -----------------------------------------------------------------------------
-spec processes(NodeOrSpec :: atom() | node_spec()) -> [pid()].

processes(NodeOrSpec) ->
    processes(NodeOrSpec, '_').


%% -----------------------------------------------------------------------------
%% @doc Finds connection for a node and channel and returns each connection pid
%% @end
%% -----------------------------------------------------------------------------
-spec processes(
    NodeOrSpec :: atom() | node_spec(), Channel :: channel_spec()) -> [pid()].

processes(Node, Channel) when is_atom(Node) ->
    true = validate_channel_spec(Channel),

    MatchHead = #partisan_peer_connection{
        pid = '$1',
        node = Node,
        channel = Channel,
        listen_addr = '_'
    },
    MS = [{MatchHead, [], ['$1']}],
    ?SELECT(MS);

processes(#{name := Node}, Channel) ->
    processes(Node, Channel).


%% -----------------------------------------------------------------------------
%% @doc Returns true is this node is connected to `NodeOrName'.
%% @end
%% -----------------------------------------------------------------------------
-spec info(NodeOrSpec :: node_spec() | node()) -> {ok, info()} | error.

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
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec channel(connection()) -> channel_spec().

channel(#partisan_peer_connection{channel = Val}) ->
    Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec pid(connection()) -> pid().

pid(#partisan_peer_connection{pid = Val}) ->
    Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec listen_addr(connection()) -> listen_addr().

listen_addr(#partisan_peer_connection{listen_addr = Val}) ->
    Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node(info() | connection()) -> node().

node(#partisan_peer_info{node = Val}) ->
    Val;

node(#partisan_peer_connection{node = Val}) ->
    Val.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec(info() | connection()) -> node_spec() | no_return().

node_spec(#partisan_peer_info{node_spec = Val}) ->
    Val;

node_spec(#partisan_peer_connection{node = Node}) ->
    case info(Node) of
        {ok, Info} ->
            node_spec(Info);
        error ->
            error(badarg)
    end.



%% -----------------------------------------------------------------------------
%% @doc Store a connection
%% @end
%% -----------------------------------------------------------------------------
-spec store(
    Node :: node_spec(),
    Pid :: pid(),
    Channel :: channel_spec(),
    LitenAddr :: listen_addr()) -> ok | no_return().

store(#{name := Node} = Spec, Pid, Channel, ListenAddr)
when is_pid(Pid), is_map(ListenAddr) ->

    true = validate_channel_spec(Channel),

    %% We insert separately as we have N connections per node.
    Conn = #partisan_peer_connection{
        pid = Pid,
        node = Node,
        channel = Channel,
        listen_addr = ListenAddr
    },

    try ets:insert_new(?MODULE, Conn) of
        true ->
            %% We conditionally insert a new info record incrementing its
            %% connection count.
            Ops = [{#partisan_peer_info.connection_count, 1}],
            Default = #partisan_peer_info{node = Node, node_spec = Spec},
            _ = ets:update_counter(?MODULE, Node, Ops, Default),
            ok;
        false ->
            ok
    catch
        error:badarg ->
            error(notalive)
    end.


%% -----------------------------------------------------------------------------
%% @doc Prune all occurrences of a connection pid returns the node where the
%% pruned pid was found
%% @end
%% -----------------------------------------------------------------------------
-spec prune(pid() | node_spec()) -> {info(), connections()} | no_return().

prune(Node) when is_atom(Node) ->
    MatchHead = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '_',
        listen_addr = '_'
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
-spec erase(pid() | node_spec()) -> ok.

erase(Node) when is_atom(Node) ->
    MatchHead = #partisan_peer_connection{
        pid = '_',
        node = Node,
        channel = '_',
        listen_addr = '_'
    },
    MS = [{MatchHead, [], [true]}],

    %% Remove all connections
    _ = catch ets:select_delete(?MODULE, MS),

    %% Remove info and return spec
    _ = catch ets:delete(?MODULE, Node),

    ok;

erase(#{name := Node}) ->
    erase(Node).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec fold(
    Fun :: fun((node_spec(), connections(), Acc1 :: any()) -> Acc2 :: any()),
    AccIn :: any()) -> AccOut :: any().

fold(Fun, Acc) ->
    MatchHead = #partisan_peer_info{
        node = '_',
        node_spec = '_',
        connection_count = '$1'
    },
    MS = [{MatchHead, [{'>', '$1', 0}], ['$_']}],

    case ets:select(?MODULE, MS) of
        [] ->
            ok;
        L ->
            %% We asume we have at most a few hundreds of connections.
            %% An optimisation will be to use batches (limit + continuations).
            _ = lists:foldl(
                fun(#partisan_peer_info{node = Node, node_spec = Spec}, IAcc) ->
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
        connection_count = '$1'
    },
    MS = [{MatchHead, [{'>', '$1', 0}], ['$_']}],

    case ets:select(?MODULE, MS) of
        [] ->
            ok;
        L ->
            %% We asume we have at most a few hundreds of connections max.
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
-spec dispatch_pid(node() | node_spec()) ->
    {ok, pid()} | {error, disconnected | not_yet_connected | notalive}.

dispatch_pid(Node) ->
    dispatch_pid(Node, ?DEFAULT_CHANNEL).


%% -----------------------------------------------------------------------------
%% @doc Return a pid to use for message dispatch.
%% @end
%% -----------------------------------------------------------------------------
-spec dispatch_pid(node() | node_spec(), channel_spec()) ->
    {ok, pid()} | {error, disconnected | not_yet_connected}.

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
-spec dispatch_pid(node() | node_spec(), channel_spec(), maybe(any())) ->
    {ok, pid()} | {error, disconnected | not_yet_connected | notalive}.

dispatch_pid(Node, {monotonic, Channel}, PartitionKey) ->
    dispatch_pid(Node, Channel, PartitionKey);

dispatch_pid(Node, Channel, PartitionKey) when is_atom(Node) ->
    Connections = case connections(Node, Channel) of
        [] when Channel =/= ?DEFAULT_CHANNEL ->
            %% Fallback to default channel
            connections(Node, ?DEFAULT_CHANNEL);
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

dispatch({forward_message, Node, ServerRef, Message, _Options}) ->
    do_dispatch(Node, ServerRef, Message, ?DEFAULT_CHANNEL, undefined);

dispatch({forward_message, Node, Channel, _Clock, PartitionKey, ServerRef, Message, _Options}) ->
    do_dispatch(Node, ServerRef, Message, Channel, PartitionKey).



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
validate_channel_spec('_') ->
    true;

validate_channel_spec(Channel) when is_atom(Channel) ->
    true;

validate_channel_spec({monotonic, Channel}) when is_atom(Channel) ->
    true;

validate_channel_spec(Term) ->
    error({badarg, Term}).


%% @private
ms_channel({monotonic, _} = Channel) ->
    {Channel};

ms_channel(Channel) ->
    Channel.


%% @private
do_dispatch_pid([], _, Node) ->
    MatchHead = #partisan_peer_info{
        node = Node,
        node_spec = '_',
        connection_count = '_'
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

spec1() ->
    #{name => node1, listen_addrs => [listen_addr1()]}.

spec2() ->
    #{name => node2, listen_addrs => [listen_addr2()]}.

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
        [],
        nodes(visible)
    ),
    ?assertEqual(
        [],
        nodes(connected)
    ),
    ?assertExit(
        {noproc, _},
        nodes(known)
    ),
    ?assertEqual(
        [],
        nodes(hidden)
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
        connection_count(node1)
    ),
    ?assertEqual(
        0,
        connection_count(spec1())
    ),
    ?assertEqual(
        0,
        connection_count(node1, undefined)
    ),
    ?assertEqual(
        0,
        connection_count(spec1(), undefined)
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
    ok = init(),
    ok = store(spec1(), pid1(), undefined, listen_addr1()),

    ?assertEqual(
        [node1],
        nodes()
    ),

    ok = store(spec1(), pid1(), undefined, listen_addr1()),
    ?assertEqual(
        [node1],
        nodes(),
        "store/4 is idempotent"
    ),

    ?assertEqual(
        [node1],
        nodes(visible)
    ),
    ?assertEqual(
        [node1],
        nodes(connected)
    ),
    ?assertExit(
        {noproc, _},
        nodes(known) %% data from membership, not our table
    ),
    ?assertEqual(
        [],
        nodes(hidden)
    ),
    ?assertEqual(
        true,
        is_connected(node1)
    ),
    ?assertEqual(
        true,
        is_connected(spec1())
    ),
    ?assertEqual(
        1,
        connection_count(node1)
    ),
    ?assertEqual(
        1,
        connection_count(spec1())
    ),
    ?assertEqual(
        1,
        connection_count(node1, undefined)
    ),
    ?assertEqual(
        1,
        connection_count(spec1(), undefined)
    ),
    ?assertEqual(
        0,
        connection_count(node1, unknown_channel)
    ),
    ?assertEqual(
        0,
        connection_count(spec1(), unknown_channel)
    ),

    Spec1 = spec1(),
    Pid1 = pid1(),

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
    ).


several_connections_test() ->
    Spec1 = spec1(),
    Pid1 = pid1(),
    Addr1 = listen_addr1(),

    Pid2 = pid2(),
    Addr2 = listen_addr2(),
    Spec2 = spec2(),

    ok = init(),
    ok = store(Spec1, Pid1, undefined, Addr1),

    ?assertEqual(
        [node1],
        nodes()
    ),

    ok = store(Spec1, Pid2, undefined, Addr2),
    ?assertEqual(
        [node1],
        nodes(),
        "store/4 is idempotent"
    ),

    ?assertEqual(
        [node1],
        nodes(visible)
    ),
    ?assertEqual(
        [node1],
        nodes(connected)
    ),
    ?assertExit(
        {noproc, _},
        nodes(known) %% data from membership, not our table
    ),
    ?assertEqual(
        [],
        nodes(hidden)
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
        connection_count(node1)
    ),
    ?assertEqual(
        2,
        connection_count(Spec1)
    ),
    ?assertEqual(
        2,
        connection_count(node1, undefined)
    ),
    ?assertEqual(
        2,
        connection_count(Spec1, undefined)
    ),
    ?assertEqual(
        0,
        connection_count(node1, unknown_channel)
    ),
    ?assertEqual(
        0,
        connection_count(Spec1, unknown_channel)
    ),

    ?assertEqual(
        0,
        connection_count(node2)
    ),
    ?assertEqual(
        0,
        connection_count(Spec2)
    ),
    ?assertEqual(
        0,
        connection_count(node2, undefined)
    ),
    ?assertEqual(
        0,
        connection_count(Spec2, undefined)
    ),
    ?assertEqual(
        0,
        connection_count(node2, unknown_channel)
    ),
    ?assertEqual(
        0,
        connection_count(Spec2, unknown_channel)
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
            #partisan_peer_connection{pid = Pid1},
            #partisan_peer_connection{pid = Pid2}
        ],
        connections(Spec1)
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
