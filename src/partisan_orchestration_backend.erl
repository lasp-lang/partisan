%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(partisan_orchestration_backend).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

-include("partisan.hrl").

-define(TIMEOUT, infinity).

%% API
-export([start_link/0,
         start_link/1,
         graph/0,
         tree/0,
         orchestration/0,
         orchestrated/0,
         was_connected/0,
         servers/0,
         nodes/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% debug functions
-export([debug_get_tree/2]).

-define(REFRESH_INTERVAL, 1000).
-define(REFRESH_MESSAGE,  refresh).

-define(BUILD_GRAPH_INTERVAL, 5000).
-define(BUILD_GRAPH_MESSAGE,  build_graph).

-define(ARTIFACT_INTERVAL, 1000).
-define(ARTIFACT_MESSAGE,  artifact).

-callback(clients(term()) -> term()).
-callback(servers(term()) -> term()).
-callback(download_artifact(term(), node()) -> term()).
-callback(upload_artifact(term(), node(), term()) -> term()).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

graph() ->
    gen_server:call(?MODULE, graph, ?TIMEOUT).

tree() ->
    gen_server:call(?MODULE, tree, ?TIMEOUT).

was_connected() ->
    gen_server:call(?MODULE, was_connected, ?TIMEOUT).

orchestration() ->
    gen_server:call(?MODULE, orchestration, ?TIMEOUT).

orchestrated() ->
    gen_server:call(?MODULE, orchestrated, ?TIMEOUT).

-spec servers() -> {ok, [node()]}.
servers() ->
    gen_server:call(?MODULE, servers, ?TIMEOUT).

-spec nodes() -> {ok, [node()]}.
nodes() ->
    gen_server:call(?MODULE, nodes, ?TIMEOUT).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #orchestration_strategy_state{}}.
init([]) ->
    OrchestrationStrategy = partisan_config:get(orchestration_strategy, ?DEFAULT_ORCHESTRATION_STRATEGY),
    PeerService = partisan_config:get(peer_service_manager, ?DEFAULT_PEER_SERVICE_MANAGER),

    case OrchestrationStrategy of
        undefined ->
            lager:info("Not using container orchestration; disabling."),
            ok;
        OrchestrationStrategy ->
            lager:info("OrchestrationStrategy: ~p", [OrchestrationStrategy]),

            case OrchestrationStrategy of
                partisan_marathon_orchestration_strategy ->
                    configure_s3_bucket(OrchestrationStrategy);
                _ ->
                    ok
            end,

            %% Only construct the graph and attempt to repair the graph
            %% from the designated server node.
            case partisan_config:get(tag, client) of
                server ->
                    lager:info("Tag is server."),
                    schedule_build_graph();
                client ->
                    lager:info("Tag is client."),
                    ok;
                undefined ->
                    lager:info("Tag is undefined."),
                    ok
            end,

            %% All nodes should upload artifacts.
            schedule_artifact_upload(),

            %% All nodes should attempt to refresh the membership.
            schedule_membership_refresh()
    end,

    Servers = case OrchestrationStrategy of
        undefined ->
            %% TODO: What am I?
            case partisan_config:get(lasp_server, undefined) of
                undefined ->
                    [];
                Server ->
                    [Server]
            end;
        _ ->
            []
    end,

    Nodes = case OrchestrationStrategy of
        undefined ->
            %% Assumes full membership.
            PeerServiceManager = partisan_config:get(peer_service_manager, ?DEFAULT_PEER_SERVICE_MANAGER),
            {ok, Members} = PeerServiceManager:members_for_orchestration(),
            Members;
        _ ->
            []
    end,

    Eredis = case OrchestrationStrategy of
        partisan_kubernetes_orchestration_strategy ->
            RedisHost = os:getenv("REDIS_SERVICE_HOST", "127.0.0.1"),
            RedisPort = os:getenv("REDIS_SERVICE_PORT", "6379"),
            {ok, C} = eredis:start_link(RedisHost, list_to_integer(RedisPort)),
            C;
        partisan_compose_orchestration_strategy ->
            RedisHost = os:getenv("REDIS_SERVICE_HOST", "127.0.0.1"),
            RedisPort = os:getenv("REDIS_SERVICE_PORT", "6379"),
            {ok, C} = eredis:start_link(RedisHost, list_to_integer(RedisPort)),
            C;
        _ ->
            undefined
    end,

    {ok, #orchestration_strategy_state{
                eredis=Eredis,
                nodes=Nodes,
                peer_service=PeerService,
                servers=Servers,
                is_connected=false,
                was_connected=false,
                orchestration_strategy=OrchestrationStrategy,
                attempted_nodes=sets:new(),
                graph=digraph:new(),
                tree=digraph:new()}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #orchestration_strategy_state{}) ->
    {reply, term(), #orchestration_strategy_state{}}.

%% @private
handle_call(nodes, _From, #orchestration_strategy_state{nodes=Nodes}=State) ->
    {reply, {ok, Nodes}, State};

handle_call(servers, _From, #orchestration_strategy_state{servers=Servers}=State) ->
    {reply, {ok, Servers}, State};

handle_call(orchestration, _From, #orchestration_strategy_state{orchestration_strategy=OrchestrationStrategy}=State) ->
    Result = case OrchestrationStrategy of
        undefined ->
            false;
        partisan_marathon_orchestration_strategy ->
            mesos;
        partisan_kubernetes_orchestration_strategy ->
            kubernetes
    end,
    {reply, {ok, Result}, State};

handle_call(orchestrated, _From, #orchestration_strategy_state{orchestration_strategy=OrchestrationStrategy}=State) ->
    Result = case OrchestrationStrategy of
        undefined ->
            false;
        _ ->
            true
    end,
    {reply, Result, State};

handle_call(was_connected, _From, #orchestration_strategy_state{was_connected=WasConnected}=State) ->
    {reply, {ok, WasConnected}, State};

handle_call(graph, _From, #orchestration_strategy_state{graph=Graph}=State) ->
    {Vertices, Edges} = vertices_and_edges(Graph),
    {reply, {ok, {Vertices, Edges}}, State};

handle_call(tree, _From, #orchestration_strategy_state{tree=Tree}=State) ->
    {Vertices, Edges} = vertices_and_edges(Tree),
    {reply, {ok, {Vertices, Edges}}, State};

handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #orchestration_strategy_state{}) -> {noreply, #orchestration_strategy_state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #orchestration_strategy_state{}) -> {noreply, #orchestration_strategy_state{}}.
handle_info(?REFRESH_MESSAGE, #orchestration_strategy_state{orchestration_strategy=OrchestrationStrategy,
                                     peer_service=PeerService,
                                     attempted_nodes=SeenNodes}=State) ->
    Tag = partisan_config:get(tag, client),
    PeerServiceManager = partisan_config:get(peer_service_manager, ?DEFAULT_PEER_SERVICE_MANAGER),

    Servers = OrchestrationStrategy:servers(State),
    % lager:info("Found servers: ~p", [sets:to_list(Servers)]),

    Clients = OrchestrationStrategy:clients(State),
    % lager:info("Found clients: ~p", [sets:to_list(Clients)]),

    % {ok, Membership} = PeerService:members_for_orchestration(),
    % lager:info("Membership (~p) ~p", [length(Membership), Membership]),

    %% Get list of nodes to connect to: this specialized logic isn't
    %% required when the node count is small, but is required with a
    %% larger node count to ensure the network stabilizes correctly
    %% because HyParView doesn't guarantee graph connectivity: it is
    %% only probabilistic.
    %%
    ToConnectNodes = case {Tag, PeerServiceManager} of
        {_, partisan_pluggable_peer_service_manager} ->
            %% By default, full connectivity; but, 
            %% connect all nodes to all other nodes for now.
            sets:union(Servers, Clients);
        {client, partisan_client_server_peer_service_manager} ->
            %% If we're a client, and we're in client/server mode, then
            %% always connect with the server.
            Servers;
        {server, partisan_client_server_peer_service_manager} ->
            %% If we're a server, and we're in client/server mode, then
            %% always initiate connections with clients.
            Clients;
        {client, partisan_hyparview_peer_service_manager} ->
            %% If we're the server, and we're in HyParView, clients will
            %% ask the server to join the overlay and force outbound
            %% conenctions to the clients.
            Servers;
        {server, partisan_hyparview_peer_service_manager} ->
            %% If we're in HyParView, and we're a client, only ever
            %% do nothing -- force all connection to go through the
            %% server.
            sets:new();
        {client, partisan_hyparview_xbot_peer_service_manager} ->
            %% If we're the server, and we're in HyParView, clients will
            %% ask the server to join the overlay and force outbound
            %% conenctions to the clients.
            Servers;
        {server, partisan_hyparview_xbot_peer_service_manager} ->
            %% If we're in HyParView, and we're a client, only ever
            %% do nothing -- force all connection to go through the
            %% server.
            sets:new();
        {Tag, PeerServiceManager} ->
            %% Catch all.
            lager:info("Invalid mode: not connecting to any nodes."),
            lager:info("Tag: ~p; PeerServiceManager: ~p",
                       [Tag, PeerServiceManager]),
            sets:new()
    end,

    %% Attempt to connect nodes that are not connected.
    AttemptedNodes = maybe_connect(PeerService, ToConnectNodes, SeenNodes),

    ServerNames = node_names(sets:to_list(Servers)),
    ClientNames = node_names(sets:to_list(Clients)),
    Nodes = ServerNames ++ ClientNames,

    schedule_membership_refresh(),

    {noreply, State#orchestration_strategy_state
                          {nodes=Nodes,
                          servers=ServerNames,
                          attempted_nodes=AttemptedNodes}};

handle_info(?ARTIFACT_MESSAGE, #orchestration_strategy_state{peer_service=PeerService}=State) ->
    %% Get current membership.
    {ok, Nodes} = PeerService:members_for_orchestration(),

    %% Store membership.
    Node = prefix(atom_to_list(node())),
    Membership = term_to_binary(Nodes),

    lager:info("Membership for node ~p: ~p", [Node, Nodes]),
    upload_artifact(State, Node, Membership),

    %% Store membership with node tag.
    Tag = partisan_config:get(tag, client),
    TaggedNode = prefix(atom_to_list(Tag) ++ "/" ++ atom_to_list(node())),
    Membership = term_to_binary(Nodes),
    upload_artifact(State, TaggedNode, Membership),

    schedule_artifact_upload(),

    {noreply, State};
handle_info(?BUILD_GRAPH_MESSAGE, #orchestration_strategy_state{
                                         orchestration_strategy=OrchestrationStrategy,
                                         graph=Graph0,
                                         tree=Tree0,
                                         peer_service=PeerService,
                                         was_connected=WasConnected0}=State) ->
    % _ = lager:info("Beginning graph analysis."),
    
    %% Delete existing graphs to prevent ets table leak.
    digraph:delete(Tree0),
    digraph:delete(Graph0),

    %% Get all running nodes, because we need the list of *everything*
    %% to analyze the graph for connectedness.
    Servers = OrchestrationStrategy:servers(State),
    lager:info("Found servers: ~p", [sets:to_list(Servers)]),

    Clients = OrchestrationStrategy:clients(State),
    lager:info("Found clients: ~p", [sets:to_list(Clients)]),

    ServerNames = node_names(sets:to_list(Servers)),
    ClientNames = node_names(sets:to_list(Clients)),
    Nodes = ServerNames ++ ClientNames,

    %% Build the tree.
    Tree = digraph:new(),

    case partisan_config:get(broadcast, false) of
        true ->
            try
                Root = hd(ServerNames),
                populate_tree(Root, Nodes, Tree)
            catch
                _:_ ->
                    ok
            end;
        false ->
            ok
    end,

    %% Build the graph.
    Graph = digraph:new(),
    Orphaned = populate_graph(State, Nodes, Graph),

    {SymmetricViews, VisitedNames} = breadth_first(node(), Graph, ordsets:new()),
    AllNodesVisited = length(Nodes) == length(VisitedNames),

    Connected = SymmetricViews andalso AllNodesVisited,

    case Connected of
        true ->
            lager:info("Graph is connected!"),
            ok;
        false ->
            lager:info("Visited ~p from ~p: ~p", [length(VisitedNames), node(), VisitedNames]),
            {ok, ServerMembership} = PeerService:members_for_orchestration(),
            lager:info("Membership (~p) ~p", [length(ServerMembership), ServerMembership]),
            lager:info("Graph is not connected!"),
            ok
    end,

    WasConnected = Connected orelse WasConnected0,

    case length(Orphaned) of
        0 ->
            ok;
        Length ->
            lager:info("~p isolated nodes: ~p", [Length, Orphaned])
    end,

    schedule_build_graph(),

    {noreply, State#orchestration_strategy_state{
                          is_connected=Connected,
                          was_connected=WasConnected,
                          graph=Graph,
                          tree=Tree}};

handle_info(Msg, State) ->
    lager:warning("Unhandled info messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #orchestration_strategy_state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #orchestration_strategy_state{}, term()) -> {ok, #orchestration_strategy_state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
maybe_connect(PeerService, Nodes, SeenNodes) ->
    %% If this is the first time you've seen the node, attempt to
    %% connect; only attempt to connect once, because node might be
    %% migrated to a passive view of the membership.
    %% If the node is isolated always try to connect.
    {ok, Membership0} = PeerService:members_for_orchestration(),
    Membership1 = Membership0 -- [node()],
    Isolated = length(Membership1) == 0,

    ToConnect = case Isolated of
        true ->
            Nodes;
        false ->
            sets:subtract(Nodes, SeenNodes)
    end,

    case sets:to_list(ToConnect) of
        [] ->
            ok;
        _ ->
            lager:info("Attempting to connect: ~p",
                       [sets:to_list(ToConnect)])
    end,

    %% Attempt connection to any new nodes.
    sets:fold(fun(Node, Acc) -> [connect(PeerService, Node) | Acc] end, [], ToConnect),

    %% Return list of seen nodes with the new node.
    sets:union(Nodes, SeenNodes).

%% @private
connect(PeerService, Node) ->
    PeerService:join(Node).

breadth_first(Root, Graph, Visited0) ->
    %% Check if every link is bidirectional
    %% If not, stop traversal
    In = ordsets:from_list(digraph:in_neighbours(Graph, Root)),
    Out = ordsets:from_list(digraph:out_neighbours(Graph, Root)),

    Visited1 = ordsets:union(Visited0, [Root]),

    case In == Out of
        true ->
            {SymmetricViews, VisitedNodes} = ordsets:fold(
                fun(Peer, {SymmetricViews0, VisitedNodes0}) ->
                    {SymmetricViews1, VisitedNodes1} = breadth_first(Peer, Graph, VisitedNodes0),
                    {SymmetricViews0 andalso SymmetricViews1, ordsets:union(VisitedNodes0, VisitedNodes1)}
                end,
                {true, Visited1},
                ordsets:subtract(Out, Visited1)
            ),
            {SymmetricViews, ordsets:union(VisitedNodes, Out)};
        false ->
            lager:info("Non symmetric views for node ~p. In ~p; Out ~p", [Root, In, Out]),
            {false, ordsets:new()}
    end.

%% @private
prefix(File) ->
    DeploymentIdentifier = partisan_config:get(deployment_identifier, undefined),
    DeploymentTimestamp = partisan_config:get(deployment_timestamp, 0),
    "partisan" ++ "/" ++ atom_to_list(DeploymentIdentifier) ++ "/" ++ integer_to_list(DeploymentTimestamp) ++ "/" ++ File.

%% @private
schedule_build_graph() ->
    %% Add random jitter.
    Jitter = rand_compat:uniform(?BUILD_GRAPH_INTERVAL),
    timer:send_after(?BUILD_GRAPH_INTERVAL + Jitter, ?BUILD_GRAPH_MESSAGE).

%% @private
schedule_artifact_upload() ->
    %% Add random jitter.
    Jitter = rand_compat:uniform(?ARTIFACT_INTERVAL),
    timer:send_after(?ARTIFACT_INTERVAL + Jitter, ?ARTIFACT_MESSAGE).

%% @private
schedule_membership_refresh() ->
    %% Add random jitter.
    Jitter = rand_compat:uniform(?REFRESH_INTERVAL),
    timer:send_after(?REFRESH_INTERVAL + Jitter, ?REFRESH_MESSAGE).

%% @private
vertices_and_edges(Graph) ->
    Vertices = digraph:vertices(Graph),
    Edges = lists:map(
        fun(Edge) ->
            {_E, V1, V2, _Label} = digraph:edge(Graph, Edge),
            {V1, V2}
        end,
        digraph:edges(Graph)
    ),
    {Vertices, Edges}.

%% @private
node_names([]) ->
    [];
node_names([#{name := Name}|T]) ->
    [Name|node_names(T)];
node_names([Name|T]) ->
    [Name|node_names(T)].

%% @private
populate_graph(State, Nodes, Graph) ->
    lists:foldl(
        fun(Node, OrphanedNodes) ->
            File = prefix(atom_to_list(Node)),
            try
                case download_artifact(State, File) of
                    undefined ->
                        OrphanedNodes;
                    Body ->
                        Membership = binary_to_term(Body),
                        case Membership of
                            [Node] ->
                                add_edges(Node, [], Graph),
                                [Node|OrphanedNodes];
                            _ ->
                                add_edges(Node, Membership, Graph),
                                OrphanedNodes
                        end
                end
            catch
                _:{aws_error, Error} ->
                    %% TODO: Move me inside download artifact.
                    add_edges(Node, [], Graph),
                    lager:info("Could not get graph object; ~p", [Error]),
                    OrphanedNodes
            end
        end,
        [],
        Nodes
    ).

%% @private
populate_tree(Root, Nodes, Tree) ->
    DebugTree = debug_get_tree(Root, Nodes),
    lists:foreach(
        fun({Node, Peers}) ->
            case Peers of
                down ->
                    add_edges(Node, [], Tree);
                {Eager, _Lazy} ->
                    add_edges(Node, Eager, Tree)
            end
        end,
        DebugTree
    ).

%% @private
add_edges(Name, Membership, Graph) ->
    %% Add node to graph.
    digraph:add_vertex(Graph, Name),

    lists:foldl(
        fun(N, _) ->
            %% Add node to graph.
            digraph:add_vertex(Graph, N),

            %% Add edge to graph.
            digraph:add_edge(Graph, Name, N)
        end,
        Graph,
        Membership
    ).

-spec debug_get_tree(node(), [node()]) ->
                            [{node(), {ordsets:ordset(node()), ordsets:ordset(node())}}].
debug_get_tree(Root, Nodes) ->
    [begin
         Peers = try plumtree_broadcast:debug_get_peers(Node, Root, 5000)
                 catch _:Error ->
                           lager:info("Call to node ~p to get root tree ~p failed: ~p", [Node, Root, Error]),
                           down
                 end,
         {Node, Peers}
     end || Node <- Nodes].

%% @private
configure_s3_bucket(OrchestrationStrategy) ->
    %% Configure erlcloud.
    S3Host = "s3-us-west-2.amazonaws.com",
    AccessKeyId = os:getenv("AWS_ACCESS_KEY_ID"),
    SecretAccessKey = os:getenv("AWS_SECRET_ACCESS_KEY"),
    erlcloud_s3:configure(AccessKeyId, SecretAccessKey, S3Host),

    %% Create S3 bucket.
    try
        BucketName = OrchestrationStrategy:bucket_name(),
        lager:info("Creating bucket: ~p", [BucketName]),
        ok = erlcloud_s3:create_bucket(BucketName),
        lager:info("Bucket created.")
    catch
        _:{aws_error, Error} ->
            lager:info("Bucket creation failed: ~p", [Error]),
            ok
    end,

    lager:info("S3 bucket creation succeeded.").

%% @private
upload_artifact(#orchestration_strategy_state{orchestration_strategy=OrchestrationStrategy}=State, Node, Membership) ->
    OrchestrationStrategy:upload_artifact(State, Node, Membership).

%% @private
download_artifact(#orchestration_strategy_state{orchestration_strategy=OrchestrationStrategy}=State, Node) ->
    OrchestrationStrategy:download_artifact(State, Node).