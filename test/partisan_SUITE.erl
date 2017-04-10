%% -------------------------------------------------------------------
%%
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
%%

-module(partisan_SUITE).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(APP, partisan).
-define(CLIENT_NUMBER, 3).
-define(PEER_PORT, 9000).

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, _Config) ->
    ct:pal("Beginning test case ~p", [Case]),

    _Config.

end_per_testcase(Case, _Config) ->
    ct:pal("Ending test case ~p", [Case]),

    _Config.

all() ->
    [
     default_manager_test,
     client_server_manager_test,
     static_manager_test,
     hyparview_manager_partition_test,
     hyparview_manager_high_active_test,
     hyparview_manager_low_active_test,
     hyparview_manager_high_client_test
    ].

%% ===================================================================
%% Tests.
%% ===================================================================

default_manager_test(Config) ->
    %% Use the default peer service manager.
    Manager = partisan_default_peer_service_manager,

    %% Specify servers.
    Servers = [server],

    %% Specify clients.
    Clients = client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = start(default_manager_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology.
    %%
    VerifyFun = fun({_, Node}) ->
            {ok, Members} = rpc:call(Node, Manager, members, []),
            SortedNodes = lists:usort([N || {_, N} <- Nodes]),
            SortedMembers = lists:usort(Members),
            case SortedMembers =:= SortedNodes of
                true ->
                    ok;
                false ->
                    ct:fail("Membership incorrect; node ~p should have ~p but has ~p", [Node, Nodes, Members])
            end
    end,

    %% Verify the membership is correct.
    lists:foreach(VerifyFun, Nodes),

    %% Stop nodes.
    stop(Nodes),

    ok.

client_server_manager_test(Config) ->
    %% Use the client/server peer service manager.
    Manager = partisan_client_server_peer_service_manager,

    %% Specify servers.
    Servers = [server_1, server_2],

    %% Specify clients.
    Clients = client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = start(client_server_manager_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology.
    %%
    VerifyFun = fun({Name, Node}) ->
            {ok, Members} = rpc:call(Node, Manager, members, []),

            %% If this node is a server, it should know about all nodes.
            SortedNodes = case lists:member(Name, Servers) of
                true ->
                    lists:usort([N || {_, N} <- Nodes]);
                false ->
                    %% Otherwise, it should only know about the server
                    %% and itself.
                    lists:usort(
                        lists:map(fun(S) ->
                                    proplists:get_value(S, Nodes)
                            end, Servers) ++ [Node])
            end,

            SortedMembers = lists:usort(Members),
            case SortedMembers =:= SortedNodes of
                true ->
                    ok;
                false ->
                    ct:fail("Membership incorrect; node ~p should have ~p but has ~p", [Node, Nodes, Members])
            end
    end,

    %% Verify the membership is correct.
    lists:foreach(VerifyFun, Nodes),

    ct:pal("Nodes: ~p", [Nodes]),

    %% Stop nodes.
    stop(Nodes),

    ok.

static_manager_test(Config) ->
    %% Use the static peer service manager.
    Manager = partisan_static_peer_service_manager,

    %% Specify clients.
    Clients = client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = start(client_server_manager_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology.
    %%
    VerifyFun = fun({Name, Node}) ->
            {ok, Members} = rpc:call(Node, Manager, members, []),
            Should = [N || {_, N} <- Nodes, N /= Name],

            case lists:usort(Members) =:= lists:usort(Should) of
                true ->
                    ok;
                false ->
                    ct:fail("Membership incorrect; node ~p should have ~p but has ~p", [Node, Should, Members])
            end
    end,

    %% Verify the membership is correct.
    lists:foreach(VerifyFun, Nodes),

    ct:pal("Nodes: ~p", [Nodes]),

    %% Stop nodes.
    stop(Nodes),

    ok.

hyparview_manager_partition_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Specify servers.
    Servers = [server],

    %% Specify clients.
    Clients = client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = start(hyparview_manager_partition_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {max_active_size, 5},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Create new digraph.
    Graph = digraph:new(),

    %% Verify connectedness.
    %%
    ConnectFun = fun({_, Node}) ->
        {ok, ActiveSet} = rpc:call(Node, Manager, active, []),
        Active = sets:to_list(ActiveSet),

        %% Add vertexes and edges.
        [connect(Graph, Node, N) || {N, _, _} <- Active]
                 end,

    %% Build the graph.
    lists:foreach(ConnectFun, Nodes),

    %% Verify connectedness.
    ConnectedFun = fun({_Name, Node}=Myself) ->
        lists:foreach(fun({_, N}) ->
            Path = digraph:get_short_path(Graph, Node, N),
            case Path of
                false ->
                    ct:fail("Graph is not connected!");
                _ ->
                    ok
            end
                      end, Nodes -- [Myself])
                   end,
    lists:foreach(ConnectedFun, Nodes),

    %% Verify symmetry.
    SymmetryFun = fun({_, Node1}) ->
        %% Get first nodes active set.
        {ok, ActiveSet1} = rpc:call(Node1, Manager, active, []),
        Active1 = sets:to_list(ActiveSet1),

        lists:foreach(fun({Node2, _, _}) ->
            %% Get second nodes active set.
            {ok, ActiveSet2} = rpc:call(Node2, Manager, active, []),
            Active2 = sets:to_list(ActiveSet2),

            case lists:member(Node1, [N || {N, _, _} <- Active2]) of
                true ->
                    ok;
                false ->
                    ct:fail("~p has ~p in it's view but ~p does not have ~p in its view",
                            [Node1, Node2, Node2, Node1])
            end
                      end, Active1)
                  end,
    lists:foreach(SymmetryFun, Nodes),

    ct:pal("Nodes: ~p", [Nodes]),

    %% Inject a partition.
    {_, PNode} = hd(Nodes),
    PFullNode = rpc:call(PNode, Manager, myself, []),

    {ok, Reference} = rpc:call(PNode, Manager, inject_partition, [PFullNode, 1]),
    ct:pal("Partition generated: ~p", [Reference]),

    %% Verify partition.
    PartitionVerifyFun = fun({_Name, Node}) ->
        {ok, Partitions} = rpc:call(Node, Manager, partitions, []),
        ct:pal("Partitions for node ~p: ~p", [Node, Partitions]),
        {ok, ActiveSet} = rpc:call(Node, Manager, active, []),
        Active = sets:to_list(ActiveSet),
        ct:pal("Peers for node ~p: ~p", [Node, Active]),
        PartitionedPeers = [Peer || {_Reference, Peer} <- Partitions],
        case PartitionedPeers == Active of
            true ->
                ok;
            false ->
                ct:fail("Partitions incorrectly generated.")
        end
    end,
    lists:foreach(PartitionVerifyFun, Nodes),

    %% Resolve partition.
    ok = rpc:call(PNode, Manager, resolve_partition, [Reference]),
    ct:pal("Partition resolved: ~p", [Reference]),

    timer:sleep(1000),

    %% Verify resolved partition.
    ResolveVerifyFun = fun({_Name, Node}) ->
        {ok, Partitions} = rpc:call(Node, Manager, partitions, []),
        ct:pal("Partitions for node ~p: ~p", [Node, Partitions]),
        case Partitions == [] of
            true ->
                ok;
            false ->
                ct:fail("Partitions incorrectly resolved.")
        end
    end,
    lists:foreach(ResolveVerifyFun, Nodes),

    %% Stop nodes.
    stop(Nodes),

    ok.

hyparview_manager_high_active_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Specify servers.
    Servers = [server],

    %% Specify clients.
    Clients = client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = start(hyparview_manager_high_active_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {max_active_size, 5},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Create new digraph.
    Graph = digraph:new(),

    %% Verify connectedness.
    %%
    ConnectFun = fun({_, Node}) ->
        {ok, ActiveSet} = rpc:call(Node, Manager, active, []),
        Active = sets:to_list(ActiveSet),

        %% Add vertexes and edges.
        [connect(Graph, Node, N) || {N, _, _} <- Active]
                 end,

    %% Build the graph.
    lists:foreach(ConnectFun, Nodes),

    %% Verify connectedness.
    ConnectedFun = fun({_Name, Node}=Myself) ->
        lists:foreach(fun({_, N}) ->
            Path = digraph:get_short_path(Graph, Node, N),
            case Path of
                false ->
                    ct:fail("Graph is not connected!");
                _ ->
                    ok
            end
                      end, Nodes -- [Myself])
                   end,
    lists:foreach(ConnectedFun, Nodes),

    %% Verify symmetry.
    SymmetryFun = fun({_, Node1}) ->
        %% Get first nodes active set.
        {ok, ActiveSet1} = rpc:call(Node1, Manager, active, []),
        Active1 = sets:to_list(ActiveSet1),

        lists:foreach(fun({Node2, _, _}) ->
            %% Get second nodes active set.
            {ok, ActiveSet2} = rpc:call(Node2, Manager, active, []),
            Active2 = sets:to_list(ActiveSet2),

            case lists:member(Node1, [N || {N, _, _} <- Active2]) of
                true ->
                    ok;
                false ->
                    ct:fail("~p has ~p in it's view but ~p does not have ~p in its view",
                            [Node1, Node2, Node2, Node1])
            end
                      end, Active1)
                  end,
    lists:foreach(SymmetryFun, Nodes),

    %% Stop nodes.
    stop(Nodes),

    ok.

hyparview_manager_low_active_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Start nodes.
    MaxActiveSize = 3,

    Servers = [server],

    Clients = client_list(?CLIENT_NUMBER),

    Nodes = start(hyparview_manager_low_active_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {max_active_size, MaxActiveSize},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(4000),

    %% Create new digraph.
    Graph = digraph:new(),

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology
    %% when the active setting is high.
    %%
    ConnectFun = fun({_, Node}) ->
            {ok, ActiveSet} = rpc:call(Node, Manager, active, []),
            Active = sets:to_list(ActiveSet),

            %% Add vertexes and edges.
            [connect(Graph, Node, N) || {N, _, _} <- Active]
    end,

    %% Verify the membership is correct.
    lists:foreach(ConnectFun, Nodes),

    %% Verify connectedness.
    ConnectedFun = fun({_Name, Node}=Myself) ->
                        lists:foreach(fun({_, N}) ->
                                           Path = digraph:get_short_path(Graph, Node, N),
                                           case Path of
                                               false ->
                                                   ct:fail("Graph is not connected!");
                                               _ ->
                                                   ok
                                           end
                                      end, Nodes -- [Myself])
                 end,
    lists:foreach(ConnectedFun, Nodes),

    %% Verify symmetry.
    SymmetryFun = fun({_, Node1}) ->
                          %% Get first nodes active set.
                          {ok, ActiveSet1} = rpc:call(Node1, Manager, active, []),
                          Active1 = sets:to_list(ActiveSet1),

                          lists:foreach(fun({Node2, _, _}) ->
                                                %% Get second nodes active set.
                                                {ok, ActiveSet2} = rpc:call(Node2, Manager, active, []),
                                                Active2 = sets:to_list(ActiveSet2),

                                                case lists:member(Node1, [N || {N, _, _} <- Active2]) of
                                                    true ->
                                                        ok;
                                                    false ->
                                                        ct:fail("~p has ~p in it's view but ~p does not have ~p in its view",
                                                                [Node1, Node2, Node2, Node1])
                                                end
                                        end, Active1)
                  end,
    lists:foreach(SymmetryFun, Nodes),

    %% Stop nodes.
    stop(Nodes),

    ok.

hyparview_manager_high_client_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Start clients,.
    Clients = client_list(11),

    %% Start servers.
    Servers = [server],

    Nodes = start(hyparview_manager_low_active_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(9000),

    %% Create new digraph.
    Graph = digraph:new(),

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology
    %% when the active setting is high.
    %%
    ConnectFun = fun({_, Node}) ->
        {ok, ActiveSet} = rpc:call(Node, Manager, active, []),
        Active = sets:to_list(ActiveSet),

        %% Add vertexes and edges.
        [connect(Graph, Node, N) || {N, _, _} <- Active]
                 end,

    %% Verify the membership is correct.
    lists:foreach(ConnectFun, Nodes),

    %% Verify connectedness.
    ConnectedFun = fun({_Name, Node}=Myself) ->
        lists:foreach(fun({_, N}) ->
            Path = digraph:get_short_path(Graph, Node, N),
            case Path of
                false ->
                    ct:fail("Graph is not connected!");
                _ ->
                    ok
            end
                      end, Nodes -- [Myself])
                   end,
    lists:foreach(ConnectedFun, Nodes),

    %% Verify symmetry.
    SymmetryFun = fun({_, Node1}) ->
        %% Get first nodes active set.
        {ok, ActiveSet1} = rpc:call(Node1, Manager, active, []),
        Active1 = sets:to_list(ActiveSet1),

        lists:foreach(fun({Node2, _, _}) ->
            %% Get second nodes active set.
            {ok, ActiveSet2} = rpc:call(Node2, Manager, active, []),
            Active2 = sets:to_list(ActiveSet2),

            case lists:member(Node1, [N || {N, _, _} <- Active2]) of
                true ->
                    ok;
                false ->
                    ct:fail("~p has ~p in it's view but ~p does not have ~p in its view",
                            [Node1, Node2, Node2, Node1])
            end
                      end, Active1)
                  end,
    lists:foreach(SymmetryFun, Nodes),

    %% Stop nodes.
    stop(Nodes),

    ok.


%% ===================================================================
%% Internal functions.
%% ===================================================================

%% @private
start(_Case, _Config, Options) ->
    %% Launch distribution for the test runner.
    ct:pal("Launching Erlang distribution..."),

    os:cmd(os:find_executable("epmd") ++ " -daemon"),
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end,

    %% Load sasl.
    application:load(sasl),
    ok = application:set_env(sasl,
                             sasl_error_logger,
                             false),
    application:start(sasl),

    %% Load lager.
    {ok, _} = application:ensure_all_started(lager),

    Servers = proplists:get_value(servers, Options, []),
    Clients = proplists:get_value(clients, Options, []),

    NodeNames = lists:flatten(Servers ++ Clients),

    %% Start all nodes.
    InitializerFun = fun(Name) ->
                            ct:pal("Starting node: ~p", [Name]),

                            NodeConfig = [{monitor_master, true},
                                          {startup_functions, [{code, set_path, [codepath()]}]}],

                            case ct_slave:start(Name, NodeConfig) of
                                {ok, Node} ->
                                    {Name, Node};
                                Error ->
                                    ct:fail(Error)
                            end
                     end,
    Nodes = lists:map(InitializerFun, NodeNames),

    %% Load applications on all of the nodes.
    LoaderFun = fun({_Name, Node}) ->
                            ct:pal("Loading applications on node: ~p", [Node]),

                            PrivDir = code:priv_dir(?APP),
                            NodeDir = filename:join([PrivDir, "lager", Node]),

                            %% Manually force sasl loading, and disable the logger.
                            ok = rpc:call(Node, application, load, [sasl]),
                            ok = rpc:call(Node, application, set_env,
                                          [sasl, sasl_error_logger, false]),
                            ok = rpc:call(Node, application, start, [sasl]),

                            ok = rpc:call(Node, application, load, [partisan]),
                            ok = rpc:call(Node, application, load, [lager]),
                            ok = rpc:call(Node, application, set_env, [sasl,
                                                                       sasl_error_logger,
                                                                       false]),
                            ok = rpc:call(Node, application, set_env, [lager,
                                                                       log_root,
                                                                       NodeDir])
                     end,
    lists:map(LoaderFun, Nodes),

    %% Configure settings.
    ConfigureFun = fun({Name, Node}) ->
            %% Configure the peer service.
            PeerService = proplists:get_value(partisan_peer_service_manager, Options),
            ct:pal("Setting peer service maanger on node ~p to ~p", [Node, PeerService]),
            ok = rpc:call(Node, partisan_config, set,
                          [partisan_peer_service_manager, PeerService]),

            MaxActiveSize = proplists:get_value(max_active_size, Options, 5),
            ok = rpc:call(Node, partisan_config, set,
                          [max_active_size, MaxActiveSize]),

            Servers = proplists:get_value(servers, Options, []),
            Clients = proplists:get_value(clients, Options, []),

            %% Configure servers.
            case lists:member(Name, Servers) of
                true ->
                    ok = rpc:call(Node, partisan_config, set, [tag, server]);
                false ->
                    ok
            end,

            %% Configure clients.
            case lists:member(Name, Clients) of
                true ->
                    ok = rpc:call(Node, partisan_config, set, [tag, client]);
                false ->
                    ok
            end
    end,
    lists:map(ConfigureFun, Nodes),

    ct:pal("Starting nodes."),

    StartFun = fun({_Name, Node}) ->
                        %% Start partisan.
                        {ok, _} = rpc:call(Node, application, ensure_all_started, [partisan])
                   end,
    lists:map(StartFun, Nodes),

    ct:pal("Clustering nodes."),
    lists:map(fun(Node) -> cluster(Node, Nodes, Options) end, Nodes),

    ct:pal("Partisan fully initialized."),

    Nodes.

%% @private
omit(OmitNameList, Nodes0) ->
    FoldFun = fun({Name, _Node} = N, Nodes) ->
                    case lists:member(Name, OmitNameList) of
                        true ->
                            Nodes;
                        false ->
                            Nodes ++ [N]
                    end
              end,
    lists:foldl(FoldFun, [], Nodes0).

%% @private
codepath() ->
    lists:filter(fun filelib:is_dir/1, code:get_path()).

%% @private
%%
%% We have to cluster each node with all other nodes to compute the
%% correct overlay: for instance, sometimes you'll want to establish a
%% client/server topology, which requires all nodes talk to every other
%% node to correctly compute the overlay.
%%
cluster({Name, _Node} = Myself, Nodes, Options) when is_list(Nodes) ->
    Manager = proplists:get_value(partisan_peer_service_manager, Options),

    Servers = proplists:get_value(servers, Options, []),
    Clients = proplists:get_value(clients, Options, []),

    AmIServer = lists:member(Name, Servers),
    AmIClient = lists:member(Name, Clients),

    OtherNodes = case Manager of
                     partisan_default_peer_service_manager ->
                         %% Omit just ourselves.
                         omit([Name], Nodes);
                     partisan_client_server_peer_service_manager ->
                         case {AmIServer, AmIClient} of
                             {true, false} ->
                                %% If I'm a server, I connect to both
                                %% clients and servers!
                                omit([Name], Nodes);
                             {false, true} ->
                                %% I'm a client, pick servers.
                                omit(Clients, Nodes);
                             {_, _} ->
                                omit([Name], Nodes)
                         end;
                     partisan_static_peer_service_manager ->
                         %% Connect to everyone except me.
                         omit([Name], Nodes);
                     partisan_hyparview_peer_service_manager ->
                        case {AmIServer, AmIClient} of
                            {true, false} ->
                               %% If I'm a server, I connect to both
                               %% clients and servers!
                               omit([Name], Nodes);
                            {false, true} ->
                               %% I'm a client, pick servers.
                               omit(Clients, Nodes);
                            {_, _} ->
                               omit([Name], Nodes)
                        end
                 end,
    lists:map(fun(OtherNode) -> cluster(Myself, OtherNode) end, OtherNodes).
cluster({_, Node}, {_, OtherNode}) ->
    PeerPort = rpc:call(OtherNode,
                        partisan_config,
                        get,
                        [peer_port, ?PEER_PORT]),
    ct:pal("Joining node: ~p to ~p at port ~p", [Node, OtherNode, PeerPort]),
    ok = rpc:call(Node,
                  partisan_peer_service,
                  join,
                  [{OtherNode, {127, 0, 0, 1}, PeerPort}]).

%% @private
stop(Nodes) ->
    StopFun = fun({Name, _Node}) ->
        case ct_slave:stop(Name) of
            {ok, _} ->
                ok;
            Error ->
                ct:fail(Error)
        end
    end,
    lists:map(StopFun, Nodes),
    ok.

%% @private
connect(G, N1, N2) ->
    %% Add vertex for neighboring node.
    digraph:add_vertex(G, N1),
    % ct:pal("Adding vertex: ~p", [N1]),

    %% Add vertex for neighboring node.
    digraph:add_vertex(G, N2),
    % ct:pal("Adding vertex: ~p", [N2]),

    %% Add edge to that node.
    digraph:add_edge(G, N1, N2),
    % ct:pal("Adding edge from ~p to ~p", [N1, N2]),

    ok.

%% @private
node_list(ClientNumber) ->
    Clients = client_list(ClientNumber),
    [server | Clients].

%% @private
client_list(0) -> [];
client_list(N) -> lists:append(client_list(N - 1),
                               [list_to_atom("client_" ++ integer_to_list(N))]).
