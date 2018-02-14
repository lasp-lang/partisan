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

-include("partisan.hrl").

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0,
         groups/0,
         init_per_group/2]).

%% tests
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-define(GOSSIP_INTERVAL, 1000).
-define(TIMEOUT, 10000).
-define(CLIENT_NUMBER, 3).

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, Config) ->
    ct:pal("Beginning test case ~p", [Case]),

    [{hash, erlang:phash2({Case, Config})}|Config].

end_per_testcase(Case, _Config) ->
    ct:pal("Ending test case ~p", [Case]),

    _Config.

init_per_group(with_libp2p, Config) ->
    [{transport, libp2p}] ++ Config;
init_per_group(with_disterl, Config) ->
    [{disterl, true}] ++ Config;
init_per_group(with_broadcast, Config) ->
    [{broadcast, true}] ++ Config;
init_per_group(with_partition_key, Config) ->
    [{forward_options, [{partition_key, 1}]}] ++ Config;
init_per_group(with_binary_padding, Config) ->
    [{binary_padding, true}] ++ Config;
init_per_group(with_sync_join, Config) ->
    [{parallelism, 1}, {sync_join, true}] ++ Config;
init_per_group(with_monotonic_channels, Config) ->
    [{parallelism, 1}, {channels, [{monotonic, vnode}, gossip]}] ++ Config;
init_per_group(with_channels, Config) ->
    [{parallelism, 1}, {channels, [vnode, gossip]}] ++ Config;
init_per_group(with_parallelism, Config) ->
    parallelism() ++ [{channels, ?CHANNELS}] ++ Config;
init_per_group(with_no_channels, Config) ->
    [{parallelism, 1}, {channels, []}] ++ Config;
init_per_group(with_tls, Config) ->
    TLSOpts = make_certs(Config),
    [{parallelism, 1}, {tls, true}] ++ TLSOpts ++ Config;
init_per_group(_, Config) ->
    [{parallelism, 1}] ++ Config.

end_per_group(_, _Config) ->
    ok.

all() ->
    [
     {group, default, [parallel],
      [{simple, [shuffle]},
       {hyparview, [shuffle]}
      ]},

     {group, with_tls, [parallel]},

     {group, with_parallelism, [parallel]},

     {group, with_libp2p, [parallel]},

     {group, with_disterl, [parallel]},

     {group, with_channels, [parallel]},

     {group, with_no_channels, [parallel]},
     
     {group, with_monotonic_channels, [parallel]},

     {group, with_sync_join, [parallel]},

     {group, with_binary_padding, [parallel]},

     {group, with_partition_key, [parallel]},

     {group, with_broadcast, [parallel]}
    ].

groups() ->
    [
     {default, [],
      [{group, simple},
       {group, hyparview}
      ]},

     {simple, [],
      [default_manager_test,
       libp2p_test,
       leave_test,
       on_down_test,
       client_server_manager_test,
       %% amqp_manager_test,
       performance_test,
       rejoin_test]},

     {hyparview, [],
      [%% hyparview_manager_partition_test,
       hyparview_manager_high_active_test,
       hyparview_manager_low_active_test,
       hyparview_manager_high_client_test]},

     {with_tls, [],
      [default_manager_test]},

     {with_parallelism, [],
      [default_manager_test,
       performance_test]},

     {with_libp2p, [],
      [default_manager_test]},

     {with_disterl, [],
      [performance_test]},
     
     {with_channels, [],
      [default_manager_test]},

     {with_no_channels, [],
      [default_manager_test]},

     {with_monotonic_channels, [],
      [default_manager_test]},

     {with_sync_join, [],
      [default_manager_test]},

     {with_binary_padding, [],
      [default_manager_test]},

     {with_partition_key, [],
      [default_manager_test]},

     {with_broadcast, [],
      [hyparview_manager_low_active_test]}
    ].

%% ===================================================================
%% Tests.
%% ===================================================================

amqp_manager_test(Config) ->
    %% Use the amqp peer service manager.
    Manager = partisan_amqp_peer_service_manager,

    %% Specify servers.
    Servers = node_list(2, "server", Config), %% [server_1, server_2],

    %% Specify clients.
    Clients = node_list(?CLIENT_NUMBER, "client", Config), %% client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = start(amqp_manager_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for gossip.
    timer:sleep(10000),

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology.
    %%
    VerifyFun = fun({_Name, Node}) ->
            {ok, Members} = rpc:call(Node, Manager, members, []),
            SortedNodes = lists:usort([N || {_, N} <- Nodes]),
            SortedMembers = lists:usort(Members),
            case SortedMembers =:= SortedNodes of
                true ->
                    ok;
                false ->
                    ct:fail("Membership incorrect; node ~p should have ~p but has ~p", [Node, SortedNodes, SortedMembers])
            end
    end,

    %% Verify the membership is correct.
    lists:foreach(VerifyFun, Nodes),

    %% Verify forward message functionality.
    lists:foreach(fun({_Name, Node}) ->
                    ok = check_forward_message(Node, Manager, Nodes)
                  end, Nodes),

    %% Stop nodes.
    stop(Nodes),

    ok.

on_down_test(Config) ->
    %% Use the default peer service manager.
    Manager = partisan_default_peer_service_manager,

    %% Specify servers.
    Servers = node_list(1, "server", Config),

    %% Specify clients.
    Clients = node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = start(on_down_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Test on_down callback.
    [{_, _}, {_, _}, {Name3, Node3}, {_, Node4}] = Nodes,

    Self = self(),
    Callback = fun() ->
        Self ! down
    end,

    ok = rpc:call(Node4, Manager, on_down, [Node3, Callback]),

    %% Shutdown, wait for shutdown...
    {ok, Node3} = ct_slave:stop(Name3),
    timer:sleep(10000),

    %% Assert we receive the response.
    receive
        down ->
            ok
    after 
        ?TIMEOUT ->
            ct:fail("Didn't receive down callback.")
    end,

    %% Stop nodes.
    stop(Nodes),

    ok.

rejoin_test(Config) ->
    case os:getenv("TRAVIS") of
        false ->
            %% Use the default peer service manager.
            Manager = partisan_default_peer_service_manager,

            %% Specify servers.
            Servers = node_list(1, "server", Config),

            %% Specify clients.
            Clients = node_list(?CLIENT_NUMBER, "client", Config),

            %% Start nodes.
            Nodes = start(rejoin_test, Config,
                        [{partisan_peer_service_manager, Manager},
                        {servers, Servers},
                        {clients, Clients}]),

            verify_leave(Nodes, Manager),
            
            %% Join a node from the cluster.
            [{_, _}, {_, Node2}, {_, _}, {_, Node4}] = Nodes,
            ct:pal("Joining node ~p to the cluster.", [Node4]),
            ok = rpc:call(Node2, partisan_peer_service, join, [Node4]),
            
            %% Pause for gossip interval * node exchanges + gossip interval for full convergence.
            timer:sleep(?GOSSIP_INTERVAL * length(Nodes) + ?GOSSIP_INTERVAL),

            %% TODO: temporary
            timer:sleep(10000),

            %% Verify membership.
            %%
            %% Every node should know about every other node in this topology.
            %%
            VerifyJoinFun = fun({_, Node}) ->
                    {ok, Members} = rpc:call(Node, Manager, members, []),
                    SortedNodes = lists:usort([N || {_, N} <- Nodes]),
                    SortedMembers = lists:usort(Members),
                    case SortedMembers =:= SortedNodes of
                        true ->
                            true;
                        false ->
                            ct:pal("Membership incorrect; node ~p should have ~p but has ~p",
                                [Node, SortedNodes, SortedMembers]),
                            {false, {Node, SortedNodes, SortedMembers}}
                    end
            end,

            %% Verify the membership is correct.
            lists:foreach(fun(Node) ->
                                VerifyNodeFun = fun() -> VerifyJoinFun(Node) end,

                                case wait_until(VerifyNodeFun, 60 * 2, 100) of
                                    ok ->
                                        ok;
                                    {fail, {false, {Node, Expected, Contains}}} ->
                                        ct:fail("Membership incorrect; node ~p should have ~p but has ~p",
                                                [Node, Expected, Contains])
                                end
                        end, Nodes),

            %% Stop nodes.
            stop(Nodes);

        _ ->
            ok

        end,

        ok.

libp2p_test(Config) ->
    %% Use the default peer service manager.
    Manager = partisan_default_peer_service_manager,

    %% Specify servers.
    Servers = node_list(1, "server", Config),

    %% Specify clients.
    Clients = node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = start(libp2p_test, Config,
                  [{partisan_peer_service_manager, Manager},
                  {servers, Servers},
                  {clients, Clients}]),

    %% Ensure ranch is started.
    lists:foreach(fun({_Name, Node}) ->
        {ok, _} = rpc:call(Node, application, ensure_all_started, [ranch])
    end, Nodes),

    %% Start swarms.
    Swarms = lists:map(fun({_Name, Node}) ->
        {ok, Pid} = rpc:call(Node, libp2p_swarm, start, ["/ip4/127.0.0.1/tcp/0"]),
        {Node, Pid}
    end, Nodes),
    ct:pal("Swarms are: ~p", [Swarms]),

    %% Collect listen addrs.
    ListenAddrs = lists:map(fun({_Name, Node}) ->
        SwarmPid = proplists:get_value(Node, Swarms),
        ListenAddrs = rpc:call(Node, libp2p_swarm, listen_addrs, [SwarmPid]),
        ct:pal("Node: ~p ListenAddrs: ~p", [Node, ListenAddrs]),
        {Node, ListenAddrs}
    end, Nodes),
    ct:pal("ListenAddrs are: ~p", [ListenAddrs]),

    %% Create a new session.
    [{_Name1, Node1}, {_Name2, Node2} | _] = Nodes,
    Node2ListenAddrs = proplists:get_value(Node2, ListenAddrs),
    Node1Swarm = proplists:get_value(Node2, Swarms),
    {ok, Session1} = rpc:call(Node1, libp2p_swarm, connect, [Node1Swarm, hd(Node2ListenAddrs)]),
    ct:pal("Session ~p created from ~p to ~p", [Session1, Node1, Node2]),

    %% Ping stream.
    ct:pal("Ping sent for session: ~p", [Session1]),
    ?assertMatch({ok, _}, rpc:call(Node1, libp2p_session, ping, [Session1])),

    %% Create a new stream.
    {ok, Stream1} = rpc:call(Node1, libp2p_session, open, [Session1]),
    ct:pal("Stream ~p created for session ~p", [Stream1, Session1]),

    %% Send a message.
    ct:pal("Sending a message from ~p to ~p", [Node1, Node2]),
    ok = rpc:call(Node1, libp2p_connection, send, [Stream1, <<"test-data">>]),

    %% Closing the stream.
    ct:pal("Closing stream: ~p", [Stream1]),
    ?assertEqual(ok, rpc:call(Node1, libp2p_connection, close, [Stream1])),

    %% Stop nodes.
    stop(Nodes),

    ok.

leave_test(Config) ->
    case os:getenv("TRAVIS") of
        false ->
            %% Use the default peer service manager.
            Manager = partisan_default_peer_service_manager,

            %% Specify servers.
            Servers = node_list(1, "server", Config),

            %% Specify clients.
            Clients = node_list(?CLIENT_NUMBER, "client", Config),

            %% Start nodes.
            Nodes = start(leave_test, Config,
                        [{partisan_peer_service_manager, Manager},
                        {servers, Servers},
                        {clients, Clients}]),

            verify_leave(Nodes, Manager),

            %% Stop nodes.
            stop(Nodes);
        _ ->
            ok
    end,

    ok.

performance_test(Config) ->
    %% Use the default peer service manager.
    Manager = partisan_default_peer_service_manager,

    %% Specify servers.
    Servers = node_list(1, "server", Config),

    %% Specify clients.
    Clients = node_list(1, "client", Config),

    %% Start nodes.
    Nodes = start(default_manager_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    [{_, Node1}, {_, Node2}] = Nodes,

    %% One process per connection.
    Concurrency = case os:getenv("CONCURRENCY", "1") of
        undefined ->
            1;
        C ->
            list_to_integer(C)
    end,

    %% Latency.
    Latency = case os:getenv("LATENCY", "0") of
        undefined ->
            0;
        L ->
            list_to_integer(L)
    end,

    %% Size.
    Size = case os:getenv("SIZE", "0") of
        undefined ->
            0;
        S ->
            list_to_integer(S)
    end,

    %% Parallelism.
    Parallelism = case rpc:call(Node1, partisan_config, get, [parallelism]) of
        undefined ->
            1;
        P ->
            P
    end,
        
    NumMessages = 1000,
    BenchPid = self(),
    BytesSize = Size * 1024,

    %% Prime a binary at each node.
    ct:pal("Generating binaries!"),
    EchoBinary = rand_bits(BytesSize * 8),

    %% Spawn processes to send receive messages on node 1.
    ct:pal("Spawning processes."),
    SenderPids = lists:map(fun(PartitionKey) ->
        ReceiverFun = fun() ->
            receiver(Manager, BenchPid, NumMessages)
        end,
        ReceiverPid = rpc:call(Node2, erlang, spawn, [ReceiverFun]),

        SenderFun = fun() ->
            init_sender(EchoBinary, Manager, Node2, ReceiverPid, PartitionKey, NumMessages)
        end,
        SenderPid = rpc:call(Node1, erlang, spawn, [SenderFun]),
        SenderPid
    end, lists:seq(1, Concurrency)),

    %% Start bench.
    ProfileFun = fun() ->
        %% Start sending.
        lists:foreach(fun(SenderPid) ->
            SenderPid ! start
        end, SenderPids),

        %% Wait for them all.
        bench_receiver(Concurrency)
    end,
    {Time, _Value} = timer:tc(ProfileFun),

    %% Write results.
    RootDir = root_dir(Config),
    ResultsFile = RootDir ++ "results.csv",
    ct:pal("Writing results to: ~p", [ResultsFile]),
    {ok, FileHandle} = file:open(ResultsFile, [append]),
    Backend = case rpc:call(Node1, partisan_config, get, [disterl]) of
        true ->
            disterl;
        _ ->
            partisan
    end,
    io:format(FileHandle, "~p,~p,~p,~p,~p,~p,~p~n", [Backend, Concurrency, Parallelism, BytesSize, NumMessages, Latency, Time]),
    file:close(FileHandle),

    ct:pal("Time: ~p", [Time]),

    %% Stop nodes.
    stop(Nodes),

    ok.

default_manager_test(Config) ->
    %% Use the default peer service manager.
    Manager = partisan_default_peer_service_manager,

    %% Specify servers.
    Servers = node_list(1, "server", Config),

    %% Specify clients.
    Clients = node_list(?CLIENT_NUMBER, "client", Config),

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
                    true;
                false ->
                    ct:pal("Membership incorrect; node ~p should have ~p but has ~p",
                           [Node, SortedNodes, SortedMembers]),
                    {false, {Node, SortedNodes, SortedMembers}}
            end
    end,

    %% Verify the membership is correct.
    lists:foreach(fun(Node) ->
                          VerifyNodeFun = fun() -> VerifyFun(Node) end,

                          case wait_until(VerifyNodeFun, 60 * 2, 100) of
                              ok ->
                                  ok;
                              {fail, {false, {Node, Expected, Contains}}} ->
                                 ct:fail("Membership incorrect; node ~p should have ~p but has ~p",
                                         [Node, Expected, Contains])
                          end
                  end, Nodes),

    %% Verify forward message functionality.
    lists:foreach(fun({_Name, Node}) ->
                    ok = check_forward_message(Node, Manager, Nodes)
                  end, Nodes),

    %% Verify parallelism.
    ConfigParallelism = proplists:get_value(parallelism, Config, ?PARALLELISM),
    ct:pal("Configured parallelism: ~p", [ConfigParallelism]),

    %% Verify channels.
    ConfigChannels = proplists:get_value(channels, Config, ?CHANNELS),
    ct:pal("Configured channels: ~p", [ConfigChannels]),

    ConnectionsFun = fun(Node) ->
                             Connections = rpc:call(Node,
                                      partisan_default_peer_service_manager,
                                      connections,
                                      []),
                             % ct:pal("Connections: ~p~n", [Connections]),
                             Connections
                     end,

    VerifyConnectionsFun = fun(Node, Channel, Parallelism) ->
                                %% Get list of connections.
                                {ok, Connections} = ConnectionsFun(Node),

                                %% Verify we have enough connections.
                                dict:fold(fun(_N, Active, Acc) ->
                                    Filtered = lists:filter(fun({_, C, _}) -> 
                                        case C of
                                            Channel ->
                                                true;
                                            _ ->
                                                false
                                        end
                                    end, Active),

                                    case length(Filtered) == Parallelism of
                                        true ->
                                            Acc andalso true;
                                        false ->
                                            Acc andalso false
                                    end
                                end, true, Connections)
                          end,

    lists:foreach(fun({_Name, Node}) ->
                        %% Get enabled parallelism.
                        Parallelism = rpc:call(Node, partisan_config, get, [parallelism, ?PARALLELISM]),
                        ct:pal("Parallelism is: ~p", [Parallelism]),

                        %% Get enabled channels.
                        Channels = rpc:call(Node, partisan_config, get, [channels, ?CHANNELS]),
                        ct:pal("Channels are: ~p", [Channels]),

                        lists:foreach(fun(Channel) ->
                            %% Generate fun.
                            VerifyConnectionsNodeFun = fun() ->
                                                            VerifyConnectionsFun(Node, Channel, Parallelism)
                                                    end,

                            %% Wait until connections established.
                            case wait_until(VerifyConnectionsNodeFun, 60 * 2, 100) of
                                ok ->
                                    ok;
                                _ ->
                                    ct:fail("Not enough connections have been opened; need: ~p", [Parallelism])
                            end
                        end, Channels)
                  end, Nodes),

    %% Stop nodes.
    stop(Nodes),

    ok.

client_server_manager_test(Config) ->
    %% Use the client/server peer service manager.
    Manager = partisan_client_server_peer_service_manager,

    %% Specify servers.
    Servers = node_list(2, "server", Config), %% [server_1, server_2],

    %% Specify clients.
    Clients = node_list(?CLIENT_NUMBER, "client", Config), %% client_list(?CLIENT_NUMBER),

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

    %% Verify forward message functionality.
    lists:foreach(fun({_Name, Node}) ->
                    ok = check_forward_message(Node, Manager, Nodes)
                  end, Nodes),

    %% Stop nodes.
    stop(Nodes),

    ok.

hyparview_manager_partition_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Specify servers.
    Servers = node_list(1, "server", Config), %% [server],

    %% Specify clients.
    Clients = node_list(?CLIENT_NUMBER, "client", Config), %% client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = start(hyparview_manager_partition_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {max_active_size, 5},
                   {servers, Servers},
                   {clients, Clients}]),

    CheckStartedFun = fun() ->
                        case hyparview_membership_check(Nodes) of
                            {[], []} -> true;
                            {ConnectedFails, []} ->
                                {connected_check_failed, ConnectedFails};
                            {[], SymmetryFails} ->
                                {symmetry_check_failed, SymmetryFails};
                            {ConnectedFails, SymmetryFails} ->
                                [{connected_check_failed, ConnectedFails},
                                 {symmetry_check_failed, SymmetryFails}]
                        end
                      end,

    case wait_until(CheckStartedFun, 60 * 2, 100) of
        ok ->
            ok;
        {fail, {false, {connected_check_failed, Nodes}}} ->
            ct:fail("Graph is not connected, unable to find route between pairs of nodes ~p",
                    [Nodes]);
        {fail, {false, {symmetry_check_failed, Nodes}}} ->
            ct:fail("Symmetry is broken (ie. node1 has node2 in it's view but vice-versa is not true) between the following "
                    "pairs of nodes: ~p", [Nodes]);
        {fail, {false, [{connected_check_failed, ConnectedFails},
                        {symmetry_check_failed, SymmetryFails}]}} ->
            ct:fail("Graph is not connected, unable to find route between pairs of nodes ~p, symmetry is broken as well"
                    "(ie. node1 has node2 in it's view but vice-versa is not true) between the following "
                    "pairs of nodes: ~p", [ConnectedFails, SymmetryFails])
    end,

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

    %% Pause for clustering.
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

    %% Verify forward message functionality.
    lists:foreach(fun({_Name, Node}) ->
                    ok = check_forward_message(Node, Manager, Nodes)
                  end, Nodes),

    %% Verify correct behaviour when a node is stopped
    {_, KilledNode} = N0 = random(Nodes, []),
    ok = rpc:call(KilledNode, partisan, stop, []),
    CheckStoppedFun = fun() ->
                        case hyparview_check_stopped_member(KilledNode, Nodes -- [N0]) of
                            [] -> true;
                            FailedNodes ->
                                FailedNodes
                        end
                      end,
    case wait_until(CheckStoppedFun, 60 * 2, 100) of
        ok ->
            ok;
        {fail, FailedNodes} ->
            ct:fail("~p has been killed, it should not be in membership of nodes ~p",
                    [KilledNode, FailedNodes])
    end,

    %% Stop nodes.
    stop(Nodes),

    ok.

hyparview_manager_high_active_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Specify servers.
    Servers = node_list(1, "server", Config), %% [server],

    %% Specify clients.
    Clients = node_list(?CLIENT_NUMBER, "client", Config), %% client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = start(hyparview_manager_high_active_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {max_active_size, 5},
                   {servers, Servers},
                   {clients, Clients}]),

    CheckStartedFun = fun() ->
                        case hyparview_membership_check(Nodes) of
                            {[], []} -> true;
                            {ConnectedFails, []} ->
                                {false, {connected_check_failed, ConnectedFails}};
                            {[], SymmetryFails} ->
                                {false, {symmetry_check_failed, SymmetryFails}};
                            {ConnectedFails, SymmetryFails} ->
                                {false, [{connected_check_failed, ConnectedFails},
                                         {symmetry_check_failed, SymmetryFails}]}
                        end
                      end,

    case wait_until(CheckStartedFun, 60 * 2, 100) of
        ok ->
            ok;
        {fail, {false, {connected_check_failed, Nodes}}} ->
            ct:fail("Graph is not connected, unable to find route between pairs of nodes ~p",
                    [Nodes]);
        {fail, {false, {symmetry_check_failed, Nodes}}} ->
            ct:fail("Symmetry is broken (ie. node1 has node2 in it's view but vice-versa is not true) between the following "
                    "pairs of nodes: ~p", [Nodes]);
        {fail, {false, [{connected_check_failed, ConnectedFails},
                        {symmetry_check_failed, SymmetryFails}]}} ->
            ct:fail("Graph is not connected, unable to find route between pairs of nodes ~p, symmetry is broken as well"
                    "(ie. node1 has node2 in it's view but vice-versa is not true) between the following "
                    "pairs of nodes: ~p", [ConnectedFails, SymmetryFails])
    end,

    %% Verify forward message functionality.
    lists:foreach(fun({_Name, Node}) ->
                    ok = check_forward_message(Node, Manager, Nodes)
                  end, Nodes),

    %% Verify correct behaviour when a node is stopped
    {_, KilledNode} = N0 = random(Nodes, []),
    ok = rpc:call(KilledNode, partisan, stop, []),
    CheckStoppedFun = fun() ->
                        case hyparview_check_stopped_member(KilledNode, Nodes -- [N0]) of
                            [] -> true;
                            FailedNodes ->
                                FailedNodes
                        end
                      end,
    case wait_until(CheckStoppedFun, 60 * 2, 100) of
        ok ->
            ok;
        {fail, FailedNodes} ->
            ct:fail("~p has been killed, it should not be in membership of nodes ~p",
                    [KilledNode, FailedNodes])
    end,

    %% Stop nodes.
    stop(Nodes),

    ok.

hyparview_manager_low_active_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Start nodes.
    MaxActiveSize = 3,

    Servers = node_list(1, "server", Config), %% [server],

    Clients = node_list(?CLIENT_NUMBER, "client", Config), %% client_list(?CLIENT_NUMBER),

    Nodes = start(hyparview_manager_low_active_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {max_active_size, MaxActiveSize},
                   {servers, Servers},
                   {clients, Clients}]),

    CheckStartedFun = fun() ->
                        case hyparview_membership_check(Nodes) of
                            {[], []} -> true;
                            {ConnectedFails, []} ->
                                {false, {connected_check_failed, ConnectedFails}};
                            {[], SymmetryFails} ->
                                {false, {symmetry_check_failed, SymmetryFails}};
                            {ConnectedFails, SymmetryFails} ->
                                {false, [{connected_check_failed, ConnectedFails},
                                         {symmetry_check_failed, SymmetryFails}]}
                        end
                      end,

    case wait_until(CheckStartedFun, 60 * 2, 100) of
        ok ->
            ok;
        {fail, {false, {connected_check_failed, Nodes}}} ->
            ct:fail("Graph is not connected, unable to find route between pairs of nodes ~p",
                    [Nodes]);
        {fail, {false, {symmetry_check_failed, Nodes}}} ->
            ct:fail("Symmetry is broken (ie. node1 has node2 in it's view but vice-versa is not true) between the following "
                    "pairs of nodes: ~p", [Nodes]);
        {fail, {false, [{connected_check_failed, ConnectedFails},
                        {symmetry_check_failed, SymmetryFails}]}} ->
            ct:fail("Graph is not connected, unable to find route between pairs of nodes ~p, symmetry is broken as well"
                    "(ie. node1 has node2 in it's view but vice-versa is not true) between the following "
                    "pairs of nodes: ~p", [ConnectedFails, SymmetryFails])
    end,

    %% Verify forward message functionality.
    lists:foreach(fun({_Name, Node}) ->
                    ok = check_forward_message(Node, Manager, Nodes)
                  end, Nodes),

    %% Verify correct behaviour when a node is stopped
    {_, KilledNode} = N0 = random(Nodes, []),
    ok = rpc:call(KilledNode, partisan, stop, []),
    CheckStoppedFun = fun() ->
                        case hyparview_check_stopped_member(KilledNode, Nodes -- [N0]) of
                            [] -> true;
                            FailedNodes ->
                                FailedNodes
                        end
                      end,
    case wait_until(CheckStoppedFun, 60 * 2, 100) of
        ok ->
            ok;
        {fail, FailedNodes} ->
            ct:fail("~p has been killed, it should not be in membership of nodes ~p",
                    [KilledNode, FailedNodes])
    end,

    %% Stop nodes.
    stop(Nodes),

    ok.

hyparview_manager_high_client_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Start clients,.
    Clients = node_list(11, "client", Config), %% client_list(11),

    %% Start servers.
    Servers = node_list(1, "server", Config), %% [server],

    Nodes = start(hyparview_manager_low_active_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    CheckStartedFun = fun() ->
                        case hyparview_membership_check(Nodes) of
                            {[], []} -> true;
                            {ConnectedFails, []} ->
                                {false, {connected_check_failed, ConnectedFails}};
                            {[], SymmetryFails} ->
                                {false, {symmetry_check_failed, SymmetryFails}};
                            {ConnectedFails, SymmetryFails} ->
                                {false, [{connected_check_failed, ConnectedFails},
                                         {symmetry_check_failed, SymmetryFails}]}
                        end
                      end,

    case wait_until(CheckStartedFun, 60 * 2, 100) of
        ok ->
            ok;
        {fail, {false, {connected_check_failed, Nodes}}} ->
            ct:fail("Graph is not connected, unable to find route between pairs of nodes ~p",
                    [Nodes]);
        {fail, {false, {symmetry_check_failed, Nodes}}} ->
            ct:fail("Symmetry is broken (ie. node1 has node2 in it's view but vice-versa is not true) between the following "
                    "pairs of nodes: ~p", [Nodes]);
        {fail, {false, [{connected_check_failed, ConnectedFails},
                        {symmetry_check_failed, SymmetryFails}]}} ->
            ct:fail("Graph is not connected, unable to find route between pairs of nodes ~p, symmetry is broken as well"
                    "(ie. node1 has node2 in it's view but vice-versa is not true) between the following "
                    "pairs of nodes: ~p", [ConnectedFails, SymmetryFails])
    end,
    
    %% Verify forward message functionality.
    lists:foreach(fun({_Name, Node}) ->
                    ok = check_forward_message(Node, Manager, Nodes)
                  end, Nodes),

    %% Verify correct behaviour when a node is stopped
    {_, KilledNode} = N0 = random(Nodes, []),
    ok = rpc:call(KilledNode, partisan, stop, []),
    CheckStoppedFun = fun() ->
                        case hyparview_check_stopped_member(KilledNode, Nodes -- [N0]) of
                            [] -> true;
                            FailedNodes ->
                                FailedNodes
                        end
                      end,
    case wait_until(CheckStoppedFun, 60 * 2, 100) of
        ok ->
            ok;
        {fail, FailedNodes} ->
            ct:fail("~p has been killed, it should not be in membership of nodes ~p",
                    [KilledNode, FailedNodes])
    end,

    %% Stop nodes.
    stop(Nodes),

    ok.


%% ===================================================================
%% Internal functions.
%% ===================================================================

%% @private
start(_Case, Config, Options) ->
    %% Launch distribution for the test runner.
    ct:pal("Launching Erlang distribution..."),

    {ok, Hostname} = inet:gethostname(), 
    os:cmd(os:find_executable("epmd") ++ " -daemon"),
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
            ct:pal("Setting peer service manager on node ~p to ~p", [Node, PeerService]),
            ok = rpc:call(Node, partisan_config, set,
                          [partisan_peer_service_manager, PeerService]),

            MaxActiveSize = proplists:get_value(max_active_size, Options, 5),
            ok = rpc:call(Node, partisan_config, set,
                          [max_active_size, MaxActiveSize]),
                          
            ok = rpc:call(Node, partisan_config, set,
                          [gossip_interval, ?GOSSIP_INTERVAL]),

            ok = rpc:call(Node, application, set_env, [partisan, peer_ip, ?PEER_IP]),

            ForwardOptions = case ?config(forward_options, Config) of
                              undefined ->
                                  [];
                              FO ->
                                  FO
                          end,
            ct:pal("Setting forward_options to: ~p", [ForwardOptions]),
            ok = rpc:call(Node, partisan_config, set, [forward_options, ForwardOptions]),

            Disterl = case ?config(disterl, Config) of
                              undefined ->
                                  false;
                              true ->
                                  true
                          end,
            ct:pal("Setting disterl to: ~p", [Disterl]),
            ok = rpc:call(Node, partisan_config, set, [disterl, Disterl]),

            Transport = case ?config(transport, Config) of
                              undefined ->
                                  native;
                              T ->
                                  T
                          end,
            ct:pal("Setting transport to: ~p", [Transport]),
            ok = rpc:call(Node, partisan_config, set, [transport, Transport]),

            BinaryPadding = case ?config(binary_padding, Config) of
                              undefined ->
                                  false;
                              BP ->
                                  BP
                          end,
            ct:pal("Setting binary_padding to: ~p", [BinaryPadding]),
            ok = rpc:call(Node, partisan_config, set, [binary_padding, BinaryPadding]),

            Broadcast = case ?config(broadcast, Config) of
                              undefined ->
                                  false;
                              B ->
                                  B
                          end,
            ct:pal("Setting broadcast to: ~p", [Broadcast]),
            ok = rpc:call(Node, partisan_config, set, [broadcast, Broadcast]),

            Channels = case ?config(channels, Config) of
                              undefined ->
                                  ?CHANNELS;
                              C ->
                                  C
                          end,
            ct:pal("Setting channels to: ~p", [Channels]),
            ok = rpc:call(Node, partisan_config, set, [channels, Channels]),

            ok = rpc:call(Node, partisan_config, set, [tls, ?config(tls, Config)]),
            Parallelism = case ?config(parallelism, Config) of
                              undefined ->
                                  ?PARALLELISM;
                              P ->
                                  P
                          end,
            ct:pal("Setting parallelism to: ~p", [Parallelism]),
            ok = rpc:call(Node, partisan_config, set, [parallelism, Parallelism]),

            Servers = proplists:get_value(servers, Options, []),
            Clients = proplists:get_value(clients, Options, []),

            %% Configure servers.
            case lists:member(Name, Servers) of
                true ->
                    ok = rpc:call(Node, partisan_config, set, [tag, server]),
                    ok = rpc:call(Node, partisan_config, set, [tls_options, ?config(tls_server_opts, Config)]);
                false ->
                    ok
            end,

            %% Configure clients.
            case lists:member(Name, Clients) of
                true ->
                    ok = rpc:call(Node, partisan_config, set, [tag, client]),
                    ok = rpc:call(Node, partisan_config, set, [tls_options, ?config(tls_client_opts, Config)]);
                false ->
                    ok
            end
    end,
    lists:foreach(ConfigureFun, Nodes),

    ct:pal("Starting nodes."),

    StartFun = fun({_Name, Node}) ->
                        %% Start partisan.
                        {ok, _} = rpc:call(Node, application, ensure_all_started, [partisan]),
                        %% Start a dummy registered process that saves in the environment
                        %% whatever message it gets, it will only do this *x* amount of times
                        %% *x* being the number of nodes present in the cluster
                        Pid = rpc:call(Node, erlang, spawn,
                                       [fun() ->
                                            lists:foreach(fun(_) ->
                                                receive
                                                    {store, N} ->
                                                        %% save the number in the environment
                                                        application:set_env(partisan, forward_message_test, N)
                                                end
                                            end, lists:seq(1, length(NodeNames)))
                                        end]),
                        true = rpc:call(Node, erlang, register, [store_proc, Pid]),
                        ct:pal("registered store_proc on pid ~p, node ~p",
                               [Pid, Node])
               end,
    lists:foreach(StartFun, Nodes),

    ct:pal("Clustering nodes."),
    lists:foreach(fun(Node) -> cluster(Node, Nodes, Options, Config) end, Nodes),

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
cluster({Name, _Node} = Myself, Nodes, Options, Config) when is_list(Nodes) ->
    Manager = proplists:get_value(partisan_peer_service_manager, Options),

    Servers = proplists:get_value(servers, Options, []),
    Clients = proplists:get_value(clients, Options, []),

    AmIServer = lists:member(Name, Servers),
    AmIClient = lists:member(Name, Clients),

    OtherNodes = case Manager of
                     partisan_default_peer_service_manager ->
                         %% Omit just ourselves.
                         omit([Name], Nodes);
                     partisan_amqp_peer_service_manager ->
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
    lists:map(fun(OtherNode) -> cluster(Myself, OtherNode, Config) end, OtherNodes).
cluster({_, Node}, {_, OtherNode}, Config) ->
    PeerPort = rpc:call(OtherNode,
                        partisan_config,
                        get,
                        [peer_port, ?PEER_PORT]),
    Parallelism = case ?config(parallelism, Config) of
                      undefined ->
                          1;
                      P ->
                          P
                  end,
    Channels = case ?config(channels, Config) of
                      undefined ->
                          [];
                      C ->
                          C
                  end,
    JoinMethod = case ?config(sync_join, Config) of
                  undefined ->
                      join;
                  true ->
                      sync_join
                  end,
    ct:pal("Joining node: ~p to ~p at port ~p", [Node, OtherNode, PeerPort]),
    ok = rpc:call(Node,
                  partisan_peer_service,
                  JoinMethod,
                  [#{name => OtherNode,
                     listen_addrs => [#{ip => {127, 0, 0, 1}, port => PeerPort}],
                     channels => Channels,
                     parallelism => Parallelism}]).

%% @private
stop(Nodes) ->
    StopFun = fun({Name, _Node}) ->
        case ct_slave:stop(Name) of
            {ok, _} ->
                ok;
            {error, stop_timeout, _} ->
                ok;
            {error, not_started, _} ->
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
node_list(0, _Name, _Config) -> 
    [];
node_list(N, Name, Config) ->
    [ list_to_atom(string:join([Name,
                                integer_to_list(?config(hash, Config)),
                                integer_to_list(X)],
                               "_")) ||
        X <- lists:seq(1, N) ].

%% @private
make_certs(Config) ->
    DataDir = ?config(data_dir, Config),
    PrivDir = ?config(priv_dir, Config),
    ct:pal("Generating TLS certificates into ~s", [PrivDir]),
    MakeCertsFile = filename:join(DataDir, "make_certs.erl"),
    {ok, make_certs, ModBin} = compile:file(MakeCertsFile, 
        [binary, debug_info, report_errors, report_warnings]),
    {module, make_certs} = code:load_binary(make_certs, MakeCertsFile, ModBin),

    make_certs:all(DataDir, PrivDir),

    [{tls_server_opts,
      [
       {certfile, filename:join(PrivDir, "server/keycert.pem")},
       {cacertfile, filename:join(PrivDir, "server/cacerts.pem")}
      ]},
     {tls_client_opts,
      [
       {certfile, filename:join(PrivDir, "client/keycert.pem")},
       {cacertfile, filename:join(PrivDir, "client/cacerts.pem")}
      ]}].

%% @private
check_forward_message(Node, Manager, Nodes) ->
    {Transitive, Members} = case rpc:call(Node, partisan_config, get, [broadcast, false]) of
        true ->
            M = lists:usort([N || {_, N} <- Nodes]),
            ct:pal("Checking forward functionality for all nodes: ~p", [M]),
            {true, M};
        false ->
            {ok, M} = rpc:call(Node, Manager, members, []),
            ct:pal("Checking forward functionality for subset of nodes: ~p", [M]),
            {false, M}
    end,

    ForwardOptions = rpc:call(Node, partisan_config, get, [forward_options, []]),

    RandomMember = random(Members, Node),
    ct:pal("requesting node ~p to forward message to store_proc on node ~p",
           [Node, RandomMember]),
    Rand = rand:uniform(),
    ok = rpc:call(Node, Manager, forward_message, [RandomMember, undefined, store_proc, {store, Rand}, [{transitive, Transitive}, {forward_options, ForwardOptions}]]),
    %% now fetch the value from the random destination node
    ok = wait_until(fun() ->
                    %% it must match with what we asked the node to forward
                    case rpc:call(RandomMember, application, get_env, [partisan, forward_message_test]) of
                        {ok, R} ->
                            R =:= Rand;
                        _ ->
                            false
                    end
               end, 60 * 2, 500),

    ok.

random(List0, Omit) ->
    List = List0 -- lists:flatten([Omit]),
    %% Catch exceptions where there may not be enough members.
    try
        Index = rand:uniform(length(List)),
        lists:nth(Index, List)
    catch
        _:_ ->
            undefined
    end.

wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        true ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry-1, Delay)
    end.

%% @private
%% 
%% Kill a random node and then return a list of nodes that still have the
%% killed node in their membership
%%
hyparview_check_stopped_member(_, [_Node]) -> {undefined, []};
hyparview_check_stopped_member(KilledNode, Nodes) ->
    %% Obtain the membership from all the nodes,
    %% the killed node shouldn't be there
    lists:filtermap(fun({_, Node}) ->
                        {ok, Members} = rpc:call(Node, partisan_peer_service, members, []),
                        case lists:member(KilledNode, Members) of
                            true ->
                                {true, Node};
                            false ->
                                false
                        end
                     end, Nodes).

%% @private
hyparview_membership_check(Nodes) ->
    Manager = partisan_hyparview_peer_service_manager,
    %% Create new digraph.
    Graph = digraph:new(),

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology
    %% when the active setting is high.
    %%
    ConnectFun =
        fun({_, Node}) ->
            {ok, ActiveSet} = rpc:call(Node, Manager, active, []),
            Active = sets:to_list(ActiveSet),

            %% Add vertexes and edges.
            [connect(Graph, Node, N) || #{name := N} <- Active]
         end,
    %% Build a digraph representing the membership
    lists:foreach(ConnectFun, Nodes),

    %% Verify connectedness.
    %% Return a list of node tuples that were found not to be connected,
    %% empty otherwise
    ConnectedFails =
        lists:flatmap(fun({_Name, Node}=Myself) ->
                lists:filtermap(fun({_, N}) ->
                    Path = digraph:get_short_path(Graph, Node, N),
                    case Path of
                        false ->
                            %% print out the active view of each node
                            % lists:foreach(fun({_, N1}) ->
                            %                     {ok, ActiveSet} = rpc:call(N1, Manager, active, []),
                            %                     Active = sets:to_list(ActiveSet),
                            %                     ct:pal("node ~p active view: ~p", [N1, Active])
                            %                end, Nodes),
                            {true, {Node, N}};
                        _ ->
                            false
                    end
                 end, Nodes -- [Myself])
            end, Nodes),

    %% Verify symmetry.
    SymmetryFails =
        lists:flatmap(fun({_, Node1}) ->
                %% Get first nodes active set.
                {ok, ActiveSet1} = rpc:call(Node1, Manager, active, []),
                Active1 = sets:to_list(ActiveSet1),

                lists:filtermap(fun(#{name := Node2}) ->
                    %% Get second nodes active set.
                    {ok, ActiveSet2} = rpc:call(Node2, Manager, active, []),
                    Active2 = sets:to_list(ActiveSet2),

                    case lists:member(Node1, [N || #{name := N} <- Active2]) of
                        true ->
                            false;
                        false ->
                            {true, {Node1, Node2}}
                    end
                end, Active1)
            end, Nodes),

    {ConnectedFails, SymmetryFails}.

%% @private
verify_leave(Nodes, Manager) ->
    %% Pause for gossip interval * node exchanges + gossip interval for full convergence.
    timer:sleep(?GOSSIP_INTERVAL * length(Nodes) + ?GOSSIP_INTERVAL),

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology.
    %%
    VerifyInitialFun = fun({_, Node}) ->
            {ok, Members} = rpc:call(Node, Manager, members, []),
            SortedNodes = lists:usort([N || {_, N} <- Nodes]),
            SortedMembers = lists:usort(Members),
            case SortedMembers =:= SortedNodes of
                true ->
                    true;
                false ->
                    ct:pal("Membership incorrect; node ~p should have ~p but has ~p",
                           [Node, SortedNodes, SortedMembers]),
                    {false, {Node, SortedNodes, SortedMembers}}
            end
    end,

    %% Verify the membership is correct.
    lists:foreach(fun(Node) ->
                          VerifyNodeFun = fun() -> VerifyInitialFun(Node) end,

                          case wait_until(VerifyNodeFun, 60 * 2, 100) of
                              ok ->
                                  ok;
                              {fail, {false, {Node, Expected, Contains}}} ->
                                 ct:fail("Membership incorrect; node ~p should have ~p but has ~p",
                                         [Node, Expected, Contains])
                          end
                  end, Nodes),

    %% Remove a node from the cluster.
    [{_, _}, {_, Node2}, {_, _}, {_, Node4}] = Nodes,
    ct:pal("Removing node ~p from the cluster.", [Node4]),
    ok = rpc:call(Node2, partisan_peer_service, leave, [Node4]),
    
    %% Pause for gossip interval * node exchanges + gossip interval for full convergence.
    timer:sleep(?GOSSIP_INTERVAL * length(Nodes) + ?GOSSIP_INTERVAL),

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology.
    %%
    VerifyRemoveFun = fun({_, Node}) ->
            {ok, Members} = rpc:call(Node, Manager, members, []),
            SortedNodes = case Node of
                Node4 ->
                    [Node4];
                _ ->
                    lists:usort([N || {_, N} <- Nodes]) -- [Node4]
            end,
            SortedMembers = lists:usort(Members),
            case SortedMembers =:= SortedNodes of
                true ->
                    true;
                false ->
                    ct:pal("Membership incorrect; node ~p should have ~p but has ~p",
                           [Node, SortedNodes, SortedMembers]),
                    {false, {Node, SortedNodes, SortedMembers}}
            end
    end,

    %% Verify the membership is correct.
    lists:foreach(fun(Node) ->
                          VerifyNodeFun = fun() -> VerifyRemoveFun(Node) end,

                          case wait_until(VerifyNodeFun, 60 * 2, 100) of
                              ok ->
                                  ok;
                              {fail, {false, {Node, Expected, Contains}}} ->
                                 ct:fail("Membership incorrect; node ~p should have ~p but has ~p",
                                         [Node, Expected, Contains])
                          end
                  end, Nodes),

ok.

%% @private
rand_bits(Bits) ->
        Bytes = (Bits + 7) div 8,
        <<Result:Bits/bits, _/bits>> = crypto:strong_rand_bytes(Bytes),
        Result.

receiver(_Manager, BenchPid, 0) ->
    BenchPid ! done,
    ok;
receiver(Manager, BenchPid, Count) ->
    receive
        {_Message, _SourceNode, _SourcePid} ->
            receiver(Manager, BenchPid, Count - 1)
    end.

sender(_EchoBinary, _Manager, _DestinationNode, _DestinationPid, _PartitionKey, 0) ->
    ok;
sender(EchoBinary, Manager, DestinationNode, DestinationPid, PartitionKey, Count) ->
    Manager:forward_message(DestinationNode, undefined, DestinationPid, {EchoBinary, node(), self()}, [{partition_key, PartitionKey}]),
    sender(EchoBinary, Manager, DestinationNode, DestinationPid, PartitionKey, Count - 1).

init_sender(EchoBinary, Manager, DestinationNode, DestinationPid, PartitionKey, Count) ->
    receive
        start ->
            ok
    end,
    sender(EchoBinary, Manager, DestinationNode, DestinationPid, PartitionKey, Count).

bench_receiver(0) ->
    ok;
bench_receiver(Count) ->
    ct:pal("Waiting for ~p processes to finish...", [Count]),

    receive
        done ->
            ct:pal("Received, but still waiting for ~p", [Count -1]),
            bench_receiver(Count - 1)
    end.

%% @private
root_path(Config) ->
    DataDir = proplists:get_value(data_dir, Config, ""),
    DataDir ++ "../../../../../../".

%% @private
root_dir(Config) ->
    RootCommand = "cd " ++ root_path(Config) ++ "; pwd",
    RootOutput = os:cmd(RootCommand),
    RootDir = string:substr(RootOutput, 1, length(RootOutput) - 1) ++ "/",
    ct:pal("RootDir: ~p", [RootDir]),
    RootDir.

%% @private
parallelism() ->
    case os:getenv("PARALLELISM", "1") of
        false ->
            [{parallelism, list_to_integer("1")}];
        "1" ->
            [{parallelism, list_to_integer("1")}];
        Config ->
            [{parallelism, list_to_integer(Config)}]
    end.