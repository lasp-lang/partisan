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

-define(TIMEOUT, 10000).
-define(CLIENT_NUMBER, 3).
-define(HIGH_CLIENT_NUMBER, 10).

%% ==================================================================
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

init_per_group(with_disterl, Config) ->
    [{disterl, true}] ++ Config;
init_per_group(with_scamp_v1_membership_strategy, Config) ->
    [{membership_strategy, partisan_scamp_v1_membership_strategy}] ++ Config;
init_per_group(with_scamp_v2_membership_strategy, Config) ->
    [{membership_strategy, partisan_scamp_v2_membership_strategy}] ++ Config;
init_per_group(with_broadcast, Config) ->
    [{broadcast, true}, {forward_options, [{transitive, true}]}] ++ Config;
init_per_group(with_partition_key, Config) ->
    [{forward_options, [{partition_key, 1}]}] ++ Config;
init_per_group(with_binary_padding, Config) ->
    [{binary_padding, true}] ++ Config;
init_per_group(with_sync_join, Config) ->
    [{parallelism, 1}, {sync_join, true}] ++ Config;
init_per_group(with_monotonic_channels, Config) ->
    [{parallelism, 1}, {channels, [{monotonic, vnode}, gossip, rpc, membership]}] ++ Config;
init_per_group(with_channels, Config) ->
    [{parallelism, 1}, {channels, [vnode, gossip, rpc, membership]}] ++ Config;
init_per_group(with_parallelism, Config) ->
    parallelism() ++ [{channels, ?CHANNELS}] ++ Config;
init_per_group(with_parallelism_bypass_pid_encoding, Config) ->
    parallelism() ++ [{channels, ?CHANNELS}, {pid_encoding, false}] ++ Config;
init_per_group(with_partisan_bypass_pid_encoding, Config) ->
    [{pid_encoding, false}] ++ Config;
init_per_group(with_no_channels, Config) ->
    [{parallelism, 1}, {channels, []}] ++ Config;
init_per_group(with_causal_labels, Config) ->
    [{causal_labels, [default]}] ++ Config;
init_per_group(with_causal_send, Config) ->
    [{causal_labels, [default]}, {forward_options, [{causal_label, default}]}] ++ Config;
init_per_group(with_causal_send_and_ack, Config) ->
    [{causal_labels, [default]}, {forward_options, [{causal_label, default}, {ack, true}]}] ++ Config;
init_per_group(with_forward_interposition, Config) ->
    [{disable_fast_forward, true}] ++ Config;
init_per_group(with_receive_interposition, Config) ->
    [{disable_fast_receive, true}] ++ Config;
init_per_group(with_ack, Config) ->
    [{disable_fast_forward, true}, {forward_options, [{ack, true}]}] ++ Config;
init_per_group(with_tls, Config) ->
    TLSOpts = make_certs(Config),
    [{parallelism, 1}, {tls, true}] ++ TLSOpts ++ Config;
init_per_group(with_egress_delay, Config) ->
    [{egress_delay, 100}] ++ Config;
init_per_group(with_ingress_delay, Config) ->
    [{ingress_delay, 100}] ++ Config;
init_per_group(_, Config) ->
    [{parallelism, 1}] ++ Config.

end_per_group(_, _Config) ->
    ok.

all() ->
    [
     {group, default, [parallel],
      [{simple, [shuffle]},
       {hyparview, [shuffle]}
       %% {hyparview_xbot, [shuffle]}
      ]},

     %% Full.

     {group, with_full_membership_strategy, []},

     %% Scamp v1.

     {group, with_scamp_v1_membership_strategy, []},

     %% Scamp v2.

     {group, with_scamp_v2_membership_strategy, []},

     %% Features.

     {group, with_ack, []},

     {group, with_causal_labels, []},

     {group, with_causal_send, []},

     {group, with_causal_send_and_ack, []},

     {group, with_tls, [parallel]},

     {group, with_parallelism, [parallel]},

     {group, with_parallelism_bypass_pid_encoding, []},

     {group, with_partisan_bypass_pid_encoding, []},

     {group, with_disterl, [parallel]},

     {group, with_sync_join, [parallel]},

     {group, with_partition_key, [parallel]},

     {group, with_broadcast, [parallel]},

      %% Channels.

     {group, with_channels, [parallel]},

     {group, with_no_channels, [parallel]},
     
     {group, with_monotonic_channels, [parallel]},

     %% Debug.

     {group, with_binary_padding, [parallel]},

     %% Fault injection.

     {group, with_forward_interposition, []},

     {group, with_receive_interposition, []},

     {group, with_ingress_delay, [parallel]},

     {group, with_egress_delay, [parallel]}
    ].

groups() ->
    [
     {default, [],
      [{group, simple},
       {group, hyparview}
       %% {group, hyparview_xbot}
      ]},

     {simple, [],
      [basic_test,
       leave_test,
       self_leave_test,
       on_down_test,
       rpc_test,
       client_server_manager_test,
       pid_test,
       rejoin_test,
       transform_test,
       otp_test]},
       
     {hyparview, [],
      [ 
       hyparview_manager_partition_test,
       hyparview_manager_high_active_test,
       %% hyparview_manager_low_active_test,
       hyparview_manager_high_client_test
      ]},
       
     {hyparview_xbot, [],
      [ 
       %% hyparview_xbot_manager_high_active_test,
       %% hyparview_xbot_manager_low_active_test,
       %% hyparview_xbot_manager_high_client_test
      ]},

     {with_full_membership_strategy, [],
      [connectivity_test,
       gossip_test]},

     {with_scamp_v1_membership_strategy, [],
      [connectivity_test,
       gossip_test]},

     {with_scamp_v2_membership_strategy, [],
      [connectivity_test,
       gossip_test]},

     {with_ack, [],
      [basic_test,
       ack_test]},

     {with_causal_labels, [],
      [causal_test]},

     {with_causal_send, [],
      [basic_test]},
     
     {with_causal_send_and_ack, [],
      [basic_test]},

     {with_forward_interposition, [],
      [forward_interposition_test]},

     {with_receive_interposition, [],
      [receive_interposition_test]},

     {with_tls, [],
      [basic_test]},

     {with_parallelism, [],
      [basic_test]},

     {with_parallelism_bypass_pid_encoding, [],
      [performance_test]},

     {with_disterl, [],
      [performance_test]},

     {with_partisan_bypass_pid_encoding, [],
      [performance_test]},
     
     {with_channels, [],
      [basic_test,
       rpc_test]},

     {with_no_channels, [],
      [basic_test]},

     {with_monotonic_channels, [],
      [basic_test]},

     {with_sync_join, [],
      [basic_test]},

     {with_binary_padding, [],
      [basic_test]},

     {with_partition_key, [],
      [basic_test]},

     {with_ingress_delay, [],
      [basic_test]},

     {with_egress_delay, [],
      [basic_test]},

     {with_broadcast, [],
      [
       hyparview_manager_low_active_test,
       hyparview_manager_high_active_test
      ]}

    ].

%% ===================================================================
%% Tests.
%% ===================================================================

transform_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(transform_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Test on_down callback.
    [{_, _}, {_, _}, {_, Node3}, {_, Node4}] = Nodes,

    %% Generate message.
    Message = message,

    %% Verify local send transformation.
    case rpc:call(Node3, partisan_transformed_module, local_send, [Message]) of
        Message ->
            ok;
        LocalSendError ->
            ct:fail("Received error: ~p", [LocalSendError])
    end,

    %% Get process identifier
    case rpc:call(Node3, partisan_transformed_module, get_pid, []) of
        {partisan_remote_reference, _, _} = Node3Pid1 ->
            case rpc:call(Node3, partisan_transformed_module, send_to_pid, [Node3Pid1, Message]) of
                Message ->
                    ok;
                SendToPidError ->
                    ct:fail("Received error: ~p", [SendToPidError])
            end;
        GetPidError ->
            ct:fail("Received error: ~p", [GetPidError])
    end,

    %% Try sending and receiving.
    RunnerPid = self(),

    GetPidFunction = fun() ->
        OurPid = partisan_transformed_module:get_pid(),

        %% Send Node3 process to the runner.
        RunnerPid ! OurPid,

        %% Wait for message from Node4 at Node3. 
        receive
            Message ->
                %% Tell runner that we finished.
                RunnerPid ! finished
        after 
            1000 ->
                ct:fail("Didn't receive message in time.")
        end
    end,
    _ = rpc:call(Node3, erlang, spawn, [GetPidFunction]),

    receive
        {partisan_remote_reference, _, _} = Node3Pid2 ->
            rpc:call(Node4, partisan_transformed_module, send_to_pid, [Node3Pid2, Message])
    after
        1000 ->
            ct:fail("Received no proper response!")
    end,

    receive
        finished ->
            ok
    after 
        1000 ->
            ct:fail("Never received a response.")
    end,

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    ok.

causal_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(causal_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Test on_down callback.
    [{_, _}, {_, _}, {_, Node3}, {_, Node4}] = Nodes,

    %% Use our process identifier as the message destination.
    ServerRef = self(),

    %% Use default causal channel label.
    Label = default,

    %% Set the delivery function on all nodes to send messages here.
    DeliveryFun = fun(_ServerRef, Message) ->
        ServerRef ! Message
    end,
    lists:foreach(fun({_, N}) ->
        ok = rpc:call(N, partisan_causality_backend, set_delivery_fun, [Label, DeliveryFun])
        end, Nodes),

    %% Generate a message and vclock for that message.
    Message1 = message_1,
    {ok, _, FullMessage1} = rpc:call(Node3, partisan_causality_backend, emit, [Label, Node4, ServerRef, Message1]),
    ct:pal("Generated at node ~p full message: ~p", [Node3, FullMessage1]),

    %% Generate a second message, which should depend on the first.
    Message2 = message_2,
    {ok, _, FullMessage2} = rpc:call(Node3, partisan_causality_backend, emit, [Label, Node4, ServerRef, Message2]),
    ct:pal("Generated at node ~p full message: ~p", [Node3, FullMessage2]),

    %% Attempt to deliver message2.
    ok = rpc:call(Node4, partisan_causality_backend, receive_message, [Label, FullMessage2]),
    
    %% Message2 reception.
    receive
        Message2 ->
            ct:fail("Received message 2 first!")
    after
        1000 ->
            ok
    end,

    %% Attempt to deliver message1.
    ok = rpc:call(Node4, partisan_causality_backend, receive_message, [Label, FullMessage1]),

    %% Message1 reception.
    receive
        Message1 ->
            ct:pal("Received message 1!"),
            ok
    after
        1000 ->
            ct:fail("Didn't receive message 1!")
    end,

    %% See what messages we have received.
    receive
        Message2 ->
            ct:pal("Received message 2!"),
            ok
    after
        10000 ->
            ct:fail("Didn't receive message 2!")
    end,

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    ok.

receive_interposition_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(receive_interposition_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Test on_down callback.
    [{_, _}, {_, _}, {_, Node3}, {_, Node4}] = Nodes,

    %% Set message filter.
    InterpositionFun = 
        fun({receive_message, N, M}) ->
            case N of
                Node3 ->
                    undefined;
                _ ->
                    M
            end;
            ({_, _, M}) -> 
                M
    end,
    ok = rpc:call(Node4, Manager, add_interposition_fun, [Node3, InterpositionFun]),
    
    %% Spawn receiver process.
    Message1 = message1,
    Message2 = message2,

    Self = self(),

    ReceiverFun = fun() ->
        receive 
            X ->
                Self ! X
        end
    end,
    Pid = rpc:call(Node4, erlang, spawn, [ReceiverFun]),
    true = rpc:call(Node4, erlang, register, [receiver, Pid]),

    %% Send message.
    ok = rpc:call(Node3, Manager, forward_message, [Node4, undefined, receiver, Message1, []]),

    %% Wait to receive message.
    receive
        Message1 ->
            ct:fail("Received message we shouldn't have!")
    after 
        1000 ->
            ok
    end,

    %% Remove filter.
    ok = rpc:call(Node4, Manager, remove_interposition_fun, [Node3]),

    %% Send message.
    ok = rpc:call(Node3, Manager, forward_message, [Node4, undefined, receiver, Message2, []]),

    %% Wait to receive message.
    receive
        Message1 ->
            ct:fail("Received message we shouldn't have!");
        Message2 ->
            ok
    after 
        1000 ->
            ct:fail("Didn't receive message we should have!")
    end,

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    ok.

ack_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(ack_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Test on_down callback.
    [{_, _}, {_, _}, {_, Node3}, {_, Node4}] = Nodes,

    %% Set message filter.
    InterpositionFun = 
        fun({forward_message, N, M}) ->
            case N of
                Node4 ->
                    undefined;
                _ ->
                    M
            end;
            ({_, _, M}) -> 
                M
    end,
    ok = rpc:call(Node3, Manager, add_interposition_fun, [Node4, InterpositionFun]),
    
    %% Spawn receiver process.
    Message1 = message1,

    Self = self(),

    ReceiverFun = fun() ->
        receive 
            X ->
                Self ! X
        end
    end,
    Pid = rpc:call(Node4, erlang, spawn, [ReceiverFun]),
    true = rpc:call(Node4, erlang, register, [receiver, Pid]),

    %% Send message.
    ok = rpc:call(Node3, Manager, forward_message, [Node4, undefined, receiver, Message1, [{ack, true}]]),

    %% Wait to receive message.
    receive
        Message1 ->
            ct:fail("Received message we shouldn't have!")
    after 
        1000 ->
            ok
    end,

    %% Remove filter.
    ok = rpc:call(Node3, Manager, remove_interposition_fun, [Node4]),

    %% Wait to receive message.
    receive
        Message1 ->
            ok
    after 
        2000 ->
            ct:fail("Didn't receive message we should have!")
    end,

    %% Pause for acknowledgement.
    timer:sleep(5000),

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    ok.

forward_interposition_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(forward_interposition_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Test on_down callback.
    [{_, _}, {_, _}, {_, Node3}, {_, Node4}] = Nodes,

    %% Set message filter.
    InterpositionFun = 
        fun({forward_message, N, M}) ->
            case N of
                Node4 ->
                    undefined;
                _ ->
                    M
            end;
            ({_, _, M}) -> 
                M
    end,
    ok = rpc:call(Node3, Manager, add_interposition_fun, [Node4, InterpositionFun]),
    
    %% Spawn receiver process.
    Message1 = message1,
    Message2 = message2,

    Self = self(),

    ReceiverFun = fun() ->
        receive 
            X ->
                Self ! X
        end
    end,
    Pid = rpc:call(Node4, erlang, spawn, [ReceiverFun]),
    true = rpc:call(Node4, erlang, register, [receiver, Pid]),

    %% Send message.
    ok = rpc:call(Node3, Manager, forward_message, [Node4, undefined, receiver, Message1, []]),

    %% Wait to receive message.
    receive
        Message1 ->
            ct:fail("Received message we shouldn't have!")
    after 
        1000 ->
            ok
    end,

    %% Remove filter.
    ok = rpc:call(Node3, Manager, remove_interposition_fun, [Node4]),

    %% Send message.
    ok = rpc:call(Node3, Manager, forward_message, [Node4, undefined, receiver, Message2, []]),

    %% Wait to receive message.
    receive
        Message1 ->
            ct:fail("Received message we shouldn't have!");
        Message2 ->
            ok
    after 
        1000 ->
            ct:fail("Didn't receive message we should have!")
    end,

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    ok.

pid_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(pid_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Test on_down callback.
    [{_, _}, {_, _}, {_, Node3}, {_, Node4}] = Nodes,

    %% Spawn sender and receiver processes.
    Self = self(),

    ReceiverFun = fun() ->
        receive 
            X ->
                Self ! X
        end
    end,
    ReceiverPid = rpc:call(Node4, erlang, spawn, [ReceiverFun]),
    true = rpc:call(Node4, erlang, register, [receiver, ReceiverPid]),

    %% Send message.
    SenderFun = fun() ->
        ok = Manager:forward_message(Node4, undefined, receiver, {message, self()}, []),

        %% Process must stay alive to send the pid.
        receive
            X ->
                Self ! X
        end
    end,
    _SenderPid = rpc:call(Node3, erlang, spawn, [SenderFun]),

    %% Wait to receive message.
    receive
        {message, Pid} when is_pid(Pid) ->
            ct:fail("Received incorrect message!");
        {message, GenSym} = Message ->
            lager:info("Received correct message: ~p", [Message]),
            ok = rpc:call(Node4, Manager, forward_message, [GenSym, Message]),
            ok
    after 
        1000 ->
            ct:fail("Didn't receive message!")
    end,

    %% Wait for response.
    receive
        X ->
            X
    after
        1000 ->
            ct:fail("Didn't receive respoonse.")
    end,

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    ok.

rpc_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(rpc_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(5000),

    %% Select two of the nodes.
    [{_, _}, {_, _}, {_, Node3}, {_, Node4}] = Nodes,

    %% Issue RPC.
    ct:pal("Issuing RPC to remote node: ~p", [Node4]),
    {_, _, _} = rpc:call(Node3, partisan_rpc_backend, call, [Node4, erlang, now, [], infinity]),

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    ok.

on_down_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(on_down_test, Config,
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
    ?SUPPORT:stop(Nodes),

    ok.

rejoin_test(Config) ->
    case os:getenv("TRAVIS") of
        false ->
            %% Use the default peer service manager.
            Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

            %% Specify servers.
            Servers = ?SUPPORT:node_list(1, "server", Config),

            %% Specify clients.
            Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

            %% Start nodes.
            Nodes = ?SUPPORT:start(rejoin_test, Config,
                        [{partisan_peer_service_manager, Manager},
                        {servers, Servers},
                        {clients, Clients}]),

            NodeToLeave = lists:nth(length(Nodes), Nodes),
            ct:pal("Verifying leave for ~p", [NodeToLeave]),
            verify_leave(NodeToLeave, Nodes, Manager),
            
            %% Join a node from the cluster.
            [{_, _}, {_, Node2}, {_, _}, {_, Node4}] = Nodes,
            ct:pal("Joining node ~p to the cluster.", [Node4]),
            ok = rpc:call(Node2, partisan_peer_service, join, [Node4]),
            
            %% Pause for gossip interval * node exchanges + gossip interval for full convergence.
            timer:sleep(?OVERRIDE_PERIODIC_INTERVAL * length(Nodes) + ?OVERRIDE_PERIODIC_INTERVAL),

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
                                    {fail, {false, {IncorrectNode, Expected, Contains}}} ->
                                        ct:fail("Membership incorrect; node ~p should have ~p but has ~p",
                                                [IncorrectNode, Expected, Contains])
                                end
                        end, Nodes),

            %% Stop nodes.
            ?SUPPORT:stop(Nodes);

        _ ->
            ok

        end,

        ok.

self_leave_test(Config) ->
    case os:getenv("TRAVIS") of
        false ->
        %% Use the default peer service manager.
        Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

        %% Specify servers.
        Servers = ?SUPPORT:node_list(1, "server", Config),

        %% Specify clients.
        Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

        %% Start nodes.
        Nodes = ?SUPPORT:start(leave_test, Config,
                    [{partisan_peer_service_manager, Manager},
                    {servers, Servers},
                    {clients, Clients}]),

        NodeToLeave = lists:nth(2, Nodes),
        ct:pal("Verifying leave for ~p", [NodeToLeave]),
        verify_leave(NodeToLeave, Nodes, Manager),

        %% Stop nodes.
        ?SUPPORT:stop(Nodes);

    _ ->
        ok

    end,

    ok.

leave_test(Config) ->
    case os:getenv("TRAVIS") of
        false ->
        %% Use the default peer service manager.
        Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

        %% Specify servers.
        Servers = ?SUPPORT:node_list(1, "server", Config),

        %% Specify clients.
        Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

        %% Start nodes.
        Nodes = ?SUPPORT:start(leave_test, Config,
                    [{partisan_peer_service_manager, Manager},
                    {servers, Servers},
                    {clients, Clients}]),

        NodeToLeave = lists:nth(length(Nodes), Nodes),
        ct:pal("Verifying leave for ~p", [NodeToLeave]),
        verify_leave(NodeToLeave, Nodes, Manager),

        %% Stop nodes.
        ?SUPPORT:stop(Nodes);

    _ ->
        ok

    end,

    ok.

performance_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(1, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(performance_test, Config,
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
    ?SUPPORT:stop(Nodes),

    ok.

gossip_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = case ?config(servers, Config) of
        undefined ->
            ?SUPPORT:node_list(1, "server", Config);
        NumServers ->
            ?SUPPORT:node_list(NumServers, "server", Config)
    end,

    %% Specify clients.
    Clients = case ?config(clients, Config) of
        undefined ->
            ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config);
        NumClients ->
            ?SUPPORT:node_list(NumClients, "client", Config)
    end,

    %% Start nodes.
    Nodes = ?SUPPORT:start(gossip_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Verify forward message functionality.
    lists:foreach(fun({_Name, Node}) ->
                    ok = check_forward_message(Node, Manager, Nodes)
                  end, Nodes),

    %% Start gossip backend on all nodes.
    lists:foreach(fun({_Name, Node}) ->
        ct:pal("Starting gossip backend on node ~p", [Node]),
        {ok, _Pid} = rpc:call(Node, demers_direct_mail, start_link, [])
    end, Nodes),

    %% Pause for protocol delay and periodic intervals to fire.
    timer:sleep(10000),

    %% Gossip.
    [{_, _}, {_, Node2}, {_, _}, {_, Node4}] = Nodes,
    Self = self(),

    ReceiverFun = fun() ->
        receive
            hello ->
                lager:info("received value from gossip receiver", []),
                Self ! hello
        end
    end,
    ReceiverPid = rpc:call(Node4, erlang, spawn, [ReceiverFun]),

    %% Register, to bypass pid encoding nonsense.
    true = rpc:call(Node4, erlang, register, [receiver, ReceiverPid]),

    %% Gossip.
    ct:pal("Broadcasting hello from node ~p", [Node2]),
    ok = rpc:call(Node2, demers_direct_mail, broadcast, [receiver, hello]),

    receive
        hello ->
            ok
    after
        10000 ->
            ct:fail("Didn't receive message!")
    end,

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    ok.

connectivity_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = case ?config(servers, Config) of
        undefined ->
            ?SUPPORT:node_list(1, "server", Config);
        NumServers ->
            ?SUPPORT:node_list(NumServers, "server", Config)
    end,

    %% Specify clients.
    Clients = case ?config(clients, Config) of
        undefined ->
            ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config);
        NumClients ->
            ?SUPPORT:node_list(NumClients, "client", Config)
    end,

    %% Start nodes.
    Nodes = ?SUPPORT:start(connectivity_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Verify forward message functionality.
    lists:foreach(fun({_Name, Node}) ->
                    ok = check_forward_message(Node, Manager, Nodes)
                  end, Nodes),

    %% Pause for protocol delay and periodic intervals to fire.
    timer:sleep(10000),

    %% Verify forward message functionality again.
    lists:foreach(fun({_Name, Node}) ->
                    ok = check_forward_message(Node, Manager, Nodes)
                  end, Nodes),

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    ok.

otp_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(otp_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    %% Pause for clustering.
    timer:sleep(1000),

    %% Start the test backend on all the clients.
    lists:foreach(fun({_, Node}) ->
        ct:pal("Going to start test backend on node ~p", [Node]),
        ok = rpc:call(Node, partisan_test_backend, start_link, [])
    end, Nodes),

    %% Ensure that a regular call works.
    {_, FirstName} = FirstNode = hd(Nodes),
    ok = rpc:call(FirstName, partisan_test_backend, ok, []),

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    ok.

basic_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config),

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),

    %% Start nodes.
    Nodes = ?SUPPORT:start(basic_test, Config,
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
                                      ?DEFAULT_PEER_SERVICE_MANAGER,
                                      connections,
                                      []),
                             %% ct:pal("Connections: ~p~n", [Connections]),
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
    ?SUPPORT:stop(Nodes),

    ok.

client_server_manager_test(Config) ->
    %% Use the client/server peer service manager.
    Manager = partisan_client_server_peer_service_manager,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(2, "server", Config), %% [server_1, server_2],

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config), %% client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = ?SUPPORT:start(client_server_manager_test, Config,
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
    ?SUPPORT:stop(Nodes),

    ok.

hyparview_manager_partition_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config), %% [server],

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config), %% client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = ?SUPPORT:start(hyparview_manager_partition_test, Config,
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
    ?SUPPORT:stop(Nodes),

    ok.

hyparview_manager_high_active_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config), %% [server],

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config), %% client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = ?SUPPORT:start(hyparview_manager_high_active_test, Config,
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
    ?SUPPORT:stop(Nodes),

    ok.

hyparview_manager_low_active_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Start nodes.
    MaxActiveSize = 3,

    Servers = ?SUPPORT:node_list(1, "server", Config), %% [server],

    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config), %% client_list(?CLIENT_NUMBER),

    Nodes = ?SUPPORT:start(hyparview_manager_low_active_test, Config,
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
    ?SUPPORT:stop(Nodes),

    ok.

hyparview_manager_high_client_test(Config) ->
    %% Use hyparview.
    Manager = partisan_hyparview_peer_service_manager,

    %% Start clients,.
    Clients = ?SUPPORT:node_list(11, "client", Config), %% client_list(11),

    %% Start servers.
    Servers = ?SUPPORT:node_list(1, "server", Config), %% [server],

    Nodes = ?SUPPORT:start(hyparview_manager_high_client_test, Config,
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
        {fail, {false, {connected_check_failed, ConnectedFails}}} ->
            ct:fail("Graph is not connected, unable to find route between pairs of nodes ~p",
                    [ConnectedFails]);
        {fail, {false, {symmetry_check_failed, SymmetryFails}}} ->
            ct:fail("Symmetry is broken (ie. node1 has node2 in it's view but vice-versa is not true) between the following "
                    "pairs of nodes: ~p", [SymmetryFails]);
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
    ?SUPPORT:stop(Nodes),

    ok.


%% ===================================================================
%% Internal functions.
%% ===================================================================

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
    Members = ideally_connected_members(Node, Nodes),

    ForwardOptions = rpc:call(Node, partisan_config, get, [forward_options, []]),
    ct:pal("Using forward options: ~p", [ForwardOptions]),

    lists:foreach(fun(Member) ->
        Rand = rand:uniform(),

        %% {ok, DirectMembers} = rpc:call(Node, Manager, members, []),
        %% IsDirect = lists:member(Member, DirectMembers),
        %% ct:pal("Node ~p is directly connected: ~p; ~p", [Member, IsDirect, DirectMembers]),

        %% now fetch the value from the random destination node
        case wait_until(fun() ->
                        ct:pal("Requesting node ~p to forward message ~p to store_proc on node ~p", [Node, Rand, Member]),
                        ok = rpc:call(Node, Manager, forward_message, [Member, undefined, store_proc, {store, Rand}, ForwardOptions]),
                        ct:pal("Message dispatched..."),

                        ct:pal("Checking ~p for value...", [Member]),

                        %% it must match with what we asked the node to forward
                        case rpc:call(Member, application, get_env, [partisan, forward_message_test]) of
                            {ok, R} ->
                                Test = R =:= Rand,
                                ct:pal("Received from ~p ~p, should be ~p: ~p", [Member, R, Rand, Test]),
                                Test;
                            Other ->
                                ct:pal("Received other, failing: ~p", [Other]),
                                false
                        end
                end, 60 * 2, 500) of
            ok ->
                ok;
            {fail, false} ->
                ct:fail("Message delivery failed, Node:~p, Manager:~p, Nodes:~p~n ", [Node, Manager, Nodes])
        end
    end, Members -- [Node]),

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
hyparview_check_stopped_member(_, [_Node]) -> 
    {undefined, []};
hyparview_check_stopped_member(KilledNode, Nodes) ->
    ct:pal("Killed node ~p.", [KilledNode]),

    %% Obtain the membership from all the nodes,
    %% the killed node shouldn't be there
    lists:filtermap(fun({_, Node}) ->
        ct:pal("Making sure ~p doesn't have ~p in it's membership.", [Node, KilledNode]),

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
            [?SUPPORT:connect(Graph, Node, N) || #{name := N} <- Active]
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
verify_leave({_, NodeToLeave}, Nodes, Manager) ->
    %% Pause for gossip interval * node exchanges + gossip interval for full convergence.
    timer:sleep(?OVERRIDE_PERIODIC_INTERVAL * length(Nodes) + ?OVERRIDE_PERIODIC_INTERVAL),

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
                              {fail, {false, {IncorrenectNode, Expected, Contains}}} ->
                                 ct:fail("Membership incorrect; node ~p should have ~p but has ~p",
                                         [IncorrenectNode, Expected, Contains])
                          end
                  end, Nodes),

    %% Remove a node from the cluster.
    [{_, _}, {_, Node2}, {_, _}, {_, _}] = Nodes,
    NodeToLeaveMap = rpc:call(NodeToLeave, partisan_peer_service_manager, myself, []),
    ct:pal("Removing node ~p from the cluster with node map: ~p", [NodeToLeave, NodeToLeaveMap]),
    ok = rpc:call(Node2, partisan_peer_service, leave, [NodeToLeaveMap]),
    
    %% Pause for gossip interval * node exchanges + gossip interval for full convergence.
    timer:sleep(?OVERRIDE_PERIODIC_INTERVAL * length(Nodes) + ?OVERRIDE_PERIODIC_INTERVAL),

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology.
    %%
    VerifyRemoveFun = fun({_, Node}) ->
        try
            {ok, Members} = rpc:call(Node, Manager, members, []),
            SortedNodes = case Node of
                NodeToLeave ->
                    [NodeToLeave];
                _ ->
                    lists:usort([N || {_, N} <- Nodes]) -- [NodeToLeave]
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
        catch
            _:_ ->
                case Node of
                    NodeToLeave ->
                        %% Node terminated, OK.
                        true;
                    _ ->
                        false
                end
        end
    end,

    %% Verify the membership is correct.
    lists:foreach(fun(Node) ->
                          VerifyNodeFun = fun() -> VerifyRemoveFun(Node) end,

                          case wait_until(VerifyNodeFun, 60 * 2, 100) of
                              ok ->
                                  ok;
                              {fail, {false, {IncorrectNode, Expected, Contains}}} ->
                                 ct:fail("Membership incorrect; node ~p should have ~p but has ~p",
                                         [IncorrectNode, Expected, Contains])
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
            receiver(Manager, BenchPid, Count - 1);
        Other ->
            lager:warning("Got incorrect message: ~p", [Other])
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

%% Same test as hyparview but with xbot variant integrated
%% @private
hyparview_xbot_membership_check(Nodes) ->
    Manager = partisan_hyparview_xbot_peer_service_manager,
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
            [?SUPPORT:connect(Graph, Node, N) || #{name := N} <- Active]
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
    
hyparview_xbot_manager_high_active_test(Config) ->
    %% Use hyparview with xbot integration.
    Manager = partisan_hyparview_xbot_peer_service_manager,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(1, "server", Config), %% [server],

    %% Specify clients.
    Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config), %% client_list(?CLIENT_NUMBER),

    %% Start nodes.
    Nodes = ?SUPPORT:start(hyparview_xbot_manager_high_active_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {max_active_size, 5},
                   {servers, Servers},
                   {clients, Clients}]),
                   
    %%timer:sleep(20000),

    CheckStartedFun = fun() ->
                        case hyparview_xbot_membership_check(Nodes) of
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
                            [] -> 
                                true;
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
    ?SUPPORT:stop(Nodes),

    ok.

hyparview_xbot_manager_low_active_test(Config) ->
    %% Use hyparview with xbot integration.
    Manager = partisan_hyparview_xbot_peer_service_manager,

    %% Start nodes.
    MaxActiveSize = 2,

    Servers = ?SUPPORT:node_list(1, "server", Config), %% [server],

    Clients = ?SUPPORT:node_list(8, "client", Config), %% client_list(?CLIENT_NUMBER),

    Nodes = ?SUPPORT:start(hyparview_xbot_manager_low_active_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {max_active_size, MaxActiveSize},
                   {servers, Servers},
                   {clients, Clients}]),

	timer:sleep(60000),

    CheckStartedFun = fun() ->
                        case hyparview_xbot_membership_check(Nodes) of
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
    ?SUPPORT:stop(Nodes),

    ok.

hyparview_xbot_manager_high_client_test(Config) ->
    %% Use hyparview with xbot integration.
    Manager = partisan_hyparview_xbot_peer_service_manager,

    %% Start clients,.
    Clients = ?SUPPORT:node_list(11, "client", Config), %% client_list(11),

    %% Start servers.
    Servers = ?SUPPORT:node_list(1, "server", Config), %% [server],

    Nodes = ?SUPPORT:start(hyparview_xbot_manager_low_active_test, Config,
                  [{partisan_peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    CheckStartedFun = fun() ->
                        case hyparview_xbot_membership_check(Nodes) of
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
    ?SUPPORT:stop(Nodes),

    ok.

%% @private
ideally_connected_members(Node, Nodes) ->
    case rpc:call(Node, partisan_config, get, [partisan_peer_service_manager]) of
        ?DEFAULT_PEER_SERVICE_MANAGER ->
            M = lists:usort([N || {_, N} <- Nodes]),
            ct:pal("Fully connected: checking forward functionality for all nodes: ~p", [M]),
            M;
        Manager ->
            case rpc:call(Node, partisan_config, get, [broadcast, false]) of
                true ->
                    M = lists:usort([N || {_, N} <- Nodes]),
                    ct:pal("Checking forward functionality for all nodes: ~p", [M]),
                    M;
                false ->
                    {ok, M} = rpc:call(Node, Manager, members, []),
                    ct:pal("Checking forward functionality for subset of nodes: ~p", [M]),
                    M
            end
    end.