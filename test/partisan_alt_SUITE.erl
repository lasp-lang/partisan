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

-module(partisan_alt_SUITE).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").


-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").
-include("partisan.hrl").
-include("partisan_logger.hrl").
-include("partisan_test.hrl").


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



%% =============================================================================
%% CT CALLBACKS
%% =============================================================================



init_per_suite(Config) ->
    partisan_SUITE:init_per_suite(Config).


end_per_suite(Config) ->
    partisan_SUITE:end_per_suite(Config).


init_per_testcase(Case, Config) ->
    partisan_SUITE:init_per_testcase(Case, Config).


end_per_testcase(Case, Config) ->
    partisan_SUITE:end_per_testcase(Case, Config).



init_per_group(with_scamp_v1_membership_strategy, Config) ->
    [{membership_strategy, partisan_scamp_v1_membership_strategy}] ++ Config;

init_per_group(with_scamp_v2_membership_strategy, Config) ->
    [{membership_strategy, partisan_scamp_v2_membership_strategy}] ++ Config;
init_per_group(_, Config) ->
    [{parallelism, 1}] ++ Config.


end_per_group(_, _Config) ->
    ok.


all() ->
    [
        {group, with_full_membership_strategy, []},
        {group, with_scamp_v1_membership_strategy, []},
        {group, with_scamp_v2_membership_strategy, []}
    ].


groups() ->
    [
        {with_full_membership_strategy, [], [
            connectivity_test
            ,gossip_demers_direct_mail_test
        ]},

        {with_scamp_v1_membership_strategy, [],[
            connectivity_test
        ]},

        {with_scamp_v2_membership_strategy, [],[
            connectivity_test
            ,gossip_demers_direct_mail_test
        ]}
    ].



%% =============================================================================
%% TESTS
%% =============================================================================



gossip_demers_direct_mail_test(Config) ->
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
    Nodes = ?SUPPORT:start(
        gossip_demers_direct_mail_test,
        Config,
        [
            {peer_service_manager, Manager},
            {servers, Servers},
            {clients, Clients}
        ]
    ),

    ?PUT_NODES(Nodes),
    ?PAUSE_FOR_CLUSTERING,

    %% Verify forward message functionality.
    lists:foreach(
        fun({_Name, Node}) ->
            ok = check_forward_message(Node, Manager, Nodes)
        end,
        Nodes
    ),

    %% Start gossip backend on all nodes.
    lists:foreach(
        fun({_Name, Node}) ->

            case rpc:call(Node, demers_direct_mail, start_link, []) of
                {ok, Pid} ->
                    ct:pal(
                        "Started gossip backend on node ~p (~p)",
                        [Node, Pid]
                    );
                {error, Reason} ->
                    ct:pal(
                        "Couldn't start gossip backend on node ~p. Reason: ~p",
                        [Node, Reason]
                    )
            end
        end,
        Nodes
    ),

    %% Pause for protocol delay and periodic intervals to fire.
    timer:sleep(10000),

    %% Gossip.
    [{_, _}, {_, Node2}, {_, _}, {_, Node4}] = Nodes,
    Self = self(),

    ReceiverFun = fun() ->
        receive
            hello ->
                ?LOG_DEBUG("received value from gossip receiver", []),
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
                  [{peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    ?PUT_NODES(Nodes),

    ?PAUSE_FOR_CLUSTERING,

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



    ok.

otp_test(Config) ->
    %% Use the default peer service manager.
    Manager = ?DEFAULT_PEER_SERVICE_MANAGER,

    %% Specify servers.
    Servers = ?SUPPORT:node_list(4, "server", Config),

    %% Specify clients.
    %% Clients = ?SUPPORT:node_list(?CLIENT_NUMBER, "client", Config),
    Clients = [],

    %% Start nodes.
    Nodes = ?SUPPORT:start(
        otp_test, Config, [
            {peer_service_manager, Manager},
            {servers, Servers},
            {clients, Clients}
        ]
    ),

    ?PUT_NODES(Nodes),

    ?PAUSE_FOR_CLUSTERING,


    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% gen_server tests.
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    %% Start the test backend on all the nodes.
    lists:foreach(
        fun({_, Node}) ->
            Pid = rpc:call(Node, erlang, whereis, [partisan_test_server]),
            true = rpc:call(Node, erlang, is_process_alive, [Pid]),
            ct:pal("partisan_test_server ~p is alive on node ~p", [Pid, Node])
        end,
        Nodes
    ),

    [{_, Node1}, {_, Node2} | _] = Nodes,

    ct:print(
        "Runner is connected via disterl to ~p", [erlang:nodes()]
    ),


    CallResult = rpc:call(
        Node1,
        partisan_gen_server, call,
        [{partisan_test_server, Node2}, call, 5000]
    ),

    ?assertEqual(
        ok,
        CallResult,
        "Ensure that a regular call works."
    ),

    DelayedCallResult = rpc:call(
        Node1,
        partisan_gen_server, call,
        [{partisan_test_server, Node2}, delayed_reply_call, 5000]
    ),

    ?assertEqual(
        ok,
        DelayedCallResult,
        "Ensure that a regular call with delayed response works."
    ),

    %% Ensure that a cast works.
    Self = self(),

    CastReceiverFun = fun() ->
        receive
            ok ->
                Self ! ok
        end
    end,

    CastReceiverPid = rpc:call(Node2, erlang, spawn, [CastReceiverFun]),

    true = rpc:call(
        Node2, erlang, register, [cast_receiver, CastReceiverPid]
    ),

    [{_, Node1}, {_, Node2} | _] = Nodes,

    ok = rpc:call(
        Node1,
        partisan_gen_server,
        cast,
        [{partisan_test_server, Node2}, {cast, cast_receiver}]
    ),

    receive
        ok ->
            ok;
        Other ->
            error_logger:format("Received invalid response: ~p", [Other]),
            ct:fail({error, wrong_message})
    after
        1000 ->
            ct:fail({error, no_message})
    end,



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
                  [{peer_service_manager, Manager},
                   {servers, Servers},
                   {clients, Clients}]),

    ?PUT_NODES(Nodes),

    ?PAUSE_FOR_CLUSTERING,

    %% Verify membership.
    %%
    %% Every node should know about every other node in this topology.
    %%
    VerifyFun = fun(Node) ->
            {ok, Members} = rpc:call(Node, Manager, members, []),
            SortedNodes = lists:usort([N || {_, N} <- Nodes]),
            SortedMembers = lists:usort(Members),
            case SortedMembers =:= SortedNodes of
                true ->
                    true;
                false ->
                    ct:pal(
                        "Membership incorrect; node ~p should have ~p ~n"
                        "but has ~p",
                        [Node, SortedNodes, SortedMembers]
                    ),
                    {false, {Node, SortedNodes, SortedMembers}}
            end
    end,

    %% Verify the membership is correct.
    lists:foreach(
        fun({_, Node}) ->
            VerifyNodeFun = fun() -> VerifyFun(Node) end,

            case wait_until(VerifyNodeFun, 60 * 2, 100) of
                ok ->
                    ok;
                {fail, {false, {Node, Expected, Contains}}} ->
                    ct:fail(
                        "Membership incorrect; node ~p should have ~p ~n"
                        "but has ~p",
                        [Node, Expected, Contains]
                    )
            end
        end,
        Nodes
    ),

    %% Verify forward message functionality.
    lists:foreach(
        fun({_Name, Node}) ->
            ok = check_forward_message(Node, Manager, Nodes)
        end,
        Nodes
    ),

    %% Verify parallelism.
    ConfigParallelism = proplists:get_value(parallelism, Config, ?PARALLELISM),
    ct:pal("Configured parallelism: ~p", [ConfigParallelism]),

    %% Verify channels.
    ConfigChannelSpecs = proplists:get_value(channels, Config, ?CHANNELS),
    ct:pal("Configured channels: ~p", [ConfigChannelSpecs]),

    %% Verify we have enough connections.
    VerifyConnectionsFun = fun(Node, Channel, Parallelism) ->

        FoldFun = fun(_NodeSpec, NodeConnections, Acc) ->
            ChannelConnections = lists:filter(
                fun(Conn) ->
                    partisan_peer_connections:channel(Conn)
                    == Channel
                end,
                NodeConnections
            ),

            case length(ChannelConnections) == Parallelism of
                true ->
                    Acc andalso true;
                false ->
                    Acc andalso false
            end
        end,

        rpc:call(Node, partisan_peer_connections, fold, [FoldFun, true])

    end,

    lists:foreach(
        fun({_Name, Node}) ->
            %% Get enabled parallelism.
            Parallelism = rpc:call(
                Node, partisan_config, get, [parallelism, ?PARALLELISM]
            ),
            ct:pal("Parallelism is: ~p", [Parallelism]),

            %% Get enabled channels.
            ChannelsMap = rpc:call(
                Node, partisan_config, get, [channels, ?CHANNELS]
            ),
            Channels = maps:keys(ChannelsMap),

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
                        ct:fail(
                            "Not enough connections have been opened; need: ~p",
                            [Parallelism]
                        )
                end
            end, Channels)
        end,
        Nodes
    ),



    ok.


%% ===================================================================
%% Internal functions.
%% ===================================================================


check_forward_message(Node, Manager, Nodes) ->
    Members = ideally_connected_members(Node, Nodes),

    ForwardOptions = rpc:call(
        Node, partisan_config, get, [forward_options, #{}]
    ),

    ct:pal("Using forward options: ~p", [ForwardOptions]),

    lists:foreach(
        fun(Member) ->
            Rand = rand:uniform(),

            %% {ok, DirectMembers} = rpc:call(Node, Manager, members, []),
            %% IsDirect = lists:member(Member, DirectMembers),
            %% ct:pal("Node ~p is directly connected: ~p; ~p", [Member, IsDirect, DirectMembers]),

            %% now fetch the value from the random destination node

            Fun = fun() ->
                ct:pal(
                    "Requesting node ~p to forward message ~p to "
                    "store_proc on node ~p",
                    [Node, Rand, Member]
                ),
                ok = rpc:call(
                    Node,
                    Manager,
                    forward_message,
                    [Member, store_proc, {store, Rand}, ForwardOptions]
                ),
                ct:pal("Message dispatched..."),

                ct:pal("Checking ~p for value...", [Member]),

                %% it must match with what we asked the node to forward
                Response = rpc:call(
                    Member,
                    application,
                    get_env,
                    [partisan, forward_message_test]
                ),

                case Response of
                    {ok, R} ->
                        Test = R =:= Rand,
                        ct:pal(
                            "Received from ~p ~p, should be ~p: ~p",
                            [Member, R, Rand, Test]
                        ),
                        Test;
                    Other ->
                        ct:pal("Received other, failing: ~p", [Other]),
                        false
                end
            end,

            case wait_until(Fun, 60 * 2, 500) of
                ok ->
                    ok;
                {fail, false} ->
                    ct:fail(
                        "Message delivery failed, "
                        "Node:~p, Manager:~p, Nodes:~p~n ",
                        [Node, Manager, Nodes]
                    )
            end
        end,
        Members -- [Node]
    ),

    ok.


wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        true ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry - 1, Delay)
    end.



%% @private
ideally_connected_members(Node, Nodes) ->
    case rpc:call(Node, partisan_config, get, [peer_service_manager]) of
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
