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
%%

-module(partisan_support).

-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").
-include("partisan_logger.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-if(?OTP_RELEASE >= 25).
    %% already defined by OTP
-else.
    -define(CT_PEER, ct_slave).
-endif.

-compile([nowarn_export_all, export_all]).



%% @private
start(Case, Config, Options) ->
    debug("Beginning test case: ~p", [Case]),

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
    ok = application:set_env(sasl, sasl_error_logger, false),
    application:start(sasl),

    Servers = proplists:get_value(servers, Options, []),
    Clients = proplists:get_value(clients, Options, []),

    NodeNames = case proplists:get_value(num_nodes, Options, undefined) of
        undefined ->
            lists:flatten(Servers ++ Clients);
        NumNodes ->
            node_list(NumNodes, "node", Config)
    end,

    %% Start all nodes.
    InitializerFun = fun(Name) ->
        debug("Starting node: ~p", [Name]),

        NodeConfig = [
            {boot_timeout, 10}, % seconds
            {monitor_master, true},
            {kill_if_fail, true},
            {startup_functions, [
                {code, set_path, [codepath()]},
                {logger, set_handler_config, [
                    default, config, #{file => "log/ct_console.log"}
                ]},
                {logger, set_handler_config, [
                    default, formatter, {logger_formatter, #{}}
                ]}
            ]}
        ],

        case ?CT_PEER:start(Name, NodeConfig) of
            {ok, Node} ->
                {Name, Node};
            Error ->
                ct:fail(Error)
        end
    end,
    Nodes = lists:map(InitializerFun, NodeNames),

        %% Configure settings.
    ConfigureFun = fun({Name, Node}) ->
        %% Configure the peer service.
        PeerService = proplists:get_value(
            peer_service_manager, Options
            ),
        debug(
            "Setting peer service manager on node ~p to ~p",
            [Node, PeerService]
        ),
        ok = rpc:call(
            Node, partisan_config, set,
            [peer_service_manager, PeerService],
            5000
        ),

         ok = rpc:call(
            Node, application, set_env,
            [partisan, hyparview, ?HYPARVIEW_DEFAULTS],
            5000
        ),

        ok = rpc:call(
            Node, application, set_env, [partisan, peer_ip, ?PEER_IP],
            5000
        ),

        ok = rpc:call(
            Node, partisan_config, set,
            [periodic_interval, ?OVERRIDE_PERIODIC_INTERVAL],
            5000
        ),



        DistanceEnabled = case ?config(distance_enabled, Config) of
                          undefined ->
                              true;
                          DE ->
                              DE
                      end,
        debug("Setting distance_enabled to: ~p", [DistanceEnabled]),
        ok = rpc:call(
            Node, partisan_config, set, [distance_enabled, DistanceEnabled],
            5000
        ),

        PeriodicEnabled =
            case ?config(periodic_enabled, Config) of
                undefined ->
                    true;
                PDE ->
                    PDE
            end,

        debug("Setting periodic_enabled to: ~p", [PeriodicEnabled]),
        ok = rpc:call(
            Node, partisan_config, set, [periodic_enabled, PeriodicEnabled],
            5000
        ),

        MembershipStrategyTracing = case ?config(membership_strategy_tracing, Config) of
                          undefined ->
                              false;
                          MST ->
                              MST
                      end,
        debug("Setting membership_strategy_tracing to: ~p", [MembershipStrategyTracing]),
        ok = rpc:call(
            Node,
            partisan_config,
            set,
            [membership_strategy_tracing, MembershipStrategyTracing],
            5000
        ),

        ForwardOptions = case ?config(forward_options, Config) of
                          undefined ->
                              [];
                          FO ->
                              FO
                      end,
        debug("Setting forward_options to: ~p", [ForwardOptions]),
        ok = rpc:call(
            Node, partisan_config, set, [forward_options, ForwardOptions],
            5000
        ),

        %% Configure random seed on the runner.
        ok = partisan_config:set(random_seed, {1, 1, 1}),

        %% Configure random seed on the nodes.
        PHashNode = erlang:phash2([Node]),
        ok = rpc:call(
            Node, partisan_config, set, [random_seed, {PHashNode, 1, 1}],
            5000
        ),

        Replaying = case ?config(replaying, Config) of
                          undefined ->
                              false;
                          RP ->
                              RP
                      end,
        debug("Setting replaying to: ~p", [Replaying]),
        ok = rpc:call(Node, partisan_config, set, [replaying, Replaying]),

        Shrinking = case ?config(shrinking, Config) of
                          undefined ->
                              false;
                          SH ->
                              SH
                      end,
        debug("Setting shrinking to: ~p", [Shrinking]),
        ok = rpc:call(Node, partisan_config, set, [shrinking, Shrinking]),

        MembershipStrategy = case ?config(membership_strategy, Config) of
                          undefined ->
                              ?DEFAULT_MEMBERSHIP_STRATEGY;
                          S ->
                              S
                      end,
        debug("Setting membership_strategy to: ~p", [MembershipStrategy]),
        ok = rpc:call(Node, partisan_config, set, [membership_strategy, MembershipStrategy]),

        debug("Enabling tracing since we are in test mode....", []),
        ok = rpc:call(Node, partisan_config, set, [tracing, true]),

        Disterl = case ?config(connect_disterl, Config) of
                          undefined ->
                              false;
                          false ->
                              false;
                          true ->
                              true
                      end,
        debug("Setting disterl to: ~p", [Disterl]),
        ok = rpc:call(
            Node, partisan_config, set, [connect_disterl, Disterl]
        ),

        DisableFastReceive = case ?config(disable_fast_receive, Config) of
                          undefined ->
                              false;
                          FR ->
                              FR
                      end,
        debug("Setting disable_fast_receive to: ~p", [DisableFastReceive]),
        ok = rpc:call(Node, partisan_config, set, [disable_fast_receive, DisableFastReceive]),

        DisableFastForward = case ?config(disable_fast_forward, Config) of
                          undefined ->
                              false;
                          FF ->
                              FF
                      end,
        debug("Setting disable_fast_forward to: ~p", [DisableFastForward]),
        ok = rpc:call(Node, partisan_config, set, [disable_fast_forward, DisableFastForward]),

        BinaryPadding = case ?config(binary_padding, Config) of
                          undefined ->
                              false;
                          BP ->
                              BP
                      end,
        debug("Setting binary_padding to: ~p", [BinaryPadding]),
        ok = rpc:call(Node, partisan_config, set, [binary_padding, BinaryPadding]),

        Broadcast = case ?config(broadcast, Config) of
                          undefined ->
                              false;
                          B ->
                              B
                      end,
        debug("Setting broadcast to: ~p", [Broadcast]),
        ok = rpc:call(Node, partisan_config, set, [broadcast, Broadcast]),

        IngressDelay = case ?config(ingress_delay, Config) of
                          undefined ->
                              0;
                          ID ->
                              ID
                      end,
        debug("Setting ingress_delay to: ~p", [IngressDelay]),
        ok = rpc:call(Node, partisan_config, set, [ingress_delay, IngressDelay]),

        EgressDelay = case ?config(egress_delay, Config) of
                          undefined ->
                              0;
                          ED ->
                              ED
                      end,
        debug("Setting egress_delay to: ~p", [EgressDelay]),
        ok = rpc:call(Node, partisan_config, set, [egress_delay, EgressDelay]),

        Channels = case ?config(channels, Config) of
                          undefined ->
                              ?CHANNELS;
                          C ->
                              C
                      end,
        debug("Setting channels to: ~p", [Channels]),
        ok = rpc:call(Node, partisan_config, set, [channels, Channels]),

        CausalLabels = case ?config(causal_labels, Config) of
                          undefined ->
                              [];
                          CL ->
                              CL
                      end,
        debug("Setting causal_labels to: ~p", [CausalLabels]),
        ok = rpc:call(Node, partisan_config, set, [causal_labels, CausalLabels]),

        PidEncoding = case ?config(pid_encoding, Config) of
                          undefined ->
                              true;
                          PE ->
                              PE
                      end,
        debug("Setting pid_encoding to: ~p", [PidEncoding]),
        ok = rpc:call(Node, partisan_config, set, [pid_encoding, PidEncoding]),

        ok = rpc:call(Node, partisan_config, set, [tls, ?config(tls, Config)]),

        ok = rpc:call(
            Node, partisan_config, set,
            [tls_client_options, ?config(tls_client_options, Config)]
        ),
        ok = rpc:call(
            Node, partisan_config, set,
            [tls_server_options, ?config(tls_server_options, Config)]
        ),

        Parallelism = case ?config(parallelism, Config) of
                          undefined ->
                              ?PARALLELISM;
                          P ->
                              P
                      end,
        debug("Setting parallelism to: ~p", [Parallelism]),
        ok = rpc:call(Node, partisan_config, set, [parallelism, Parallelism]),

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

    %% Load and configure applications on all of the nodes.
    LoaderFun = fun({_Name, Node} = NameNode) ->
        debug("Loading applications on node: ~p", [Node]),

        %% Manually force sasl loading, and disable the logger.
        ok = rpc:call(Node, application, load, [sasl]),
        ok = rpc:call(
            Node, application, set_env,
            [sasl, sasl_error_logger, false]
        ),
        ok = rpc:call(Node, application, start, [sasl]),

        ok = rpc:call(
            Node, application, set_env,
            [kernel, logger_level, debug]
        ),

        %% Set Partisan Env
        ConfigureFun(NameNode),

        %% Finally load Partisan
        ok = rpc:call(Node, application, load, [partisan])

    end,
    lists:map(LoaderFun, Nodes),


    %% lists:foreach(ConfigureFun, Nodes),

    debug("Starting nodes.", []),

    StartFun = fun({_Name, Node}) ->
        %% Start partisan.
        {ok, _} = rpc:call(Node, application, ensure_all_started, [partisan]),
        %% Start a dummy registered process that saves in the env whatever
        %% message it gets.
        Pid = rpc:call(
            Node, erlang, spawn, [fun() -> store_proc_receiver() end]
        ),
        true = rpc:call(Node, erlang, register, [store_proc, Pid]),
        debug("Registered store_proc on pid ~p, node ~p", [Pid, Node])
    end,
    lists:foreach(StartFun, Nodes),

    debug("Clustering nodes.", []),
    lists:foreach(
        fun(Node) ->
            cluster(Node, Nodes, Options, Config)
        end,
        Nodes
    ),

    debug("Partisan fully initialized.", []),
    {_, Node} = hd(Nodes),
    {ok, Members} = rpc:call(
        Node, partisan_peer_service, members, [], 3000
    ),
    debug("Cluster members: ~p", [Members]),

    Nodes.

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
    Manager = proplists:get_value(peer_service_manager, Options),

    Servers = proplists:get_value(servers, Options, []),
    Clients = proplists:get_value(clients, Options, []),

    AmIServer = lists:member(Name, Servers),
    AmIClient = lists:member(Name, Clients),

    OtherNodes =
        case {Manager, AmIServer, AmIClient} of
            {?DEFAULT_PEER_SERVICE_MANAGER, _, _} ->
                %% We are all peers, I connect to everyone (but myself)
                omit([Name], Nodes);
            {partisan_client_server_peer_service_manager, true, false} ->
                %% If I'm a server, I connect to both
                %% clients and servers!
                omit([Name], Nodes);
            {partisan_client_server_peer_service_manager, false, true} ->
                %% I'm a client, pick servers.
                omit(Clients, Nodes);
            {partisan_client_server_peer_service_manager, _, _} ->
                %% I assume I'm a server, I connect to both
                %% clients and servers!
                omit([Name], Nodes);
            {partisan_hyparview_peer_service_manager, true, false} ->
                %% If I'm a server, I connect to both
                %% clients and servers!
                omit([Name], Nodes);
            {partisan_hyparview_peer_service_manager, false, true} ->
                %% I'm a client, pick servers.
                omit(Clients, Nodes);
            {partisan_hyparview_peer_service_manager, _, _} ->
                omit([Name], Nodes);
            {partisan_hyparview_xbot_peer_service_manager, true, false} ->
                %% If I'm a server, I connect to both
                %% clients and servers!
                omit([Name], Nodes);
            {partisan_hyparview_xbot_peer_service_manager, false, true} ->
                %% I'm a client, pick servers.
                omit(Clients, Nodes);
            {partisan_hyparview_xbot_peer_service_manager, _, _} ->
                omit([Name], Nodes)
        end,

    lists:map(
        fun(OtherNode) -> cluster(Myself, OtherNode, Config) end,
        OtherNodes
    ).


cluster({_, Node}, {_, Peer}, Config) ->
    PeerSpec = rpc:call(Peer, partisan, node_spec, [], 5000),

    JoinMethod =
        case ?config(sync_join, Config) of
            undefined ->
                join;
            true ->
                sync_join;
            false ->
                join
        end,

    debug("Joining node: ~p with: ~p", [Node, Peer]),

    ok = rpc:call(
        Node, partisan_peer_service, JoinMethod, [PeerSpec], 60000
    ).


%% @private
stop(Nodes) ->
    StopFun = fun({_, Node}) ->
        case ?CT_PEER:stop(Node) of
            {ok, _} ->
                ok;
            {error, stop_timeout, _} ->
                debug("Failed to stop node ~p: stop_timeout!", [Node]),
                ok;
            {error, not_started, _} ->
                ok;
            Error ->
                ct:pal("Error while stopping CT_PEER ~p", [Error])
        end
    end,

    _ = catch lists:foreach(StopFun, Nodes),
    ok.

%% @private
connect(G, N1, N2) ->
    %% Add vertex for neighboring node.
    digraph:add_vertex(G, N1),
    % debug("Adding vertex: ~p", [N1]),

    %% Add vertex for neighboring node.
    digraph:add_vertex(G, N2),
    % debug("Adding vertex: ~p", [N2]),

    %% Add edge to that node.
    digraph:add_edge(G, N1, N2),
    % debug("Adding edge from ~p to ~p", [N1, N2]),

    ok.

%% @private
store_proc_receiver() ->
    receive
        {store, N} ->
            %% save the number in the environment
            application:set_env(partisan, forward_message_test, N)
    end,
    store_proc_receiver().

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
node_list(0, _Name, _Config) ->
    [];
node_list(N, Name, Config) ->
    case ?config(hash, Config) of
        undefined ->
            [ list_to_atom(string:join([Name,
                                        integer_to_list(X)],
                                    "_")) ||
                X <- lists:seq(1, N) ];
        _ ->
            [ list_to_atom(string:join([Name,
                                        integer_to_list(?config(hash, Config)),
                                        integer_to_list(X)],
                                    "_")) ||
                X <- lists:seq(1, N) ]
    end.

%% @private
debug(Message, Format) ->
    case partisan_config:get(tracing, true) of
        true ->
            ct:print(Message, Format);
        false ->
            ok
    end.