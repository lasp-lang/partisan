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

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

-compile([export_all]).

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
    ok = application:set_env(sasl,
                             sasl_error_logger,
                             false),
    application:start(sasl),

    %% Load lager.
    {ok, _} = application:ensure_all_started(lager),

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
                            debug("Loading applications on node: ~p", [Node]),

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
            debug("Setting peer service manager on node ~p to ~p", [Node, PeerService]),
            ok = rpc:call(Node, partisan_config, set,
                          [partisan_peer_service_manager, PeerService]),

            MaxActiveSize = proplists:get_value(max_active_size, Options, 5),
            ok = rpc:call(Node, partisan_config, set,
                          [max_active_size, MaxActiveSize]),
                          
            ok = rpc:call(Node, partisan_config, set,
                          [periodic_interval, ?OVERRIDE_PERIODIC_INTERVAL]),

            ok = rpc:call(Node, application, set_env, [partisan, peer_ip, ?PEER_IP]),

            ForwardOptions = case ?config(forward_options, Config) of
                              undefined ->
                                  [];
                              FO ->
                                  FO
                          end,
            debug("Setting forward_options to: ~p", [ForwardOptions]),
            ok = rpc:call(Node, partisan_config, set, [forward_options, ForwardOptions]),

            %% Configure random seed on the runner.
            ok = partisan_config:set(random_seed, {1, 1, 1}),

            %% Configure random seed on the nodes.
            ok = rpc:call(Node, partisan_config, set, [random_seed, {1, 1, 1}]),

            MembershipStrategy = case ?config(membership_strategy, Config) of
                              undefined ->
                                  ?DEFAULT_MEMBERSHIP_STRATEGY;
                              S ->
                                  S
                          end,
            debug("Setting membership_strategy to: ~p", [MembershipStrategy]),
            ok = rpc:call(Node, partisan_config, set, [membership_strategy, MembershipStrategy]),

            debug("Enabling tracing since we are in test mode....", []),
            ok = rpc:call(Node, partisan_config, set, [tracing, false]),

            Disterl = case ?config(disterl, Config) of
                              undefined ->
                                  false;
                              false ->
                                  false;
                              true ->
                                  true
                          end,
            debug("Setting disterl to: ~p", [Disterl]),
            ok = rpc:call(Node, partisan_config, set, [disterl, Disterl]),

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

    debug("Starting nodes.", []),

    StartFun = fun({_Name, Node}) ->
                        %% Start partisan.
                        {ok, _} = rpc:call(Node, application, ensure_all_started, [partisan]),
                        %% Start a dummy registered process that saves in the env whatever message it gets.
                        Pid = rpc:call(Node, erlang, spawn, [fun() -> store_proc_receiver() end]),
                        true = rpc:call(Node, erlang, register, [store_proc, Pid]),
                        debug("Registered store_proc on pid ~p, node ~p", [Pid, Node])
               end,
    lists:foreach(StartFun, Nodes),

    debug("Clustering nodes.", []),
    lists:foreach(fun(Node) -> cluster(Node, Nodes, Options, Config) end, Nodes),

    debug("Partisan fully initialized.", []),

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
    Manager = proplists:get_value(partisan_peer_service_manager, Options),

    Servers = proplists:get_value(servers, Options, []),
    Clients = proplists:get_value(clients, Options, []),

    AmIServer = lists:member(Name, Servers),
    AmIClient = lists:member(Name, Clients),

    OtherNodes = case Manager of
                     ?DEFAULT_PEER_SERVICE_MANAGER ->
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
                        end;
                     %same as hyparview but for hyparview with xbot integration
                     partisan_hyparview_xbot_peer_service_manager ->
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
                          ?CHANNELS;
                      C ->
                          C
                  end,
    JoinMethod = case ?config(sync_join, Config) of
                    undefined ->
                        join;
                    true ->
                        sync_join;
                    false ->
                        join
                  end,
    debug("Joining node: ~p to ~p at port ~p", [Node, OtherNode, PeerPort]),
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
                %% debug("Failed to stop node ~p: stop_timeout!", [Name]),
                stop(Nodes),
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
    case partisan_config:get(tracing, ?TRACING) of
        true ->
            ct:print(Message, Format);
        false ->
            ok
    end.