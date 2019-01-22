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

-module(prop_partisan).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include_lib("proper/include/proper.hrl").

-compile([export_all]).

%% System model.
-define(SYSTEM_MODEL, prop_partisan_reliable_broadcast).

-import(?SYSTEM_MODEL,
        [node_commands/0,
         node_initial_state/0,
         node_functions/0,
         node_precondition/2,
         node_postcondition/3,
         node_next_state/3,
         node_begin_property/0,
         node_begin_case/0,
         node_end_case/0]).

%% Fault model.
-define(FAULT_MODEL, prop_partisan_crash_fault_model).

-import(?FAULT_MODEL,
        [fault_commands/0,
         fault_initial_state/0,
         fault_functions/0,
         fault_precondition/2,
         fault_postcondition/3,
         fault_next_state/3,
         fault_is_crashed/2]).

-define(SUPPORT, partisan_support).

%% General test configuration
-define(NUM_NODES, 3).
-define(COMMAND_MULTIPLE, 10).
-define(CLUSTER_NODES, true).
-define(MANAGER, partisan_pluggable_peer_service_manager).

-define(PERFORM_LEAVES_AND_JOINS, false).           %% Do we allow cluster transitions during test execution:
                                                    %% EXTREMELY slow, given a single join can take ~30 seconds.

-define(PERFORM_FAULT_INJECTION, false).            %% Do we perform fault-injection?                                            

%% Debug.
-define(DEBUG, true).
-define(INITIAL_STATE_DEBUG, false).
-define(PRECONDITION_DEBUG, false).
-define(POSTCONDITION_DEBUG, false).

%% Partisan connection and forwarding settings.
-define(EGRESS_DELAY, 0).                           %% How many milliseconds to delay outgoing messages?
-define(INGRESS_DELAY, 0).                          %% How many millisconds to delay incoming messages?
-define(VNODE_PARTITIONING, false).                 %% Should communication be partitioned by vnode identifier?
-define(PARALLELISM, 1).                            %% How many connections should exist between nodes?
-define(CHANNELS,                                   %% What channels should be established?
        [undefined, broadcast, vnode, {monotonic, gossip}]).   
-define(CAUSAL_LABELS, []).                         %% What causal channels should be established?

-export([command/1, 
         initial_state/0, 
         next_state/3,
         precondition/2, 
         postcondition/3]).

%%%===================================================================
%%% Properties
%%%===================================================================

prop_sequential() ->
    node_begin_property(),

    ?FORALL(Cmds, more_commands(?COMMAND_MULTIPLE, commands(?MODULE)), 
        begin
            start_nodes(),
            node_begin_case(),
            {History, State, Result} = run_commands(?MODULE, Cmds), 
            node_end_case(),
            stop_nodes(),
            ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                [History,State,Result]),
                      aggregate(command_names(Cmds), Result =:= ok))
        end).

prop_parallel() ->
    node_begin_property(),
    
    ?FORALL(Cmds, more_commands(?COMMAND_MULTIPLE, parallel_commands(?MODULE)), 
        begin
            start_nodes(),
            node_begin_case(),
            {History, State, Result} = run_parallel_commands(?MODULE, Cmds), 
            node_end_case(),
            stop_nodes(),
            ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                [History,State,Result]),
                      aggregate(command_names(Cmds), Result =:= ok))
        end).

%%%===================================================================
%%% Initial state
%%%===================================================================

-record(state, {joined_nodes :: [node()],
                nodes :: [node()],
                node_state :: {dict:dict(), dict:dict()}, 
                fault_model_state :: term()}).

%% Initial model value at system start. Should be deterministic.
initial_state() -> 
    %% Initialize empty dictionary for process state.
    NodeState = node_initial_state(),

    %% Initialize fault model.
    FaultModelState = fault_initial_state(),

    %% Get the list of nodes.
    Nodes = names(),

    %% Assume first is joined -- node_1 will be the join point.
    JoinedNodes = case ?CLUSTER_NODES of
        false ->
            [hd(Nodes)];
        true ->
            Nodes
    end,

    %% Debug message.
    initial_state_debug("initial_state: nodes ~p joined_nodes ~p", [Nodes, JoinedNodes]),

    #state{joined_nodes=JoinedNodes,
           fault_model_state=FaultModelState,
           nodes=Nodes,
           node_state=NodeState}.

command(State) -> 
    ?LET(Commands, 
        %% Cluster maintenance commands.
        lists:flatmap(fun(Command) -> 
            case ?PERFORM_LEAVES_AND_JOINS of 
                true ->
                    [{1, Command}];
                false ->
                    []
            end
        end, cluster_commands(State)) ++ 

        %% Fault model commands.
        lists:flatmap(fun(Command) -> 
            case ?PERFORM_FAULT_INJECTION of 
                true ->
                    [{1, Command}];
                false ->
                    []
            end
        end, fault_commands()) ++

        %% System model commands.
        lists:map(fun(Command) -> {1, Command} end, node_commands()), 

        frequency(Commands)).

%% Picks whether a command should be valid under the current state.
precondition(#state{nodes=Nodes, joined_nodes=JoinedNodes}, {call, _Mod, join_cluster, [Node, JoinedNodes]}) -> 
    %% Only allow dropping of the first unjoined node in the nodes list, for ease of debugging.
    precondition_debug("precondition join_cluster: invoked for node ~p joined_nodes ~p", [Node, JoinedNodes]),

    ToBeJoinedNodes = Nodes -- JoinedNodes,
    precondition_debug("precondition join_cluster: remaining nodes to be joined are: ~p", [ToBeJoinedNodes]),

    case length(ToBeJoinedNodes) > 0 of
        true ->
            ToBeJoinedNode = hd(ToBeJoinedNodes),
            precondition_debug("precondition join_cluster: attempting to join ~p", [ToBeJoinedNode]),
            case ToBeJoinedNode of
                Node ->
                    precondition_debug("precondition join_cluster: YES attempting to join ~p is ~p", [ToBeJoinedNode, Node]),
                    true;
                OtherNode ->
                    precondition_debug("precondition join_cluster: NO attempting to join ~p not ~p", [ToBeJoinedNode, OtherNode]),
                    false
            end;
        false ->
            precondition_debug("precondition join_cluster: no nodes left to join.", []),
            false %% Might need to be changed when there's no read/write operations.
    end;
precondition(#state{joined_nodes=JoinedNodes}, {call, _Mod, leave_cluster, [Node, JoinedNodes]}) -> 
    %% Only allow dropping of the last node in the join list, for ease of debugging.
    precondition_debug("precondition leave_cluster: invoked for node ~p joined_nodes ~p", [Node, JoinedNodes]),

    ToBeRemovedNodes = JoinedNodes,
    precondition_debug("precondition leave_cluster: remaining nodes to be removed are: ~p", [ToBeRemovedNodes]),

    case length(ToBeRemovedNodes) > 3 of
        true ->
            ToBeRemovedNode = lists:last(ToBeRemovedNodes),
            precondition_debug("precondition leave_cluster: attempting to leave ~p", [ToBeRemovedNode]),
            case ToBeRemovedNode of
                Node ->
                    precondition_debug("precondition leave_cluster: YES attempting to leave ~p is ~p", [ToBeRemovedNode, Node]),
                    true;
                OtherNode ->
                    precondition_debug("precondition leave_cluster: NO attempting to leave ~p not ~p", [ToBeRemovedNode, OtherNode]),
                    false
            end;
        false ->
            precondition_debug("precondition leave_cluster: no nodes left to remove.", []),
            false %% Might need to be changed when there's no read/write operations.
    end;
precondition(#state{fault_model_state=FaultModelState, node_state=NodeState, joined_nodes=JoinedNodes}, {call, Mod, Fun, [Node|_]=Args}=Call) -> 
    precondition_debug("precondition fired for node function: ~p", [Fun]),

    case lists:member(Fun, node_functions()) of
        true ->
            ClusterCondition = enough_nodes_connected(JoinedNodes) andalso is_joined(Node, JoinedNodes),
            NodePrecondition = node_precondition(NodeState, Call),
            FaultPrecondition = not fault_is_crashed(FaultModelState, Node),
            ClusterCondition andalso NodePrecondition andalso FaultPrecondition;
        false ->
            case lists:member(Fun, fault_functions()) of 
                true ->
                    ClusterCondition = enough_nodes_connected(JoinedNodes) andalso is_joined(Node, JoinedNodes),
                    FaultModelPrecondition = fault_precondition(FaultModelState, Call),
                    ClusterCondition andalso FaultModelPrecondition;
                false ->
                    debug("general precondition fired for mod ~p and fun ~p and args ~p", [Mod, Fun, Args]),
                    false
            end
    end.

%% Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(State, _Res, {call, ?MODULE, join_cluster, [Node, JoinedNodes]}) -> 
    case is_joined(Node, JoinedNodes) of
        true ->
            %% no-op for the join
            State;
        false ->
            %% add to the joined list.
            State#state{joined_nodes=JoinedNodes ++ [Node]}
    end;
next_state(#state{joined_nodes=JoinedNodes}=State, _Res, {call, ?MODULE, leave_cluster, [Node, JoinedNodes]}) -> 
    case enough_nodes_connected_to_issue_remove(JoinedNodes) of
        true ->
            %% removed from the list.
            State#state{joined_nodes=JoinedNodes -- [Node]};
        false ->
            %% no-op for the leave
            State
    end;
next_state(#state{fault_model_state=FaultModelState0, node_state=NodeState0}=State, Res, {call, _Mod, Fun, _Args}=Call) -> 
    case lists:member(Fun, node_functions()) of
        true ->
            NodeState = node_next_state(NodeState0, Res, Call),
            State#state{node_state=NodeState};
        false ->
            case lists:member(Fun, fault_functions()) of 
                true ->
                    FaultModelState = fault_next_state(FaultModelState0, Res, Call),
                    State#state{fault_model_state=FaultModelState};
                false ->
                    debug("general next_state fired", []),
                    State
            end
    end.

%% Given the state `State' *prior* to the call `{call, Mod, Fun, Args}',
%% determine whether the result `Res' (coming from the actual system)
%% makes sense.
postcondition(_State, {call, ?MODULE, join_cluster, [_Node, _JoinedNodes]}, ok) ->
    postcondition_debug("postcondition join_cluster: succeeded", []),
    %% Accept joins that succeed.
    true;
postcondition(_State, {call, ?MODULE, leave_cluster, [_Node, _JoinedNodes]}, ok) ->
    postcondition_debug("postcondition leave_cluster: succeeded", []),
    %% Accept leaves that succeed.
    true;
postcondition(#state{fault_model_state=FaultModelState, node_state=NodeState}, {call, Mod, Fun, Args}=Call, Res) -> 
    case lists:member(Fun, node_functions()) of
        true ->
            PostconditionResult = node_postcondition(NodeState, Call, Res),

            case PostconditionResult of 
                false ->
                    debug("postcondition result: ~p; command: ~p:~p(~p)", [PostconditionResult, Mod, Fun, Args]),
                    ok;
                true ->
                    ok
            end,

            PostconditionResult;
        false ->
            case lists:member(Fun, fault_functions()) of 
                true ->
                    PostconditionResult = fault_postcondition(FaultModelState, Call, Res),

                    case PostconditionResult of 
                        false ->
                            debug("postcondition result: ~p; command: ~p:~p(~p)", [PostconditionResult, Mod, Fun, Args]),
                            ok;
                        true ->
                            ok
                    end,

                    PostconditionResult;
                false ->
                    postcondition_debug("general postcondition fired for ~p:~p with response ~p", [Mod, Fun, Res]),
                    %% All other commands pass.
                    false
            end
    end.

%%%===================================================================
%%% Generators
%%%===================================================================

node_name() ->
    ?LET(Names, names(), oneof(Names)).

corrupted_value() ->
    ?LET(Binary, binary(), 
        {erlang:timestamp(), Binary}).

names() ->
    NameFun = fun(N) -> 
        list_to_atom("node_" ++ integer_to_list(N)) 
    end,
    lists:map(NameFun, lists:seq(1, ?NUM_NODES)).

%%%===================================================================
%%% Trace Support
%%%===================================================================

command_preamble(Node, Command) ->
    debug("command preamble fired for command at node ~p: ~p", [Node, Command]),

    %% Log command entrance trace.
    partisan_trace_orchestrator:trace(enter_command, {Node, Command}),

    %% Under replay, perform the trace replay.
    partisan_trace_orchestrator:replay(enter_command, {Node, Command}),

    ok.

command_conclusion(Node, Command) ->
    debug("command conclusion fired for command at node ~p: ~p", [Node, Command]),

    %% Log command entrance trace.
    partisan_trace_orchestrator:trace(exit_command, {Node, Command}),

    %% Under replay, perform the trace replay.
    partisan_trace_orchestrator:replay(exit_command, {Node, Command}),

    ok.

%%%===================================================================
%%% Commands
%%%===================================================================

join_cluster(Name, [JoinedName|_]=JoinedNames) ->
    command_preamble(Name, {join_cluster, JoinedNames}),

    Result = case is_joined(Name, JoinedNames) of
        true ->
            ok;
        false ->
            Node = name_to_nodename(Name),
            JoinedNode = name_to_nodename(JoinedName),
            debug("join_cluster: joining node ~p to node ~p", [Node, JoinedNode]),

            %% Stage join.
            ok = ?SUPPORT:staged_join(Node, JoinedNode),

            %% Plan will only succeed once the ring has been gossiped.
            ok = ?SUPPORT:plan_and_commit(JoinedNode),

            %% Verify appropriate number of connections.
            NewCluster = lists:map(fun name_to_nodename/1, JoinedNames ++ [Name]),

            %% Ensure each node owns a portion of the ring
            ConvergeFun = fun() ->
                ok = ?SUPPORT:wait_until_all_connections(NewCluster),
                ok = ?SUPPORT:wait_until_nodes_agree_about_ownership(NewCluster),
                ok = ?SUPPORT:wait_until_no_pending_changes(NewCluster),
                ok = ?SUPPORT:wait_until_ring_converged(NewCluster)
            end,
            {ConvergeTime, _} = timer:tc(ConvergeFun),

            debug("join_cluster: converged at ~p", [ConvergeTime]),
            ok
    end,

    command_conclusion(Name, {join_cluster, JoinedNames}),

    Result.

leave_cluster(Name, JoinedNames) ->
    command_preamble(Name, {leave_cluster, JoinedNames}),

    Node = name_to_nodename(Name),
    debug("leave_cluster: leaving node ~p from cluster with members ~p", [Node, JoinedNames]),

    Result = case enough_nodes_connected_to_issue_remove(JoinedNames) of
        false ->
            ok;
        true ->
            %% Issue remove.
            ok = ?SUPPORT:leave(Node),

            %% Verify appropriate number of connections.
            NewCluster = lists:map(fun name_to_nodename/1, JoinedNames -- [Name]),

            %% Ensure each node owns a portion of the ring
            ConvergeFun = fun() ->
                ok = ?SUPPORT:wait_until_all_connections(NewCluster),
                ok = ?SUPPORT:wait_until_nodes_agree_about_ownership(NewCluster),
                ok = ?SUPPORT:wait_until_no_pending_changes(NewCluster),
                ok = ?SUPPORT:wait_until_ring_converged(NewCluster)
            end,
            {ConvergeTime, _} = timer:tc(ConvergeFun),

            debug("leave_cluster: converged at ~p", [ConvergeTime]),
            ok
    end,

    command_conclusion(Name, {leave_cluster, JoinedNames}),

    Result.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

start_nodes() ->
    %% Create an ets table for test configuration.
    ?MODULE = ets:new(?MODULE, [named_table]),

    %% Special configuration for the cluster.
    Config = [{partisan_dispatch, true},
              {parallelism, ?PARALLELISM},
              {tls, false},
              {binary_padding, false},
              {channels, ?CHANNELS},
              {vnode_partitioning, ?VNODE_PARTITIONING},
              {causal_labels, ?CAUSAL_LABELS},
              {pid_encoding, false},
              {sync_join, false},
              {forward_options, []},
              {broadcast, false},
              {disterl, false},
              {hash, undefined},
              {egress_delay, ?EGRESS_DELAY},
              {ingress_delay, ?INGRESS_DELAY},
              {disable_fast_forward, true},
              {disable_fast_receive, true},
              {membership_strategy, partisan_full_membership_strategy}],

    %% Initialize a cluster.
    Nodes = ?SUPPORT:start(prop_partisan,
                           Config,
                           [{partisan_peer_service_manager, ?MANAGER},
                           {num_nodes, ?NUM_NODES},
                           {cluster_nodes, ?CLUSTER_NODES}]),

    Self = node(),
    lager:info("~p: ~p started nodes: ~p", [?MODULE, Self, Nodes]),

    %% Deterministically seed the random number generator.
    partisan_config:seed(),

    %% Reset trace.
    ok = partisan_trace_orchestrator:reset(),

    %% Identify trace.
    TraceRandomNumber = rand:uniform(100000),
    %% lager:info("~p: trace random generated: ~p", [?MODULE, TraceRandomNumber]),
    TraceIdentifier = atom_to_list(?SYSTEM_MODEL) ++ "_" ++ integer_to_list(TraceRandomNumber),
    ok = partisan_trace_orchestrator:identify(TraceIdentifier),

    %% Add send and receive pre-interposition functions to enforce message ordering.
    PreInterpositionFun = fun({Type, OriginNode, OriginalMessage}) ->
        TracingNode = node(),

        %% Record message incoming and outgoing messages.
        ok = rpc:call(Self, 
                      partisan_trace_orchestrator, 
                      trace, 
                      [pre_interposition_fun, {TracingNode, Type, OriginNode, OriginalMessage}]),

        %% Under replay ensure they match the trace order (but only for pre-interposition messages).
        ok = rpc:call(Self, 
                      partisan_trace_orchestrator, 
                      replay, 
                      [pre_interposition_fun, {TracingNode, Type, OriginNode, OriginalMessage}]),

        ok
    end, 

    lists:foreach(fun({_Name, Node}) ->
        rpc:call(Node, 
                 ?MANAGER, 
                 add_pre_interposition_fun, 
                 ['$tracing', PreInterpositionFun])
        end, Nodes),

    %% Add send and receive post-interposition functions to perform tracing.
    PostInterpositionFun = fun({Type, OriginNode, OriginalMessage}, {Type, OriginNode, RewrittenMessage}) ->
        TracingNode = node(),

        ok = rpc:call(Self, 
                      partisan_trace_orchestrator, 
                      trace, 
                      [post_interposition_fun, {TracingNode, OriginNode, Type, OriginalMessage, RewrittenMessage}]),

        ok
    end, 

    lists:foreach(fun({_Name, Node}) ->
        rpc:call(Node, 
                 ?MANAGER, 
                 add_post_interposition_fun, 
                 ['$tracing', PostInterpositionFun])
        end, Nodes),

    %% Enable tracing.
    lists:foreach(fun({_Name, Node}) ->
        rpc:call(Node, 
                 partisan_config,
                 set,
                 [tracing, false])
        end, Nodes),

    %% Insert all nodes into group for all nodes.
    true = ets:insert(?MODULE, {nodes, Nodes}),

    %% Insert name to node mappings for lookup.
    %% Caveat, because sometimes we won't know ahead of time what FQDN the node will
    %% come online with when using partisan.
    lists:foreach(fun({Name, Node}) ->
        true = ets:insert(?MODULE, {Name, Node})
    end, Nodes),

    ok.

stop_nodes() ->
    %% Get list of nodes that were started at the start
    %% of the test.
    [{nodes, Nodes}] = ets:lookup(?MODULE, nodes),

    %% Print trace.
    partisan_trace_orchestrator:print(),

    %% Stop nodes.
    ?SUPPORT:stop(Nodes),

    %% Delete the table.
    ets:delete(?MODULE),

    ok.

%% Determine if a bunch of operations succeeded or failed.
all_to_ok_or_error(List) ->
    case lists:all(fun(X) -> X =:= ok end, List) of
        true ->
            ok;
        false ->
            {error, some_operations_failed, List}
    end.

%% Select a random grouping of nodes.
majority_nodes() ->
    ?LET(MajorityCount, ?NUM_NODES / 2 + 1,
        ?LET(Names, names(), 
            ?LET(Sublist, lists:sublist(Names, trunc(MajorityCount)), Sublist))).

enough_nodes_connected(Nodes) ->
    length(Nodes) >= 3.

enough_nodes_connected_to_issue_remove(Nodes) ->
    length(Nodes) > 3.

initial_state_debug(Line, Args) ->
    case ?INITIAL_STATE_DEBUG of
        true ->
            lager:info("~p: " ++ Line, [?MODULE] ++ Args);
        false ->
            ok
    end.

precondition_debug(Line, Args) ->
    case ?PRECONDITION_DEBUG of
        true ->
            lager:info("~p: " ++ Line, [?MODULE] ++ Args);
        false ->
            ok
    end.

postcondition_debug(Line, Args) ->
    case ?POSTCONDITION_DEBUG of
        true ->
            lager:info("~p: " ++ Line, [?MODULE] ++ Args);
        false ->
            ok
    end.

debug(Line, Args) ->
    case ?DEBUG of
        true ->
            lager:info("~p: " ++ Line, [?MODULE] ++ Args);
        false ->
            ok
    end.

is_joined(Node, Cluster) ->
    lists:member(Node, Cluster).

cluster_commands(#state{joined_nodes=JoinedNodes}) ->
    [
    {call, ?MODULE, join_cluster, [node_name(), JoinedNodes]},
    {call, ?MODULE, leave_cluster, [node_name(), JoinedNodes]}
    ].

name_to_nodename(Name) ->
    [{_, NodeName}] = ets:lookup(?MODULE, Name),
    NodeName.

ensure_tracing_started() ->
    partisan_trace_orchestrator:start_link().