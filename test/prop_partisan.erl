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

-include("partisan.hrl").

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
         node_next_state/4,
         node_begin_property/0,
         node_begin_case/0,
         node_end_case/0,
         node_assertion_functions/0,
         node_global_functions/0]).

%% Fault model.
-define(FAULT_MODEL, prop_partisan_crash_fault_model).

-import(?FAULT_MODEL,
        [fault_commands/0,
         fault_initial_state/0,
         fault_functions/1,
         fault_precondition/2,
         fault_postcondition/3,
         fault_next_state/4,
         fault_is_crashed/2,
         fault_begin_functions/0,
         fault_end_functions/0,
         fault_global_functions/0,
         fault_num_resolvable_faults/1]).

%% General test configuration
-define(CLUSTER_NODES, true).
-define(MANAGER, partisan_pluggable_peer_service_manager).

%% Debug.
-define(DEBUG, true).
-define(INITIAL_STATE_DEBUG, false).
-define(PRECONDITION_DEBUG, true).
-define(POSTCONDITION_DEBUG, true).

%% Partisan connection and forwarding settings.
-define(EGRESS_DELAY, 0).                           %% How many milliseconds to delay outgoing messages?
-define(INGRESS_DELAY, 0).                          %% How many millisconds to delay incoming messages?
-define(VNODE_PARTITIONING, false).                 %% Should communication be partitioned by vnode identifier?

%%%===================================================================
%%% Helper Macros
%%%===================================================================

-define(ETS, prop_partisan).

-define(NAME, fun(Name) -> [{_, NodeName}] = ets:lookup(?ETS, Name), NodeName end).

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

    case scheduler() of 
        default ->
            ?FORALL(Cmds, commands(?MODULE), 
                begin
                    start_nodes(),
                    node_begin_case(),
                    {History, State, Result} = run_commands(?MODULE, Cmds), 
                    node_end_case(),
                    stop_nodes(),
                    ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                        [History,State,Result]),
                            aggregate(command_names(Cmds), Result =:= ok))
                end);
        finite_fault ->
            ?FORALL(Cmds, finite_fault_commands(?MODULE), 
                begin
                    start_nodes(),
                    node_begin_case(),
                    {History, State, Result} = run_commands(?MODULE, Cmds), 
                    node_end_case(),
                    stop_nodes(),
                    ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                        [History,State,Result]),
                            aggregate(command_names(Cmds), Result =:= ok))
                end);
        single_success ->
            ?FORALL(Cmds, single_success_commands(?MODULE), 
                begin
                    start_nodes(),
                    node_begin_case(),
                    {History, State, Result} = run_commands(?MODULE, Cmds), 
                    node_end_case(),
                    stop_nodes(),
                    ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                        [History,State,Result]),
                            aggregate(command_names(Cmds), Result =:= ok))
                end)
    end.

%%%===================================================================
%%% Command sequences
%%%===================================================================

finite_fault_commands(Module) ->
    ?LET(Commands, commands(Module), 
        begin 
            debug("~p: original command sequence...~n", [?MODULE]),

            lists:foreach(fun(Command) -> 
                debug("-> ~p~n", [Command])
            end, Commands),

            %% Filter out global commands.
            CommandsWithoutGlobalNodeCommands = lists:filter(fun({set,{var,_Nth},{call,_Mod,Fun,_Args}}) ->
                case lists:member(Fun, node_global_functions()) of 
                    true ->
                        false;
                    _ ->
                        true
                end
            end, Commands),

            %% Add a command to resolve all partitions with a heal.
            ResolveCommands = case fault_injection_enabled() of 
                true ->
                    case rand:uniform(10) rem 2 =:= 0 of 
                        true ->
                            [{set,{var,0},{call,?FAULT_MODEL,resolve_all_faults_with_heal,[]}}];
                        false ->
                            [{set,{var,0},{call,?FAULT_MODEL,resolve_all_faults_with_crash,[]}}]
                    end;
                false ->
                    [{set,{var,0},{call,?FAULT_MODEL,resolve_all_faults_with_heal,[]}}]
            end,

            %% Only global node commands.
            CommandsWithOnlyGlobalNodeCommands = lists:map(fun(Fun) ->
                {set,{var,0},{call,?SYSTEM_MODEL,Fun,[]}}
            end, node_global_functions()), 

            %% Derive final command sequence.
            FinalCommands0 = lists:flatten(
                %% Node commands and failures, without global assertions.
                CommandsWithoutGlobalNodeCommands ++ 
                
                %% Commands to resolve failures.
                ResolveCommands ++ 

                %% Global assertions only.
                CommandsWithOnlyGlobalNodeCommands),

            %% Renumber command sequence.
            {FinalCommands, _} = lists:foldl(fun({set,{var,_Nth},{call,Mod,Fun,Args}}, {Acc, Next}) ->
                {Acc ++ [{set,{var,Next},{call,Mod,Fun,Args}}], Next + 1}
            end, {[], 1}, FinalCommands0),

            %% Print final command sequence.
            debug("altered command sequence...~n", []),

            lists:foreach(fun(Command) -> 
                debug("=> ~p~n", [Command])
            end, FinalCommands),

            %% Return the final command sequence.
            FinalCommands
        end).

single_success_commands(Module) ->
    ?LET(Commands, commands(Module), 
        begin 
            debug("original command sequence...~n", []),

            lists:foreach(fun(Command) -> 
                debug("-> ~p~n", [Command])
            end, Commands),

            %% Filter out global commands.
            CommandsWithoutGlobalNodeCommands = lists:filter(fun({set,{var,_Nth},{call,_Mod,Fun,_Args}}) ->
                case lists:member(Fun, node_global_functions()) of 
                    true ->
                        false;
                    _ ->
                        true
                end
            end, Commands),

            %% Get first non-global command.
            FirstNonGlobalCommand = case length(CommandsWithoutGlobalNodeCommands) > 0 of
                true ->
                    [hd(CommandsWithoutGlobalNodeCommands)];
                false ->
                    []
            end,

            %% Generate failure command.
            FailureCommands = case length(CommandsWithoutGlobalNodeCommands) > 0 of
                true ->
                    %% Only fail if we have at least *one* command that
                    %% performs application behavior.
                    [{set,{var,0},{call,?MODULE,forced_failure,[]}}];
                false ->
                    []
            end,

            %% Only global node commands.
            CommandsWithOnlyGlobalNodeCommands = lists:map(fun(Fun) ->
                {set,{var,0},{call,?SYSTEM_MODEL,Fun,[]}}
            end, node_global_functions()), 

            %% Derive final command sequence.
            FinalCommands0 = lists:flatten(
                %% Node commands, without global assertions.  Take only the first.
                FirstNonGlobalCommand ++

                %% Global assertions only.
                CommandsWithOnlyGlobalNodeCommands ++ 
            
                %% Final failure commands
                FailureCommands),

            %% Renumber command sequence.
            {FinalCommands, _} = lists:foldl(fun({set,{var,_Nth},{call,Mod,Fun,Args}}, {Acc, Next}) ->
                {Acc ++ [{set,{var,Next},{call,Mod,Fun,Args}}], Next + 1}
            end, {[], 1}, FinalCommands0),

            %% Print final command sequence.
            debug("altered command sequence...~n", []),

            lists:foreach(fun(Command) -> 
                debug("=> ~p~n", [Command])
            end, FinalCommands),

            %% Return the final command sequence.
            FinalCommands
        end).

%%%===================================================================
%%% Initial state
%%%===================================================================

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

    %% Initialize command counter at 0.
    Counter = 0,

    #property_state{counter=Counter,
           joined_nodes=JoinedNodes,
           fault_model_state=FaultModelState,
           nodes=Nodes,
           node_state=NodeState}.

command(State) -> 
    %% Cluster maintenance commands.
    ClusterCommands = lists:flatmap(fun(Command) -> 
        case membership_changes_enabled() of 
            true ->
                [{1, Command}];
            false ->
                []
        end
    end, cluster_commands(State)),

    %% Fault model commands.
    FaultModelCommands = lists:flatmap(fun(Command) -> 
        case fault_injection_enabled() of 
            true ->
                [{1, Command}];
            false ->
                []
        end
    end, fault_commands()),

    %% System model commands.
    SystemCommands = lists:map(fun(Command) -> 
        {1, Command} 
    end, node_commands()), 

    frequency(ClusterCommands ++ FaultModelCommands ++ SystemCommands).

%% Picks whether a command should be valid under the current state.
precondition(#property_state{nodes=Nodes, joined_nodes=JoinedNodes}, {call, _Mod, sync_join_cluster, [Node, JoinNode]}) -> 
    %% Only allow dropping of the first unjoined node in the nodes list, for ease of debugging.
    precondition_debug("precondition sync_join_cluster: invoked for node ~p joined_nodes ~p", [Node, JoinedNodes]),

    CanBeJoinedNodes = Nodes -- JoinedNodes,
    precondition_debug("precondition sync_join_cluster: remaining nodes to be joined are: ~p", [CanBeJoinedNodes]),

    case length(CanBeJoinedNodes) > 0 of
        true ->
            precondition_debug("precondition sync_join_cluster: attempting to join ~p", [Node]),

            case lists:member(Node, CanBeJoinedNodes) of
                true ->
                    precondition_debug("precondition sync_join_cluster: POSSIBLE YES attempting to join ~p is in ~p", [Node, CanBeJoinedNodes]),
                    lists:member(JoinNode, JoinedNodes);
                _ ->
                    precondition_debug("precondition sync_join_cluster: NO attempting to join ~p not in ~p", [Node, CanBeJoinedNodes]),
                    false
            end;
        false ->
            precondition_debug("precondition sync_join_cluster: no nodes left to join.", []),
            false %% Might need to be changed when there's no read/write operations.
    end;
precondition(#property_state{joined_nodes=JoinedNodes}, {call, _Mod, sync_leave_cluster, [Node]}) -> 
    %% Only allow dropping of the last node in the join list, for ease of debugging.
    precondition_debug("precondition sync_leave_cluster: invoked for node ~p joined_nodes ~p", [Node, JoinedNodes]),

    ToBeRemovedNodes = JoinedNodes,
    precondition_debug("precondition sync_leave_cluster: remaining nodes to be removed are: ~p", [ToBeRemovedNodes]),

    case length(ToBeRemovedNodes) > 3 of
        true ->
            TotalNodes = length(ToBeRemovedNodes),
            CanBeRemovedNodes = lists:sublist(ToBeRemovedNodes, 4, TotalNodes),
            precondition_debug("precondition sync_leave_cluster: attempting to leave ~p", [Node]),

            case lists:member(Node, CanBeRemovedNodes) of
                true ->
                    precondition_debug("precondition sync_leave_cluster: YES attempting to leave ~p is in ~p", [Node, CanBeRemovedNodes]),
                    true;
                _ ->
                    precondition_debug("precondition sync_leave_cluster: NO attempting to leave ~p is not in ~p", [Node, CanBeRemovedNodes]),
                    false
            end;
        false ->
            precondition_debug("precondition sync_leave_cluster: no nodes left to remove.", []),
            false
    end;
precondition(#property_state{fault_model_state=FaultModelState, node_state=NodeState, joined_nodes=JoinedNodes, counter=Counter}, 
             {call, Mod, Fun, [Node|_]=Args}=Call) -> 
    precondition_debug("precondition fired for counter ~p and node function: ~p(~p)", [Counter, Fun, Args]),

    case lists:member(Fun, node_functions()) of
        true ->
            ClusterCondition = enough_nodes_connected(JoinedNodes) andalso is_joined(Node, JoinedNodes),
            NodePrecondition = node_precondition(NodeState, Call),
            FaultPrecondition = not fault_is_crashed(FaultModelState, Node),
            ClusterCondition andalso NodePrecondition andalso FaultPrecondition;
        false ->
            case lists:member(Fun, fault_functions(JoinedNodes)) of 
                true ->
                    ClusterCondition = enough_nodes_connected(JoinedNodes) andalso is_joined(Node, JoinedNodes),
                    FaultModelPrecondition = fault_precondition(FaultModelState, Call),
                    ClusterCondition andalso FaultModelPrecondition;
                false ->
                    debug("general precondition fired for mod ~p and fun ~p and args ~p", [Mod, Fun, Args]),
                    false
            end
    end;

precondition(#property_state{}, {call, _Mod, forced_failure, _Args}) ->
    precondition_debug("forced failure precondition fired!", []),
    true;

precondition(#property_state{counter=Counter}, {call, _Mod, Fun, Args}=_Call) ->
    precondition_debug("fallthrough precondition fired for counter ~p and node function: ~p(~p)", [Counter, Fun, Args]),

    case lists:member(Fun, fault_global_functions()) of 
        true ->
            precondition_debug("=> allowing global fault command.", []),
            true;
        false ->
            case lists:member(Fun, node_global_functions()) of
                true ->
                    precondition_debug("=> allowing global node command.", []),
                    true;
                false ->
                    false
            end
    end.

%% Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(#property_state{counter=Counter, joined_nodes=JoinedNodes}=State, _Res, {call, ?MODULE, sync_join_cluster, [Node, _JoinNode]}) -> 
    State#property_state{joined_nodes=JoinedNodes ++ [Node], counter=Counter+1};
next_state(#property_state{counter=Counter, joined_nodes=JoinedNodes}=State, _Res, {call, ?MODULE, sync_leave_cluster, [Node]}) -> 
    State#property_state{joined_nodes=JoinedNodes -- [Node], counter=Counter+1};
next_state(#property_state{}=State, _Res, {call, ?MODULE, forced_failure, []}) -> 
    State;
next_state(#property_state{counter=Counter, fault_model_state=FaultModelState0, node_state=NodeState0, joined_nodes=JoinedNodes}=State, Res, {call, _Mod, Fun, _Args}=Call) -> 
    case lists:member(Fun, node_functions()) of
        true ->
            NodeState = node_next_state(State, NodeState0, Res, Call),
            State#property_state{node_state=NodeState, counter=Counter+1};
        false ->
            case lists:member(Fun, fault_functions(JoinedNodes)) of 
                true ->
                    FaultModelState = fault_next_state(State, FaultModelState0, Res, Call),
                    State#property_state{fault_model_state=FaultModelState, counter=Counter+1};
                false ->
                    debug("general next_state fired for fun: ~p", [Fun]),
                    State#property_state{counter=Counter+1}
            end
    end.

%% Given the state `State' *prior* to the call `{call, Mod, Fun, Args}',
%% determine whether the result `Res' (coming from the actual system)
%% makes sense.
postcondition(_State, {call, ?MODULE, forced_failure, []}, ok) ->
    postcondition_debug("postcondition forced_failure fired", []),
    false;
postcondition(_State, {call, ?MODULE, sync_join_cluster, [_Node, _JoinNode]}, {ok, _Members}) ->
    postcondition_debug("postcondition sync_join_cluster fired", []),
    true;
postcondition(_State, {call, ?MODULE, sync_leave_cluster, [_Node]}, {ok, _Members}) ->
    postcondition_debug("postcondition sync_leave_cluster fired", []),
    true;
postcondition(#property_state{fault_model_state=FaultModelState, node_state=NodeState, joined_nodes=JoinedNodes}, {call, Mod, Fun, [_Node|_]=Args}=Call, Res) -> 
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
            case lists:member(Fun, fault_functions(JoinedNodes)) of 
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
    end;
postcondition(#property_state{node_state=NodeState}, {call, _Mod, Fun, Args}=Call, Res) ->
    postcondition_debug("fallthrough postcondition fired node function: ~p(~p)", [Fun, Args]),

    case lists:member(Fun, fault_global_functions()) of 
        true ->
            postcondition_debug("=> allowing global fault command.", []),
            true;
        false ->
            case lists:member(Fun, node_global_functions()) of 
                true ->
                    Result = node_postcondition(NodeState, Call, Res),
                    postcondition_debug("=> postcondition returned ~p", [Result]),
                    Result;
                false ->
                    false
            end
    end.

%%%===================================================================
%%% Generators
%%%===================================================================

node_name() ->
    oneof(names()).

names() ->
    NameFun = fun(N) -> 
        list_to_atom("node_" ++ integer_to_list(N)) 
    end,
    lists:map(NameFun, lists:seq(1, ?TEST_NUM_NODES)).

%%%===================================================================
%%% Trace Support
%%%===================================================================

%% @private
normalize_name(Node) ->
    RunnerNode = node(),

    case Node of 
        RunnerNode ->
            RunnerNode;
        _ ->
            ?NAME(Node)
    end.

command_preamble(Node, Command) ->
    debug("command preamble fired for command at node ~p: ~p", [Node, Command]),

    %% Log command entrance trace.
    partisan_trace_orchestrator:trace(enter_command, {normalize_name(Node), Command}),

    %% Under replay, perform the trace replay.
    partisan_trace_orchestrator:replay(enter_command, {normalize_name(Node), Command}),

    ok.

command_conclusion(Node, Command) ->
    debug("command conclusion fired for command at node ~p: ~p", [Node, Command]),

    %% Log command entrance trace.
    partisan_trace_orchestrator:trace(exit_command, {normalize_name(Node), Command}),

    %% Under replay, perform the trace replay.
    partisan_trace_orchestrator:replay(exit_command, {normalize_name(Node), Command}),

    ok.

%%%===================================================================
%%% Commands
%%%===================================================================

forced_failure() ->
    RunnerNode = node(),

    command_preamble(RunnerNode, [forced_failure]),

    %% Do nothing.

    command_conclusion(RunnerNode, [forced_failure]),

    ok.

sync_join_cluster(Node, JoinNode) ->
    command_preamble(Node, [sync_join_cluster, JoinNode]),

    %% Get an existing member of the cluster.
    {ok, Members} = rpc:call(?NAME(JoinNode), ?MANAGER, members, []),
    debug("sync_join_cluster: joining node ~p to cluster at node ~p with current members ~p", 
          [?NAME(Node), ?NAME(JoinNode), Members]),

    %% Get my information.
    Myself = rpc:call(?NAME(Node), partisan_peer_service_manager, myself, []),

    %% Issue remove.
    ok = rpc:call(?NAME(JoinNode), ?MANAGER, join, [Myself]),

    %% Wait until all nodes agree about membership.
    DesiredMembership = Members ++ [?NAME(Node)],
    debug("=> nodes should wait for desired membership: ~p", [DesiredMembership]),
    ok = wait_until_nodes_agree_on_membership(DesiredMembership),

    %% Return new membership, make sure it matches model state.
    {ok, NewMembers} = rpc:call(JoinNode, ?MANAGER, members, []),
    debug("=> members after transition are: ~p", [NewMembers]),

    command_conclusion(Node, [sync_join_cluster, JoinNode]),

    {ok, NewMembers}.

sync_leave_cluster(Node) ->
    command_preamble(Node, [sync_leave_cluster]),

    %% Get an existing member of the cluster.
    {ok, Members} = rpc:call(?NAME(Node), ?MANAGER, members, []),
    debug("sync_leave_cluster: leaving node ~p from cluster with current members ~p", 
          [?NAME(Node), Members]),

    %% Select first member of membership -- *that's not us*.
    FirstMember = hd(lists:filter(fun(N) -> N =/= ?NAME(Node) end, Members)),

    %% Get my information.
    Myself = rpc:call(?NAME(Node), partisan_peer_service_manager, myself, []),

    %% Issue remove.
    ok = rpc:call(FirstMember, ?MANAGER, leave, [Myself]),

    %% Wait until all nodes agree about membership.
    DesiredMembership = Members -- [?NAME(Node)],
    debug("=> nodes should wait for desired membership: ~p", [DesiredMembership]),
    ok = wait_until_nodes_agree_on_membership(DesiredMembership),

    %% Return new membership, make sure it matches model state.
    {ok, NewMembers} = rpc:call(FirstMember, ?MANAGER, members, []),
    debug("=> members after transition are: ~p", [NewMembers]),

    command_conclusion(Node, [sync_leave_cluster]),

    {ok, NewMembers}.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

start_nodes() ->
    Self = node(),
    lager:info("~p: ~p starting nodes!", [?MODULE, Self]),

    %% Nuke epmd first.
    [] = os:cmd("pkill -9 epmd"),

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
              {membership_strategy_tracing, false},
              {periodic_enabled, false},
              {distance_enabled, false},
              {disable_fast_forward, true},
              {disable_fast_receive, true},
              {membership_strategy, partisan_full_membership_strategy}],

    %% Initialize a cluster.
    Nodes = ?SUPPORT:start(prop_partisan,
                           Config,
                           [{partisan_peer_service_manager, ?MANAGER},
                           {num_nodes, ?TEST_NUM_NODES},
                           {cluster_nodes, ?CLUSTER_NODES}]),

    lager:info("~p: ~p started nodes: ~p", [?MODULE, Self, Nodes]),

    %% Deterministically seed the random number generator.
    partisan_config:seed(),

    %% Reset trace.
    ok = partisan_trace_orchestrator:reset(),

    %% Perform preloads.
    ok = partisan_trace_orchestrator:perform_preloads(),

    %% Identify trace.
    TraceRandomNumber = rand:uniform(100000),
    %% lager:info("~p: trace random generated: ~p", [?MODULE, TraceRandomNumber]),
    TraceIdentifier = atom_to_list(?SYSTEM_MODEL) ++ "_" ++ integer_to_list(TraceRandomNumber),
    ok = partisan_trace_orchestrator:identify(TraceIdentifier),

    %% Add send and receive pre-interposition functions to enforce message ordering.
    PreInterpositionFun = fun({Type, OriginNode, OriginalMessage}) ->
        %% Record message incoming and outgoing messages.
        ok = partisan_trace_orchestrator:trace(pre_interposition_fun, {node(), Type, OriginNode, OriginalMessage}),

        %% Under replay ensure they match the trace order (but only for pre-interposition messages).
        ok = partisan_trace_orchestrator:replay(pre_interposition_fun, {node(), Type, OriginNode, OriginalMessage}),

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
        %% Record outgoing message after transformation.
        ok = partisan_trace_orchestrator:trace(post_interposition_fun, {node(), OriginNode, Type, OriginalMessage, RewrittenMessage}),
        
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

cluster_commands(_State) ->
    [
    % TODO: Disabled, because nodes shutdown on removal from the cluster.
    % {call, ?MODULE, sync_join_cluster, [node_name(), node_name()]},
    {call, ?MODULE, sync_leave_cluster, [node_name()]}
    ].

name_to_nodename(Name) ->
    [{_, NodeName}] = ets:lookup(?MODULE, Name),
    NodeName.

ensure_tracing_started() ->
    partisan_trace_orchestrator:start_link().

wait_until_nodes_agree_on_membership(Nodes) ->
    AgreementFun = fun(Node) ->
        %% Get membership at node.
        {ok, Members} = rpc:call(Node, ?MANAGER, members, []),

        %% Sort.
        SortedNodes = lists:usort(Nodes),
        SortedMembers = lists:usort(Members),

        %% Ensure the lists are the same -- barrier for proceeding.
        case SortedNodes =:= SortedMembers of
            true ->
                debug("node ~p agrees on membership: ~p", 
                      [Node, SortedMembers]),
                true;
            false ->
                debug("node ~p disagrees on membership: ~p != ~p", 
                      [Node, SortedMembers, SortedNodes]),
                error
        end
    end,
    [ok = wait_until(Node, AgreementFun) || Node <- Nodes],
    lager:info("All nodes agree on membership!"),
    ok.

%% @private
wait_until(Fun) when is_function(Fun) ->
    MaxTime = 600000, %% @TODO use config,
        Delay = 1000, %% @TODO use config,
        Retry = MaxTime div Delay,
    wait_until(Fun, Retry, Delay).

%% @private
wait_until(Node, Fun) when is_atom(Node), is_function(Fun) ->
    wait_until(fun() -> Fun(Node) end).

%% @private
wait_until(Fun, Retry, Delay) when Retry > 0 ->
    wait_until_result(Fun, true, Retry, Delay).

%% @private
wait_until_result(Fun, Result, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        Result ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until_result(Fun, Result, Retry-1, Delay)
    end.

%% @private
fault_injection_enabled() ->
    case os:getenv("FAULT_INJECTION") of 
        false ->
            false;
        _ ->
            true
    end.            

%% @private
membership_changes_enabled() ->
    case os:getenv("MEMBERSHIP_CHANGES") of 
        false ->
            false;
        _ ->
            true
    end.

%% @private
scheduler() ->
    case os:getenv("SCHEDULER") of
        false ->
            default;
        Other ->
            list_to_atom(Other)
    end.
