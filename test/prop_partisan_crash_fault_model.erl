%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(prop_partisan_crash_fault_model).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-include_lib("proper/include/proper.hrl").

-compile([export_all]).

-define(MANAGER, partisan_pluggable_peer_service_manager).

-record(fault_model_state, {tolerance,
                            crashed_nodes,
                            send_omissions,
                            receive_omissions}).

%%%===================================================================
%%% Generators
%%%===================================================================

message() ->
    ?LET(Id, erlang:unique_integer([positive, monotonic]), 
        ?LET(Random, integer(),
            {Id, Random})).

node_name() ->
    oneof(names()).

names() ->
    NameFun = fun(N) -> 
        list_to_atom("node_" ++ integer_to_list(N)) 
    end,
    lists:map(NameFun, lists:seq(1, ?TEST_NUM_NODES)).

%%%===================================================================
%%% Commands
%%%===================================================================

-define(PROPERTY_MODULE, prop_partisan).

-define(FAULT_DEBUG, true).

-define(ETS, prop_partisan).
-define(NAME, fun(Name) -> [{_, NodeName}] = ets:lookup(?ETS, Name), NodeName end).

%% Stop the node.
%% Fail-stop model, assume synchronous failure detection.
stop(Name, JoinedNames) ->
    ?PROPERTY_MODULE:command_preamble(Name, [stop, JoinedNames]),

    %% TODO: Implement me.

    ?PROPERTY_MODULE:command_conclusion(Name, [stop, JoinedNames]),

    {error, not_implemented}.

%% Crash the node.
%% Crash is a stop that doesn't wait for all members to know about the crash.
%% Crash-stop, assume asynchronous failure detection.
crash(Name, JoinedNames) ->
    ?PROPERTY_MODULE:command_preamble(Name, [crash, JoinedNames]),

    fault_debug("crashing node: ~p", [Name]),

    internal_crash(Name),

    ?PROPERTY_MODULE:command_conclusion(Name, [crash, JoinedNames]),

    ok.

%% Create a receive omission failure.
begin_receive_omission(SourceNode0, DestinationNode) ->
    ?PROPERTY_MODULE:command_preamble(DestinationNode, [begin_receive_omission, SourceNode0]),

    fault_debug("begin_receive_omission: source_node ~p destination_node ~p", [SourceNode0, DestinationNode]),

    %% Convert to real node name and not symbolic name.
    SourceNode = ?NAME(SourceNode0),

    InterpositionFun = fun({receive_message, N, Message}) ->
        case N of
            SourceNode ->
                lager:info("~p: dropping packet from ~p to ~p due to interposition.", [node(), SourceNode, DestinationNode]),
                undefined;
            OtherNode ->
                lager:info("~p: allowing message, doesn't match interposition as destination is ~p and not ~p", [node(), OtherNode, DestinationNode]),
                Message
        end;
        ({forward_message, _N, Message}) -> Message
    end,
    Result = rpc:call(?NAME(DestinationNode), ?MANAGER, add_interposition_fun, [{receive_omission, SourceNode}, InterpositionFun]),

    ?PROPERTY_MODULE:command_conclusion(DestinationNode, [begin_receive_omission, SourceNode0]),

    Result.

%% End receive omission failure period.
end_receive_omission(SourceNode0, DestinationNode) ->
    ?PROPERTY_MODULE:command_preamble(DestinationNode, [end_receive_omission, SourceNode0]),

    fault_debug("end_receive_omission: source_node ~p destination_node ~p", [SourceNode0, DestinationNode]),

    %% Convert to real node name and not symbolic name.
    SourceNode = ?NAME(SourceNode0),

    Result = rpc:call(?NAME(DestinationNode), ?MANAGER, remove_interposition_fun, [{receive_omission, SourceNode}]),

    ?PROPERTY_MODULE:command_conclusion(DestinationNode, [end_receive_omission, SourceNode0]),

    Result.

%% Create a send omission failure.
begin_send_omission(SourceNode, DestinationNode0) ->
    ?PROPERTY_MODULE:command_preamble(SourceNode, [begin_send_omission, DestinationNode0]),

    fault_debug("begin_send_omission: source_node ~p destination_node ~p", [SourceNode, DestinationNode0]),

    %% Convert to real node name and not symbolic name.
    DestinationNode = ?NAME(DestinationNode0),

    InterpositionFun = fun({forward_message, N, Message}) ->
        case N of
            DestinationNode ->
                lager:info("~p: dropping packet from ~p to ~p due to interposition.", [node(), SourceNode, DestinationNode]),
                undefined;
            OtherNode ->
                lager:info("~p: allowing message, doesn't match interposition as destination is ~p and not ~p", [node(), OtherNode, DestinationNode]),
                Message
        end;
        ({receive_message, _N, Message}) -> Message
    end,
    Result = rpc:call(?NAME(SourceNode), ?MANAGER, add_interposition_fun, [{send_omission, DestinationNode}, InterpositionFun]),

    ?PROPERTY_MODULE:command_conclusion(SourceNode, [begin_send_omission, DestinationNode0]),

    Result.

%% End send omission failure period.
end_send_omission(SourceNode, DestinationNode0) ->
    ?PROPERTY_MODULE:command_preamble(SourceNode, [end_send_omission, DestinationNode0]),

    fault_debug("end_send_omission: source_node ~p destination_node ~p", [SourceNode, DestinationNode0]),

    %% Convert to real node name and not symbolic name.
    DestinationNode = ?NAME(DestinationNode0),

    Result = rpc:call(?NAME(SourceNode), ?MANAGER, remove_interposition_fun, [{send_omission, DestinationNode}]),

    ?PROPERTY_MODULE:command_conclusion(SourceNode, [end_send_omission, DestinationNode0]),

    Result.

%% Resolve all faults with heal.
resolve_all_faults_with_heal() ->
    fault_debug("executing resolve_all_faults_with_heal command", []),

    RunnerNode = node(),

    ?PROPERTY_MODULE:command_preamble(RunnerNode, [resolve_all_faults_with_heal]),

    %% Remove all interposition funs.
    lists:foreach(fun(Node) ->
        % fault_debug("getting interposition funs at node ~p", [Node]),

        case rpc:call(?NAME(Node), ?MANAGER, get_interposition_funs, []) of 
            {badrpc, nodedown} ->
                ok;
            {ok, InterpositionFuns0} ->
                InterpositionFuns = dict:to_list(InterpositionFuns0),
                % fault_debug("=> ~p", [InterpositionFuns]),

                lists:foreach(fun({InterpositionName, _Function}) ->
                    % fault_debug("=> removing interposition: ~p", [InterpositionName]),
                    ok = rpc:call(?NAME(Node), ?MANAGER, remove_interposition_fun, [InterpositionName])
            end, InterpositionFuns)
        end
    end, names()),

    %% Sleep.
    timer:sleep(5000),

    ?PROPERTY_MODULE:command_conclusion(RunnerNode, [resolve_all_faults_with_heal]),

    ok.

%% Resolve all faults with crash.
resolve_all_faults_with_crash() ->
    fault_debug("executing resolve_all_faults_with_crash command", []),

    RunnerNode = node(),

    ?PROPERTY_MODULE:command_preamble(RunnerNode, [resolve_all_faults_with_crash]),

    %% Remove all interposition funs.
    lists:foreach(fun(Node) ->
        % fault_debug("getting interposition funs at node ~p", [Node]),

        case rpc:call(?NAME(Node), ?MANAGER, get_interposition_funs, []) of 
            {badrpc, nodedown} ->
                ok;
            {ok, InterpositionFuns0} ->
                InterpositionFuns = dict:to_list(InterpositionFuns0),
                % fault_debug("=> ~p", [InterpositionFuns]),

                lists:foreach(fun({InterpositionName, _Function}) ->
                    % fault_debug("=> removing interposition: ~p", [InterpositionName]),
                    ok = rpc:call(?NAME(Node), ?MANAGER, remove_interposition_fun, [InterpositionName]),

                %% Crash node.
                fault_debug("=> crashing node!", []),
                internal_crash(Node),

                ok
            end, InterpositionFuns)
        end
    end, names()),

    %% Sleep.
    timer:sleep(5000),

    ?PROPERTY_MODULE:command_conclusion(RunnerNode, [resolve_all_faults_with_crash]),

    ok.

%%%===================================================================
%%% Fault Model
%%%===================================================================

fault_commands() ->
    [
     %% Crashes.
     %% {call, ?MODULE, crash, [node_name(), JoinedNodes]},

     %% Failures: fail-stop.
     %% {call, ?MODULE, stop, [node_name(), JoinedNodes]},

     %% Send omission failures.
     {call, ?MODULE, begin_send_omission, [node_name(), node_name()]},
     {call, ?MODULE, end_send_omission, [node_name(), node_name()]},

     %% Receive omission failures.
     {call, ?MODULE, begin_receive_omission, [node_name(), node_name()]},
     {call, ?MODULE, end_receive_omission, [node_name(), node_name()]}
    ].

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
fault_functions(_JoinedNodes) ->
    fault_begin_functions() ++ fault_end_functions().

%% Commands to induce failures.
fault_begin_functions() ->
    [begin_receive_omission, begin_send_omission].

%% Commands to resolve failures.
fault_end_functions() ->
    [end_send_omission, end_receive_omission].

%% Commands to resolve global failures.
fault_global_functions() ->
    [resolve_all_faults_with_heal, resolve_all_faults_with_crash].

%% Initialize failure state.
fault_initial_state() ->
    Tolerance = case os:getenv("FAULT_TOLERANCE") of 
        false ->
            1;
        ToleranceString ->
            list_to_integer(ToleranceString)
    end,

    fault_debug("setting fault tolerance level to: ~p", [Tolerance]),

    CrashedNodes = [],
    SendOmissions = dict:new(),
    ReceiveOmissions = dict:new(),

    #fault_model_state{tolerance=Tolerance,
                       crashed_nodes=CrashedNodes, 
                       send_omissions=SendOmissions,
                       receive_omissions=ReceiveOmissions}.

%% Receive omission.
fault_precondition(#fault_model_state{crashed_nodes=CrashedNodes, receive_omissions=ReceiveOmissions}=FaultModelState, {call, _Mod, begin_receive_omission, [SourceNode, DestinationNode]}=Call) ->
    %% We must not already have a receive omission for these nodes.
    BeginCondition = case dict:find({SourceNode, DestinationNode}, ReceiveOmissions) of 
        {ok, _Value} ->
            false;
        error ->
            true
    end,

    %% Fault must be allowed at this moment.
    fault_allowed(Call, FaultModelState) andalso 

    %% Nodes must not be the same node.
    SourceNode =/= DestinationNode andalso

    %% Both nodes have to be non-crashed.
    not lists:member(SourceNode, CrashedNodes) andalso not lists:member(DestinationNode, CrashedNodes) andalso

    %% Can't already have a receive omission for these nodes.
    BeginCondition;

fault_precondition(#fault_model_state{crashed_nodes=CrashedNodes, receive_omissions=ReceiveOmissions}, {call, _Mod, end_receive_omission, [SourceNode, DestinationNode]}) ->
    %% We must be in the middle of a send omission to resolve it.
    EndCondition = case dict:find({SourceNode, DestinationNode}, ReceiveOmissions) of 
        {ok, _Value} ->
            true;
        error ->
            false
    end,

    EndCondition andalso not lists:member(DestinationNode, CrashedNodes);

%% Send omission.
fault_precondition(#fault_model_state{crashed_nodes=CrashedNodes, send_omissions=SendOmissions}=FaultModelState, {call, _Mod, begin_send_omission, [SourceNode, DestinationNode]}=Call) ->
    %% We must not already have a receive omission for these nodes.
    BeginCondition = case dict:find({SourceNode, DestinationNode}, SendOmissions) of 
        {ok, _Value} ->
            false;
        error ->
            true
    end,

    %% Fault must be allowed at this moment.
    fault_allowed(Call, FaultModelState) andalso 

    %% Nodes must not be the same node.
    SourceNode =/= DestinationNode andalso

    %% Both nodes have to be non-crashed.
    not lists:member(SourceNode, CrashedNodes) andalso not lists:member(DestinationNode, CrashedNodes) andalso

    %% Can't already have a receive omission for these nodes.
    BeginCondition;

fault_precondition(#fault_model_state{crashed_nodes=CrashedNodes, send_omissions=SendOmissions}, {call, _Mod, end_send_omission, [SourceNode, DestinationNode]}) ->
    %% We must be in the middle of a send omission to resolve it.
    EndCondition = case dict:find({SourceNode, DestinationNode}, SendOmissions) of 
        {ok, _Value} ->
            true;
        error ->
            false
    end,

    EndCondition andalso not lists:member(SourceNode, CrashedNodes);

%% Stop failures.
fault_precondition(#fault_model_state{crashed_nodes=CrashedNodes}=FaultModelState, {call, _Mod, stop, [Node, _JoinedNames]}=Call) ->
    %% Fault must be allowed at this moment.
    fault_allowed(Call, FaultModelState) andalso 

    %% Node to crash must be online at the time.
    not lists:member(Node, CrashedNodes);

%% Crash failures.
fault_precondition(#fault_model_state{crashed_nodes=CrashedNodes}=FaultModelState, {call, _Mod, crash, [Node, _JoinedNames]}=Call) ->
    %% Fault must be allowed at this moment.
    fault_allowed(Call, FaultModelState) andalso 

    %% Node to crash must be online at the time.
    not lists:member(Node, CrashedNodes);

fault_precondition(_FaultModelState, {call, Mod, Fun, [_Node|_]=Args}) ->
    fault_debug("fault precondition fired for ~p:~p(~p)", [Mod, Fun, Args]),
    false.

%% Receive omission.
fault_next_state(#fault_model_state{receive_omissions=ReceiveOmissions0} = FaultModelState, _Res, {call, _Mod, begin_receive_omission, [SourceNode, DestinationNode]}) ->
    ReceiveOmissions = dict:store({SourceNode, DestinationNode}, true, ReceiveOmissions0),
    FaultModelState#fault_model_state{receive_omissions=ReceiveOmissions};

fault_next_state(#fault_model_state{receive_omissions=ReceiveOmissions0} = FaultModelState, _Res, {call, _Mod, end_receive_omission, [SourceNode, DestinationNode]}) ->
    ReceiveOmissions = dict:erase({SourceNode, DestinationNode}, ReceiveOmissions0),
    FaultModelState#fault_model_state{receive_omissions=ReceiveOmissions};

%% Send omission.
fault_next_state(#fault_model_state{send_omissions=SendOmissions0} = FaultModelState, _Res, {call, _Mod, begin_send_omission, [SourceNode, DestinationNode]}) ->
    SendOmissions = dict:store({SourceNode, DestinationNode}, true, SendOmissions0),
    FaultModelState#fault_model_state{send_omissions=SendOmissions};

fault_next_state(#fault_model_state{send_omissions=SendOmissions0} = FaultModelState, _Res, {call, _Mod, end_send_omission, [SourceNode, DestinationNode]}) ->
    SendOmissions = dict:erase({SourceNode, DestinationNode}, SendOmissions0),
    FaultModelState#fault_model_state{send_omissions=SendOmissions};

%% Crashing a node adds a node to the crashed state.
fault_next_state(#fault_model_state{crashed_nodes=CrashedNodes} = FaultModelState, _Res, {call, _Mod, crash, [Node, _JoinedNodes]}) ->
    FaultModelState#fault_model_state{crashed_nodes=CrashedNodes ++ [Node]};

%% Stopping a node assumes a crash that's immediately detected.
fault_next_state(#fault_model_state{crashed_nodes=CrashedNodes} = FaultModelState, _Res, {call, _Mod, stop, [Node, _JoinedNodes]}) ->
    FaultModelState#fault_model_state{crashed_nodes=CrashedNodes ++ [Node]};

%% Remove faults.
fault_next_state(FaultModelState, _Res, {call, _Mod, resolve_all_faults_with_heal, []}) ->
    SendOmissions = dict:new(),
    ReceiveOmissions = dict:new(),
    FaultModelState#fault_model_state{send_omissions=SendOmissions, receive_omissions=ReceiveOmissions};

fault_next_state(#fault_model_state{crashed_nodes=CrashedNodes0}=FaultModelState, 
                 _Res, 
                 {call, _Mod, resolve_all_faults_with_crash, []}) ->
    SendOmissions = dict:new(),
    ReceiveOmissions = dict:new(),
    CrashedNodes = lists:usort(CrashedNodes0 ++ active_faults(FaultModelState)),

    FaultModelState#fault_model_state{crashed_nodes=CrashedNodes, 
                                      send_omissions=SendOmissions, 
                                      receive_omissions=ReceiveOmissions};

fault_next_state(FaultModelState, _Res, _Call) ->
    FaultModelState.

%% Receive omission.
fault_postcondition(_FaultModelState, {call, _Mod, begin_receive_omission, [_SourceNode, _DestinationNode]}, ok) ->
    true;

fault_postcondition(_FaultModelState, {call, _Mod, end_receive_omission, [_SourceNode, _DestinationNode]}, ok) ->
    true;

%% Send omission.
fault_postcondition(_FaultModelState, {call, _Mod, begin_send_omission, [_SourceNode, _DestinationNode]}, ok) ->
    true;

fault_postcondition(_FaultModelState, {call, _Mod, end_send_omission, [_SourceNode, _DestinationNode]}, ok) ->
    true;

%% Stops are allowed.
fault_postcondition(_FaultModelState, {call, _Mod, stop, [_Node, _JoinedNodes]}, ok) ->
    true;

%% Crashes are allowed.
fault_postcondition(_FaultModelState, {call, _Mod, crash, [_Node, _JoinedNodes]}, ok) ->
    true;

fault_postcondition(_FaultModelState, {call, _Mod, resolve_all_faults_with_heal, []}, ok) ->
    true;

fault_postcondition(_FaultModelState, {call, _Mod, resolve_all_faults_with_crash, []}, ok) ->
    true;

fault_postcondition(_FaultModelState, {call, Mod, Fun, [_Node|_]=Args}, Res) ->
    fault_debug("fault postcondition fired for ~p:~p(~p) with response ~p", [Mod, Fun, Args, Res]),
    false.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

%% The number of active faults.
num_active_faults(FaultModelState) ->
    length(active_faults(FaultModelState)).

%% Resolvable faults.
fault_num_resolvable_faults(#fault_model_state{send_omissions=SendOmissions0, receive_omissions=ReceiveOmissions0}) ->
    SendOmissions = lists:map(fun({{SourceNode, _DestinationNode}, true}) -> SourceNode end, 
                        dict:to_list(SendOmissions0)),
    ReceiveOmissions = lists:map(fun({{_SourceNode, DestinationNode}, true}) -> DestinationNode end, 
                        dict:to_list(ReceiveOmissions0)),
    ResolvableFaults = lists:usort(SendOmissions ++ ReceiveOmissions),
    length(ResolvableFaults).

%% The nodes that are faulted.
active_faults(#fault_model_state{crashed_nodes=CrashedNodes, send_omissions=SendOmissions0, receive_omissions=ReceiveOmissions0}) ->
    SendOmissions = lists:map(fun({{SourceNode, _DestinationNode}, true}) -> SourceNode end, 
                        dict:to_list(SendOmissions0)),
    ReceiveOmissions = lists:map(fun({{_SourceNode, DestinationNode}, true}) -> DestinationNode end, 
                        dict:to_list(ReceiveOmissions0)),
    lists:usort(SendOmissions ++ ReceiveOmissions ++ CrashedNodes).

%% Is crashed?
fault_is_crashed(#fault_model_state{crashed_nodes=CrashedNodes}, Name) ->
    lists:member(Name, CrashedNodes).

%% Is this fault allowed?
fault_allowed({call, _Mod, _Fun, [Node|_] = _Args}, #fault_model_state{tolerance=Tolerance}=FaultModelState) ->
    %% We can tolerate another failure.
    NumActiveFaults = num_active_faults(FaultModelState),

    %% Node is already in faulted state -- send or receive omission.
    IsAlreadyFaulted = lists:member(Node, active_faults(FaultModelState)),

    %% Compute and log result.
    Result = NumActiveFaults < Tolerance orelse IsAlreadyFaulted,

    %% fault_debug("=> ~p num_active_faults: ~p is_already_faulted: ~p: result: ~p", [Fun, NumActiveFaults, IsAlreadyFaulted, Result]),

    Result.

%% Should we do node debugging?
fault_debug(Line, Args) ->
    case ?FAULT_DEBUG of
        true ->
            lager:info("~p: " ++ Line, [?MODULE] ++ Args);
        false ->
            ok
    end.

wait_until_nodes_agree_on_membership(Nodes) ->
    AgreementFun = fun(Node) ->
        %% Get membership at node.
        {ok, Members} = rpc:call(?NAME(Node), ?MANAGER, members, []),

        %% Convert started nodes to longnames.
        Names = lists:map(fun(N) -> ?NAME(N) end, Nodes),

        %% Sort.
        SortedNames = lists:usort(Names),
        SortedMembers = lists:usort(Members),

        %% Ensure the lists are the same -- barrier for proceeding.
        case SortedNames =:= SortedMembers of
            true ->
                fault_debug("node ~p agrees on membership: ~p", [Node, SortedMembers]),
                true;
            false ->
                fault_debug("node ~p disagrees on membership: ~p != ~p", [Node, SortedMembers, SortedNames]),
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
internal_crash(Name) ->
    case ct_slave:stop(Name) of
        {ok, _} ->
            ok;
        {error, stop_timeout, _} ->
            fault_debug("Failed to stop node ~p: stop_timeout!", [Name]),
            internal_crash(Name),
            ok;
        {error, not_started, _} ->
            ok;
        Error ->
            ct:fail(Error)
    end.