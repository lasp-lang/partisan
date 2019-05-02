%% -------------------------------------------------------------------
%%
%% Copyright (c) 2010 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(prop_partisan_paxoid).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-include_lib("proper/include/proper.hrl").

-compile([export_all]).

-define(GROUP, paxoid).

-define(TIMEOUT, 10000).

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
%%% Node Functions
%%%===================================================================

-record(node_state, {counter, node_writes}).

%% What node-specific operations should be called.
node_commands() ->
    [
     {call, ?MODULE, next_id, [node_name()]},
    %  {call, ?MODULE, wait, [node_name()]},
     {call, ?MODULE, sleep, []}
    ].

%% Assertion commands.
node_assertion_functions() ->
    [max_id].

%% Global functions.
node_global_functions() ->
    [max_id, sleep].

%% What should the initial node state be.
node_initial_state() ->
    node_debug("initializing", []),
    #node_state{counter=0, node_writes=dict:new()}.

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
node_functions() ->
    lists:map(fun({call, _Mod, Fun, _Args}) -> Fun end, node_commands()).

%% Precondition.
node_precondition(_NodeState, {call, ?MODULE, sleep, []}) ->
    true;
node_precondition(_NodeState, {call, ?MODULE, wait, [_Node]}) ->
    true;
node_precondition(_NodeState, {call, ?MODULE, next_id, [_Node]}) ->
    true;
node_precondition(_NodeState, _Command) ->
    false.

%% Next state.
node_next_state(_State, NodeState, {badrpc, _}, {call, ?MODULE, next_id, [_Node]}) ->
    %% don't advance expected counter if a badrpc is generated.
    NodeState;
node_next_state(_State, #node_state{counter=Counter, node_writes=NodeWrites0}=NodeState, _Value, {call, ?MODULE, next_id, [Node]}) ->
    NodeWrites = dict:update_counter(Node, 1, NodeWrites0),
    NodeState#node_state{counter=Counter+1, node_writes=NodeWrites};
node_next_state(_State, NodeState, _Response, _Command) ->
    NodeState.

%% Postconditions for node commands.
node_postcondition(#node_state{node_writes=NodeWrites, counter=Counter}, {call, ?MODULE, max_id, []}, Results) ->
    node_debug("postcondition received ~p from max_id", [Results]),

    %% Find the crashed nodes.
    CrashedNodes = lists:filter(fun({_Node, Result}) -> 
        case Result of 
            undefined ->
                true;
            _ ->
                false
        end
    end, Results),

    %% Find the number of writes done by crashed nodes.
    ConditionalWrites = lists:foldl(fun({Node, _Results}, Writes) ->
        case dict:find(Node, NodeWrites) of 
            {ok, Value} ->
                node_debug("=> node ~p found writes ~p", [Node, Value]),
                Writes + Value;
            Other ->
                node_debug("=> node ~p FOUND NO writes ~p", [Node, Other]),
                Writes
        end
    end, 0, CrashedNodes),

    LowerBound = Counter - ConditionalWrites,

    node_debug("NodeWrites: ~p", [dict:to_list(NodeWrites)]),
    node_debug("CrashedNodes: ~p", [CrashedNodes]),
    node_debug("ConditionalWrites: ~p", [ConditionalWrites]),
    node_debug("LowerBound: ~p", [LowerBound]),

    %% Ensure that a majority of nodes account for all the writes.
    CorrectNodes = lists:filter(fun({Node, Result}) -> 
        case Result of 
            undefined ->
                false;
            _ ->
                case Result >= LowerBound of 
                    true ->
                        true;
                    false ->
                        node_debug("=> node: ~p has wrong value: ~p, should be ~p", [Node, Result, Counter]),
                        false
                end
        end
    end, Results),

    AllResults = lists:map(fun({_Node, Result}) -> Result end, CorrectNodes),

    Agreement = case lists:usort(AllResults) of
        [_] ->
            true;
        _ ->
            false
    end,

    node_debug("lists:usort(AllResults): ~p", [lists:usort(AllResults)]),
    node_debug("AllResults: ~p", [AllResults]),
    node_debug("Agreement: ~p", [Agreement]),
    node_debug("length(CorrectNodes): ~p", [length(CorrectNodes)]),

    MajorityNodes = (length(names()) / 2) + 1,

    case length(CorrectNodes) >= MajorityNodes andalso Agreement of 
        true ->
            node_debug("=> majority present!", []),
            true;
        false ->
            node_debug("=> majority NOT present!", []),
            false
    end;
node_postcondition(_NodeState, {call, ?MODULE, wait, [_Node]}, _Result) ->
    true;
node_postcondition(_NodeState, {call, ?MODULE, sleep, []}, _Result) ->
    true;
node_postcondition(_NodeState, {call, ?MODULE, next_id, [_Node]}, {badrpc,timeout}) ->
    case os:getenv("MODEL_CHECKING") of 
        "true" ->
            true;
        _ ->
            false
    end;
node_postcondition(#node_state{counter=Counter}, {call, ?MODULE, next_id, [_Node]}, Value) ->
    node_debug("postcondition received ~p from next_id when value should be: ~p", [Value, Counter + 1]),

    case Counter + 1 =:= Value of 
        false ->
            node_debug("postcondition: sequence has been duplicated: value is ~p should be ~p", [Value, Counter + 1]),
            ok;
        true ->
            ok
    end,

    true;
node_postcondition(_NodeState, Command, Response) ->
    node_debug("generic postcondition fired (this probably shouldn't be hit) for command: ~p with response: ~p", 
               [Command, Response]),
    false.

%%%===================================================================
%%% Commands
%%%===================================================================

-define(PROPERTY_MODULE, prop_partisan).

-define(TABLE, table).
-define(RECEIVER, receiver).

-define(ETS, prop_partisan).
-define(NAME, fun(Name) -> [{_, NodeName}] = ets:lookup(?ETS, Name), NodeName end).

%% @private
wait(Node) ->
    ?PROPERTY_MODULE:command_preamble(Node, [wait]),

    timer:sleep(2000),

    ?PROPERTY_MODULE:command_conclusion(Node, [wait]),

    ok.

%% @private
sleep() ->
    RunnerNode = node(),

    ?PROPERTY_MODULE:command_preamble(RunnerNode, [sleep]),

    timer:sleep(30000),

    ?PROPERTY_MODULE:command_conclusion(RunnerNode, [sleep]),

    ok.

%% @private
next_id(Node) ->
    ?PROPERTY_MODULE:command_preamble(Node, [next_id, Node]),

    Result = rpc:call(?NAME(Node), paxoid, next_id, [?GROUP], ?TIMEOUT),
    node_debug("next_id for node: ~p yieleded: ~p", [node(), Result]),

    % node_debug("sleeping 250ms...", []),
    % timer:sleep(250),

    ?PROPERTY_MODULE:command_conclusion(Node, [next_id, Node]),

    Result.

%% @private
max_id() ->
    node_debug("executing max_id command", []),

    RunnerNode = node(),

    ?PROPERTY_MODULE:command_preamble(RunnerNode, [max_id]),

    Results = lists:map(fun(Node) ->
        case rpc:call(?NAME(Node), paxoid, max_id, [?GROUP], ?TIMEOUT) of 
            {ok, Result} ->
                node_debug("node: ~p result of max_id: ~p~n", [Node, Result]),
                {Node, Result};
            {badrpc, _} ->
                node_debug("node: ~p result of max_id undefined: crash", [Node]),
                {Node, undefined}
        end
    end, names()),

    ?PROPERTY_MODULE:command_conclusion(RunnerNode, [max_id]),

    Results.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

-define(NODE_DEBUG, true).

%% Should we do node debugging?
node_debug(Line, Args) ->
    case ?NODE_DEBUG of
        true ->
            lager:info("~p: " ++ Line, [?MODULE] ++ Args);
        false ->
            ok
    end.

%% @private
loop() ->
    receive
        terminate ->
            ok
    end,
    loop().

%% @private
node_begin_property() ->
    partisan_trace_orchestrator:start_link().

%% @private
node_begin_case() ->
    %% Get nodes.
    [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

    %% Enable pid encoding.
    lists:foreach(fun({ShortName, _}) ->
        % node_debug("enabling pid_encoding at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [pid_encoding, true])
    end, Nodes),

    %% Enable register_pid_for_encoding.
    lists:foreach(fun({ShortName, _}) ->
        % node_debug("enabling register_pid_for_encoding at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [register_pid_for_encoding, true])
    end, Nodes),

    %% Load, configure, and start paxoid.
    lists:foreach(fun({ShortName, _}) ->
        % node_debug("starting paxoid at node ~p", [ShortName]),
        case rpc:call(?NAME(ShortName), application, load, [paxoid]) of 
            ok ->
                ok;
            {error, {already_loaded, paxoid}} ->
                ok;
            Other ->
                exit({error, {load_failed, Other}})
        end,

        % node_debug("configuring paxoid at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), application, set_env, [paxoid, predefined, [paxoid]]),

        % node_debug("starting paxoid at node ~p", [ShortName]),
        {ok, _} = rpc:call(?NAME(ShortName), application, ensure_all_started, [paxoid])
    end, Nodes),

    %% Join.
    OtherNodes = lists:map(fun({ShortName, _}) -> ?NAME(ShortName) end, tl(Nodes)),
    {FirstName, _} = hd(Nodes),
    % node_debug("joining all nodes with paxoid to first node: ~p: ~p", [FirstName, OtherNodes]),
    ok = rpc:call(?NAME(FirstName), paxoid, join, [?GROUP, OtherNodes]),

    %% Info.
    % node_debug("getting info from paxoid on first node", []),
    % {ok, Info} = rpc:call(?NAME(FirstName), paxoid, info, [?GROUP]),
    % node_debug("=> info: ~p", [Info]),

    %% Sleep.
    % node_debug("sleeping for convergence", []),
    timer:sleep(1000),
    % node_debug("done.", []),

    ok.

%% @private
node_crash(Node) ->
    %% Stop paxoid.
    % node_debug("stopping paxoid on node ~p", [Node]),
    ok = rpc:call(?NAME(Node), application, stop, [paxoid]),

    ok.

%% @private
node_end_case() ->
    node_debug("ending case", []),

    %% Get nodes.
    [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

    %% Stop paxoid.
    node_debug("stopping paxoid", []),
    lists:foreach(fun({ShortName, _}) ->
        node_debug("stopping paxoid on node ~p", [ShortName]),
        case rpc:call(?NAME(ShortName), application, stop, [paxoid]) of 
            ok ->
                ok;
            {error, {not_started, paxoid}} ->
                ok;
            Other ->
                ct:fail({error, Other})
        end
    end, Nodes),

    ok.