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

-record(node_state, {counter}).

%% What node-specific operations should be called.
node_commands() ->
    [
    %  {call, ?MODULE, set_fault, [node_name(), boolean()]},
     {call, ?MODULE, next_id, [node_name()]}
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
    #node_state{counter=0}.

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
node_functions() ->
    lists:map(fun({call, _Mod, Fun, _Args}) -> Fun end, node_commands()).

%% Precondition.
node_precondition(_NodeState, {call, ?MODULE, sleep, []}) ->
    true;
node_precondition(_NodeState, {call, ?MODULE, set_fault, [_Node, _Value]}) ->
    true;
node_precondition(_NodeState, {call, ?MODULE, next_id, [_Node]}) ->
    true;
node_precondition(_NodeState, _Command) ->
    false.

%% Next state.
node_next_state(_State, NodeState, {badrpc, _}, {call, ?MODULE, next_id, [_Node]}) ->
    %% don't advance expected counter if a badrpc is generated.
    NodeState;
node_next_state(_State, #node_state{counter=Counter}=NodeState, _Value, {call, ?MODULE, next_id, [_Node]}) ->
    NodeState#node_state{counter=Counter+1};
node_next_state(_State, NodeState, _Response, _Command) ->
    NodeState.

%% Postconditions for node commands.
node_postcondition(#node_state{counter=Counter}, {call, ?MODULE, max_id, []}, Results) ->
    node_debug("postcondition received ~p from max_id", [Results]),

    CorrectNodes = lists:filter(fun({Node, Result}) -> 
        case Counter =:= Result of
            true ->
                true;
            false ->
                case Result of 
                    undefined ->
                        %% Crashed node.
                        false; 
                    _ ->
                        node_debug("=> node: ~p has wrong value: ~p, should be ~p", [Node, Result, Counter]),
                        false
                end
        end
    end, Results),

    node_debug("=> number of correct nodes: ~p", [length(CorrectNodes)]),
    case length(CorrectNodes) >= (length(names()) / 2 + 1) of 
        true ->
            node_debug("=> majority present!", []),
            true;
        false ->
            node_debug("=> majority NOT present!", []),
            false
    end;
node_postcondition(_NodeState, {call, ?MODULE, set_fault, [_Node, _Value]}, ok) ->
    true;
node_postcondition(_NodeState, {call, ?MODULE, sleep, []}, _Result) ->
    true;
node_postcondition(_NodeState, {call, ?MODULE, next_id, [_Node]}, {badrpc,timeout}) ->
    true;
node_postcondition(_NodeState, {call, ?MODULE, next_id, [_Node]}, {badrpc, _}) ->
    %% badrpc is fine, timeout is acceptable if no write occurred.
    true;
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
sleep() ->
    RunnerNode = node(),

    ?PROPERTY_MODULE:command_preamble(RunnerNode, [sleep]),

    timer:sleep(4000),

    ?PROPERTY_MODULE:command_conclusion(RunnerNode, [sleep]),

    ok.

%% @private
set_fault(Node, Value) ->
    ?PROPERTY_MODULE:command_preamble(Node, [set_fault, Node, Value]),

    Result = rpc:call(?NAME(Node), partisan_config, set, [faulted, Value], ?TIMEOUT),

    ?PROPERTY_MODULE:command_conclusion(Node, [set_fault, Node, Value]),

    Result.

%% @private
next_id(Node) ->
    ?PROPERTY_MODULE:command_preamble(Node, [next_id, Node]),

    Result = rpc:call(?NAME(Node), paxoid, next_id, [?GROUP], ?TIMEOUT),
    node_debug("next_id for node: ~p yieleded: ~p", [node(), Result]),

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