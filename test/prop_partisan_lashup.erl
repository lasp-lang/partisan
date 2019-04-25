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

-module(prop_partisan_lashup).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-compile([export_all]).

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

key() ->
    [a, b, c].

value() ->
    integer().

%%%===================================================================
%%% Node Functions
%%%===================================================================

-record(node_state, {}).

%% What node-specific operations should be called.
node_commands() ->
    [
     {call, ?MODULE, update, [node_name(), key(), value()]}
    ].

%% Assertion commands.
node_assertion_functions() ->
    [check_delivery].

%% Global functions.
node_global_functions() ->
    [sleep, check_delivery].

%% What should the initial node state be.
node_initial_state() ->
    node_debug("initializing", []),
    #node_state{}.

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
node_functions() ->
    lists:map(fun({call, _Mod, Fun, _Args}) -> Fun end, node_commands()).

%% Precondition.
node_precondition(_NodeState, {call, ?MODULE, check_delivery, []}) ->
    true;
node_precondition(_NodeState, {call, ?MODULE, update, [_Node, _Key, _Value]}) ->
    true;
node_precondition(_NodeState, {call, ?MODULE, sleep, []}) ->
    true;
node_precondition(_NodeState, _Command) ->
    false.

%% Next state.
node_next_state(_State, NodeState, _Response, _Command) ->
    NodeState.

%% Postconditions for node commands.
node_postcondition(_NodeState, {call, ?MODULE, check_delivery, []}, _Result) ->
    true;
node_postcondition(_NodeState, {call, ?MODULE, update, [_Node, _Key, _Value]}, {ok, _Result}) ->
    true;
node_postcondition(_NodeState, {call, ?MODULE, sleep, []}, _Result) ->
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
update(Node, Key, Value) ->
    ?PROPERTY_MODULE:command_preamble(Node, [update, Node]),

    Key1 = [a, b, Key],

    Result = rpc:call(?NAME(Node), lashup_kv, request_op, [Key1, {update, 
                    [{update, 
                        {flag, riak_dt_lwwreg}, 
                        {assign, Value, erlang:system_time(nano_seconds)}
                    }]
                }]),
    node_debug("received result to write with result: ~p", [Result]),

    ?PROPERTY_MODULE:command_conclusion(Node, [update, Node]),

    Result.

%% @private
sleep() ->
    RunnerNode = node(),

    ?PROPERTY_MODULE:command_preamble(RunnerNode, [sleep]),

    timer:sleep(4000),

    ?PROPERTY_MODULE:command_conclusion(RunnerNode, [sleep]),

    ok.

%% @private
check_delivery() ->
    node_debug("executing check_delivery command", []),

    RunnerNode = node(),

    ?PROPERTY_MODULE:command_preamble(RunnerNode, [check_delivery]),

    Results = lists:foldl(fun(Node, Dict) ->
        MatchSpec = ets:fun2ms(fun({[a, b, '_']}) -> true end),
        Result = rpc:call(?NAME(Node), lashup_kv, keys, [MatchSpec]),
        dict:store(Node, Result, Dict)
    end, dict:new(), names()),

    node_debug("check_delivery: ~p", [dict:to_list(Results)]),

    ?PROPERTY_MODULE:command_conclusion(RunnerNode, [check_delivery]),

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
node_begin_property() ->
    partisan_trace_orchestrator:start_link().

%% @private
node_begin_case() ->
    %% Get nodes.
    [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

    %% Enable pid encoding.
    lists:foreach(fun({ShortName, _}) ->
        node_debug("enabling pid_encoding at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [pid_encoding, true])
    end, Nodes),

    %% Enable register_pid_for_encoding.
    lists:foreach(fun({ShortName, _}) ->
        node_debug("enabling register_pid_for_encoding at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [register_pid_for_encoding, true])
    end, Nodes),

    %% Load, configure, and start lashup.
    lists:foreach(fun({ShortName, _}) ->
        node_debug("starting lashup at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), application, load, [lashup]),

        node_debug("starting lashup at node ~p", [ShortName]),
        {ok, _} = rpc:call(?NAME(ShortName), application, ensure_all_started, [lashup])
    end, Nodes),

    %% Sleep.
    node_debug("sleeping for convergence", []),
    timer:sleep(1000),
    node_debug("done.", []),

    ok.

%% @private
node_end_case() ->
    node_debug("ending case", []),

    %% Get nodes.
    [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

    %% Stop lashup.
    node_debug("stopping lashup", []),
    lists:foreach(fun({ShortName, _}) ->
        node_debug("stopping lashup on node ~p", [ShortName]),
        case rpc:call(?NAME(ShortName), application, stop, [lashup]) of 
            ok ->
                ok;
            {badrpc, nodedown} ->
                ok;
            Other ->
                node_debug("cannot terminate instance: ~p", [Other]),
                exit({error, shutdown_failed})
        end
    end, Nodes),

    ok.