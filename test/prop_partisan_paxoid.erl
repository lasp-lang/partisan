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

-define(PERFORM_SUMMARY, false).

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
    [{call, ?MODULE, next_id, [node_name()]}].

%% Assertion commands.
node_assertion_functions() ->
    [].

%% Global functions.
node_global_functions() ->
    [].

%% What should the initial node state be.
node_initial_state() ->
    node_debug("initializing", []),
    #node_state{counter=1}.

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
node_functions() ->
    lists:map(fun({call, _Mod, Fun, _Args}) -> Fun end, node_commands()).

%% Precondition.
node_precondition(_NodeState, {call, ?MODULE, next_id, [_Node]}) ->
    true;
node_precondition(_NodeState, _Command) ->
    false.

%% Next state.
node_next_state(_State, #node_state{counter=Counter}=NodeState, _Value, {call, ?MODULE, next_id, [_Node]}) ->
    NodeState#node_state{counter=Counter+1};
node_next_state(_State, NodeState, _Response, _Command) ->
    NodeState.

%% Postconditions for node commands.
node_postcondition(#node_state{counter=Counter}, {call, ?MODULE, next_id, [_Node]}, Value) ->
    node_debug("postcondition received ~p from next_id", [Value]),
    Counter =:= Value;
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
next_id(Node) ->
    ?PROPERTY_MODULE:command_preamble(Node, [next_id]),

    Result = rpc:call(?NAME(Node), paxoid, next_id, [test], 5000),

    ?PROPERTY_MODULE:command_conclusion(Node, [next_id]),

    Result.

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
        node_debug("enabling pid_encoding at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [pid_encoding, true])
    end, Nodes),

    %% Enable register_pid_for_encoding.
    lists:foreach(fun({ShortName, _}) ->
        node_debug("enabling register_pid_for_encoding at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [register_pid_for_encoding, true])
    end, Nodes),

    %% Load, configure, and start paxoid.
    lists:foreach(fun({ShortName, _}) ->
        node_debug("starting paxoid at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), application, load, [paxoid]),

        node_debug("starting paxoid at node ~p", [ShortName]),
        {ok, _} = rpc:call(?NAME(ShortName), paxoid, start_link, [test])
    end, Nodes),

    %% Join.
    OtherNodes = lists:map(fun({ShortName, _}) -> ?NAME(ShortName) end, tl(Nodes)),
    {FirstName, _} = hd(Nodes),
    node_debug("joining all nodes with paxoid to first node: ~p: ~p", [FirstName, OtherNodes]),
    ok = rpc:call(?NAME(FirstName), paxoid, join, [test, OtherNodes]),

    %% Info.
    node_debug("getting info from paxoid on first node", []),
    {ok, Info} = rpc:call(?NAME(FirstName), paxoid, info, [test]),
    node_debug("=> info: ~p", [Info]),

    ok.

%% @private
node_end_case() ->
    node_debug("ending case", []),

    ok.