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

-module(prop_partisan_primary_backup).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-include_lib("proper/include/proper.hrl").

-compile([export_all]).

%%%===================================================================
%%% Generators
%%%===================================================================

key() ->
    oneof([x, y, z]).

value() ->
    oneof([1, 2, 3]).

primary_node_name() ->
    oneof([hd(names())]).

node_name() ->
    oneof(names()).

names() ->
    NameFun = fun(N) -> 
        list_to_atom("node_" ++ integer_to_list(N)) 
    end,
    lists:map(NameFun, lists:seq(1, node_num_nodes())).

%%%===================================================================
%%% Node Functions
%%%===================================================================

-record(node_state, {store}).

%% How many nodes to run?
node_num_nodes() ->
    3.

%% What node-specific operations should be called.
node_commands() ->
    [{call, ?MODULE, write, [primary_node_name(), key(), value()]},
     {call, ?MODULE, read, [primary_node_name(), key()]}].

%% Assertion commands.
node_assertion_functions() ->
    [verify].

%% Global functions.
node_global_functions() ->
    [verify].

%% What should the initial node state be.
node_initial_state() ->
    node_debug("initializing", []),
    #node_state{store=dict:new()}.

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
node_functions() ->
    lists:map(fun({call, _Mod, Fun, _Args}) -> Fun end, node_commands()).

%% Precondition.
node_precondition(_NodeState, {call, ?MODULE, write, [_Node, _Key, _Value]}) ->
    true;
node_precondition(_NodeState, {call, ?MODULE, read, [_Node, _Key]}) ->
    true;
node_precondition(_NodeState, {call, ?MODULE, verify, []}) ->
    true;
node_precondition(_NodeState, _Command) ->
    false.

%% Next state.

node_next_state(_State, NodeState, _Response, {call, ?MODULE, verify, []}) ->
    NodeState;
node_next_state(_State, NodeState, _Response, {call, ?MODULE, read, [_Node, _Key]}) ->
    NodeState;
node_next_state(_State, #node_state{store=Store0}=NodeState, _Response, {call, ?MODULE, write, [_Node, Key, Value]}) ->
    Store = dict:store(Key, Value, Store0),
    NodeState#node_state{store=Store};
node_next_state(_State, NodeState, Response, Command) ->
    node_debug("generic state transition made for response: ~p command: ~p", [Response, Command]),
    NodeState.

%% Postconditions for node commands.
node_postcondition(#node_state{store=Store}, {call, ?MODULE, read, [Node, Key]}, {ok, Value}) ->
    case dict:find(Key, Store) of
        {ok, Value} ->
            true;
        error ->
            %% Didn't find the value in the dict, fine, if we never wrote it.
            Value =:= not_found;
        Other ->
            node_debug("read at node ~p for key ~p returned other value: ~p when expecting: ~p",
                    [Node, Key, Other, Value]),
            false
    end;
node_postcondition(_NodeState, {call, ?MODULE, write, [_Node, _Key, _Value]}, {ok, _Value}) ->
    true;
node_postcondition(#node_state{store=Store}=_NodeState, {call, ?MODULE, verify, []}, AllResults) ->
    FoldResult = dict:fold(fun(Key, Value, Acc) ->
        All = dict:fold(fun(Node, Results, Acc1) ->
            case dict:find(Key, Results) of 
                {ok, Value} ->
                    Acc1 andalso true;
                Other ->
                    node_debug("Node ~p did not contain result for key: ~p, instead: ~p, should have: ~p", [Node, Key, Other, Value]),
                    node_debug("=> Results for node ~p are: ~p", [Node, dict:to_list(Results)]),
                    Acc1 andalso false
            end
        end, true, AllResults),

        Acc andalso All
    end, true, Store),

    FoldResult;
node_postcondition(_NodeState, Command, Response) ->
    node_debug("generic postcondition fired (this probably shouldn't be hit) for command: ~p with response: ~p", 
               [Command, Response]),
    false.

%%%===================================================================
%%% Commands
%%%===================================================================

-define(PROPERTY_MODULE, prop_partisan).

-define(ETS, prop_partisan).
-define(NAME, fun(Name) -> [{_, NodeName}] = ets:lookup(?ETS, Name), NodeName end).

%% @private
read(Node, Key) ->
    ?PROPERTY_MODULE:command_preamble(Node, [read, Node, Key]),

    Result = rpc:call(?NAME(Node), implementation_module(), read, [Key], 4000),

    ?PROPERTY_MODULE:command_conclusion(Node, [write, Node, Key]),

    Result.

%% @private
write(Node, Key, Value) ->
    ?PROPERTY_MODULE:command_preamble(Node, [write, Node, Key, Value]),

    Result = rpc:call(?NAME(Node), implementation_module(), write, [Key, Value], 4000),

    ?PROPERTY_MODULE:command_conclusion(Node, [write, Node, Key, Value]),

    Result.

%% @private
verify() ->
    RunnerNode = node(),

    ?PROPERTY_MODULE:command_preamble(RunnerNode, [verify]),

    Results = lists:foldl(fun(Node, Acc) ->
        {ok, State} = rpc:call(?NAME(Node), implementation_module(), state, [], 4000),
        dict:store(Node, State, Acc)
    end, dict:new(), names()),

    ?PROPERTY_MODULE:command_conclusion(RunnerNode, [verify]),

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

    %% Print nodes.
    node_debug("nodes are: ~p", [Nodes]),

    %% Start the backend.
    lists:foreach(fun({ShortName, _}) ->
        {ok, _Pid} = rpc:call(?NAME(ShortName), implementation_module(), start_link, []),
        % node_debug("backend started with pid ~p at node ~p", [Pid, ShortName]),
        ok
    end, Nodes),

    ok.

%% @private
node_crash(Node) ->
    ok = rpc:call(?NAME(Node), implementation_module(), stop, []).

%% @private
node_end_case() ->
    node_debug("ending case by terminating ~p", [implementation_module()]),

    %% Get nodes.
    [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

    %% Stop the backend.
    lists:foreach(fun({ShortName, _}) ->
        case rpc:call(?NAME(ShortName), implementation_module(), stop, []) of
            ok ->
                ok;
            {badrpc, _} ->
                ok;
            Error ->
                ct:fail("Couldn't terminate process for reason: ~p", [Error]),
                ok
        end
    end, Nodes),

    node_debug("ended.", []),

    ok.

%% @private
implementation_module() ->
    case os:getenv("IMPLEMENTATION_MODULE") of 
        false ->
            error({error, no_implementation_module});
        Other ->
            list_to_atom(Other)
    end.