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
    [verify, read].

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
node_next_state(_State, NodeState, {badrpc, timeout}, {call, ?MODULE, write, [_Node, _Key, _Value]}) ->
    NodeState;
node_next_state(_State, #node_state{store=Store0}=NodeState, _Response, {call, ?MODULE, write, [_Node, Key, Value]}) ->
    Store = dict:store(Key, Value, Store0),
    node_debug("store is now: ~p", [dict:to_list(Store)]),
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
            Result = Value =:= not_found,

            case Result of 
                false ->
                    node_debug("read at node ~p for key ~p returned other value: ~p when expecting: not_found",
                            [Node, Key, Value]);
                _ ->
                    ok
            end,

            Result;
        Other ->
            node_debug("read at node ~p for key ~p returned other value: ~p when expecting: ~p",
                       [Node, Key, Other, Value]),
            false
    end;
node_postcondition(_NodeState, {call, ?MODULE, write, [_Node, _Key, Value]}, {ok, Value}) ->
    true;
node_postcondition(_NodeState, {call, ?MODULE, write, [_Node, _Key, _Value]}, {badrpc, timeout}) ->
    true;
node_postcondition(_NodeState, {call, ?MODULE, write, [_Node, _Key, _Value]}, deadlock) ->
    false;
node_postcondition(#node_state{store=Store}=_NodeState, {call, ?MODULE, verify, []}, AllResults) ->
    %% Everything we think we wrote is there.
    StoreToNodeResult = dict:fold(fun(Key, Value, Acc) ->
        All = dict:fold(fun(Node, Results, Acc1) ->
            % node_debug("looking at reuslts: ~p", [Results]),

            case Results of 
                badrpc ->
                    node_debug("=> node crashed, considering valid.", []),
                    Acc1 andalso true;
                _ ->
                    case dict:find(Key, Results) of 
                        {ok, Value} ->
                            Acc1 andalso true;
                        Other ->
                            node_debug("Node ~p did not contain result for key: ~p, instead: ~p, should have: ~p", [Node, Key, Other, Value]),
                            node_debug("=> Results for node ~p are: ~p", [Node, dict:to_list(Results)]),
                            Acc1 andalso false
                    end
            end
        end, true, AllResults),

        Acc andalso All
    end, true, Store),

    %% Everything we think we wrote is there.
    NodesMissingValues = dict:fold(fun(Key, Value, Acc) ->
        NodesAcc = dict:fold(fun(Node, Results, Acc1) ->
            % node_debug("looking at reuslts: ~p", [Results]),

            case Results of 
                badrpc ->
                    node_debug("=> node crashed, considering valid.", []),
                    Acc1 ++ [Node];
                _ ->
                    case dict:find(Key, Results) of 
                        {ok, Value} ->
                            Acc1;
                        Other ->
                            node_debug("Node ~p did not contain result for key: ~p, instead: ~p, should have: ~p", [Node, Key, Other, Value]),
                            node_debug("=> Results for node ~p are: ~p", [Node, dict:to_list(Results)]),
                            Acc1 ++ [Node]
                    end
            end
        end, [], AllResults),

        lists:usort(Acc ++ NodesAcc)
    end, [], Store),

    %% Everything there is something we wrote.
    NodeToStoreResult = dict:fold(fun(Node, Results, Acc) ->
        case Results of 
            badrpc ->
                node_debug("=> node crashed, considering valid.", []),
                Acc andalso true;
            _ ->
                All = dict:fold(fun(Key, Value, Acc1) ->
                    case dict:find(Key, Store) of 
                        {ok, Value} ->
                            Acc1 andalso true;
                        Other ->
                            node_debug("Node ~p had result key: ~p value: ~p but in the store we had ~p", [Node, Key, Value, Other]),
                            Acc1 andalso false
                    end
                end, true, Results),

                Acc andalso All
        end
    end, true, AllResults),

    Tolerance = fault_tolerance(),
    node_debug("=> Tolerance: ~p", [Tolerance]),

    node_debug("=> written values: ~p", [dict:to_list(Store)]),
    node_debug("=> StoreToNodeResult: ~p", [StoreToNodeResult]),
    node_debug("=> NodeToStoreResult: ~p", [NodeToStoreResult]),
    node_debug("=> NodesMissingValues: ~p", [NodesMissingValues]),

    (StoreToNodeResult andalso NodeToStoreResult) orelse
    (not StoreToNodeResult andalso NodeToStoreResult andalso length(NodesMissingValues) =< Tolerance);
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
    Result = rpc_with_timeout(Node, read, [Key]),
    ?PROPERTY_MODULE:command_conclusion(Node, [read, Node, Key]),
    Result.

%% @private
write(Node, Key, Value) ->
    ?PROPERTY_MODULE:command_preamble(Node, [write, Node, Key, Value]),
    Result = rpc_with_timeout(Node, write, [Key, Value]),
    ?PROPERTY_MODULE:command_conclusion(Node, [write, Node, Key, Value]),
    Result.

%% @private
verify() ->
    RunnerNode = node(),

    ?PROPERTY_MODULE:command_preamble(RunnerNode, [verify]),

    node_debug("waiting 1s for quiesence...", []),
    timer:sleep(1000),

    Results = lists:foldl(fun(Node, Acc) ->
        case rpc:call(?NAME(Node), implementation_module(), store, [], infinity) of
            {ok, State} ->
                dict:store(Node, State, Acc);
            {badrpc, _} ->
                dict:store(Node, badrpc, Acc)
        end
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
            logger:info("~p: " ++ Line, [?MODULE] ++ Args);
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

%% @private
rpc_with_timeout(Node, Function, Args) ->
    ImplementationModule = implementation_module(),
    Timeout = ImplementationModule:timeout(),
    
    Result = case scheduler() of 
        finite_fault ->
            Self = self(),

            spawn_link(fun() -> 
                RpcResult = rpc:call(?NAME(Node), ImplementationModule, Function, Args, Timeout),
                Self ! {result, RpcResult}
            end),

            receive 
                {result, RpcResult} ->
                    RpcResult
            after
                20000 ->
                    deadlock
            end;
        _ ->
            rpc:call(?NAME(Node), ImplementationModule, Function, Args, Timeout)
    end,

    node_debug("result of RPC for ~p(~p) => ~p", [Function, Args, Result]),
    Result.

%% @private
scheduler() ->
    case os:getenv("SCHEDULER") of
        false ->
            default;
        Other ->
            list_to_atom(Other)
    end.

%% @private
fault_tolerance() ->
    case os:getenv("FAULT_TOLERANCE") of 
        false ->
            0;
        ToleranceString ->
            list_to_integer(ToleranceString)
    end.