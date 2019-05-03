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

-module(prop_partisan_zraft).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-include_lib("proper/include/proper.hrl").

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
    oneof([a, b, c]).

value() ->
    integer().

%%%===================================================================
%%% Node Functions
%%%===================================================================

-record(node_state, {values}).

%% What node-specific operations should be called.
node_commands() ->
    [
        {call, ?MODULE, read, [node_name(), key()]},
        {call, ?MODULE, write, [node_name(), key(), value()]}
    ].

%% Assertion commands.
node_assertion_functions() ->
    [].

%% Global functions.
node_global_functions() ->
    [sleep].

%% What should the initial node state be.
node_initial_state() ->
    node_debug("initializing", []),
    #node_state{values=dict:new()}.

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
node_functions() ->
    lists:map(fun({call, _Mod, Fun, _Args}) -> Fun end, node_commands()).

%% Precondition.
node_precondition(_NodeState, {call, ?MODULE, write, [_Node, _Key, _Value]}) ->
    true;
node_precondition(_NodeState, {call, ?MODULE, sleep, []}) ->
    true;
node_precondition(#node_state{values=Values}, {call, ?MODULE, read, [_Node, Key]}) ->
    % node_debug("checking precondition for read operation on key: ~p with values: ~p", [Key, dict:to_list(Values)]),

    case dict:find(Key, Values) of 
        {ok, _} ->
            true;
        Other ->
            node_debug("=> did NOT find key in values: ~p", [Other]),
            false
    end;
node_precondition(_NodeState, _Command) ->
    node_debug("general precondition fired, returning false.", []),
    false.

%% Next state.
node_next_state(_State, #node_state{values=Values0}=NodeState, _Response, {call, ?MODULE, write, [_Node, Key, Value]}) ->
    Values = dict:store(Key, Value, Values0),
    NodeState#node_state{values=Values};
node_next_state(_State, NodeState, _Response, {call, ?MODULE, read, [_Node, _Key]}) ->
    NodeState;
node_next_state(_State, NodeState, _Response, {call, ?MODULE, sleep, []}) ->
    NodeState;
node_next_state(_State, NodeState, Response, Command) ->
    node_debug("generic next_state called (this probably shouldn't be hit), command: ~p response: ~p", [Command, Response]),
    NodeState.

%% Postconditions for node commands.
node_postcondition(#node_state{values=Values}, {call, ?MODULE, read, [_Node, Key]}, {{ok, Value}, _}) ->
    case dict:find(Key, Values) of 
        {ok, Value} ->
            true;
        _ ->
            false
    end;
node_postcondition(_NodeState, {call, ?MODULE, sleep, []}, _Result) ->
    true;
node_postcondition(_NodeState, {call, ?MODULE, write, [_Node, _Key, _Value]}, {ok, _}) ->
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

    node_debug("sleeping for convergence...", []),
    timer:sleep(40000),

    ?PROPERTY_MODULE:command_conclusion(RunnerNode, [sleep]),

    ok.

%% @private
write(Node, Key, Value) ->
    ?PROPERTY_MODULE:command_preamble(Node, [write, Node, Key, Value]),

    Result = rpc:call(?NAME(Node), zraft_client, write, [{Node, ?NAME(Node)}, {Key, Value}, 1000]),

    ?PROPERTY_MODULE:command_conclusion(Node, [write, Node, Key, Value]),

    Result.

%% @private
read(Node, Key) ->
    ?PROPERTY_MODULE:command_preamble(Node, [read, Node, Key]),

    Result = rpc:call(?NAME(Node), zraft_client, query, [{Node, ?NAME(Node)}, Key, 1000]),

    ?PROPERTY_MODULE:command_conclusion(Node, [read, Node, Key]),

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
node_begin_property() ->
    partisan_trace_orchestrator:start_link().

%% @private
node_begin_case() ->
    %% Get nodes.
    [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

    %% Remove old state.
    node_debug("removing old state.", []),
    os:cmd("rm -rf data/"),

    %% Enable pid encoding.
    lists:foreach(fun({ShortName, _}) ->
        % node_debug("enabling pid_encoding at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [pid_encoding, true])
    end, Nodes),

    %% Load, configure, and start zraft.
    lists:foreach(fun({ShortName, _}) ->
        % node_debug("starting zraft_lib at node ~p", [ShortName]),
        case rpc:call(?NAME(ShortName), application, load, [zraft_lib]) of 
            ok ->
                ok;
            {error, {already_loaded, zraft_lib}} ->
                ok;
            Other ->
                exit({error, {load_failed, Other}})
        end,

        % node_debug("starting zraft_lib at node ~p", [ShortName]),
        {ok, _} = rpc:call(?NAME(ShortName), application, ensure_all_started, [zraft_lib])
    end, Nodes),

    %% Sleep.
    % node_debug("sleeping for convergence", []),
    timer:sleep(1000),
    % node_debug("done.", []),

    %% Get first node.
    FirstNode = {FirstName, _} = hd(Nodes),

    %% Initialize a Raft cluster.
    {ok, Nodes} = rpc:call(?NAME(FirstName), zraft_client, create, [Nodes, zraft_dict_backend]),

    %% Perform a single write.
    {ok, FirstNode} = rpc:call(?NAME(FirstName), zraft_client, write, [FirstNode, {1, 1}, 1000]),

    %% Perform a single read.
    {{ok, 1}, FirstNode} = rpc:call(?NAME(FirstName), zraft_client, query, [FirstNode, 1, 1000]),

    node_debug("zraft_lib fully initializaed...", []),

    ok.

%% @private
node_crash(Node) ->
    %% Stop zraft_lib.
    % node_debug("stopping zraft_lib on node ~p", [Node]),
    ok = rpc:call(?NAME(Node), application, stop, [zraft_lib]),

    ok.

%% @private
node_end_case() ->
    node_debug("ending case", []),

    %% Get nodes.
    [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

    %% Stop zraft_lib.
    lists:foreach(fun({ShortName, _}) ->
        % node_debug("stopping zraft_lib on node ~p", [ShortName]),
        case rpc:call(?NAME(ShortName), application, stop, [zraft_lib]) of 
            ok ->
                ok;
            {badrpc, nodedown} ->
                ok;
            {error, {not_started, zraft_lib}} ->
                ok;
            Error ->
                node_debug("cannot terminate zraft_lib: ~p", [Error]),
                exit({error, shutdown_failed})
        end
    end, Nodes),

    ok.