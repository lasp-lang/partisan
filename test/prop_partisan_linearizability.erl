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

-module(prop_partisan_linearizability).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include_lib("proper/include/proper.hrl").

-compile([export_all]).

-define(NUM_NODES, 2).
-define(NODE_DEBUG, true).
-define(ETS, prop_partisan).
-define(NAME, fun(Name) -> [{_, NodeName}] = ets:lookup(?ETS, Name), NodeName end).
-define(PB_MODULE, pb1_host).

%%%===================================================================
%%% Generators
%%%===================================================================

key() ->
    oneof([key_1, key_2, key_3]).

value() ->
    binary().

node_name() ->
    ?LET(Names, names(), oneof(Names)).

names() ->
    NameFun = fun(N) -> 
        list_to_atom("node_" ++ integer_to_list(N)) 
    end,
    lists:map(NameFun, lists:seq(1, ?NUM_NODES)).

%%%===================================================================
%%% Node Functions
%%%===================================================================

-record(state, {store}).

%% What node-specific operations should be called.
node_commands() ->
    [{call, ?MODULE, write, [node_name(), key(), value()]},
     {call, ?MODULE, read, [node_name(), key()]}].

%% What should the initial node state be.
node_initial_state() ->
    node_debug("initializing", []),
    Store = dict:new(),
    #state{store=Store}.

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
node_functions() ->
    lists:map(fun({call, _Mod, Fun, _Args}) -> Fun end, node_commands()).

%% Precondition.
node_precondition(_State, {call, ?MODULE, write, [_Node, _Key, _Value]}) ->
    true;
node_precondition(_State, {call, ?MODULE, read, [_Node, _Key]}) ->
    true;
node_precondition(_State, _Command) ->
    false.

%% Next state.
node_next_state(#state{store=Store0}=State, ok, {call, ?MODULE, write, [_Node, Key, Value]}) ->
    Store = dict:store(Key, Value, Store0),
    State#state{store=Store};
node_next_state(State, _Result, {call, ?MODULE, read, [_Node, _Key]}) ->
    State;
node_next_state(State, _Response, _Command) ->
    State.

%% Postconditions for node commands.
node_postcondition(_State, {call, ?MODULE, write, [Node, Key, Value]}, ok) ->
    node_debug("node ~p: writing key ~p with value ~p", [Node, Key, Value]),
    true;
node_postcondition(#state{store=Store}=_State, {call, ?MODULE, read, [Node, Key]}, {ok, Value}) ->
    case dict:find(Key, Store) of 
        {ok, Value} ->
            node_debug("node ~p: read key ~p with value ~p", [Node, Key, Value]),
            true;
        {ok, Other} ->
            node_debug("node ~p: read key ~p with value ~p when it should be ~p", [Node, Key, Value, Other]),
            false;
        error ->
            case Value of 
                not_found ->
                    node_debug("node ~p: read key ~p with value not_found", [Node, Key]),
                    true;
                Value ->
                    node_debug("node ~p: received other value for key ~p: ~p", [Node, Key, Value]),
                    false
            end
    end;
node_postcondition(_State, _Command, {error, {primary, _Node}}) ->
    %% Ignore failures from contacting the wrong node -- should this be a precondition?
    true;
node_postcondition(_State, _Command, _Response) ->
    false.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

write(Node, Key, Value) ->
    rpc:call(?NAME(Node), ?PB_MODULE, write, [Key, Value]).

read(Node, Key) ->
    rpc:call(?NAME(Node), ?PB_MODULE, read, [Key]).

node_debug(Line, Args) ->
    case ?NODE_DEBUG of
        true ->
            lager:info("~p: " ++ Line, [?MODULE] ++ Args);
        false ->
            ok
    end.

%% @private
begin_property() ->
    partisan_trace_orchestrator:start_link().

%% @private
begin_case() ->
    %% Get nodes.
    [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

    %% Get list of FQDNs.
    NodeProjection = lists:map(fun({ShortName, _}) -> ?NAME(ShortName) end, Nodes),
    SublistNodeProjection = lists:sublist(NodeProjection, 1, ?NUM_NODES),

    %% Start the backend.
    lists:foreach(fun({ShortName, _}) ->
        {ok, _Pid} = rpc:call(?NAME(ShortName), ?PB_MODULE, start_link, [SublistNodeProjection]),
        node_debug("starting ~p at node ~p with node list ~p ", [?PB_MODULE, ShortName, SublistNodeProjection])
    end, Nodes),

    ok.

%% @private
end_case() ->
    node_debug("ending case", []),
    ok.