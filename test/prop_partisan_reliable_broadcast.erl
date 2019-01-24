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

-module(prop_partisan_reliable_broadcast).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-include_lib("proper/include/proper.hrl").

-compile([export_all]).

-define(ASSERT_MAILBOX, true).
-define(BROADCAST_MODULE, demers_direct_mail).

-define(PERFORM_SUMMARY, false).

%%%===================================================================
%%% Generators
%%%===================================================================

message() ->
    ?LET(Id, erlang:unique_integer([positive, monotonic]), 
        ?LET(Random, integer(),
            {Id, Random})).

node_name() ->
    ?LET(Names, names(), oneof(Names)).

names() ->
    NameFun = fun(N) -> 
        list_to_atom("node_" ++ integer_to_list(N)) 
    end,
    lists:map(NameFun, lists:seq(1, ?TEST_NUM_NODES)).

%%%===================================================================
%%% Node Functions
%%%===================================================================

-record(state, {sent}).

%% What node-specific operations should be called.
node_commands() ->
    CoreCommands = [{call, ?MODULE, broadcast, [node_name(), message()]}],

    AssertionCommands = case ?ASSERT_MAILBOX of
        true ->
            [{call, ?MODULE, check_mailbox, [node_name()]}];
        false ->
            []
    end,

    AssertionCommands ++ CoreCommands.

%% What should the initial node state be.
node_initial_state() ->
    node_debug("initializing", []),
    Sent = [],
    #state{sent=Sent}.

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
node_functions() ->
    lists:map(fun({call, _Mod, Fun, _Args}) -> Fun end, node_commands()).

%% Postconditions for node commands.
node_postcondition(_State, {call, ?MODULE, broadcast, [_Node, _Message]}, ok) ->
    true;
node_postcondition(#state{sent=Sent}, {call, ?MODULE, check_mailbox, [Node]}, {ok, Messages}) ->
    node_debug("verifying mailbox at node ~p: sent: ~p, received: ~p", [Node, Sent, Messages]),

    %% Figure out which messages we have.
    Result = lists:all(fun(M) -> lists:member(M, Messages) end, Sent),

    case Result of 
        true ->
            node_debug("verification of mailbox at node ~p complete.", [Node]),
            true;
        _ ->
            Missing = Sent -- Messages,
            node_debug("verification of mailbox at node ~p failed, missing: ~p, received: ~p", [Node, Missing, Messages]),
            false
    end;
node_postcondition(_State, _Command, _Response) ->
    false.

%% Next state.
node_next_state(#state{sent=Sent0}=State, _Result, {call, ?MODULE, broadcast, [Node, {Id, Value}]}) ->
    Message = {Id, Node, Value},
    Sent = Sent0 ++ [Message],
    State#state{sent=Sent};
node_next_state(State, _Response, _Command) ->
    State.

%% Precondition.
node_precondition(_State, {call, ?MODULE, broadcast, [_Node, _Message]}) ->
    true;
node_precondition(_State, {call, ?MODULE, check_mailbox, [_Node]}) ->
    true;
node_precondition(_State, _Command) ->
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
broadcast(Node, {Id, Value}) ->
    node_debug("executing broadcast command: ~p => ~p", [Node, {Id, Value}]),

    ?PROPERTY_MODULE:command_preamble(Node, [broadcast, Node, Id, Value]),

    FullMessage = {Id, Node, Value},
    node_debug("broadcast from node ~p message: ~p", [Node, FullMessage]),
    Result = rpc:call(?NAME(Node), ?BROADCAST_MODULE, broadcast, [?RECEIVER, FullMessage]),

    ?PROPERTY_MODULE:command_conclusion(Node, [broadcast, Node, Id, Value]),

    Result.

%% @private
check_mailbox(Node) ->
    node_debug("executing check mailbox command: ~p", [Node]),

    ?PROPERTY_MODULE:command_preamble(Node, [check_mailbox, Node]),

    Self = self(),

    node_debug("waiting for message quiescence at node ~p", [Node]),
    timer:sleep(20000),

    %% Ask for what messages they have received.
    erlang:send({?RECEIVER, ?NAME(Node)}, {received, Self}),

    Result = receive
        Messages ->
            {ok, Messages}
    after 
        10000 ->
            {error, no_response_from_mailbox}
    end,

    ?PROPERTY_MODULE:command_conclusion(Node, [check_mailbox, Node]),

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

    %% Start the backend.
    lists:foreach(fun({ShortName, _}) ->
        %% node_debug("starting ~p at node ~p with node list ~p ", [?BROADCAST_MODULE, ShortName, SublistNodeProjection]),
        {ok, _Pid} = rpc:call(?NAME(ShortName), ?BROADCAST_MODULE, start_link, [])
    end, Nodes),

    lists:foreach(fun({ShortName, _}) ->
        %% node_debug("spawning broadcast receiver on node ~p", [ShortName]),

        Self = self(),

        RemoteFun = fun() ->
            %% Create ETS table for the results.
            ?TABLE = ets:new(?TABLE, [set, named_table, public]),

            %% Define loop function for receiving and registering values.
            ReceiverFun = fun(F) ->
                receive
                    {received, Sender} ->
                        Received = ets:foldl(fun(Term, Acc) -> Acc ++ [Term] end, [], ?TABLE),
                        Sorted = lists:keysort(1, Received),
                        node_debug("node ~p received request for stored values: ~p", [ShortName, Sorted]),
                        Sender ! Sorted;
                    {Id, SourceNode, Value} ->
                        node_debug("node ~p received origin: ~p id ~p and value: ~p", [ShortName, SourceNode, Id, Value]),
                        true = ets:insert(?TABLE, {Id, SourceNode, Value});
                    Other ->
                        node_debug("node ~p received other: ~p", [ShortName, Other])
                end,
                F(F)
            end,

            %% Spawn locally.
            Pid = erlang:spawn(fun() -> ReceiverFun(ReceiverFun) end),

            %% Register name.
            erlang:register(?RECEIVER, Pid),

            %% Prevent races by notifying process is registered.
            Self ! ready,

            %% Block indefinitely so the table doesn't close.
            loop()
        end,
        
        Pid = rpc:call(?NAME(ShortName), erlang, spawn, [RemoteFun]),

        receive
            ready ->
                {ok, Pid}
        after 
            10000 ->
                error
        end
    end, Nodes),

    ok.

%% @private
node_end_case() ->
    node_debug("ending case", []),

    case ?PERFORM_SUMMARY of 
        true ->
            %% Get nodes.
            [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

            %% Aggregate the results from the run.
            NodeMessages = lists:map(fun({Name, _Node}) ->
                case check_mailbox(Name) of 
                    {ok, Messages} ->
                        node_debug("received at node ~p are ~p: ~p", [Name, length(Messages), Messages]),
                        Messages;
                    {error, _} ->
                        node_debug("cannot get messages received at node ~p", [Name]),
                        []
                end
            end, Nodes),

            %% Compute total messages.
            TotalMessages = length(lists:usort(lists:flatten(NodeMessages))),
            node_debug("total messages sent: ~p", [TotalMessages]),

            %% Keep percentage of nodes that have received all messages.
            NodeCount = length(Nodes),

            NodesRececivedAll = lists:foldl(fun(M, Acc) ->
                case length(M) =:= TotalMessages of
                    true ->
                        Acc + 1;
                    false ->
                        Acc
                end
            end, 0, NodeMessages),

            Percentage = NodesRececivedAll / NodeCount,
            node_debug("nodes received all: ~p, total nodes: ~p, percentage received: ~p", [NodesRececivedAll, NodeCount, Percentage]);
        false ->
            ok
    end,

    ok.