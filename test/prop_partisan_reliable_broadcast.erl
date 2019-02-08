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

-define(PERFORM_SUMMARY, false).

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
%%% Node Functions
%%%===================================================================

-record(state, {sent}).

%% What node-specific operations should be called.
node_commands() ->
    [{call, ?MODULE, broadcast, [node_name(), message()]}].

%% Assertion commands.
node_assertion_functions() ->
    [check_mailbox].

%% Global functions.
node_global_functions() ->
    [check_mailbox].

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
node_postcondition(#state{sent=Sent}, {call, ?MODULE, check_mailbox, []}, Results) ->

    %% Figure out which nodes have crashed.
    CrashedNodes = lists:foldl(fun(Node, Acc) ->
        Messages = dict:fetch(Node, Results),

        case Messages of 
            nodedown ->
                Acc ++ [Node];
            _ ->
                Acc
        end
    end, [], names()),

    %% Assert that all non-crashed nodes got messages sent from non-crashed nodes.
    MustReceiveMessages = lists:flatmap(fun({_Id, SourceNode, _Payload} = Message) ->
        case lists:member(SourceNode, CrashedNodes) of 
            true ->
                [];
            false ->
                [Message]
        end
    end, Sent),

    %% For crashed nodes, either everyone had to get it or no one should have.
    _ConditionalReceiveMessages = lists:flatmap(fun({_Id, SourceNode, _Payload} = Message) ->
        case lists:member(SourceNode, CrashedNodes) of 
            false ->
                [];
            true ->
                [Message]
        end
    end, Sent),

    %% Verify the must receive messages.
    MustReceives = lists:foldl(fun(Node, All) ->
        Messages = dict:fetch(Node, Results),

        node_debug("verifying mailboxes at node ~p: ", [Node]),
        node_debug(" => sent: ~p", [Sent]),
        node_debug(" => received: ~p", [Messages]),

        case Messages of 
            %% Crashed nodes are allowed to have any number of messages.
            nodedown ->
                true andalso All;
            _ ->
                %% Figure out which messages we have.
                Result = lists:all(fun(M) -> lists:member(M, Messages) end, MustReceiveMessages),

                case Result of 
                    true ->
                        node_debug("verification of mailbox must receives at node ~p complete.", [Node]),
                        true andalso All;
                    _ ->
                        Missing = Sent -- Messages,
                        node_debug("verification of mailbox must receives at node ~p failed.", [Node]),
                        node_debug(" => missing: ~p", [Missing]),
                        node_debug(" => received: ~p", [Messages]),
                        false andalso All
                end
        end
    end, true, names()),

    %% TODO: Verify conditional receives.
    ConditionalReceives = true,

    MustReceives andalso ConditionalReceives;
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
node_precondition(_State, {call, ?MODULE, check_mailbox, []}) ->
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

    %% Transmit message.
    FullMessage = {Id, Node, Value},
    node_debug("broadcast from node ~p message: ~p", [Node, FullMessage]),
    Result = rpc:call(?NAME(Node), broadcast_module(), broadcast, [?RECEIVER, FullMessage]),

    %% Sleep for 2 second, giving time for message to propagate (1 second timer.)
    node_debug("=> sleeping for propagation", []),
    timer:sleep(5000),

    ?PROPERTY_MODULE:command_conclusion(Node, [broadcast, Node, Id, Value]),

    Result.

%% @private
check_mailbox() ->
    node_debug("executing check_mailbox command", []),

    RunnerNode = node(),

    ?PROPERTY_MODULE:command_preamble(RunnerNode, [check_mailbox]),

    Self = self(),

    Results = lists:foldl(fun(Node, Dict) ->
        %% Ask for what messages they have received.
        erlang:send({?RECEIVER, ?NAME(Node)}, {received, Self}),

        receive
            Messages ->
                dict:store(Node, Messages, Dict)
        after 
            10000 ->
                case rpc:call(?NAME(Node), erlang, is_process_alive, []) of 
                    {badrpc, nodedown} ->
                        dict:store(Node, nodedown, Dict);
                    Other ->
                        node_debug("=> no response, asked if node was alive: ~p", [Other]),
                        dict:store(Node, [], Dict)
                end
        end
    end, dict:new(), names()),

    ?PROPERTY_MODULE:command_conclusion(RunnerNode, [check_mailbox]),

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

    %% Start the backend.
    lists:foreach(fun({ShortName, _}) ->
        %% node_debug("starting ~p at node ~p with node list ~p ", [?BROADCAST_MODULE, ShortName, SublistNodeProjection]),
        {ok, _Pid} = rpc:call(?NAME(ShortName), broadcast_module(), start_link, [])
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

    ok.

%% @private
broadcast_module() ->
    Module = case os:getenv("BROADCAST_MODULE") of 
        false ->
            demers_direct_mail;
        Other ->
            list_to_atom(Other)
    end,

    node_debug("broadcast module is defined as ~p", [Module]),
    Module.