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

-record(node_state, {sent, failed_to_send}).

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
    #node_state{sent=[], failed_to_send=[]}.

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
node_functions() ->
    lists:map(fun({call, _Mod, Fun, _Args}) -> Fun end, node_commands()).

%% Precondition.
node_precondition(_NodeState, {call, ?MODULE, broadcast, [_Node, _Message]}) ->
    true;
node_precondition(_NodeState, {call, ?MODULE, check_mailbox, []}) ->
    true;
node_precondition(_NodeState, _Command) ->
    false.

%% Next state.

%% If the broadcast returned 'ok', we have to assume it was sent (asynchronous / synchronous.)
node_next_state(#property_state{joined_nodes=JoinedNodes}, 
                #node_state{sent=Sent0}=NodeState, 
                ok, 
                {call, ?MODULE, broadcast, [Node, {Id, Value}]}) ->
    Recipients = JoinedNodes,
    Message = {Id, Node, Value},
    Sent = Sent0 ++ [{Message, Recipients}],
    NodeState#node_state{sent=Sent};

node_next_state(#property_state{joined_nodes=JoinedNodes}, 
                #node_state{failed_to_send=FailedToSend0}=NodeState, 
                error, 
                {call, ?MODULE, broadcast, [Node, {Id, Value}]}) ->
    Recipients = JoinedNodes,
    Message = {Id, Node, Value},
    FailedToSend = FailedToSend0 ++ [{Message, Recipients}],
    NodeState#node_state{failed_to_send=FailedToSend};

%% If the broadcast returned 'error', we have to assume it was aborted (synchronous.)
node_next_state(_State, NodeState, _Response, _Command) ->
    NodeState.

%% Postconditions for node commands.
node_postcondition(_NodeState, {call, ?MODULE, broadcast, [_Node, _Message]}, {badrpc, timeout}) ->
    lager:info("Broadcast error with timeout, must have been synchronous!"),
    false;
node_postcondition(_NodeState, {call, ?MODULE, broadcast, [_Node, _Message]}, error) ->
    lager:info("Broadcast error, must have been synchronous!"),
    true;
node_postcondition(_NodeState, {call, ?MODULE, broadcast, [_Node, _Message]}, ok) ->
    true;
node_postcondition(#node_state{failed_to_send=FailedToSend, sent=Sent}, {call, ?MODULE, check_mailbox, []}, Results) ->
    AllSentAndReceived = lists:foldl(fun({Message, Recipients}, MessageAcc) ->
        node_debug("verifying that message ~p was received by ~p", [Message, Recipients]),

        All = lists:foldl(fun(Recipient, NodeAcc) ->
            node_debug("=> verifying that message ~p was received by ~p", [Message, Recipient]),

            %% Did this node receive the message?
            Messages = dict:fetch(Recipient, Results),

            %% Carry forward.
            case lists:member(Message, Messages) of 
                true ->
                    NodeAcc andalso true;
                false ->
                    node_debug("=> => message not received at node ~p: ~p", [Message, Recipient]),
                    NodeAcc andalso false
            end
        end, true, Recipients),

        All andalso MessageAcc
    end, true, Sent),

    FailedToSendNotReceived = lists:foldl(fun({Message, Recipients}, MessageAcc) ->
        node_debug("verifying that message ~p was NOT received by ~p", [Message, Recipients]),

        All = lists:foldl(fun(Recipient, NodeAcc) ->
            node_debug("=> verifying that message ~p was NOT received by ~p", [Message, Recipient]),

            %% Did this node receive the message?
            Messages = dict:fetch(Recipient, Results),

            %% Carry forward.
            case lists:member(Message, Messages) of 
                true ->
                    node_debug("=> => message received at node ~p: ~p", [Message, Recipient]),
                    NodeAcc andalso false;
                false ->
                    NodeAcc andalso true
            end
        end, true, Recipients),

        All andalso MessageAcc
    end, true, FailedToSend),

    AllSentAndReceived andalso FailedToSendNotReceived;
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
-define(RECEIVER_LOOP, receiver_loop).

-define(ETS, prop_partisan).
-define(NAME, fun(Name) -> [{_, NodeName}] = ets:lookup(?ETS, Name), NodeName end).

%% @private
broadcast(Node, {Id, Value}) ->
    node_debug("executing broadcast command: ~p => ~p", [Node, {Id, Value}]),

    ?PROPERTY_MODULE:command_preamble(Node, [broadcast, Node, Id, Value]),

    %% Transmit message.
    FullMessage = {Id, Node, Value},
    node_debug("broadcast from node ~p message: ~p", [Node, FullMessage]),
    Result = rpc:call(?NAME(Node), broadcast_module(), broadcast, [?RECEIVER, FullMessage], 4000),

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
            ok;
        _ ->
            loop()
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
        % node_debug("starting ~p at node ~p with node list ~p ", [broadcast_module(), ShortName, SublistNodeProjection]),
        {ok, Pid} = rpc:call(?NAME(ShortName), broadcast_module(), start_link, []),
        node_debug("backend started with pid ~p at node ~p", [Pid, ShortName])
    end, Nodes),

    lists:foreach(fun({ShortName, _}) ->
        Self = self(),

        node_debug("spawning broadcast receiver on node ~p, our pid is: ~p", [ShortName, Self]),

        RemoteFun = fun() ->
            %% Create ETS table for the results.
            ?TABLE = ets:new(?TABLE, [set, named_table, public]),

            %% Register ourselves.
            true = erlang:register(?RECEIVER_LOOP, self()),

            %% Define loop function for receiving and registering values.
            ReceiverFun = fun(F) ->
                receive
                    {received, Sender} ->
                        Received = ets:foldl(fun(Term, Acc) -> Acc ++ [Term] end, [], ?TABLE),
                        Sorted = lists:keysort(1, Received),
                        node_debug("node ~p received request for stored values: ~p", [ShortName, Sorted]),
                        Sender ! Sorted,
                        F(F);
                    {Id, SourceNode, Value} ->
                        node_debug("node ~p received origin: ~p id ~p and value: ~p", [ShortName, SourceNode, Id, Value]),
                        true = ets:insert(?TABLE, {Id, SourceNode, Value}),
                        F(F);
                    terminate ->
                        node_debug("node ~p terminating receiver", [ShortName]),
                        ok;
                    Other ->
                        node_debug("node ~p received other: ~p", [ShortName, Other]),
                        F(F)
                end
            end,

            %% Spawn locally.
            Pid = erlang:spawn(fun() -> ReceiverFun(ReceiverFun) end),

            %% Register name.
            true = erlang:register(?RECEIVER, Pid),

            %% Prevent races by notifying process is registered.
            Self ! ready,

            %% Block indefinitely so the table doesn't close.
            loop()
        end,
        
        Pid = rpc:call(?NAME(ShortName), erlang, spawn, [RemoteFun]),

        receive
            ready ->
                {ok, Pid};
            Other ->
                node_debug("received other message: ~p", [Other]),
                error
        after 
            10000 ->
                node_debug("timer fired, never received the message!", []),
                error
        end
    end, Nodes),

    ok.

%% @private
node_end_case() ->
    node_debug("ending case by terminating ~p", [broadcast_module()]),

    %% Get nodes.
    [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

    %% Stop the backend.
    lists:foreach(fun({ShortName, _}) ->
        %% node_debug("starting ~p at node ~p with node list ~p ", [?BROADCAST_MODULE, ShortName, SublistNodeProjection]),
        Pid = rpc:call(?NAME(ShortName), erlang, whereis, [broadcast_module()]),
        node_debug("process is running on node ~p with id ~p~n", [ShortName, Pid]),
        node_debug("asking node ~p to terminate process.", [ShortName]),
        ok = rpc:call(?NAME(ShortName), broadcast_module(), stop, [])
    end, Nodes),

    %% Stop the receiver.
    lists:foreach(fun({ShortName, _}) ->
        terminate = rpc:call(?NAME(ShortName), erlang, send, [?RECEIVER, terminate])
    end, Nodes),

    %% Stop the receiver loop.
    lists:foreach(fun({ShortName, _}) ->
        terminate = rpc:call(?NAME(ShortName), erlang, send, [?RECEIVER_LOOP, terminate])
    end, Nodes),

    node_debug("ended.", []),

    ok.

%% @private
broadcast_module() ->
    Module = case os:getenv("IMPLEMENTATION_MODULE") of 
        false ->
            lampson_2pc;
        Other ->
            list_to_atom(Other)
    end,

    node_debug("broadcast module is defined as ~p", [Module]),
    Module.