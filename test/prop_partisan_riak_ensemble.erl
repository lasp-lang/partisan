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

-module(prop_partisan_riak_ensemble).

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

    %% Load, configure, and start ensemble.
    lists:foreach(fun({ShortName, _}) ->
        node_debug("starting riak_ensemble at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), application, load, [riak_ensemble]),

        DataRoot = "data-root-" ++ atom_to_list(?NAME(ShortName)),

        node_debug("removing old data root: ~p", [DataRoot]),
        os:cmd("rm -rf ./" ++ DataRoot),

        ok = rpc:call(?NAME(ShortName), application, set_env, [riak_ensemble, data_root, DataRoot]),

        node_debug("configuring riak_ensemble at node ~p for data_root ~p", [ShortName, DataRoot]),
        ok = rpc:call(?NAME(ShortName), application, set_env, [riak_ensemble, data_root, DataRoot]),

        node_debug("starting riak_ensemble at node ~p", [ShortName]),
        {ok, _} = rpc:call(?NAME(ShortName), application, ensure_all_started, [riak_ensemble])
    end, Nodes),

    %% Cluster ensemble.
    {FirstName, _} = hd(Nodes),

    %% Enable enable at node.
    node_debug("calling riak_ensemble_manager:enable/0 on node ~p", [FirstName]),
    ok = rpc:call(?NAME(FirstName), riak_ensemble_manager, enable, []),

    node_debug("waiting for cluster stability on node: ~p", [FirstName]),
    wait_stable(?NAME(FirstName), root),

    %% Join remote nodes to the root.
    lists:foreach(fun({ShortName, _}) ->
        ok = rpc:call(?NAME(FirstName), riak_ensemble_manager, join, [?NAME(FirstName), ?NAME(ShortName)])
    end, tl(Nodes)),

    %% Get members of the ensemble.
    node_debug("getting members of the root ensemble on node ~p", [FirstName]),
    [{root, EnsembleNode}] = rpc:call(?NAME(FirstName), riak_ensemble_manager, get_members, [root]),
    node_debug("=> ~p", [EnsembleNode]),

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
    Module = case os:getenv("IMPLEMENTATION_MODULE") of 
        false ->
            lampson_2pc;
        Other ->
            list_to_atom(Other)
    end,

    node_debug("broadcast module is defined as ~p", [Module]),
    Module.

%% @private
wait_stable(Node, Ensemble) ->
    case check_stable(Node, Ensemble) of
        true ->
            ok;
        false ->
            wait_stable(Node, Ensemble)
    end.

check_stable(Node, Ensemble) ->
    case rpc:call(Node, riak_ensemble_manager, check_quorum, [Ensemble, 1000]) of
        true ->
            case rpc:call(Node, riak_ensemble_peer, stable_views, [Ensemble, 1000]) of
                {ok, true} ->
                    true;
                _Other ->
                    false
            end;
        false ->
            false
    end.
