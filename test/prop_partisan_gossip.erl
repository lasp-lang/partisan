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

-module(prop_partisan_gossip).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include_lib("proper/include/proper.hrl").

-compile([export_all]).

-define(NUM_NODES, 3).

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
    lists:map(NameFun, lists:seq(1, ?NUM_NODES)).

%%%===================================================================
%%% Node Functions
%%%===================================================================

-record(state, {receivers, sent}).

%% What node-specific operations should be called.
node_commands() ->
    [{call, ?MODULE, spawn_gossip_receiver, [node_name()]},
     {call, ?MODULE, check_mailbox, [node_name()]},
     {call, ?MODULE, gossip, [node_name(), message()]}].

%% What should the initial node state be.
node_initial_state() ->
    node_debug("initializing", []),
    Receivers = dict:new(),
    Sent = [],
    #state{receivers=Receivers, sent=Sent}.

%% Names of the node functions so we kow when we can dispatch to the node
%% pre- and postconditions.
node_functions() ->
    lists:map(fun({call, _Mod, Fun, _Args}) -> Fun end, node_commands()).

%% Postconditions for node commands.
node_postcondition(_State, {call, ?MODULE, spawn_gossip_receiver, [_Node]}, {error, _}) ->
    false;
node_postcondition(_State, {call, ?MODULE, spawn_gossip_receiver, [_Node]}, {ok, _}) ->
    true;
node_postcondition(_State, {call, ?MODULE, gossip, [_Node, _Message]}, _Result) ->
    true;
node_postcondition(#state{sent=Sent}, {call, ?MODULE, check_mailbox, [Node]}, {ok, Messages}) ->
    node_debug("verifying mailbox at node ~p: sent: ~p, received: ~p", [Node, Sent, Messages]),

    %% Intuition. 
    %%
    %% We should receive messages roughly in the order they are sent, but that's not necessarily true.
    %% We should receive messages from each origin in roughly the order they are sent, assuming they take the same path, but that also might not be true.
    %%

    %% Filter out messages that originated from us.
    FilteredSent = lists:filter(fun({{SourceNode, _Id}, _Value}) ->
        case SourceNode of
            Node ->
                false;
            _ ->
                true
            end
        end, Sent),

    %% Figure out which messages we have.
    Results = lists:map(fun(M) -> lists:member(M, Messages) end, FilteredSent),

    %% Ensure we have a prefix of messages.
    Result = lists:foldl(fun(X, {_SeenPrefix, SeenFirstOmission}) ->
        case SeenFirstOmission of
            false ->
                %% If we haven't seen an omission yet.
                case X of
                    true ->
                        {true, false};
                    false ->
                        %% This is our first omission, we've seen a prefix, but we've also started seeing omissions.
                        {true, true}
                end;
            true ->
                case X of
                    true ->
                        %% We've seen omissions and now we have a value, mark seen prefix as false.
                        {false, true};
                    false ->
                        %% Seen prefix, started seeing omissions.
                        {true, true}
                end
            end
        end, {true, false}, Results),

    case Result of 
        {true, _} ->
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
node_next_state(#state{sent=Sent0}=State, _Result, {call, ?MODULE, gossip, [Node, {Id, Value}]}) ->
    Message = {{Node, Id}, Value},
    Sent = Sent0 ++ [Message],
    State#state{sent=Sent};
node_next_state(#state{receivers=Receivers0}=State, Result, {call, ?MODULE, spawn_gossip_receiver, [Node]}) ->
    Receivers = dict:store(Node, Result, Receivers0),
    State#state{receivers=Receivers};
node_next_state(State, _Response, _Command) ->
    State.

%% Precondition.
node_precondition(#state{receivers=Receivers}, {call, ?MODULE, spawn_gossip_receiver, [Node]}) ->
    case dict:find(Node, Receivers) of
        {ok, _} ->
            false;
        error ->
            true
    end;
node_precondition(#state{receivers=Receivers}, {call, ?MODULE, gossip, [_Node, _Message]}) ->
    length(dict:to_list(Receivers)) == length(names());
node_precondition(#state{receivers=Receivers}, {call, ?MODULE, check_mailbox, [_Node]}) ->
    length(dict:to_list(Receivers)) == length(names());
node_precondition(_State, _Command) ->
    false.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

-define(GOSSIP_TABLE, gossip_table).
-define(GOSSIP_RECEIVER, gossip_receiver).

-define(NODE_DEBUG, true).

-define(ETS, prop_partisan).
-define(NAME, fun(Name) -> [{_, NodeName}] = ets:lookup(?ETS, Name), NodeName end).

%% @private
gossip(Node, {Id, Value}) ->
    FullMessage = {{Node, Id}, Value},
    node_debug("gossiping from node ~p message: ~p", [Node, FullMessage]),
    ok = rpc:call(?NAME(Node), partisan_gossip, gossip, [?GOSSIP_RECEIVER, FullMessage]),
    ok.

%% @private
check_mailbox(Node) ->
    Self = self(),

    %% Ask for what messages they have received.
    erlang:send({?GOSSIP_RECEIVER, ?NAME(Node)}, {received, Self}),

    receive
        Messages ->
            {ok, Messages}
    after 
        10000 ->
            {error, no_response_from_mailbox}
    end.

%% @private
spawn_gossip_receiver(Node) ->
    node_debug("spawning gossip receiver on node ~p", [Node]),

    Self = self(),

    RemoteFun = fun() ->
        %% Create ETS table for the results.
        ?GOSSIP_TABLE = ets:new(?GOSSIP_TABLE, [set, named_table, public]),

        %% Define loop function for receiving and registering values.
        ReceiverFun = fun(F) ->
            receive
                {received, Sender} ->
                    Received = ets:foldl(fun(Term, Acc) -> Acc ++ [Term] end, [], ?GOSSIP_TABLE),
                    node_debug("node ~p received request for stored values: ~p", [Node, Received]),
                    Sender ! Received;
                {{SourceNode, Id}, Value} ->
                    node_debug("node ~p received origin: ~p id ~p and value: ~p", [Node, SourceNode, Id, Value]),
                    true = ets:insert(?GOSSIP_TABLE, {{SourceNode, Id}, Value})
            end,
            F(F)
        end,

        %% Spawn locally.
        Pid = erlang:spawn(fun() -> ReceiverFun(ReceiverFun) end),

        %% Register name.
        erlang:register(?GOSSIP_RECEIVER, Pid),

        %% Prevent races by notifying process is registered.
        Self ! ready,

        %% Block indefinitely so the table doesn't close.
        loop()
    end,
    
    Pid = rpc:call(?NAME(Node), erlang, spawn, [RemoteFun]),

    receive
        ready ->
            {ok, Pid}
    after 
        10000 ->
            error
    end.

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