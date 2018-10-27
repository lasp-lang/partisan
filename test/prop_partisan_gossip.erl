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
    ?LET(Id, erlang:unique_integer([positive, monotonic]), {Id, integer()}).

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
node_postcondition(#state{sent=Sent}, {call, ?MODULE, check_mailbox, [_Node]}, {ok, Messages}) ->
    ct:pal("Verifying mailbox: Sent: ~p, Received: ~p", [Sent, Messages]),
    lists:all(fun(M) -> lists:member(M, Messages) end, Sent);
node_postcondition(_State, _Command, _Response) ->
    false.

%% Next state.
node_next_state(#state{sent=Sent0}=State, _Result, {call, ?MODULE, gossip, [_Node, Message]}) ->
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
gossip(Node, Message) ->
    ok = rpc:call(?NAME(Node), partisan_gossip, gossip, [?GOSSIP_RECEIVER, Message]),
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
            ct:pal("Didn't receive response!", []),
            error
    end.

%% @private
spawn_gossip_receiver(Node) ->
    node_debug("Spwaning gossip receiver on node ~p", [Node]),

    Self = self(),

    RemoteFun = fun() ->
        %% Create ETS table for the results.
        ?GOSSIP_TABLE = ets:new(?GOSSIP_TABLE, [set, named_table, public]),
        lager:info("Gossip table opened."),

        %% Define loop function for receiving and registering values.
        ReceiverFun = fun(F) ->
            receive
                {received, Sender} ->
                    lager:info("Received request for stored values..."),
                    Received = ets:foldl(fun(Term, Acc) -> Acc ++ [Term] end, [], ?GOSSIP_TABLE),
                    lager:info("Received request for stored values: ~p", [Received]),
                    Sender ! Received;
                {Id, Value} ->
                    lager:info("Received id ~p and value: ~p", [Id, Value]),
                    true = ets:insert(?GOSSIP_TABLE, {Id, Value})
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
            lager:info(Line, Args);
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