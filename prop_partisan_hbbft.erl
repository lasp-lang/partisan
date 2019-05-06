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

-module(prop_partisan_hbbft).

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

%%%===================================================================
%%% Node Functions
%%%===================================================================

-record(node_state, {}).

%% What node-specific operations should be called.
node_commands() ->
    [
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
node_precondition(_NodeState, {call, ?MODULE, sleep, []}) ->
    true;
node_precondition(_NodeState, _Command) ->
    false.

%% Next state.
node_next_state(_State, NodeState, _Response, _Command) ->
    NodeState.

%% Postconditions for node commands.
node_postcondition(_NodeState, {call, ?MODULE, sleep, []}, _Result) ->
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

    %% Enable pid encoding.
    lists:foreach(fun({ShortName, _}) ->
        % node_debug("enabling pid_encoding at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [pid_encoding, true])
    end, Nodes),

    %% Enable register_pid_for_encoding.
    lists:foreach(fun({ShortName, _}) ->
        % node_debug("enabling register_pid_for_encoding at node ~p", [ShortName]),
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [register_pid_for_encoding, true])
    end, Nodes),

    %% Load, configure, and start hbbft.
    lists:foreach(fun({ShortName, _}) ->
        node_debug("starting hbbft at node ~p", [ShortName]),
        case rpc:call(?NAME(ShortName), application, load, [hbbft]) of 
            ok ->
                ok;
            {error, {already_loaded, hbbft}} ->
                ok;
            Other ->
                exit({error, {load_failed, Other}})
        end,

        node_debug("starting hbbft at node ~p", [ShortName]),
        {ok, _} = rpc:call(?NAME(ShortName), application, ensure_all_started, [hbbft])
    end, Nodes),

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    %% Start hbbft test
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 

    %% Get the first node.
    {FirstName, _} = hd(Nodes),

    %% Master starts the dealer.
    N = length(Nodes),
    F = (N div 3),
    BatchSize = 20,
    {ok, Dealer} = rpc:call(?NAME(Firstname), dealer, new, [N, F+1, 'SS512']),
    {ok, {PubKey, PrivateKeys}} = rpc:call(?NAME(FirstName), dealer, deal, [Dealer]),

    %% each node gets a secret key
    NodesSKs = lists:zip(Nodes, PrivateKeys),

    %% load hbbft_worker on each node
    {Mod, Bin, _} = code:get_object_code(hbbft_worker),
    _ = hbbft_ct_utils:pmap(fun(Node) -> rpc:call(Node, erlang, load_module, [Mod, Bin]) end, Nodes),

%     %% start a hbbft_worker on each node
%     Workers = [{Node, rpc:call(Node, hbbft_worker, start_link, [N, F, I, tpke_privkey:serialize(SK), BatchSize, false])} || {I, {Node, SK}} <- enumerate(NodesSKs)],
%     ok = global:sync(),

%     [ link(W) || {_, {ok, W}} <- Workers ],

%     %% bunch of msgs
%     Msgs = [ crypto:strong_rand_bytes(128) || _ <- lists:seq(1, N*20)],

%     %% feed the nodes some msgs
%     lists:foreach(fun(Msg) ->
%                           Destinations = random_n(rand:uniform(N), Workers),
%                           ct:pal("destinations ~p~n", [Destinations]),
%                           [hbbft_worker:submit_transaction(Msg, Destination) || {_Node, {ok, Destination}} <- Destinations]
%                   end, Msgs),

%     %% wait for all the worker's mailboxes to settle and.
%     %% wait for the chains to converge
%     ok = hbbft_ct_utils:wait_until(fun() ->
%                                            Chains = sets:from_list(lists:map(fun({_Node, {ok, W}}) ->
%                                                                                      {ok, Blocks} = hbbft_worker:get_blocks(W),
%                                                                                      Blocks
%                                                                              end, Workers)),

%                                            0 == lists:sum([element(2, rpc:call(Node, erlang, process_info, [W, message_queue_len])) || {Node, {ok, W}} <- Workers ]) andalso
%                                            1 == sets:size(Chains) andalso
%                                            0 /= length(hd(sets:to_list(Chains)))
%                                    end, 60*2, 500),


%     Chains = sets:from_list(lists:map(fun({_Node, {ok, Worker}}) ->
%                                               {ok, Blocks} = hbbft_worker:get_blocks(Worker),
%                                               Blocks
%                                       end, Workers)),
%     ct:pal("~p distinct chains~n", [sets:size(Chains)]),
%     %true = (2 > sets:size(Chains)),
%     %true = (2 < length(hd(sets:to_list(Chains)))),

%     lists:foreach(fun(Chain) ->
%                           %ct:pal("Chain: ~p~n", [Chain]),
%                           ct:pal("chain is of height ~p~n", [length(Chain)]),

%                           %% verify they are cryptographically linked,
%                           true = hbbft_worker:verify_chain(Chain, PubKey),

%                           %% check all transactions are unique
%                           BlockTxns = lists:flatten([ hbbft_worker:block_transactions(B) || B <- Chain ]),
%                           true = length(BlockTxns) == sets:size(sets:from_list(BlockTxns)),

%                           %% check they're all members of the original message list
%                           true = sets:is_subset(sets:from_list(BlockTxns), sets:from_list(Msgs)),
%                           ct:pal("chain contains ~p distinct transactions~n", [length(BlockTxns)])
%                   end, sets:to_list(Chains)),

%     %% check we actually converged and made a chain

%     true = (1 == sets:size(Chains)),
%     true = (0 < length(hd(sets:to_list(Chains)))),

    %% Start the dealer.


    %% Sleep.
    node_debug("sleeping for convergence", []),
    timer:sleep(1000),
    node_debug("done.", []),

    ok.

%% @private
node_crash(Node) ->
    %% Stop hbbft.
    % node_debug("stopping hbbft on node ~p", [Node]),
    ok = rpc:call(?NAME(Node), application, stop, [hbbft]),

    ok.

%% @private
node_end_case() ->
    node_debug("ending case", []),

    %% Get nodes.
    [{nodes, Nodes}] = ets:lookup(prop_partisan, nodes),

    %% Stop hbbft.
    lists:foreach(fun({ShortName, _}) ->
        % node_debug("stopping hbbft on node ~p", [ShortName]),
        case rpc:call(?NAME(ShortName), application, stop, [hbbft]) of 
            ok ->
                ok;
            {badrpc, nodedown} ->
                ok;
            {error, {not_started, hbbft}} ->
                ok;
            Error ->
                node_debug("cannot terminate hbbft: ~p", [Error]),
                exit({error, shutdown_failed})
        end
    end, Nodes),

    ok.