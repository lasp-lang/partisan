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

%% NOTE: This protocol doesn't cover recovery. It's merely here for
%% demonstration purposes.

-module(bernstein_ctp).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").
-include("partisan_logger.hrl").

%% API
-export([start_link/0,
         broadcast/2,
         update/1,
         stop/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {next_id, membership}).

-record(transaction, {id,
                      coordinator,
                      from,
                      participants,
                      coordinator_status,
                      participant_status,
                      prepared,
                      committed,
                      aborted,
                      uncertain,
                      server_ref,
                      message}).

-define(COORDINATING_TRANSACTIONS, coordinating_transactions_table).

-define(PARTICIPATING_TRANSACTIONS, participating_transactions_table).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE, normal, infinity).

%% @doc Broadcast.
%% Avoid using call by sending a message and waiting for a response.
broadcast(ServerRef, Message) ->
    %% TODO: Bit of a hack just to get this working.
    true = erlang:register(txn_coordinator, self()),
    From = partisan_remote_ref:from_term(txn_coordinator),

    gen_server:cast(?MODULE, {broadcast, From, ServerRef, Message}),

    receive
        Response ->
            Response
    end.

%% @doc Membership update.
update(LocalState0) ->
    LocalState = partisan_peer_service:decode(LocalState0),
    gen_server:cast(?MODULE, {update, LocalState}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    %% Register membership update callback.
    partisan_peer_service:add_sup_callback(fun ?MODULE:update/1),

    %% Open ETS table to track coordinated transactions.
    ?COORDINATING_TRANSACTIONS = ets:new(?COORDINATING_TRANSACTIONS, [set, named_table, public]),

    %% Open ETS table to track participating transactions.
    ?PARTICIPATING_TRANSACTIONS = ets:new(?PARTICIPATING_TRANSACTIONS, [set, named_table, public]),

    %% Start with initial membership.
    {ok, Membership} = partisan_peer_service:members(),
    ?LOG_INFO("Starting with membership: ~p", [Membership]),

    {ok, #state{next_id=0, membership=membership(Membership)}}.

%% @private
handle_call(Msg, _From, State) ->
    ?LOG_WARNING("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
handle_cast({broadcast, From, ServerRef, Message}, #state{next_id=NextId, membership=Membership}=State) ->
    %% Generate unique transaction id.
    MyNode = partisan:node(),
    Id = {MyNode, NextId},

    %% Create transaction in a preparing state.
    Transaction = #transaction{
        id=Id,
        coordinator=MyNode,
        from=From,
        participants=Membership,
        coordinator_status=preparing,
        participant_status=undefined,
        prepared=[],
        committed=[],
        aborted=[],
        uncertain=[],
        server_ref=ServerRef,
        message=Message
    },

    %% Store transaction.
    true = ets:insert(?COORDINATING_TRANSACTIONS, {Id, Transaction}),

    %% Set transaction timer.
    erlang:send_after(1000, self(), {coordinator_timeout, Id}),

    %% Send prepare message to all participants including ourself.
    lists:foreach(fun(N) ->
        ?LOG_INFO("~p: sending prepare message to node ~p: ~p", [node(), N, Message]),
        partisan:forward_message(
            N, ?MODULE, {prepare, Transaction}, #{channel => ?DEFAULT_CHANNEL}
        )
    end, membership(Membership)),

    {noreply, State#state{next_id=NextId + 1}};
handle_cast({update, Membership0}, State) ->
    Membership = membership(Membership0),
    {noreply, State#state{membership=Membership}};
handle_cast(Msg, State) ->
    ?LOG_WARNING("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
%% Incoming messages.
handle_info({decision, FromNode, Id, Decision}, State) ->
    MyNode = partisan:node(),

    %% Find transaction record.
    case ets:lookup(?PARTICIPATING_TRANSACTIONS, Id) of
        [{_Id, #transaction{participant_status=ParticipantStatus, uncertain=Uncertain0, server_ref=ServerRef, message=Message}=Transaction}] ->
            case ParticipantStatus of
                abort ->
                    ?LOG_ERROR("~p: decision already reached: ~p, ignoring decision.", [node(), ParticipantStatus]);
                commit ->
                    ?LOG_ERROR("~p: decision already reached: ~p, ignoring decision.", [node(), ParticipantStatus]);
                _ ->
                    case Decision of
                        abort ->
                            ?LOG_INFO("~p: decision was abort.", [node()]),

                            %% Write log record showing abort occurred.
                            true = ets:insert(?PARTICIPATING_TRANSACTIONS, {Id, Transaction#transaction{participant_status=abort, uncertain=[]}}),

                            %% Notify uncertain.
                            lists:foreach(fun(N) ->
                                ?LOG_INFO("~p: sending decision message to uncertain node ~p: ~p", [node(), N, Decision]),
                                partisan:forward_message(
                                    N,
                                    ?MODULE,
                                    {decision, MyNode, Id, Decision},
                                    #{channel => ?DEFAULT_CHANNEL}
                                )
                            end, Uncertain0),

                            ok;
                        uncertain ->
                            ?LOG_INFO("~p: decision was uncertain.", [node()]),

                            %% Don't know, do nothing, possibly block.

                            %% Keep track of who is uncertain.
                            Uncertain = lists:usort(Uncertain0 ++ [FromNode]),
                            true = ets:insert(?PARTICIPATING_TRANSACTIONS, {Id, Transaction#transaction{uncertain=Uncertain}}),

                            ok;
                        commit ->
                            ?LOG_INFO("~p: decision was commit.", [node()]),

                            %% Write log record showing commit occurred.
                            true = ets:insert(?PARTICIPATING_TRANSACTIONS, {Id, Transaction#transaction{participant_status=commit, uncertain=[]}}),

                            %% Notify uncertain.
                            lists:foreach(fun(N) ->
                                ?LOG_INFO("~p: sending decision message to uncertain node ~p: ~p", [node(), N, Decision]),
                                partisan:forward_message(
                                    N,
                                    ?MODULE,
                                    {decision, MyNode, Id, Decision},
                                    #{channel => ?DEFAULT_CHANNEL}
                                )
                            end, Uncertain0),

                            %% Forward to process.
                            partisan_peer_service_manager:process_forward(ServerRef, Message),

                            ok
                    end
            end;
        [] ->
            ?LOG_ERROR("Notification for decision message but no transaction found!", [])
    end,

    {noreply, State};
handle_info({decision_request, FromNode, Id}, State) ->
    MyNode = partisan:node(),

    %% Find transaction record.
    case ets:lookup(?PARTICIPATING_TRANSACTIONS, Id) of
        [{_Id, #transaction{participant_status=ParticipantStatus}}] ->
            Decision = case ParticipantStatus of
                abort ->
                    %% We aborted.
                    abort;
                undefined ->
                    %% We haven't voted.
                    abort;
                commit ->
                    %% We committed.
                    commit;
                _ ->
                    uncertain
            end,

            ?LOG_INFO("~p: sending decision-request message to node ~p: ~p", [node(), FromNode, Decision]),
            partisan:forward_message(
                FromNode,
                ?MODULE,
                {decision, MyNode, Id, Decision},
                #{channel => ?DEFAULT_CHANNEL}
            );
        [] ->
            ?LOG_ERROR("Notification for decision-request message but no transaction found!", []),

            Decision = uncertain,

            ?LOG_INFO("~p: sending decision-request message to node ~p: ~p", [node(), FromNode, Decision]),
            partisan:forward_message(
                FromNode,
                ?MODULE,
                {decision, MyNode, Id, Decision},
                #{channel => ?DEFAULT_CHANNEL}
            )
    end,

    {noreply, State};
handle_info({participant_timeout, Id}, State) ->
    MyNode = partisan:node(),

    %% Find transaction record.
    case ets:lookup(?PARTICIPATING_TRANSACTIONS, Id) of
        [{_Id, #transaction{participant_status=ParticipantStatus, participants=Participants}}] ->
            case ParticipantStatus of
                abort ->
                    ok;
                commit ->
                    ok;
                _Other ->
                    %% Send decision request to all participants.
                    lists:foreach(fun(N) ->
                        ?LOG_INFO("~p: decision locally is ~p; sending decision-request message to node ~p: ~p", [node(), ParticipantStatus, N, Id]),
                        partisan:forward_message(
                            N,
                            ?MODULE,
                            {decision_request, MyNode, Id},
                            #{channel => ?DEFAULT_CHANNEL}
                        )
                    end, membership(Participants)),

                    ok
            end;
        [] ->
            ?LOG_ERROR("Notification for participant timeout message but no transaction found!", [])
    end,

    {noreply, State};
handle_info({coordinator_timeout, Id}, State) ->
    %% Find transaction record.
    case ets:lookup(?COORDINATING_TRANSACTIONS, Id) of
        [{_Id, #transaction{coordinator_status=CoordinatorStatus, participants=Participants, from=From} = Transaction0}] ->
            case CoordinatorStatus of
                committing ->
                    %% Can't do anything; block.
                    ok;
                aborting ->
                    %% Can't do anything; block.
                    ok;
                preparing ->
                    ?LOG_INFO("Received coordinator timeout for transaction id ~p", [Id]),

                    %% Update local state.
                    Transaction = Transaction0#transaction{coordinator_status=aborting},
                    true = ets:insert(?COORDINATING_TRANSACTIONS, {Id, Transaction}),

                    %% Reply to caller.
                    ?LOG_INFO("Aborting transaction: ~p", [Id]),
                    partisan:forward_message(From, error),

                    %% Send notification to abort.
                    lists:foreach(fun(N) ->
                        ?LOG_INFO("~p: sending abort message to node ~p: ~p", [node(), N, Id]),
                        partisan:forward_message(
                            N,
                            ?MODULE,
                            {abort, Transaction},
                            #{channel => ?DEFAULT_CHANNEL}
                        )
                    end, membership(Participants))
            end;
        [] ->
            ?LOG_ERROR("Notification for coordinator timeout message but no transaction found!", [])
    end,

    {noreply, State};
handle_info({abort_ack, FromNode, Id}, State) ->
    %% Find transaction record.
    case ets:lookup(?COORDINATING_TRANSACTIONS, Id) of
        [{_Id, #transaction{participants=Participants, aborted=Aborted0} = Transaction}] ->
            ?LOG_INFO("Received abort_ack from node ~p", [FromNode]),

            %% Update aborted.
            Aborted = lists:usort(Aborted0 ++ [FromNode]),

            %% Are we all committed?
            case lists:usort(Participants) =:= lists:usort(Aborted) of
                true ->
                    %% Remove record from storage.
                    true = ets:delete(?COORDINATING_TRANSACTIONS, Id),

                    ok;
                false ->
                    ?LOG_INFO("Not all participants have aborted yet: ~p != ~p", [Aborted, Participants]),

                    %% Update local state.
                    true = ets:insert(?COORDINATING_TRANSACTIONS, {Id, Transaction#transaction{aborted=Aborted}}),

                    ok
            end;
        [] ->
            ?LOG_ERROR("Notification for abort_ack message but no transaction found!", [])
    end,

    {noreply, State};
handle_info({commit_ack, FromNode, Id}, State) ->
    %% Find transaction record.
    case ets:lookup(?COORDINATING_TRANSACTIONS, Id) of
        [{_Id, #transaction{participants=Participants, committed=Committed0} = Transaction}] ->
            ?LOG_INFO("Received commit_ack from node ~p", [FromNode]),

            %% Update committed.
            Committed = lists:usort(Committed0 ++ [FromNode]),

            %% Are we all committed?
            case lists:usort(Participants) =:= lists:usort(Committed) of
                true ->
                    %% Remove record from storage.
                    true = ets:delete(?COORDINATING_TRANSACTIONS, Id),

                    ok;
                false ->
                    ?LOG_INFO("Not all participants have committed yet: ~p != ~p", [Committed, Participants]),

                    %% Update local state.
                    true = ets:insert(?COORDINATING_TRANSACTIONS, {Id, Transaction#transaction{committed=Committed}}),

                    ok
            end;
        [] ->
            ?LOG_ERROR("Notification for commit_ack message but no transaction found!", [])
    end,

    {noreply, State};
handle_info({abort, #transaction{id=Id, coordinator=Coordinator}}, State) ->
    true = ets:delete(?PARTICIPATING_TRANSACTIONS, Id),

    MyNode = partisan:node(),
    ?LOG_INFO("~p: sending abort ack message to node ~p: ~p", [node(), Coordinator, Id]),
    partisan:forward_message(
        Coordinator,
        ?MODULE,
        {abort_ack, MyNode, Id},
        #{channel => ?DEFAULT_CHANNEL}
    ),

    {noreply, State};
handle_info({commit, #transaction{id=Id, coordinator=Coordinator, server_ref=ServerRef, message=Message} = Transaction}, State) ->
    %% Write log record showing commit occurred.
    true = ets:insert(?PARTICIPATING_TRANSACTIONS, {Id, Transaction#transaction{participant_status=commit}}),

    %% Forward to process.
    partisan_peer_service_manager:process_forward(ServerRef, Message),

    %% Repond to coordinator that we are now committed.
    MyNode = partisan:node(),
    ?LOG_INFO("~p: sending commit ack message to node ~p: ~p", [node(), Coordinator, Id]),
    partisan:forward_message(
        Coordinator,
        ?MODULE,
        {commit_ack, MyNode, Id},
        #{channel => ?DEFAULT_CHANNEL}
    ),

    {noreply, State};
handle_info({prepared, FromNode, Id}, State) ->
    %% Find transaction record.
    case ets:lookup(?COORDINATING_TRANSACTIONS, Id) of
        [{_Id, #transaction{participants=Participants, prepared=Prepared0, from=From} = Transaction0}] ->
            %% Update prepared.
            Prepared = lists:usort(Prepared0 ++ [FromNode]),

            %% Are we all prepared?
            case lists:usort(Participants) =:= lists:usort(Prepared) of
                true ->
                    %% Change state to committing.
                    CoordinatorStatus = committing,

                    %% Update local state before sending decision to participants.
                    Transaction = Transaction0#transaction{coordinator_status=CoordinatorStatus, prepared=Prepared},
                    true = ets:insert(?COORDINATING_TRANSACTIONS, {Id, Transaction}),

                    %% Reply to caller.
                    ?LOG_INFO("replying to the caller: ~p", [From]),
                    partisan:forward_message(From, ok),

                    %% Send notification to commit.
                    lists:foreach(fun(N) ->
                        ?LOG_INFO("~p: sending commit message to node ~p: ~p", [node(), N, Id]),
                        partisan:forward_message(
                            N,
                            ?MODULE,
                            {commit, Transaction},
                            #{channel => ?DEFAULT_CHANNEL}
                        )
                    end, membership(Participants));
                false ->
                    %% Update local state before sending decision to participants.
                    true = ets:insert(?COORDINATING_TRANSACTIONS, {Id, Transaction0#transaction{prepared=Prepared}})
            end;
        [] ->
            ?LOG_ERROR("Notification for prepared message but no transaction found!", [])
    end,

    {noreply, State};
handle_info({prepare, #transaction{coordinator=Coordinator, id=Id}=Transaction}, State) ->
    %% Durably store the message for recovery.
    true = ets:insert(?PARTICIPATING_TRANSACTIONS, {Id, Transaction#transaction{participant_status=prepared}}),

    %% Set a timeout to hear about a decision.
    erlang:send_after(2000, self(), {participant_timeout, Id}),

    %% Repond to coordinator that we are now prepared.
    MyNode = partisan:node(),
    ?LOG_INFO("~p: sending prepared message to node ~p: ~p", [node(), Coordinator, Id]),
    partisan:forward_message(
        Coordinator,
        ?MODULE,
        {prepared, MyNode, Id},
        #{channel => ?DEFAULT_CHANNEL}
    ),

    {noreply, State};
handle_info(Msg, State) ->
    ?LOG_INFO("~p received unhandled message: ~p", [node(), Msg]),
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private -- sort to remove nondeterminism in node selection.
membership(Membership) ->
    lists:usort(Membership).