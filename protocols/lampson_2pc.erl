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

-module(lampson_2pc).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

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
    From = partisan_util:registered_name(txn_coordinator),

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
    partisan_logger:info("Starting with membership: ~p", [Membership]),

    %% Schedule heartbeat.
    schedule_heartbeat(),

    {ok, #state{next_id=0, membership=membership(Membership)}}.

%% @private
handle_call(Msg, _From, State) ->
    partisan_logger:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
handle_cast({broadcast, From, ServerRef, Message}, #state{next_id=NextId, membership=Membership}=State) ->
    %% Generate unique transaction id.
    MyNode = partisan_peer_service_manager:mynode(),
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
        partisan_logger:info("~p: sending prepare message to node ~p: ~p", [node(), N, Message]),
        partisan_pluggable_peer_service_manager:forward_message(N, undefined, ?MODULE, {prepare, Transaction}, [])
    end, membership(Membership)),

    {noreply, State#state{next_id=NextId + 1}};
handle_cast({update, Membership0}, State) ->
    Membership = membership(Membership0),
    {noreply, State#state{membership=Membership}};
handle_cast(Msg, State) ->
    partisan_logger:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
%% Incoming messages.
handle_info({heartbeat, FromNode}, State) ->
    partisan_logger:info("~p: received heartbeat at node: ~p from node: ~p", [node(), node(), FromNode]),
    {noreply, State};
handle_info(heartbeat, #state{membership=Membership}=State) ->
    MyNode = partisan_peer_service_manager:mynode(),

    %% Send heartbeat.
    lists:foreach(fun(N) ->
        partisan_logger:info("~p: sending heartbeat message to node ~p", [node(), N]),

        %% Catch shutdown race with sending heartbeat.
        try
            partisan_pluggable_peer_service_manager:forward_message(N, undefined, ?MODULE, {heartbeat, MyNode}, [])
        catch
            _:_ ->
                ok
        end
    end, membership(Membership)),

    %% Reschedule.
    schedule_heartbeat(),

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
                    partisan_logger:info("Received coordinator timeout for transaction id ~p", [Id]),

                    %% Update local state.
                    Transaction = Transaction0#transaction{coordinator_status=aborting},
                    true = ets:insert(?COORDINATING_TRANSACTIONS, {Id, Transaction}),

                    %% Reply to caller.
                    partisan_logger:info("Aborting transaction: ~p", [Id]),
                    partisan_pluggable_peer_service_manager:forward_message(From, error),

                    %% Send notification to abort.
                    lists:foreach(fun(N) ->
                        partisan_logger:info("~p: sending abort message to node ~p: ~p", [node(), N, Id]),
                        partisan_pluggable_peer_service_manager:forward_message(N, undefined, ?MODULE, {abort, Transaction}, [])
                    end, membership(Participants))
            end;
        [] ->
            partisan_logger:error("Notification for coordinator timeout message but no transaction found!", [])
    end,

    {noreply, State};
handle_info({abort_ack, FromNode, Id}, State) ->
    %% Find transaction record.
    case ets:lookup(?COORDINATING_TRANSACTIONS, Id) of 
        [{_Id, #transaction{participants=Participants, aborted=Aborted0} = Transaction}] ->
            partisan_logger:info("Received abort_ack from node ~p", [FromNode]),

            %% Update aborted.
            Aborted = lists:usort(Aborted0 ++ [FromNode]),

            %% Are we all committed?
            case lists:usort(Participants) =:= lists:usort(Aborted) of 
                true ->
                    %% Remove record from storage.
                    true = ets:delete(?COORDINATING_TRANSACTIONS, Id),

                    ok;
                false ->
                    partisan_logger:info("Not all participants have aborted yet: ~p != ~p", [Aborted, Participants]),

                    %% Update local state.
                    true = ets:insert(?COORDINATING_TRANSACTIONS, {Id, Transaction#transaction{aborted=Aborted}}),

                    ok
            end;
        [] ->
            partisan_logger:error("Notification for abort_ack message but no transaction found!", [])
    end,

    {noreply, State};
handle_info({commit_ack, FromNode, Id}, State) ->
    %% Find transaction record.
    case ets:lookup(?COORDINATING_TRANSACTIONS, Id) of 
        [{_Id, #transaction{participants=Participants, committed=Committed0} = Transaction}] ->
            partisan_logger:info("Received commit_ack from node ~p", [FromNode]),

            %% Update committed.
            Committed = lists:usort(Committed0 ++ [FromNode]),

            %% Are we all committed?
            case lists:usort(Participants) =:= lists:usort(Committed) of 
                true ->
                    %% Remove record from storage.
                    true = ets:delete(?COORDINATING_TRANSACTIONS, Id),

                    ok;
                false ->
                    partisan_logger:info("Not all participants have committed yet: ~p != ~p", [Committed, Participants]),

                    %% Update local state.
                    true = ets:insert(?COORDINATING_TRANSACTIONS, {Id, Transaction#transaction{committed=Committed}}),

                    ok
            end;
        [] ->
            partisan_logger:error("Notification for commit_ack message but no transaction found!", [])
    end,

    {noreply, State};
handle_info({abort, #transaction{id=Id, coordinator=Coordinator}}, State) ->
    true = ets:delete(?PARTICIPATING_TRANSACTIONS, Id),

    MyNode = partisan_peer_service_manager:mynode(),

    partisan_logger:info("~p: sending abort ack message to node ~p: ~p", [node(), Coordinator, Id]),
    partisan_pluggable_peer_service_manager:forward_message(Coordinator, undefined, ?MODULE, {abort_ack, MyNode, Id}, []),

    {noreply, State};
handle_info({commit, #transaction{id=Id, coordinator=Coordinator, server_ref=ServerRef, message=Message} = Transaction}, State) ->
    %% Write log record showing commit occurred.
    true = ets:insert(?PARTICIPATING_TRANSACTIONS, {Id, Transaction#transaction{participant_status=commit}}),

    %% Forward to process.
    partisan_util:process_forward(ServerRef, Message),

    %% Repond to coordinator that we are now committed.
    MyNode = partisan_peer_service_manager:mynode(),
    partisan_logger:info("~p: sending commit ack message to node ~p: ~p", [node(), Coordinator, Id]),
    partisan_pluggable_peer_service_manager:forward_message(Coordinator, undefined, ?MODULE, {commit_ack, MyNode, Id}, []),

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
                    partisan_logger:info("replying to the caller: ~p", [From]),
                    partisan_pluggable_peer_service_manager:forward_message(From, ok),

                    %% Send notification to commit.
                    lists:foreach(fun(N) ->
                        partisan_logger:info("~p: sending commit message to node ~p: ~p", [node(), N, Id]),
                        partisan_pluggable_peer_service_manager:forward_message(N, undefined, ?MODULE, {commit, Transaction}, [])
                    end, membership(Participants));
                false ->
                    %% Update local state before sending decision to participants.
                    true = ets:insert(?COORDINATING_TRANSACTIONS, {Id, Transaction0#transaction{prepared=Prepared}})
            end;
        [] ->
            partisan_logger:error("Notification for prepared message but no transaction found!",  [])
    end,

    {noreply, State};
handle_info({prepare, #transaction{coordinator=Coordinator, id=Id}=Transaction}, State) ->
    %% Durably store the message for recovery.
    true = ets:insert(?PARTICIPATING_TRANSACTIONS, {Id, Transaction#transaction{participant_status=prepared}}),

    %% Respond to coordinator that we are now prepared.
    MyNode = partisan_peer_service_manager:mynode(),
    partisan_logger:info("~p: sending prepared message to node ~p: ~p", [node(), Coordinator, Id]),
    partisan_pluggable_peer_service_manager:forward_message(Coordinator, undefined, ?MODULE, {prepared, MyNode, Id}, []),

    {noreply, State};
handle_info(Msg, State) ->
    partisan_logger:info("~p received unhandled message: ~p", [node(), Msg]),
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

%% @private
schedule_heartbeat() ->
    case os:getenv("NOISE") of 
        "true" ->
            erlang:send_after(10, self(), heartbeat);
        _ ->
            ok
    end.