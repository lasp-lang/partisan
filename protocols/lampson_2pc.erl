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

%% NOTE: This protocol doesn't cover durability nor recovery. It's merely
%% here for demonstration purposes.
%% TODO: Handle abort message at participants.
%% TODO: Handle durability at participants.

-module(lampson_2pc).

-include("partisan.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

%% API
-export([start_link/0,
         broadcast/2,
         update/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {membership}).

-record(transaction, {from,
                      participants, 
                      status, 
                      prepared, 
                      committed, 
                      server_ref, 
                      message}).

-define(TIMEOUT, 2000).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Broadcast.
broadcast(ServerRef, Message) ->
    gen_server:call(?MODULE, {broadcast, ServerRef, Message}, infinity).

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

    %% Open ETS table to track received messages.
    ?MODULE = ets:new(?MODULE, [set, named_table, public]),

    %% Start with initial membership.
    {ok, Membership} = partisan_peer_service:members(),
    lager:info("Starting with membership: ~p", [Membership]),

    {ok, #state{membership=membership(Membership)}}.

%% @private
handle_call({broadcast, ServerRef, Message}, From, #state{membership=Membership}=State) ->
    Manager = manager(),

    %% Generate unique transaction id.
    MyNode = partisan_peer_service_manager:mynode(),
    Id = {MyNode, erlang:unique_integer([monotonic, positive])},

    %% Create transaction in a preparing state.
    Transaction = #transaction{
        from=From,
        participants=Membership, 
        status=preparing, 
        prepared=[], 
        committed=[], 
        server_ref=ServerRef, 
        message=Message
    },

    %% Store transaction.
    true = ets:insert(?MODULE, {Id, Transaction}),

    %% Set transaction timer.
    erlang:send_after(1000, self(), {timeout, Id}),

    %% Send prepare message to all participants including ourself.
    lists:foreach(fun(N) ->
        lager:info("~p: sending prepare message to node ~p: ~p", [node(), N, Message]),
        Manager:forward_message(N, ?GOSSIP_CHANNEL, ?MODULE, {prepare, MyNode, Id, ServerRef, Message}, [])
    end, membership(Membership)),

    {noreply, State};
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
handle_cast({update, Membership0}, State) ->
    Membership = membership(Membership0),
    {noreply, State#state{membership=Membership}};
handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
%% Incoming messages.
handle_info({timeout, Id}, State) ->
    Manager = manager(),

    %% Find transaction record.
    case ets:lookup(?MODULE, Id) of 
        [{_Id, #transaction{participants=Participants, from=From, server_ref=ServerRef, message=Message} = Transaction}] ->
            lager:info("Received timeout for transaction id ~p", [Id]),

            %% Reply to caller.
            lager:info("Aborting transaction: ~p", [Id]),
            gen_server:reply(From, error),

            %% Update local state.
            true = ets:insert(?MODULE, {Id, Transaction#transaction{status=aborting}}),

            %% Send notification to abort.
            MyNode = partisan_peer_service_manager:mynode(),
            lists:foreach(fun(N) ->
                lager:info("~p: sending abort message to node ~p: ~p", [node(), N, Id]),
                Manager:forward_message(N, ?GOSSIP_CHANNEL, ?MODULE, {abort, MyNode, Id, ServerRef, Message}, [])
            end, membership(Participants));
        [] ->
            lager:error("Notification for timeout message but no transaction found!")
    end,

    {noreply, State};
handle_info({ack, FromNode, Id}, State) ->
    %% Find transaction record.
    case ets:lookup(?MODULE, Id) of 
        [{_Id, #transaction{participants=Participants, committed=Committed0, from=From} = Transaction}] ->
            lager:info("Received ack from node ~p", [FromNode]),

            %% Update committed.
            Committed = Committed0 ++ [FromNode],

            %% Are we all committed?
            case lists:usort(Participants) =:= lists:usort(Committed) of 
                true ->
                    %% Reply to caller.
                    lager:info("replying to the caller: ~p", From),
                    gen_server:reply(From, ok),

                    %% Remove record from storage.
                    true = ets:delete(?MODULE, Id),

                    ok;
                false ->
                    lager:info("Not all participants have committed yet: ~p != ~p", [Committed, Participants]),

                    %% Update local state.
                    true = ets:insert(?MODULE, {Id, Transaction#transaction{committed=Committed}}),

                    ok
            end;
        [] ->
            lager:error("Notification for commit message but no transaction found!")
    end,

    {noreply, State};
handle_info({commit, FromNode, Id, ServerRef, Message}, State) ->
    Manager = manager(),

    %% Forward to process.
    partisan_util:process_forward(ServerRef, Message),

    %% TODO: Write log record showing commit occurred.

    %% Repond to coordinator that we are now committed.
    MyNode = partisan_peer_service_manager:mynode(),
    lager:info("~p: sending commit ack message to node ~p: ~p", [node(), FromNode, Id]),
    Manager:forward_message(FromNode, ?GOSSIP_CHANNEL, ?MODULE, {ack, MyNode, Id}, []),

    {noreply, State};
handle_info({prepared, FromNode, Id}, State) ->
    Manager = manager(),

    %% Find transaction record.
    case ets:lookup(?MODULE, Id) of 
        %% TODO: Eventually, don't send the payload once the participant has full persistence.
        [{_Id, #transaction{participants=Participants, prepared=Prepared0, server_ref=ServerRef, message=Message} = Transaction}] ->
            %% Update prepared.
            Prepared = Prepared0 ++ [FromNode],

            %% Are we all prepared?
            case lists:usort(Participants) =:= lists:usort(Prepared) of 
                true ->
                    %% Change state to committing.
                    Status = committing,

                    %% Update local state before sending decision to participants.
                    true = ets:insert(?MODULE, {Id, Transaction#transaction{status=Status, prepared=Prepared}}),

                    %% Send notification to commit.
                    MyNode = partisan_peer_service_manager:mynode(),
                    lists:foreach(fun(N) ->
                        lager:info("~p: sending commit message to node ~p: ~p", [node(), N, Id]),
                        Manager:forward_message(N, ?GOSSIP_CHANNEL, ?MODULE, {commit, MyNode, Id, ServerRef, Message}, [])
                    end, membership(Participants));
                false ->
                    %% Update local state before sending decision to participants.
                    true = ets:insert(?MODULE, {Id, Transaction#transaction{prepared=Prepared}})
            end;
        [] ->
            lager:error("Notification for prepared message but no transaction found!")
    end,

    {noreply, State};
handle_info({prepare, FromNode, Id, _ServerRef, _Message}, State) ->
    Manager = manager(),

    %% TODO: Durably store the message for recovery.

    %% Repond to coordinator that we are now prepared.
    MyNode = partisan_peer_service_manager:mynode(),
    lager:info("~p: sending prepared message to node ~p: ~p", [node(), FromNode, Id]),
    Manager:forward_message(FromNode, ?GOSSIP_CHANNEL, ?MODULE, {prepared, MyNode, Id}, []),

    {noreply, State};
handle_info(Msg, State) ->
    lager:info("~p received unhandled message: ~p", [node(), Msg]),
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

%% @private
manager() ->
    partisan_config:get(partisan_peer_service_manager).

%% @private -- sort to remove nondeterminism in node selection.
membership(Membership) ->
    lists:usort(Membership).