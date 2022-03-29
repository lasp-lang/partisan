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

-module(demers_anti_entropy).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

%% API
-export([start_link/0,
         stop/0,
         broadcast/2,
         update/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(FANOUT, 2).

-record(state, {next_id, membership}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

%% @doc Broadcast.
broadcast(ServerRef, Message) ->
    gen_server:cast(?MODULE, {broadcast, ServerRef, Message}).

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
    partisan_logger:info("Starting with membership: ~p", [Membership]),

    %% Schedule anti-entropy.
    schedule_anti_entropy(),

    {ok, #state{next_id=0, membership=membership(Membership)}}.

%% @private
handle_call(Msg, _From, State) ->
    partisan_logger:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
handle_cast({broadcast, ServerRef, Message}, #state{next_id=NextId}=State) ->
    %% Generate message id.
    MyNode = partisan:node(),
    Id = {MyNode, NextId},

    %% Forward to process.
    partisan_util:process_forward(ServerRef, Message),

    %% Store outgoing message.
    true = ets:insert(?MODULE, {Id, {ServerRef, Message}}),

    {noreply, State#state{next_id=NextId}};

handle_cast({update, Membership0}, State) ->
    Membership = membership(Membership0),
    {noreply, State#state{membership=Membership}};

handle_cast(Msg, State) ->
    partisan_logger:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
%% Incoming messages.
handle_info(antientropy, #state{membership=Membership}=State) ->
    MyNode = partisan:node(),

    %% Get all of our messages.
    OurMessages = ets:foldl(fun({Id, {ServerRef, Message}}, Acc) ->
        Acc ++ [{Id, {ServerRef, Message}}]
    end, [], ?MODULE),

    %% Forward to random subset of peers.
    AntiEntropyMembers = select_random_sublist(membership(Membership), ?FANOUT),

    lists:foreach(fun(N) ->
        partisan_pluggable_peer_service_manager:forward_message(N, undefined, ?MODULE, {push, MyNode, OurMessages}, [])
    end, AntiEntropyMembers -- [MyNode]),

    %% Reschedule.
    schedule_anti_entropy(),

    {noreply, State};

handle_info({push, FromNode, TheirMessages}, State) ->
    MyNode = partisan:node(),

    %% Encorporate their messages and process them if we didn't see them.
    lists:foreach(fun({Id, {ServerRef, Message}}) ->
        case ets:lookup(?MODULE, Id) of
            [] ->
                %% Forward to process.
                partisan_util:process_forward(ServerRef, Message),

                %% Store.
                true = ets:insert(?MODULE, {Id, {ServerRef, Message}}),

                ok;
            _ ->
                ok
        end
    end, TheirMessages),

    %% Get all of our messages.
    OurMessages = ets:foldl(fun({Id, {ServerRef, Message}}, Acc) ->
        Acc ++ [{Id, {ServerRef, Message}}]
    end, [], ?MODULE),

    %% Forward message back to sender.
    partisan_logger:info("~p: sending messages to node ~p", [node(), FromNode]),
    partisan_pluggable_peer_service_manager:forward_message(FromNode, undefined, ?MODULE, {pull, MyNode, OurMessages}, []),

    {noreply, State};

handle_info({pull, _FromNode, Messages}, State) ->
    %% Process all incoming.
    lists:foreach(fun({Id, {ServerRef, Message}}) ->
        case ets:lookup(?MODULE, Id) of
            [] ->
                %% Forward to process.
                partisan_util:process_forward(ServerRef, Message),

                %% Store.
                true = ets:insert(?MODULE, {Id, {ServerRef, Message}}),

                ok;
            _ ->
                ok
        end
    end, Messages),

    {noreply, State};

handle_info(_Msg, State) ->
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
schedule_anti_entropy() ->
    Interval = 2000,
    erlang:send_after(Interval, ?MODULE, antientropy).

%% @private
select_random_sublist(Membership, K) ->
    lists:sublist(shuffle(Membership), K).

%% @reference http://stackoverflow.com/questions/8817171/shuffling-elements-in-a-list-randomly-re-arrange-list-elements/8820501#8820501
shuffle(L) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), N} || N <- L])].