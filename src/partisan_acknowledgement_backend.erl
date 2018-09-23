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

-module(partisan_acknowledgement_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         store/2,
         ack/1,
         outstanding/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {storage}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

store(MessageClock, Message) ->
    gen_server:call(?MODULE, {store, MessageClock, Message}, infinity).

ack(MessageClock) ->
    gen_server:call(?MODULE, {ack, MessageClock}, infinity).

outstanding() ->
    gen_server:call(?MODULE, outstanding, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    Storage = ets:new(?MODULE, [named_table]),
    {ok, #state{storage=Storage}}.

%% @private
handle_call({ack, MessageClock}, _From, #state{storage=Storage}=State) ->
    true = ets:delete(Storage, MessageClock),
    {reply, ok, State};
handle_call({store, MessageClock, Message}, _From, #state{storage=Storage}=State) ->
    true = ets:insert(Storage, {MessageClock, Message}),
    {reply, ok, State};
handle_call(outstanding, _From, #state{storage=Storage}=State) ->
    Objects = ets:foldl(fun(X, Acc) -> Acc ++ [X] end, [], Storage),
    {reply, {ok, Objects}, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
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