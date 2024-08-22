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

-module(partisan_test_server).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(partisan_gen_server).

-include("partisan_logger.hrl").


%% API
-export([start_link/0]).
-export([call/0]).
-export([call/1]).
-export([cast/1]).
-export([cast/2]).
-export([crash/0]).
-export([delayed_reply_call/0]).
-export([delayed_reply_call/1]).
-export([is_alive/1]).
-export([reply_crash/0]).

%% partisan_gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

call() ->
    call(?MODULE).

call(ServerRef) ->
    partisan_gen_server:call(ServerRef, call, infinity).

delayed_reply_call() ->
    delayed_reply_call(?MODULE).

delayed_reply_call(ServerRef) ->
    partisan_gen_server:call(ServerRef, delay_reply_call, infinity).

cast(ReplyTo) ->
    cast(?MODULE, ReplyTo).

cast(ServerRef, ReplyTo) ->
    partisan_gen_server:cast(ServerRef, {cast, ReplyTo}).


is_alive(Pid) ->
    Pid == whereis(?MODULE)
        orelse error({badarg, pid_to_list(Pid), whereis(?MODULE)}),
    erlang:is_process_alive(Pid).

crash() ->
    partisan_gen_server:cast(?MODULE, crash).

reply_crash() ->
    partisan_gen_server:cast(?MODULE, reply_crash).





%%%===================================================================
%%% partisan_gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    catch erlang:register(?MODULE, self()),
    ?LOG_INFO("Initialised server on node ~p.", [partisan:node()]),
    {ok, #state{}}.

%% @private
handle_call(delayed_reply_call, From, State) ->
    ?LOG_INFO("Received delayed_reply_call message from ~p in the handle_call handler.", [From]),
    partisan_gen_server:reply(From, ok),
    {noreply, State};
handle_call(call, From, State) ->
    ?LOG_INFO("Received call message from ~p in the handle_call handler.", [From]),
    {reply, ok, State};
handle_call({sleep, T}, From, State) ->
    ?LOG_INFO("Received call message from ~p in the handle_call handler.", [From]),
    timer:sleep(T),
    {reply, ok, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({cast, ServerRef}, State) ->
    ?LOG_INFO("Received cast message with server_ref: ~p in the handle_call handler.", [ServerRef]),
    partisan:send(ServerRef, ok),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(Reason, _State) ->
    ?LOG_INFO("Terminating, reason:~p", [Reason]),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================