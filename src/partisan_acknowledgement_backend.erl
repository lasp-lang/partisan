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

-include("partisan_logger.hrl").

%% API
-export([
    start_link/0,
    store/2,
    ack/1,
    outstanding/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {storage}).

%%%===================================================================
%%% API
%%%===================================================================

%% The outstanding-message table is a plain key/value store keyed by an
%% already-unique message clock: there is no state here that needs a process to
%% arbitrate it. It is therefore `public' with write concurrency, and `store/2'
%% and `ack/1' run as single ETS operations in the *calling* process. Routing
%% them through this server instead would put a node-global serialisation
%% point — and a cross-process round trip — on the path of every acknowledged
%% message, on top of the one the peer service manager already imposes.
%%
%% The server exists to own the table, and to be the supervised, registered
%% process the rest of the system expects to find.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

store(MessageClock, Message) ->
    true = ets:insert(?MODULE, {MessageClock, Message}),
    ok.

ack(MessageClock) ->
    true = ets:delete(?MODULE, MessageClock),
    ok.

outstanding() ->
    %% A match specification returning whole objects, in one BIF call. This
    %% runs on every retransmission tick, so a fold accumulating with
    %% `Acc ++ [X]' — quadratic in the size of the outstanding set — is not an
    %% option.
    {ok, ets:select(?MODULE, [{'_', [], ['$_']}])}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    Storage = ets:new(?MODULE, [
        named_table,
        set,
        public,
        {write_concurrency, true},
        {read_concurrency, true}
    ]),
    logger:set_process_metadata(#{node => partisan:node()}),
    {ok, #state{storage = Storage}}.

%% @private
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
