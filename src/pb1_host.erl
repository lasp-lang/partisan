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

-module(pb1_host).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/1,
         write/2,
         read/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {store, nodes=[]}).

-include("partisan.hrl").

-define(PB_TIMEOUT, 1000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Nodes) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Nodes], []).

write(Key, Value) ->
    gen_server:call(?MODULE, {write, Key, Value}, ?PB_TIMEOUT).

read(Key) ->
    gen_server:call(?MODULE, {read, Key}, ?PB_TIMEOUT).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Nodes]) ->
    Store = dict:new(),
    {ok, #state{nodes=Nodes, store=Store}}.

%% @private
handle_call({write, Key, Value}, _From, #state{nodes=[Node|Rest], store=Store0}=State) ->
    case node() of 
        Node ->
            lager:info("~p: node ~p received write for key ~p with value ~p", [?MODULE, node(), Key, Value]),

            %% Forward to replicas and wait for reply, then write locally and return.
            From = self(),
            Manager = partisan_config:get(partisan_peer_service_manager),

            lists:foreach(fun(N) ->
                ok = Manager:forward_message(N, undefined, ?MODULE, {replicate, From, Key, Value}, [])
            end, Rest),

            lager:info("~p: node ~p sent replication request for key ~p with value ~p", [?MODULE, node(), Key, Value]),

            %% Write value.
            Store = dict:store(Key, Value, Store0),
            lager:info("~p: node ~p stored key ~p with value ~p", [?MODULE, node(), Key, Value]),

            {reply, ok, State#state{store=Store}};
        _ ->
            {reply, {error, {primary, Node}}, State}
    end;
handle_call({read, Key}, _From, #state{store=Store}=State) ->
    %% Allow reads from the backup node.
    Value = case dict:find(Key, Store) of 
        {ok, V} ->
            V;
        error ->
            not_found
    end,
    lager:info("~p: node ~p received read for key ~p and returning value ~p", [?MODULE, node(), Key, Value]),

    {reply, {ok, Value}, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({replicate, _From, Key, Value}, #state{store=Store0}=State) ->
    Store = dict:store(Key, Value, Store0),
    {noreply, State#state{store=Store}};
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