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

-module(pb_alsberg_day).
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
handle_call({write, Key, Value}, From, #state{nodes=[Primary, Collaborator|_Rest], store=Store0}=State) ->
    case node() of 
        Primary ->
            lager:info("~p: node ~p received write for key ~p with value ~p", [?MODULE, node(), Key, Value]),

            %% Write value locally.
            Store = dict:store(Key, Value, Store0),

            %% Forward to collaboration message.
            FromNode = partisan_peer_service_manager:myself(),
            Manager = partisan_config:get(partisan_peer_service_manager),
            ok = Manager:forward_message(Collaborator, undefined, ?MODULE, {collaborate, From, FromNode, Key, Value}, []),
            lager:info("~p: node ~p sent replication request for key ~p with value ~p", [?MODULE, node(), Key, Value]),

            {noreply, State#state{store=Store}};
        _ ->
            {reply, {error, {primary, Primary}}, State}
    end;
handle_call({read, Key}, _From, #state{nodes=[Primary|_Rest], store=Store}=State) ->
    case node() of 
        Primary ->
            Value = case dict:find(Key, Store) of 
                {ok, V} ->
                    V;
                error ->
                    not_found
            end,
            lager:info("~p: node ~p received read for key ~p and returning value ~p", [?MODULE, node(), Key, Value]),
            {reply, {ok, Value}, State};
        _ ->
            {reply, {error, {primary, Primary}}, State}
    end;
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({collaborate_ack, _From, Key, Value}, State) ->
    lager:info("~p: node ~p ack received for key ~p value ~p", [?MODULE, node(), Key, Value]),

    {noreply, State};
handle_info({collaborate, From, FromNode, Key, Value}, #state{nodes=[_Primary, _Collaborator | Backups], store=Store0}=State) ->
    %% Write value locally.
    Store = dict:store(Key, Value, Store0),
    lager:info("~p: node ~p storing updated value key ~p value ~p", [?MODULE, node(), Key, Value]),

    %% Send write acknowledgement.
    Manager = partisan_config:get(partisan_peer_service_manager),
    ok = Manager:forward_message(FromNode, undefined, ?MODULE, {collaborate_ack, From, Key, Value}, []),
    lager:info("~p: node ~p acknowledging value for key ~p value ~p", [?MODULE, node(), Key, Value]),

    %% Send backup message to backups.
    lists:foreach(fun(Backup) ->
        ok = Manager:forward_message(Backup, undefined, ?MODULE, {backup, From, Key, Value}, [])
    end, Backups),

    %% On ack, reply to caller.
    gen_server:reply(From, ok),

    {noreply, State#state{store=Store}};
handle_info({backup, _From, _FromNode, Key, Value}, #state{store=Store0}=State) ->
    %% Write value locally.
    Store = dict:store(Key, Value, Store0),
    lager:info("~p: node ~p storing updated value key ~p value ~p", [?MODULE, node(), Key, Value]),

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