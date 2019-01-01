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

-define(PB_TIMEOUT,       1000).
-define(PB_RETRY_TIMEOUT, 100).

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
            Store = write(Key, Value, Store0),

            %% Forward to collaboration message.
            Myself = myself(),
            forward_message(Collaborator, {collaborate, From, Myself, Key, Value}),
            lager:info("~p: node ~p sent replication request for key ~p with value ~p", [?MODULE, node(), Key, Value]),

            %% Wait for collaboration ack before proceeding for n-host resilience (n = 2).
            receive
                {collaborate_ack, From, Key, Value} ->
                    lager:info("~p: node ~p ack received for key ~p value ~p", [?MODULE, node(), Key, Value]),
                    {reply, ok, State#state{store=Store}}
            after
                %% We have to timeout, otherwise we block the gen_server.
                ?PB_RETRY_TIMEOUT ->
                    {reply, {error, timeout}, State}
            end;
        _ ->
            {reply, {error, {primary, Primary}}, State}
    end;
handle_call({read, Key}, From, #state{nodes=[Primary|_Rest], store=Store}=State) ->
    case node() of 
        Primary ->
            Value = read(Key, Store),
            lager:info("~p: node ~p received read for key ~p and returning value ~p", [?MODULE, node(), Key, Value]),
            {reply, {ok, Value}, State};
        _ ->
            %% Forward the read request to the primary.
            forward_message(Primary, {forwarded_read, From, Key}),

            %% Return control, because backup requests may arrive before response does 
            %% undre concurrent scheduling.
            {noreply, State}
    end;
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({forwarded_read, From, Key}, #state{store=Store}=State) ->
    Value = read(Key, Store),
    lager:info("~p: node ~p received forwarded read for key ~p and returning value ~p", [?MODULE, node(), Key, Value]),

    %% Send the response to the caller.
    gen_server:reply(From, {ok, Value}),

    %% No need to send acknowledgement back to the forwarder: 
    %%  - not needed for control flow.

    {noreply, State};

%% @private
handle_info({collaborate, From, SourceNode, Key, Value}, #state{nodes=[_Primary, _Collaborator | Backups], store=Store0}=State) ->
    %% Write value locally.
    Store = write(Key, Value, Store0),
    lager:info("~p: node ~p storing updated value key ~p value ~p", [?MODULE, node(), Key, Value]),

    %% On ack, reply to caller.
    gen_server:reply(From, ok),

    %% Send write acknowledgement.
    forward_message(SourceNode, {collaborate_ack, From, Key, Value}),
    lager:info("~p: node ~p acknowledging value for key ~p value ~p", [?MODULE, node(), Key, Value]),

    %% Send backup message to backups.
    lists:foreach(fun(Backup) -> forward_message(Backup, {backup, From, Key, Value}) end, Backups),

    {noreply, State#state{store=Store}};

handle_info({backup, _From, _SourceNode, Key, Value}, #state{store=Store0}=State) ->
    %% Write value locally.
    Store = write(Key, Value, Store0),
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

%% @private
forward_message(Destination, Message) ->
    Manager = partisan_config:get(partisan_peer_service_manager),
    Manager:forward_message(Destination, undefined, ?MODULE, Message, [{ack, true}]).

%% @private
read(Key, Store) ->
    case dict:find(Key, Store) of 
        {ok, V} ->
            V;
        error ->
            not_found
    end.

%% @private
write(Key, Value, Store) ->
    dict:store(Key, Value, Store).

%% @private
myself() ->
    node().