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
-export([start_link/0,
         write/2,
         read/1,
         update/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {store, membership=[]}).

-include("partisan.hrl").

-define(PB_TIMEOUT,       1000).
-define(PB_RETRY_TIMEOUT, 100).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Notifies us of membership update.
update(LocalState0) ->
    LocalState = partisan_peer_service:decode(LocalState0),
    gen_server:cast(?MODULE, {update, LocalState}).

%% @doc Issue write operations.
write(Key, Value) ->
    From = pself(),
    RequestId = prequestid(),

    gen_server:cast(?MODULE, {write, {From, RequestId}, Key, Value}),

    receive
        {response, RequestId, Response} ->
            Response
    after
        ?PB_TIMEOUT ->
            {error, timeout}
    end.

%% @doc Issue read operations.
read(Key) ->
    From = pself(),
    RequestId = prequestid(),

    gen_server:cast(?MODULE, {read, {From, RequestId}, Key}),

    receive
        {response, RequestId, Response} ->
            Response
    after
        ?PB_TIMEOUT ->
            {error, timeout}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    Store = dict:new(),

    %% Seed the random number generator using the deterministic seed.
    partisan_config:seed(),

    %% Register membership update callback.
    partisan_peer_service:add_sup_callback(fun ?MODULE:update/1),

    %% Start with initial membership.
    {ok, Membership} = partisan_peer_service:members(),
    lager:info("Starting with membership: ~p", [Membership]),

    {ok, #state{membership=Membership, store=Store}}.

%% @private
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({update, Membership0}, State) ->
    Membership = lists:usort(Membership0), %% Must sort list or random selection with seed is *nondeterministic.*
    {noreply, State#state{membership=Membership}};

handle_cast({write, {FromPid, FromRequestId}=From, Key, Value}, #state{membership=[Primary, Collaborator|_Rest], store=Store0}=State) ->
    case node() of 
        Primary ->
            lager:info("~p: node ~p received write for key ~p with value ~p", [?MODULE, node(), Key, Value]),

            %% Write value locally.
            Store = write(Key, Value, Store0),

            %% Forward to collaboration message.
            Myself = pnode(),
            psend(Collaborator, {collaborate, From, Myself, Key, Value}),
            lager:info("~p: node ~p sent replication request for key ~p with value ~p", [?MODULE, node(), Key, Value]),

            %% Wait for collaboration ack before proceeding for n-host resilience (n = 2).
            receive
                {collaborate_ack, From, Key, Value} ->
                    lager:info("~p: node ~p ack received for key ~p value ~p", [?MODULE, node(), Key, Value]),

                    %% Reply to caller.
                    psend(FromPid, {response, FromRequestId, ok}),

                    {noreply, State#state{store=Store}}
            after
                %% We have to timeout, otherwise we block the gen_server.
                ?PB_RETRY_TIMEOUT ->
                    %% Reply to caller.
                    psend(FromPid, {response, FromRequestId, {error, timeout}}),

                    {noreply, State}
            end;
        _ ->
            %% Forward the write request to the primary.
            psend(Primary, {forwarded_write, From, Key, Value}),

            %% Return control, because backup requests may arrive before response does 
            %% under concurrent scheduling.
            {noreply, State}
    end;
%% @private
handle_cast({read, {FromPid, FromRequestId}=From, Key}, #state{membership=[Primary|_Rest], store=Store}=State) ->
    case node() of 
        Primary ->
            Value = read(Key, Store),
            lager:info("~p: node ~p received read for key ~p and returning value ~p", [?MODULE, node(), Key, Value]),

            %% Send the response back to the user.
            psend(FromPid, {response, FromRequestId, {ok, Value}}),

            {noreply, State};
        _ ->
            %% Forward the read request to the primary.
            psend(Primary, {forwarded_read, From, Key}),

            %% Return control, because backup requests may arrive before response does 
            %% under concurrent scheduling.
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({forwarded_write, {FromPid, FromRequestId}=From, Key, Value}, #state{membership=[_Primary|Backups], store=Store0}=State) ->
    %% Figure 2c: I think this algorithm is not correct, but we'll see.

    Store = write(Key, Value, Store0),
    lager:info("~p: node ~p received forwarded write for key ~p with value ~p", [?MODULE, node(), Key, Value]),

    %% Send backup message to backups.
    lists:foreach(fun(Backup) -> psend(Backup, {backup, From, Key, Value}) end, Backups),

    %% Send the response to the caller.
    psend(FromPid, {response, FromRequestId, ok}),

    %% No need to send acknowledgement back to the forwarder: 
    %%  - not needed for control flow.

    {noreply, State#state{store=Store}};

%% @private
handle_info({forwarded_read, {FromPid, FromRequestId}, Key}, #state{store=Store}=State) ->
    Value = read(Key, Store),
    lager:info("~p: node ~p received forwarded read for key ~p and returning value ~p", [?MODULE, node(), Key, Value]),

    %% Send the response to the caller.
    psend(FromPid, {response, FromRequestId, {ok, Value}}),

    %% No need to send acknowledgement back to the forwarder: 
    %%  - not needed for control flow.

    {noreply, State};

%% @private
handle_info({collaborate, {FromPid, FromRequestId}=From, SourceNode, Key, Value}, #state{membership=[_Primary, _Collaborator | Backups], store=Store0}=State) ->
    %% Write value locally.
    Store = write(Key, Value, Store0),
    lager:info("~p: node ~p storing updated value key ~p value ~p", [?MODULE, node(), Key, Value]),

    %% On ack, reply to caller.
    psend(FromPid, {response, FromRequestId, ok}),

    %% Send write acknowledgement.
    psend(SourceNode, {collaborate_ack, From, Key, Value}),
    lager:info("~p: node ~p acknowledging value for key ~p value ~p", [?MODULE, node(), Key, Value]),

    %% Send backup message to backups.
    lists:foreach(fun(Backup) -> psend(Backup, {backup, From, Key, Value}) end, Backups),

    {noreply, State#state{store=Store}};

%% @private
handle_info({backup, _From, _SourceNode, Key, Value}, #state{store=Store0}=State) ->
    %% Write value locally.
    Store = write(Key, Value, Store0),
    lager:info("~p: node ~p storing updated value key ~p value ~p", [?MODULE, node(), Key, Value]),

    {noreply, State#state{store=Store}};

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
%%% Partisan functions
%%%===================================================================

%% @private
pmanager() ->
    partisan_config:get(partisan_peer_service_manager).

%% @private
%%
%% [{ack, true}] ensures all messages are retried until acknowledged in the runtime
%% so, no retry logic is required.
psend({partisan_remote_reference, Destination, ServerRef}, Message) ->
    Manager = pmanager(),
    Manager:forward_message(Destination, undefined, ServerRef, Message, [{ack, true}]);
psend(Destination, Message) ->
    Manager = pmanager(),
    Manager:forward_message(Destination, undefined, ?MODULE, Message, [{ack, true}]).

%% @private
pnode() ->
    node().

%%%===================================================================
%%% Conditional support for determinism
%%%===================================================================

%% @private
prequestid() ->
    case partisan_config:get(tracing, false) of 
        true ->
            99999;
        false ->
            erlang:unique_integer([monotonic, positive])
    end.

%% @private
pself() ->
    case partisan_config:get(tracing, false) of 
        true ->
            proxy;
        false ->
            partisan_util:pid()
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

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