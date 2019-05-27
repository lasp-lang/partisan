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

-module(alsberg_day).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         stop/0,
         write/2,
         read/1,
         read_local/1,
         state/0,
         update/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {store, membership=[], outstanding}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

%% @doc Notifies us of membership update.
update(LocalState0) ->
    LocalState = partisan_peer_service:decode(LocalState0),
    gen_server:cast(?MODULE, {update, LocalState}).

%% @doc Issue write operations.
state() ->
    gen_server:call(?MODULE, state).

%% @doc Issue write operations.
write(Key, Value) ->
    %% TODO: Bit of a hack just to get this working.
    true = erlang:register(write_coordinator, self()),
    From = partisan_util:registered_name(write_coordinator),

    gen_server:cast(?MODULE, {write, From, Key, Value}),

    receive
        Response ->
            Response
    end.

%% @doc Issue read operations.
read(Key) ->
    %% TODO: Bit of a hack just to get this working.
    true = erlang:register(read_coordinator, self()),
    From = partisan_util:registered_name(read_coordinator),

    gen_server:cast(?MODULE, {read, From, Key}),

    receive
        Response ->
            Response
    end.

%% @doc Issue read operations.
read_local(Key) ->
    %% TODO: Bit of a hack just to get this working.
    true = erlang:register(read_coordinator, self()),
    From = partisan_util:registered_name(read_coordinator),

    gen_server:cast(?MODULE, {read_local, From, Key}),

    receive
        Response ->
            Response
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    %% Register membership update callback.
    partisan_peer_service:add_sup_callback(fun ?MODULE:update/1),

    %% Start with initial membership.
    {ok, Membership0} = partisan_peer_service:members(),
    partisan_logger:info("Starting with membership: ~p", [Membership0]),

    %% Initialization values.
    Store = dict:new(),
    Membership = membership(Membership0),
    Outstanding = dict:new(),

    {ok, #state{store=Store,
                membership=Membership,
                outstanding=Outstanding}}.

%% @private
handle_call(state, _From, #state{store=Store}=State) ->
    {reply, {ok, Store}, State};

%% @private
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({update, Membership0}, State) ->
    Membership = membership(Membership0),
    {noreply, State#state{membership=Membership}};

%% @private
handle_cast({read_local, From, Key}, #state{store=Store}=State) ->
    %% Get the value.
    Value = read(Key, Store),

    %% Reply to the caller.
    partisan_pluggable_peer_service_manager:forward_message(From, {ok, Value}),

    {noreply, State};
handle_cast({read, From, Key}, #state{membership=[Primary|_Rest], store=Store}=State) ->
    %% TODO: HACK to get around problem in interprocedural analysis.
    Request = {read, From, Key}, 

    case node() of 
        Primary ->
            %% Get the value.
            Value = read(Key, Store),

            %% Reply to the caller.
            partisan_pluggable_peer_service_manager:forward_message(From, {ok, Value}),

            {noreply, State};
        _ ->
            %% Reply to caller.
            partisan_logger:info("Node ~p is not the primary for request: ~p", [node(), Request]),
            partisan_pluggable_peer_service_manager:forward_message(From, {error, not_primary}),

            {noreply, State}
    end;
handle_cast({write, From, Key, Value}, #state{membership=[Primary|Rest]=Membership, outstanding=Outstanding0, store=Store0}=State) ->
    %% TODO: HACK to get around problem in interprocedural analysis.
    Request = {write, From, Key, Value},

    case node() of 
        Primary ->
            %% Add to list of outstanding requests.
            Replies = [node()],
            Outstanding = dict:store(Request, {Membership, Replies}, Outstanding0),

            %% Send collaboration message to other nodes.
            CoordinatorNode = node(),

            lists:foreach(fun(Node) ->
                partisan_pluggable_peer_service_manager:forward_message(Node, undefined, ?MODULE, {collaborate, CoordinatorNode, Request}, [])
            end, Rest),

            %% Write to storage.
            Store = write(Key, Value, Store0),

            %% Reply to the caller: this is a bug in the implementation.
            partisan_pluggable_peer_service_manager:forward_message(From, {ok, Value}),

            {noreply, State#state{store=Store, outstanding=Outstanding}};
        _ ->
            %% Reply to caller.
            partisan_logger:info("Node ~p is not the primary for request: ~p", [node(), Request]),
            partisan_pluggable_peer_service_manager:forward_message(From, {error, not_primary}),

            {noreply, State}
    end.

%% @private
handle_info({collaborate_ack, ReplyingNode, {write, From, Key, Value}}, #state{outstanding=Outstanding0}=State) ->
    %% TODO: HACK to get around problem in interprocedural analysis.
    Request = {write, From, Key, Value}, 

    case dict:find(Request, Outstanding0) of
        {ok, {Membership, Replies0}} ->
            %% Update list of nodes that have acknowledged.
            Replies = Replies0 ++ [ReplyingNode],

            case lists:usort(Membership) =:= lists:usort(Replies) of 
                true ->
                    partisan_logger:info("Node ~p received all replies for request ~p, acknowleding to user.", [node(), Request]),
                    partisan_pluggable_peer_service_manager:forward_message(From, {ok, Value});
                false ->
                    partisan_logger:info("Received replies from: ~p, but need replies from: ~p", [Replies, Membership -- Replies]),
                    ok
            end,

            %% Update list.
            Outstanding = dict:store(Request, {Membership, Replies}, Outstanding0),

            {noreply, State#state{outstanding=Outstanding}};
        _ ->
            partisan_logger:info("Received reply for unknown request: ~p", [Request]),

            {noreply, State}
    end;
handle_info({collaborate, CoordinatorNode, {write, From, Key, Value}}, #state{membership=[Primary|_Rest], store=Store0}=State) ->
    %% TODO: HACK to get around problem in interprocedural analysis.
    Request = {write, From, Key, Value},

    case node() of 
        Primary ->
            %% Do nothing.
            {noreply, State};
        _ ->
            %% Write to storage.
            Store = write(Key, Value, Store0),

            %% Reply with collaborate acknowledgement.
            ReplyingNode = node(),
            partisan_logger:info("Node ~p is backup, responding to the primary ~p with acknowledgement", [node(), CoordinatorNode]),
            partisan_pluggable_peer_service_manager:forward_message(CoordinatorNode, undefined, ?MODULE, {collaborate_ack, ReplyingNode, Request}, []),

            {noreply, State#state{store=Store}}
    end;
handle_info(Msg, State) ->
    partisan_logger:info("Received message ~p with no handler!", [Msg]),

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
membership(Membership) ->
    lists:usort(Membership).