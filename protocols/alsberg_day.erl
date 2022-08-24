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

-include("partisan.hrl").
-include("partisan_logger.hrl").

%% API
-export([start_link/0,
         stop/0,
         timeout/0,
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

-record(state, {next_id, store, membership=[], outstanding}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

timeout() ->
    4000.

%% @doc Notifies us of membership update.
update(LocalState0) ->
    LocalState = partisan_peer_service:decode(LocalState0),
    gen_server:cast(?MODULE, {update, LocalState}).

%% @doc Issue write operations.
state() ->
    gen_server:call(?MODULE, state).

%% @doc Issue write operations.
write(Key, Value) ->
    gen_server:cast(?MODULE, {write, self(), Key, Value}),

    receive
        Response ->
            Response
    end.

%% @doc Issue read operations.
read(Key) ->
    gen_server:cast(?MODULE, {read, self(), Key}),

    receive
        Response ->
            Response
    end.

%% @doc Issue read operations.
read_local(Key) ->
    %% TODO: Bit of a hack just to get this working.
    true = erlang:register(read_coordinator, self()),
    From = partisan_remote_ref:from_term(read_coordinator),

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
    ?LOG_INFO("Starting with membership: ~p", [Membership0]),

    %% Initialization values.
    Store = dict:new(),
    Membership = membership(Membership0),
    Outstanding = dict:new(),

    {ok, #state{next_id=0,
                store=Store,
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
    partisan:forward_message(From, {ok, Value}),

    {noreply, State};
handle_cast({read, From0, Key}, #state{next_id=NextId, membership=[Primary|_Rest], store=Store}=State) ->
    %% TODO: Bit of a hack just to get this working.
    From = list_to_atom("read_coordinator_" ++ integer_to_list(NextId)),
    try
        true = erlang:unregister(From)
    catch
        _:_ ->
            ok
    end,
    true = erlang:register(From, From0),
    EncodedFrom = partisan_remote_ref:from_term(From),

    %% TODO: HACK to get around problem in interprocedural analysis.
    Request = {read, EncodedFrom, Key},

    case node() of
        Primary ->
            %% Get the value.
            Value = read(Key, Store),

            %% Reply to the caller.
            partisan:forward_message(EncodedFrom, {ok, Value}),

            {noreply, State#state{next_id=NextId+1}};
        _ ->
            %% Reply to caller.
            ?LOG_INFO("Node ~p is not the primary for request: ~p", [node(), Request]),
            partisan:forward_message(EncodedFrom, {error, not_primary}),

            {noreply, State#state{next_id=NextId+1}}
    end;
handle_cast({write, From0, Key, Value}, #state{next_id=NextId, membership=[Primary|Rest]=Membership, outstanding=Outstanding0, store=Store0}=State) ->
    %% TODO: Bit of a hack just to get this working.
    From = list_to_atom("write_coordinator_" ++ integer_to_list(NextId)),
    try
        true = erlang:unregister(From)
    catch
        _:_ ->
            ok
    end,
    true = erlang:register(From, From0),
    EncodedFrom = partisan_remote_ref:from_term(From),

    %% TODO: HACK to get around problem in interprocedural analysis.
    Request = {write, EncodedFrom, Key, Value},

    case node() of
        Primary ->

            %% Add to list of outstanding requests.
            Replies = [node()],
            Outstanding = dict:store(Request, {Membership, Replies}, Outstanding0),

            %% Send collaboration message to other nodes.
            CoordinatorNode = node(),

            lists:foreach(fun(Node) ->
                ?LOG_INFO("sending collaborate message to node ~p for request ~p", [Node, Request]),
                partisan:forward_message(
                    Node,
                    ?MODULE,
                    {collaborate, CoordinatorNode, Request},
                    #{channel => ?DEFAULT_CHANNEL}
                )
            end, Rest),

            %% Write to storage.
            Store = write(Key, Value, Store0),

            {noreply, State#state{store=Store, outstanding=Outstanding, next_id=NextId+1}};
        _ ->
            %% Reply to caller.
            ?LOG_INFO("Node ~p is not the primary for request: ~p", [node(), Request]),
            partisan:forward_message(EncodedFrom, {error, not_primary}),

            {noreply, State#state{next_id=NextId+1}}
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
                    ?LOG_INFO("Node ~p received all replies for request ~p, acknowleding to user.", [node(), Request]),
                    partisan:forward_message(From, {ok, Value});
                false ->
                    ?LOG_INFO("Received replies from: ~p, but need replies from: ~p", [Replies, Membership -- Replies]),
                    ok
            end,

            %% Update list.
            Outstanding = dict:store(Request, {Membership, Replies}, Outstanding0),

            {noreply, State#state{outstanding=Outstanding}};
        _ ->
            ?LOG_INFO("Received reply for unknown request: ~p", [Request]),

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
            ?LOG_INFO("Node ~p is backup, responding to the primary ~p with acknowledgement", [node(), CoordinatorNode]),
            partisan:forward_message(
                CoordinatorNode,
                ?MODULE,
                {collaborate_ack, ReplyingNode, Request},
                #{channel => ?DEFAULT_CHANNEL}
            ),

            {noreply, State#state{store=Store}}
    end;
handle_info(Msg, State) ->
    ?LOG_INFO("Received message ~p with no handler!", [Msg]),

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