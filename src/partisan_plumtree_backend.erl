%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(partisan_plumtree_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(partisan_plumtree_broadcast_handler).

-include("partisan_logger.hrl").

%% API
-export([start_link/0,
         start_link/1]).

%% partisan_plumtree_broadcast_handler callbacks
-export([broadcast_data/1,
         merge/2,
         is_stale/1,
         graft/1,
         exchange/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% transmission callbacks
-export([extract_log_type_and_payload/1]).

%% State record.
-record(state, {}).

%% Broadcast record.
-record(broadcast, {timestamp}).


-type timestamp() :: non_neg_integer().
-type broadcast_message() :: #broadcast{}.
-type broadcast_id() :: timestamp().
-type broadcast_payload() :: timestamp().



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Same as start_link([]).
%% @end
%% -----------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.

start_link() ->
    start_link([]).

%% -----------------------------------------------------------------------------
%% @doc Start and link to calling process.
%% @end
%% -----------------------------------------------------------------------------
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).



%% =============================================================================
%% PARTISAN_PLUMTREE_BROADCAST_HANDLER CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Returns from the broadcast message the identifier and the payload.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_data(broadcast_message()) ->
    {broadcast_id(), broadcast_payload()}.

broadcast_data(#broadcast{timestamp=Timestamp}) ->
    {Timestamp, Timestamp}.


%% -----------------------------------------------------------------------------
%% @doc Perform a merge of an incoming object with an object in the
%% local datastore.
%% @end
%% -----------------------------------------------------------------------------
-spec merge(broadcast_id(), broadcast_payload()) -> boolean().

merge(Timestamp, Timestamp) ->
    ?LOG_DEBUG("Heartbeat received: ~p", [Timestamp]),

    case is_stale(Timestamp) of
        true ->
            false;
        false ->
            gen_server:call(?MODULE, {merge, Timestamp}, infinity),
            true
    end.


%% -----------------------------------------------------------------------------
%% @doc Use the clock on the object to determine if this message is
%% stale or not.
%% @end
%% -----------------------------------------------------------------------------
-spec is_stale(broadcast_id()) -> boolean().

is_stale(Timestamp) ->
    gen_server:call(?MODULE, {is_stale, Timestamp}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Given a message identifier and a clock, return a given message.
%% @end
%% -----------------------------------------------------------------------------
-spec graft(broadcast_id()) ->
    stale | {ok, broadcast_payload()} | {error, term()}.

graft(Timestamp) ->
    gen_server:call(?MODULE, {graft, Timestamp}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Anti-entropy mechanism.
%% @end
%% -----------------------------------------------------------------------------
-spec exchange(node()) -> {ok, pid()} | {error, any()} | ignore.

exchange(_Peer) ->
    %% Ignore the standard anti-entropy mechanism from plumtree.
    %%
    %% This is because we don't need to worry about reliable delivery: we always
    %% know we'll have another message to further repair during the next
    %% interval.
    ignore.



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



-spec init([]) -> {ok, #state{}}.

init([]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    schedule_heartbeat(),

    %% Open an ETS table for tracking heartbeat messages.
    ets:new(?MODULE, [named_table]),

    {ok, #state{}}.


-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

handle_call({is_stale, Timestamp}, _From, State) ->
    Result = case ets:lookup(?MODULE, Timestamp) of
        [] ->
            false;
        _ ->
            true
    end,
    {reply, Result, State};

handle_call({graft, Timestamp}, _From, State) ->
    Result = case ets:lookup(?MODULE, Timestamp) of
        [] ->
            ?LOG_INFO("Timestamp: ~p not found for graft.", [Timestamp]),
            {error, {not_found, Timestamp}};
        [{Timestamp, _}] ->
            {ok, Timestamp}
    end,
    {reply, Result, State};

handle_call({merge, Timestamp}, _From, State) ->
    true = ets:insert(?MODULE, [{Timestamp, true}]),
    {reply, ok, State};

handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.


-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),    {noreply, State}.


handle_info(heartbeat, State) ->
    %% Generate message with monotonically increasing integer.
    Counter = erlang:unique_integer([monotonic, positive]),

    %% Make sure the node prefixes the timestamp with it's own
    %% identifier: this means that we can have this tree
    %% participate in multiple trees, each rooted at a different
    %% node.
    Timestamp = {partisan:node(), Counter},

    %% Insert a new message into the table.
    true = ets:insert(?MODULE, [{Timestamp, true}]),

    %% Send message with monotonically increasing integer.
    ok = partisan_plumtree_broadcast:broadcast(#broadcast{timestamp=Timestamp}, ?MODULE),

    ?LOG_DEBUG(
        "Heartbeat triggered: sending ping ~p to ensure tree.",
        [Timestamp]
    ),

    %% Schedule report.
    schedule_heartbeat(),

    {noreply, State};

handle_info(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled info event", event => Event}),
    {noreply, State}.


-spec terminate(term(), #state{}) -> term().

terminate(_Reason, _State) ->
    ok.


-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
schedule_heartbeat() ->
    case partisan_config:get(broadcast, false) of
        true ->
            Interval = partisan_config:get(broadcast_heartbeat_interval, 10000),
            timer:send_after(Interval, heartbeat);
        false ->
            ok
    end.


%% @private
extract_log_type_and_payload({prune, Root, From}) ->
    [{broadcast_protocol, {Root, From}}];

extract_log_type_and_payload({ignored_i_have, MessageId, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {MessageId, Round, Root, From}}];

extract_log_type_and_payload({graft, MessageId, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {MessageId, Round, Root, From}}];

extract_log_type_and_payload(
    {broadcast, MessageId, Timestamp, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {Timestamp, MessageId, Round, Root, From}}];

extract_log_type_and_payload({i_have, MessageId, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {MessageId, Round, Root, From}}];

extract_log_type_and_payload(Message) ->
    ?LOG_INFO("No match for extracted payload: ~p", [Message]),
    [].
