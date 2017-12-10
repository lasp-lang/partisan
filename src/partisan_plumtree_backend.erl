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

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%%%===================================================================
%%% partisan_plumtree_broadcast_handler callbacks
%%%===================================================================

-type timestamp() :: non_neg_integer().

-type broadcast_message() :: #broadcast{}.
-type broadcast_id() :: timestamp().
-type broadcast_payload() :: timestamp().

%% @doc Returns from the broadcast message the identifier and the payload.
-spec broadcast_data(broadcast_message()) ->
    {broadcast_id(), broadcast_payload()}.
broadcast_data(#broadcast{timestamp=Timestamp}) ->
    {Timestamp, Timestamp}.

%% @doc Perform a merge of an incoming object with an object in the
%%      local datastore.
-spec merge(broadcast_id(), broadcast_payload()) -> boolean().
merge(Timestamp, Timestamp) ->
    lager:debug("Heartbeat received: ~p", [Timestamp]),

    case is_stale(Timestamp) of
        true ->
            false;
        false ->
            gen_server:call(?MODULE, {merge, Timestamp}, infinity),
            true
    end.

%% @doc Use the clock on the object to determine if this message is
%%      stale or not.
-spec is_stale(broadcast_id()) -> boolean().
is_stale(Timestamp) ->
    gen_server:call(?MODULE, {is_stale, Timestamp}, infinity).

%% @doc Given a message identifier and a clock, return a given message.
-spec graft(broadcast_id()) ->
    stale | {ok, broadcast_payload()} | {error, term()}.
graft(Timestamp) ->
    gen_server:call(?MODULE, {graft, Timestamp}, infinity).

%% @doc Anti-entropy mechanism.
-spec exchange(node()) -> {ok, pid()}.
exchange(_Peer) ->
    %% Ignore the standard anti-entropy mechanism from plumtree.
    %%
    %% Spawn a process that terminates immediately, because the
    %% broadcast exchange timer tracks the number of in progress
    %% exchanges and bounds it by that limit.
    %%
    %% Ignore the anti-entropy mechanism because we don't need to worry
    %% about reliable delivery: we always know we'll have another
    %% message to further repair duing the next interval.
    %%
    Pid = spawn_link(fun() -> ok end),
    {ok, Pid}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Seed the process at initialization.
    rand_compat:seed(erlang:phash2([node()]),
                     erlang:monotonic_time(),
                     erlang:unique_integer()),

    schedule_heartbeat(),

    %% Open an ETS table for tracking heartbeat messages.
    ets:new(?MODULE, [named_table]),

    {ok, #state{}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
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
            lager:info("Timestamp: ~p not found for graft.", [Timestamp]),
            {error, {not_found, Timestamp}};
        [{Timestamp, _}] ->
            {ok, Timestamp}
    end,
    {reply, Result, State};
handle_call({merge, Timestamp}, _From, State) ->
    true = ets:insert(?MODULE, [{Timestamp, true}]),
    {reply, ok, State};
handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
%% @private
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(heartbeat, State) ->
    %% Generate message with monotonically increasing integer.
    Counter = time_compat:unique_integer([monotonic, positive]),

    %% Make sure the node prefixes the timestamp with it's own
    %% identifier: this means that we can have this tree
    %% participate in multiple trees, each rooted at a different
    %% node.
    Timestamp = {node(), Counter},

    %% Insert a new message into the table.
    true = ets:insert(?MODULE, [{Timestamp, true}]),

    %% Send message with monotonically increasing integer.
    ok = partisan_plumtree_broadcast:broadcast(#broadcast{timestamp=Timestamp}, ?MODULE),

    lager:debug("Heartbeat triggered: sending ping ~p to ensure tree.", [Timestamp]),

    %% Schedule report.
    schedule_heartbeat(),

    {noreply, State};
handle_info(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
schedule_heartbeat() ->
    case partisan_config:get(broadcast, false) of
        true ->
            Interval = partisan_config:get(broadcast_heartbeat_interval, 10000),
            timer:send_after(Interval, heartbeat);
        false ->
            ok
    end.

%%%===================================================================
%%% Transmission functions
%%%===================================================================

extract_log_type_and_payload({prune, Root, From}) ->
    [{broadcast_protocol, {Root, From}}];
extract_log_type_and_payload({ignored_i_have, MessageId, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {MessageId, Round, Root, From}}];
extract_log_type_and_payload({graft, MessageId, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {MessageId, Round, Root, From}}];
extract_log_type_and_payload({broadcast, MessageId, Timestamp, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {Timestamp, MessageId, Round, Root, From}}];
extract_log_type_and_payload({i_have, MessageId, _Mod, Round, Root, From}) ->
    [{broadcast_protocol, {MessageId, Round, Root, From}}];
extract_log_type_and_payload(Message) ->
    lager:info("No match for extracted payload: ~p", [Message]),
    [].