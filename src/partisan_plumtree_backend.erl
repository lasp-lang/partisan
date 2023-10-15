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

%% -----------------------------------------------------------------------------
%% @doc This modules implements a server that realises the
%% {@link partisan_plumtree_broadcast_handler} behaviour in order to diseminate
%% heartbeat messages. Partisan uses these heartbeat messages to stimulate the
%% Epidemic Broadcast Tree construction.
%%
%% The server will schedule the sending of a `hearbeat` message periodically
%% using the `broadcast_heartbeat_interval` option.
%%
%% Notice that this handler does not perform AAE Exchanges, as we will always
%% have a periodic heartbeat. For that reason, the implementation
%% of the {@link partisan_plumtree_broadcast_handler:exchange/1}
%% callback always returns `ignore'.
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_plumtree_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).
-behaviour(partisan_plumtree_broadcast_handler).

-include("partisan.hrl").
-include("partisan_logger.hrl").


-record(state, {
    node                    ::  node(),
    epoch                   ::  integer(),
    monotonic = 0           ::  non_neg_integer()
}).

-type state()   ::  #state{}.

-record(broadcast, {
    timestamp               :: timestamp()
}).

-type broadcast_message()   :: #broadcast{}.
-type timestamp()           :: {
                                    Node :: node(),
                                    Epoch :: integer(),
                                    Monotonic :: integer()
                                }.
-type broadcast_id()        :: timestamp().
-type broadcast_payload()   :: timestamp().


%% API
-export([start_link/0]).
-export([start_link/1]).
-export([timestamps/0]).

%% transmission callbacks
-export([extract_log_type_and_payload/1]).

%% partisan_plumtree_broadcast_handler callbacks
-export([broadcast_channel/0]).
-export([broadcast_data/1]).
-export([exchange/1]).
-export([graft/1]).
-export([is_stale/1]).
-export([merge/2]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).



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


%% -----------------------------------------------------------------------------
%% @doc Returns all the timestamps.
%%
%% Notice this can copy a lot of data from the ets table to the calling process.
%%
%% In case `partisan_config:get(broadcast)' returns `false', this function
%% returns an empty list.
%% @end
%% -----------------------------------------------------------------------------
-spec timestamps() -> [timestamp()].

timestamps() ->
    try
        ets:tab2list(?MODULE)
    catch
        error:badarg ->
            []
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
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



%% =============================================================================
%% PARTISAN_PLUMTREE_BROADCAST_HANDLER CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Returns the channel to be used when broadcasting a message
%% on behalf of this handler.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_channel() -> partisan:channel().

broadcast_channel() ->
    ?MEMBERSHIP_CHANNEL.


%% -----------------------------------------------------------------------------
%% @doc Returns from the broadcast message the identifier and the payload.
%% In this case a tuple where both arguments have the broadcast message
%% `timestamp'. These messages are used by Partisan as a stimulus for the
%% Epidemic Broadcast Tree (Plumtree) construction.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_data(broadcast_message()) ->
    {broadcast_id(), broadcast_payload()}.

broadcast_data(#broadcast{timestamp = Timestamp}) ->
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
%% A message is stale if the received message is causually newer than an
%% existing one. If the message is missing or if the context does not represent
%% an anscestor of message , false is returned. Otherwise, true is
%% returned.
%% @end
%% -----------------------------------------------------------------------------
-spec is_stale(broadcast_id()) -> boolean().

is_stale({Node, Epoch, Monotonic}) ->
    try ets:lookup(?MODULE, Node) of
        [] ->
            false;

        [{_, Epoch, ISet}] ->
            partisan_interval_sets:is_element(Monotonic, ISet);

        [{_, Epoch0, _}] ->
            Epoch0 > Epoch

    catch
        _:_ ->
            %% we return true so that merge is not called
            true
    end.


%% -----------------------------------------------------------------------------
%% @doc Given a message identifier and a clock, return a given message.
%% @end
%% -----------------------------------------------------------------------------
-spec graft(broadcast_id()) ->
    stale | {ok, broadcast_payload()} | {error, {not_found, timestamp()}}.

graft({Node, Epoch, Monotonic} = Timestamp) ->
    try ets:lookup(?MODULE, Node) of
        [] ->
            ?LOG_DEBUG(#{
                description => "Heartbeat message not found for graft.",
                timestamp => Timestamp
            }),
            not_found(Timestamp);

        [{Node, Epoch, ISet}] ->
            case partisan_interval_sets:is_element(Monotonic, ISet) of
                true ->
                    {ok, Timestamp};
                false ->
                    not_found(Timestamp)
            end;

        [{_, Epoch0, _}] when Epoch0 > Epoch ->
            stale;

         [{_, Epoch0, _}] when Epoch0 < Epoch ->
            not_found(Timestamp)

    catch
        _:_ ->
            not_found(Timestamp)
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns `ignore`.
%% This is because we don't need to worry about reliable delivery: we always
%% know we'll have another heartbeat message to further repair during the next
%% interval.
%% @end
%% -----------------------------------------------------------------------------
-spec exchange(node()) -> {ok, pid()} | {error, any()} | ignore.

exchange(_Peer) ->
    ignore.



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



-spec init([]) -> {ok, state()}.

init([]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    schedule_heartbeat(),

    %% Open an ETS table for tracking heartbeat messages.
    ets:new(?MODULE, [named_table, set, protected]),

    State = #state{
        node = node(),
        epoch = erlang:system_time(),
        monotonic = 0
    },
    {ok, State}.


-spec handle_call(term(), {pid(), term()}, state()) ->
    {reply, term(), state()}.

handle_call({merge, {_, _, _} = Timestamp}, _From, State) ->
    true = add_timestamp(Timestamp),
    {reply, ok, State};

handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.


-spec handle_cast(term(), state()) -> {noreply, state()}.

handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),
    {noreply, State}.


handle_info(heartbeat, State) ->
    %% Generate message with monotonically increasing integer.
    Monotonic = State#state.monotonic + 1,

    %% Make sure the node prefixes the timestamp with it's own
    %% identifier: this means that we can have this tree
    %% participate in multiple trees, each rooted at a different
    %% node.
    Timestamp = {State#state.node, State#state.epoch, Monotonic},

    %% Insert a new message into the table.
    true = add_timestamp(Timestamp),

    %% Send message with monotonically increasing integer.
    ok = partisan_plumtree_broadcast:broadcast(
        #broadcast{timestamp = Timestamp},
        ?MODULE
    ),

    ?LOG_DEBUG(
        "Heartbeat triggered: sending ping ~p to ensure tree.",
        [Timestamp]
    ),

    %% Schedule report.
    schedule_heartbeat(),

    {noreply, State#state{monotonic = Monotonic}};

handle_info(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled info event", event => Event}),
    {noreply, State}.


-spec terminate(term(), state()) -> term().

terminate(_Reason, _State) ->
    ok.


-spec code_change(term() | {down, term()}, state(), term()) ->
    {ok, state()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================


%% @private
not_found(Timestamp) ->
    {error, {not_found, Timestamp}}.


%% @private
add_timestamp({Node, Epoch, Monotonic}) ->
    case ets:lookup(?MODULE, Node) of
        [] ->
            ISet = partisan_interval_sets:from_list([Monotonic]),
            true = ets:insert(?MODULE, [{Node, Epoch, ISet}]);

        [{_, Epoch0, _}] when Epoch0 < Epoch ->
            ISet = partisan_interval_sets:from_list([Monotonic]),
            true = ets:insert(?MODULE, [{Node, Epoch, ISet}]);

        [{_, Epoch, ISet0}] ->
            ISet = partisan_interval_sets:add_element(Monotonic, ISet0),
            true = ets:insert(?MODULE, [{Node, Epoch, ISet}]);

        [{_, Epoch0, _}] when Epoch0 > Epoch ->
            %% We ignore as it is an old message
            true
    end.


%% @private
schedule_heartbeat() ->
    case partisan_config:get(broadcast, false) of
        true ->
            Interval = partisan_config:get(broadcast_heartbeat_interval, 10000),
            timer:send_after(Interval, heartbeat);
        false ->
            ok
    end.

