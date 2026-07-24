%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Test handler exercising the non-blocking broadcast contract
%% (PDDR-000001): implements `claim/2' (on-path, atomic) + `handle_broadcast/2'
%% (off-path apply). The apply can be made artificially slow via {@link
%% set_delay/1} and reports each applied id to a pid registered via {@link
%% set_notify/1}, so a test can assert both delivery and non-blocking.
%% @end
%% =============================================================================
-module(partisan_bcast_test_handler).

-behaviour(gen_server).
-behaviour(partisan_plumtree_broadcast_handler).

%% control API
-export([start_link/0]).
-export([stop/0]).
-export([reset/0]).
-export([set_notify/1]).
-export([set_delay/1]).
-export([applied/1]).
-export([applied_count/0]).

%% partisan_plumtree_broadcast_handler callbacks
-export([broadcast_data/1]).
-export([broadcast_channel/0]).
-export([claim/2]).
-export([handle_broadcast/2]).
-export([is_stale/1]).
-export([graft/1]).
-export([exchange/1]).
-export([merge/2]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(SEEN, partisan_bcast_test_seen).
-define(APPLIED, partisan_bcast_test_applied).
-define(CFG, partisan_bcast_test_cfg).

%% =============================================================================
%% CONTROL API
%% =============================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

reset() ->
    true = ets:delete_all_objects(?SEEN),
    true = ets:delete_all_objects(?APPLIED),
    ok.

set_notify(Pid) ->
    true = ets:insert(?CFG, {notify, Pid}),
    ok.

set_delay(Ms) ->
    true = ets:insert(?CFG, {delay, Ms}),
    ok.

applied(MessageId) ->
    ets:member(?APPLIED, MessageId).

applied_count() ->
    ets:info(?APPLIED, size).

%% =============================================================================
%% HANDLER CALLBACKS
%% =============================================================================

broadcast_data(Payload) ->
    {Payload, Payload}.

broadcast_channel() ->
    undefined.

%% On-path, atomic: novel iff we win the insert race for this id.
claim(MessageId, _Payload) ->
    ets:insert_new(?SEEN, {MessageId}).

%% Off-path apply. Runs in this handler's process; may be made slow to prove
%% the broadcast server is not blocked by it.
handle_broadcast(MessageId, _Payload) ->
    case cfg(delay, 0) of
        0 -> ok;
        Ms -> timer:sleep(Ms)
    end,
    true = ets:insert(?APPLIED, {MessageId}),
    case cfg(notify, undefined) of
        undefined ->
            ok;
        Pid ->
            Pid ! {applied, MessageId},
            ok
    end.

is_stale(MessageId) ->
    ets:member(?SEEN, MessageId).

graft(MessageId) ->
    case ets:member(?SEEN, MessageId) of
        true -> {ok, MessageId};
        false -> {error, {not_found, MessageId}}
    end.

exchange(_Peer) ->
    ignore.

%% Retained for parity with the reference handler; not used by the broadcast
%% server while `claim/2' is exported.
merge(MessageId, Payload) ->
    case claim(MessageId, Payload) of
        true ->
            ok = handle_broadcast(MessageId, Payload),
            true;
        false ->
            false
    end.

%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================

init([]) ->
    ?SEEN = ets:new(?SEEN, [named_table, set, public]),
    ?APPLIED = ets:new(?APPLIED, [named_table, set, public]),
    ?CFG = ets:new(?CFG, [named_table, set, public]),
    {ok, #{}}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast({'$partisan_apply', MessageId, Payload}, State) ->
    ok = handle_broadcast(MessageId, Payload),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

cfg(Key, Default) ->
    case ets:lookup(?CFG, Key) of
        [{_, Value}] -> Value;
        [] -> Default
    end.
