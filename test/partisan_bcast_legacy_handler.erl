%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Legacy broadcast handler: implements only the pre-PDDR-000001 contract
%% (`merge/2', run synchronously on the broadcast process) and deliberately does
%% NOT export `claim/2'. Used to prove the broadcast server's backward-compatible
%% fallback path still delivers.
%% @end
%% =============================================================================
-module(partisan_bcast_legacy_handler).

-behaviour(gen_server).
-behaviour(partisan_plumtree_broadcast_handler).

%% control API
-export([start_link/0]).
-export([stop/0]).
-export([reset/0]).
-export([set_notify/1]).
-export([applied/1]).

%% partisan_plumtree_broadcast_handler callbacks (legacy set only)
-export([broadcast_data/1]).
-export([merge/2]).
-export([is_stale/1]).
-export([graft/1]).
-export([exchange/1]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(SEEN, partisan_bcast_legacy_seen).
-define(APPLIED, partisan_bcast_legacy_applied).
-define(CFG, partisan_bcast_legacy_cfg).

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

applied(MessageId) ->
    ets:member(?APPLIED, MessageId).

%% =============================================================================
%% HANDLER CALLBACKS
%% =============================================================================

broadcast_data(Payload) ->
    {Payload, Payload}.

%% Synchronous, on the broadcast process — the legacy behaviour.
merge(MessageId, _Payload) ->
    case ets:insert_new(?SEEN, {MessageId}) of
        true ->
            true = ets:insert(?APPLIED, {MessageId}),
            case cfg(notify, undefined) of
                undefined -> ok;
                Pid -> Pid ! {applied, MessageId}
            end,
            true;
        false ->
            false
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
