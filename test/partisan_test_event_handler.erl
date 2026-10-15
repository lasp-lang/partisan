%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc A `partisan_gen_event' handler that reports every event it receives to
%% a process fixed when the handler is added. Used by
%% `partisan_SUITE:gen_event_notify_test/1'.
%% @end
%% =============================================================================
-module(partisan_test_event_handler).

-behaviour(partisan_gen_event).

-export([start/1]).
-export([init/1]).
-export([handle_event/2]).
-export([handle_call/2]).


%% Starts a manager registered as this module, with this handler added. The
%% manager is not linked to the caller, which is a short-lived rpc process.
start(ReportTo) ->
    {ok, Pid} = partisan_gen_event:start({local, ?MODULE}),
    ok = partisan_gen_event:add_handler(?MODULE, ?MODULE, ReportTo),
    {ok, Pid}.

init(ReportTo) ->
    {ok, ReportTo}.

handle_event(Event, ReportTo) ->
    ReportTo ! {?MODULE, Event},
    {ok, ReportTo}.

handle_call(_Request, ReportTo) ->
    {ok, ok, ReportTo}.
