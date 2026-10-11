%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc A `partisan_gen_statem' that reports every cast it receives to a
%% process fixed at start. Used by `partisan_SUITE:gen_statem_cast_test/1'.
%%
%% The report target is passed in at start, over disterl, and not inside the
%% cast. A pid that travels inside a cast is subject to Partisan's pid encoding,
%% which is not what the test is about.
%% @end
%% =============================================================================
-module(partisan_test_statem).

-behaviour(partisan_gen_statem).

-export([start/1]).
-export([init/1]).
-export([callback_mode/0]).
-export([handle_event/4]).


start(ReportTo) ->
    partisan_gen_statem:start({local, ?MODULE}, ?MODULE, ReportTo, []).

init(ReportTo) ->
    {ok, ready, ReportTo}.

callback_mode() ->
    handle_event_function.

handle_event(cast, Tag, _State, ReportTo) ->
    ReportTo ! {?MODULE, Tag},
    keep_state_and_data.
