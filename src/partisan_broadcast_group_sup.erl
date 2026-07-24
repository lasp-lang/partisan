%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Supervisor for Partisan's broadcast groups.
%%
%% Starts the groups declared by configuration (see
%% {@link partisan_broadcast:child_specs/0}) and supports adding or retiring
%% groups at runtime via `supervisor:start_child/2' and `terminate_child/2'
%% (driven by {@link partisan_broadcast:start_group/1} / `stop_group/1'). Each
%% child is an independent {@link partisan_plumtree_broadcast} instance, so one
%% group crashing or restarting does not disturb the others.
%% @end
%% =============================================================================
-module(partisan_broadcast_group_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% one_for_one: distinct, independently-restartable group children, known at
    %% boot from config and extendable at runtime.
    RestartStrategy = {one_for_one, 10, 10},
    {ok, {RestartStrategy, partisan_broadcast:child_specs()}}.
