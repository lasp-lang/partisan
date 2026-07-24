%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Helium Systems, Inc.  All Rights Reserved.
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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

-module(partisan_sup).

-behaviour(supervisor).

-include("partisan_logger.hrl").
-include("partisan.hrl").

-export([start_link/0]).

-export([init/1]).

-ifdef(TEST).
-define(CHILDREN, [?WORKER(partisan_test_server, [], permanent, 5000)]).
-else.
-define(CHILDREN, []).
-endif.

-define(SUPERVISOR(Id, Args, Restart, Timeout), #{
    id => Id,
    start => {Id, start_link, Args},
    restart => Restart,
    shutdown => Timeout,
    type => supervisor,
    modules => [Id]
}).

-define(WORKER(Id, Args, Restart, Timeout), #{
    id => Id,
    start => {Id, start_link, Args},
    restart => Restart,
    shutdown => Timeout,
    type => worker,
    modules => [Id]
}).

-define(EVENT_MANAGER(Id, Restart, Timeout), #{
    id => Id,
    start => {gen_event, start_link, [{local, Id}]},
    restart => Restart,
    shutdown => Timeout,
    type => worker,
    modules => [dynamic]
}).

%% =============================================================================
%% API
%% =============================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% =============================================================================
%% SUPERVISO CALLBACKS
%% =============================================================================

init([]) ->
    %% Make sure we always call this function first.
    partisan_config:init(),

    Children = lists:flatten([
        ?WORKER(partisan_inet, [], permanent, 5000),
        ?WORKER(partisan_rpc_backend, [], permanent, 5000),
        ?WORKER(partisan_acknowledgement_backend, [], permanent, 5000),
        ?WORKER(partisan_orchestration_backend, [], permanent, 5000),
        ?SUPERVISOR(partisan_peer_service_sup, [], permanent, infinity),
        %% The peer service (and its membership snapshot) must be started before
        %% the broadcast handlers and groups.
        ?WORKER(partisan_plumtree_backend, [], permanent, 5000),
        ?SUPERVISOR(partisan_broadcast_group_sup, [], permanent, infinity)
        | ?CHILDREN
    ]),

    %% Run a single backend for each label.
    CausalLabels = partisan_config:get(causal_labels, []),

    CausalBackendFun = fun(Label) ->
        {partisan_causality_backend,
            {partisan_causality_backend, start_link, [Label]}, permanent, 5000,
            worker, [partisan_causality_backend]}
    end,

    CausalBackends = lists:map(CausalBackendFun, CausalLabels),

    %% Lock-free membership snapshot (see partisan_membership). Owned by this
    %% supervisor so it survives peer service manager restarts.
    ?PARTISAN_MEMBERS = ets:new(
        ?PARTISAN_MEMBERS,
        [public, named_table, set, {read_concurrency, true}]
    ),

    %% Open connection pool.
    %% This MUST be the last child to be started
    PoolSup = {
        partisan_acceptor_socket_pool_sup,
        {partisan_acceptor_socket_pool_sup, start_link, []},
        permanent,
        20000,
        supervisor,
        [partisan_acceptor_socket_pool_sup]
    },

    RestartStrategy = {one_for_one, 10, 10},

    AllChildren = Children ++ CausalBackends ++ [PoolSup],

    {ok, {RestartStrategy, AllChildren}}.
