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

-define(CHILD(I, Type, Timeout),
        {I, {I, start_link, []}, permanent, Timeout, Type, [I]}).
-define(CHILD(I, Type), ?CHILD(I, Type, 5000)).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    partisan_config:init(),
    Manager = partisan_peer_service:manager(),

    Children = lists:flatten(
                 [
                 ?CHILD(partisan_rpc_backend, worker),
                 ?CHILD(partisan_acknowledgement_backend, worker),
                 ?CHILD(partisan_orchestration_backend, worker),
                 ?CHILD(Manager, worker),
                 ?CHILD(partisan_peer_service_events, worker),
                 ?CHILD(partisan_plumtree_backend, worker),
                 ?CHILD(partisan_plumtree_broadcast, worker),
                 ?CHILD(partisan_monitor, worker)
                 ]),

    %% Run a single backend for each label.
    CausalLabels = partisan_config:get(causal_labels, []),

    CausalBackendFun = fun(Label) ->
        {partisan_causality_backend,
         {partisan_causality_backend, start_link, [Label]},
          permanent, 5000, worker, [partisan_causality_backend]}
    end,

    CausalBackends = lists:map(CausalBackendFun, CausalLabels),
    ?LOG_INFO(#{
        description => "Partisan listening",
        ip_address => partisan_config:get(peer_ip),
        port_number => partisan_config:get(peer_port),
        listen_addr => partisan_config:get(listen_addrs)
    }),

    %% Open connection pool.
    PoolSup = {partisan_pool_sup,
               {partisan_pool_sup, start_link, []},
                permanent, 20000, supervisor, [partisan_pool_sup]},

    %% Initialize the connection cache supervised by the supervisor.
    ?CACHE = ets:new(?CACHE, [public, named_table, set, {read_concurrency, true}]),

    %% Initialize the plumtree outstanding messages table
    %% supervised by the supervisor.
    %% The table is used by partisan_plumtree_broadcast and maps a nodename()
    %% to set of outstanding messages. It uses a duplicate_bag to quickly
    %% delete all messages for a nodename.
    ?PLUMTREE_OUTSTANDING = ets:new(
        ?PLUMTREE_OUTSTANDING,
        [public, named_table, duplicate_bag, {read_concurrency, true}]
    ),

    RestartStrategy = {one_for_one, 10, 10},
    {ok, {RestartStrategy, Children ++ CausalBackends ++ [PoolSup]}}.