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

-include("partisan.hrl").

-export([start_link/0]).

-export([init/1]).

-define(CHILD(I, Type, Timeout),
        {I, {I, start_link, []}, permanent, Timeout, Type, [I]}).
-define(CHILD(I, Type), ?CHILD(I, Type, 5000)).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = lists:flatten(
                 [
                 ?CHILD(partisan_default_peer_service_manager, worker),
                 ?CHILD(partisan_hyparview_peer_service_manager, worker),
                 ?CHILD(partisan_peer_service_events, worker),
                 ?CHILD(ranch_sup, supervisor)
                 ]),

    %% Configure peer service manager.
    case partisan_config:get(partisan_peer_service_manager, undefined) of
        undefined ->
            partisan_config:set(partisan_peer_service_manager,
                                partisan_default_peer_service_manager);
        _ ->
            %% Use previously configured settings.
            ok
    end,

    %% Configure fanout.
    partisan_config:set(fanout, 5),

    %% Configure interval.
    partisan_config:set(gossip_interval, 10000),

    PeerConfig = partisan_config:peer_config(),
    lager:info("Initializing listener for peer protocol; config: ~p",
               [PeerConfig]),

    ListenerSpec = ranch:child_spec(?PEER_SERVICE_SERVER,
                                    10,
                                    ranch_tcp,
                                    PeerConfig,
                                    ?PEER_SERVICE_SERVER,
                                    []),
    Listeners = [ListenerSpec],

    RestartStrategy = {one_for_one, 10, 10},
    {ok, {RestartStrategy, Children ++ Listeners}}.
