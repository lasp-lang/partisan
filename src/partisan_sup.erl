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
    %% Initialize the listener for the peer protocol.
    {ok, Pid} = case listen() of
        {ok, NewPid} ->
            lager:info("Listener started!"),
            {ok, NewPid};
        {error, {already_started, _OldPid}} ->
            %% Weird interaction here.
            %%
            %% Lasp supervises Partisan which launches the ranch
            %% listener through Partisan's supervisor.  However, when CT
            %% is running the test suite and it restarts Lasp, it
            %% restarts Partisan as part of this, but ranch stays
            %% running.
            %%
            %% @todo Figure out how to properly supervise ranch from
            %% Partisan, so when ranch restarts, Partisan also
            %% restarts as well.
            %%
            lager:info("Listener already running; restarting on new port!"),
            ranch:stop_listener(?PEER_SERVICE_SERVER),
            listen()
    end,
    link(Pid),

    Children = lists:flatten(
                 [
                 ?CHILD(partisan_peer_service_manager, worker),
                 ?CHILD(partisan_peer_service_gossip, worker),
                 ?CHILD(partisan_peer_service_events, worker)
                 ]),

    RestartStrategy = {one_for_one, 10, 10},
    {ok, {RestartStrategy, Children}}.

%% @private
listen() ->
    PeerConfig = partisan_config:peer_config(),
    lager:info("Initializing listener for peer protocol; config: ~p",
               [PeerConfig]),
    ranch:start_listener(?PEER_SERVICE_SERVER,
                         10,
                         ranch_tcp,
                         PeerConfig,
                         ?PEER_SERVICE_SERVER,
                         []).
