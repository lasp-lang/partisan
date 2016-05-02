%% -------------------------------------------------------------------
%%
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

-module(partisan_config).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-export([set/2,
         get/2,
         peer_config/0]).

get(Key, Default) ->
    mochiglobal:get(Key, Default).

set(Key, Value) ->
    application:set_env(?APP, Key, Value),
    mochiglobal:put(Key, Value).

peer_config() ->
    %% Generate a random peer port.
    random:seed(erlang:phash2([node()]),
                erlang:monotonic_time(),
                erlang:unique_integer()),
    RandomPeerPort = random:uniform(1000) + 10000,

    %% Choose either static port or fall back to random peer port.
    DCOS = os:getenv("DCOS", "false"),
    PeerPort = case DCOS of
        "false" ->
            RandomPeerPort;
        _ ->
            application:get_env(?APP, peer_port, RandomPeerPort)
    end,

    %% Make sure configuration has current peer port.
    partisan_config:set(peer_port, PeerPort),

    Config = [{port, PeerPort}],
    lager:info("Peer Protocol Configuration: ~p", [Config]),
    Config.
