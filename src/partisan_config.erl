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

-export([init/0,
         set/2,
         get/1,
         get/2]).

init() ->
    %% Generate a random peer port.
    rand_compat:seed(erlang:phash2([node()]),
                     erlang:monotonic_time(),
                     erlang:unique_integer()),
    RandomPeerPort = rand_compat:uniform(1000) + 10000,

    [env_or_default(Key, Default) ||
        {Key, Default} <- [{arwl, 6},
                           {prwl, 6},
                           {connect_disterl, false},
                           {fanout, ?FANOUT},
                           {gossip_interval, 10000},
                           {max_active_size, 6},
                           {max_passive_size, 30},
                           {min_active_size, 3},
                           {partisan_peer_service_manager,
                            partisan_default_peer_service_manager},
                           {peer_ip, ?PEER_IP},
                           {peer_port, RandomPeerPort},
                           {random_promotion, true},
                           {reservations, []},
                           {tag, undefined}]],
    ok.

env_or_default(Key, Default) ->
    case application:get_env(partisan, Key) of
        {ok, Value} ->
            set(Key, Value);
        undefined ->
            set(Key, Default)
    end.

get(Key) ->
    partisan_mochiglobal:get(Key).

get(Key, Default) ->
    partisan_mochiglobal:get(Key, Default).

set(Key, Value) ->
    application:set_env(?APP, Key, Value),
    partisan_mochiglobal:put(Key, Value).
