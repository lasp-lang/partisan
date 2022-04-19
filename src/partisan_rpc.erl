%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Christopher S. Meiklejohn. All Rights Reserved.
%% Copyright (c) 2022 Alejandro M. Ramallo. All Rights Reserved.
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

-module(partisan_rpc).

-include("partisan.hrl").

%% API
-export([call/5]).



%% =============================================================================
%% API
%% =============================================================================



call(Name, Module, Function, Arguments, Timeout) ->
    %% Make call.
    Manager = partisan_config:get(partisan_peer_service_manager),
    Self = self(),
    Options = partisan_config:get(forward_options, #{}),
    RpcChannel = rpc_channel(),
    OurName = partisan:node(),
    Manager:forward_message(Name, RpcChannel, ?MODULE, {call, Module, Function, Arguments, Timeout, {origin, OurName, Self}}, Options),

    %% Wait for response.
    receive
        {response, Response} ->
            Response
    after
        Timeout ->
            {badrpc, timeout}
    end.



%% =============================================================================
%% PRIVATE
%% =============================================================================



rpc_channel() ->
    Channels = partisan_config:get(channels),
    case lists:member(?RPC_CHANNEL, Channels) of
        true ->
            ?RPC_CHANNEL;
        false ->
            ?DEFAULT_CHANNEL
    end.