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
    %% override Erlang env configuration with OS var configurations if set
    IPAddress = case os:getenv("IP", "false") of
                    "false" ->
                        ?PEER_IP;
                    IP ->
                        {ok, ParsedIP} = inet_parse:address(IP),
                        ParsedIP
                end,

    PeerPort = case os:getenv("PEER_PORT", "false") of
                   "false" ->
                       random_port();
                   PeerPortList ->
                       list_to_integer(PeerPortList)
               end,

    DefaultPeerService = application:get_env(partisan,
                                             partisan_peer_service_manager,
                                             partisan_client_server_peer_service_manager),
    PeerService = case os:getenv("PEER_SERVICE", "false") of
                      "false" ->
                          DefaultPeerService;
                      PeerServiceList ->
                          list_to_atom(PeerServiceList)
                  end,

    DefaultTag = case os:getenv("TAG", "false") of
                    "false" ->
                        undefined;
                    TagList ->
                         list_to_atom(TagList)
                end,

    [env_or_default(Key, Default) ||
        {Key, Default} <- [{arwl, 6},
                           {prwl, 6},
                           {connect_disterl, false},
                           {fanout, ?FANOUT},
                           {gossip_interval, 10000},
                           {max_active_size, 6},
                           {max_passive_size, 30},
                           {min_active_size, 3},
                           {partisan_peer_service_manager, PeerService},
                           {peer_ip, IPAddress},
                           {peer_port, PeerPort},
                           {random_promotion, true},
                           {reservations, []},
                           {tls, false},
                           {tls_options, []},
                           {tag, DefaultTag}]],
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

%% @private
random_port() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, {_, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    Port.
