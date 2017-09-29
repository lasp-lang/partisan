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
         listen_addrs/0,
         set/2,
         get/1,
         get/2]).

init() ->
    DefaultPeerService = application:get_env(partisan,
                                             partisan_peer_service_manager,
                                             partisan_default_peer_service_manager),

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
                        Tag = list_to_atom(TagList),
                        application:set_env(partisan, tag, Tag),
                        Tag
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
                           {peer_ip, get_node_address()},
                           {peer_port, random_port()},
                           {random_promotion, true},
                           {reservations, []},
                           {tls, false},
                           {tls_options, []},
                           {tag, DefaultTag}]],
    set(listen_addrs, [#{ip => ?MODULE:get(peer_ip), port => ?MODULE:get(peer_port)}]),
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

set(peer_ip, Value) when is_list(Value) ->
    {ok, ParsedIP} = inet_parse:address(Value),
    set(peer_ip, ParsedIP);
set(Key, Value) ->
    application:set_env(?APP, Key, Value),
    partisan_mochiglobal:put(Key, Value).

listen_addrs() ->
    partisan_config:get(listen_addrs).

%% @private
random_port() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, {_, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    Port.

%% @private
get_node_address() ->
    Name = atom_to_list(node()),
    [_Name, FQDN] = string:tokens(Name, "@"),
    case inet:getaddr(FQDN, inet) of
        {ok, Address} ->
            lager:info("Resolved ~p to ~p", [Name, Address]),
            ?PEER_IP;
        {error, Error} ->
            lager:info("Cannot resolve local name ~p, resulting to 127.0.0.1: ~p", [FQDN, Error]),
            ?PEER_IP
    end.