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

-module(partisan_peer_service_manager).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-export([myself/0]).

-callback start_link() -> {ok, pid()} | ignore | {error, term()}.
-callback members() -> [name()].
-callback myself() -> node_spec().

-callback get_local_state() -> term().

-callback join(node_spec()) -> ok.
-callback leave() -> ok.
-callback leave(node_spec()) -> ok.

-callback update_members([node()]) -> ok | {error, not_implemented}.

-callback send_message(name(), message()) -> ok.
-callback receive_message(message()) -> ok.
-callback forward_message(name(), pid(), message()) -> ok.

-callback on_down(name(), function()) -> ok | {error, not_implemented}.

-callback decode(term()) -> term().

-callback reserve(atom()) -> ok | {error, no_available_slots}.

-callback partitions() -> {ok, partitions()} | {error, not_implemented}.
-callback inject_partition(node_spec(), ttl()) -> {ok, reference()} | {error, not_implemented}.
-callback resolve_partition(reference()) -> ok | {error, not_implemented}.

-spec myself() -> node_spec().

myself() ->
    Parallelism = partisan_config:get(parallelism, ?PARALLELISM),
    ListenAddrs = partisan_config:get(listen_addrs, ?LISTEN_ADDRS),
    #{name => node(), listen_addrs => ListenAddrs, parallelism => Parallelism}.
