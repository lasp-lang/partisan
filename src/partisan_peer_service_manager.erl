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



-export([myself/0, 
         mynode/0, 
         forward_message/2]).

-callback start_link() -> {ok, pid()} | ignore | {error, term()}.
-callback members() -> [name()]. %% TODO: Deprecate me.
-callback members_for_orchestration() -> [node_spec()].
-callback myself() -> node_spec().

-callback get_local_state() -> term().

-callback join(node_spec()) -> ok.
-callback sync_join(node_spec()) -> ok | {error, not_implemented}.
-callback leave() -> ok.
-callback leave(node_spec()) -> ok.

-callback update_members([node()]) -> ok | {error, not_implemented}.

-callback send_message(name(), message()) -> ok.
-callback receive_message(name(), message()) -> ok.

-callback forward_message({partisan_remote_reference, name(), atom()}, message()) -> ok.

-callback cast_message(name(), pid(), message()) -> ok.
-callback forward_message(name(), pid(), message()) -> ok.

-callback cast_message(name(), channel(), pid(), message()) -> ok.
-callback forward_message(name(), channel(), pid(), message()) -> ok.

-callback cast_message(name(), channel(), pid(), message(), options()) -> ok.
-callback forward_message(name(), channel(), pid(), message(), options()) -> ok.

-callback on_down(name(), function()) -> ok | {error, not_implemented}.
-callback on_up(name(), function()) -> ok | {error, not_implemented}.

-callback decode(term()) -> term().

-callback reserve(atom()) -> ok | {error, no_available_slots}.

-callback partitions() -> {ok, partitions()} | {error, not_implemented}.
-callback inject_partition(node_spec(), ttl()) -> {ok, reference()} | {error, not_implemented}.
-callback resolve_partition(reference()) -> ok | {error, not_implemented}.

-spec myself() -> node_spec().

myself() ->
    Parallelism = partisan_config:get(parallelism, ?PARALLELISM),
    Channels = partisan_config:get(channels, ?CHANNELS),
    Name = partisan_config:get(name),
    ListenAddrs = partisan_config:get(listen_addrs),
    #{name => Name, listen_addrs => ListenAddrs, channels => Channels, parallelism => Parallelism}.

mynode() ->
    partisan_config:get(name, node()).

forward_message({partisan_remote_reference, Name, ServerRef} = RemotePid, Message) ->
    case mynode() of
        Name ->
            logger:info("Local pid ~p, routing message accordingly: ~p", [ServerRef, Message]),
            case ServerRef of
                {partisan_process_reference, Pid} ->
                    DeserializedPid = list_to_pid(Pid),
                    DeserializedPid ! Message;
                _ ->
                    ServerRef ! Message
            end;
        _ ->
            Manager = partisan_config:get(partisan_peer_service_manager),
            Manager:forward_message(RemotePid, Message)
    end.