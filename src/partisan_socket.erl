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

-module(partisan_socket).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% public api

-export([start_link/2]).

%% gen_server api

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

%% public api

start_link(PeerIP, PeerPort) ->
    gen_server:start_link(?MODULE, [PeerIP, PeerPort], []).

%% gen_server api

init([PeerIP, PeerPort]) ->
    AcceptorPoolSize = application:get_env(partisan, acceptor_pool_size, 10),
    % Trapping exit so can close socket in terminate/2
    _ = process_flag(trap_exit, true),
    Opts = [{active, once}, {mode, binary}, {ip, PeerIP}, {packet, 4},
            {reuseaddr, true}, {nodelay, true}, {keepalive, true}],
    case gen_tcp:listen(PeerPort, Opts) of
        {ok, Socket} ->
            % if the port is to be system allocated we need to set
            % it in the config
            ok = maybe_update_port_config(PeerIP, PeerPort, Socket),
            % acceptor could close the socket if there is a problem
            MRef = monitor(port, Socket),
            partisan_pool:accept_socket(Socket, AcceptorPoolSize),
            {ok, {Socket, MRef}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(Req, _, State) ->
    {stop, {bad_call, Req}, State}.

handle_cast(Req, State) ->
    {stop, {bad_cast, Req}, State}.

handle_info({'DOWN', MRef, port, Socket, Reason}, {Socket, MRef} = State) ->
    {stop, Reason, State};
handle_info(_, State) ->
    {noreply, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_, {Socket, _MRef}) ->
    % Socket may already be down but need to ensure it is closed to avoid
    % eaddrinuse error on restart
    _ = (catch gen_tcp:close(Socket)),
    ok.

%% private
maybe_update_port_config(PeerIP, 0, Socket) ->
    case inet:sockname(Socket) of
        {ok, {_IPAddress, Port}} ->
            lager:info("partisan listening on peer ~p, system allocated port ~p",
                       [PeerIP, Port]),
            partisan_config:set(peer_port, Port),
            % search the listen addrs map for the provided ip Address
            % and update the port key
            ListenAddrs0 = partisan_config:get(listen_addrs),
            ListenAddrs = lists:map(fun(#{ip := IP} = Map) when PeerIP =:= IP ->
                                        maps:update(port, Port, Map);
                                       (Map) -> Map
                                    end, ListenAddrs0),
            partisan_config:set(listen_addrs, ListenAddrs),
            ok;
        _ -> ok
    end;
maybe_update_port_config(_, _, _) -> ok.
