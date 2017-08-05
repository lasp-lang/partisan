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

-export([start_link/3]).

%% gen_server api

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

%% public api

start_link(Label, PeerIP, PeerPort) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Label, PeerIP, PeerPort], []).

%% gen_server api

init([Label, PeerIP, PeerPort]) ->
    AcceptorPoolSize = application:get_env(partisan, acceptor_pool_size, 10),
    % Trapping exit so can close socket in terminate/2
    _ = process_flag(trap_exit, true),
    Opts = [{active, once}, {mode, binary}, {ip, PeerIP}, {packet, 4},
            {reuseaddr, true}, {nodelay, true}, {keepalive, true}],
    case gen_tcp:listen(PeerPort, Opts) of
        {ok, Socket} ->
            % acceptor could close the socket if there is a problem
            MRef = monitor(port, Socket),
            partisan_pool:accept_socket(Socket, AcceptorPoolSize),
            {ok, {Label, Socket, MRef}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(Req, _, State) ->
    {stop, {bad_call, Req}, State}.

handle_cast(Req, State) ->
    {stop, {bad_cast, Req}, State}.

handle_info({'DOWN', MRef, port, Socket, Reason}, {_Label, Socket, MRef} = State) ->
    {stop, Reason, State};
handle_info(_, State) ->
    {noreply, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_, {_Label, Socket, MRef}) ->
    % Socket may already be down but need to ensure it is closed to avoid
    % eaddrinuse error on restart
    case demonitor(MRef, [flush, info]) of
        true  ->
            gen_tcp:close(Socket);
        false ->
            ok
    end.
