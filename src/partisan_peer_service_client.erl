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

-module(partisan_peer_service_client).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {socket}).

-include("partisan.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Peer) ->
    gen_server:start_link(?MODULE, [Peer], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([iolist()]) -> {ok, #state{}}.
init([Peer]) ->
    {ok, Socket} = connect(Peer),
    {ok, #state{socket=Socket}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.

%% @private
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.

%% @private
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.

handle_info({tcp, _Socket, Data}, State0) ->
    handle_message(decode(Data), State0);
handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};
handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, #state{socket=Socket}) ->
    ok = gen_tcp:close(Socket),
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Test harness specific.
%%
%% If we're running a local test, we have to use the same IP address for
%% every bind operation, but a different port instead of the standard
%% port.
%%
connect(Peer) when is_atom(Peer) ->
    %% Bootstrap with disterl.
    PeerPort = rpc:call(Peer,
                        partisan_config,
                        get,
                        [peer_port, ?PEER_PORT]),
    lager:info("Peer ~p running on port ~p", [Peer, PeerPort]),
    connect({Peer, {127, 0, 0, 1}, PeerPort});

%% @doc Connect to remote peer.
connect({Name, {_, _, _, _}=IPAddress, Port}) ->
    Options = [binary, {packet, 2}, {keepalive, true}],
    lager:info("Connecting to ~p ~p ~p", [Name, IPAddress, Port]),
    {ok, Socket} = gen_tcp:connect(IPAddress, Port, Options),
    lager:info("Connected!"),
    {ok, Socket}.

%% @private
decode(Message) ->
    binary_to_term(Message).

%% @private
handle_message({hello, _Node}, #state{socket=Socket}=State) ->
    ok = gen_tcp:send(Socket, encode({hello, node()})),
    {noreply, State};
handle_message(Message, State) ->
    lager:info("Invalid message: ~p", [Message]),
    {stop, normal, State}.

%% @private
encode(Message) ->
    term_to_binary(Message).
