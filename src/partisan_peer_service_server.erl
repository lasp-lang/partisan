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

-module(partisan_peer_service_server).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-behaviour(ranch_protocol).
-behaviour(gen_server).

%% ranch_protocol callbacks.
-export([start_link/4,
         init/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {socket, transport}).

%%%===================================================================
%%% ranch_protocol callbacks
%%%===================================================================

%% @private
start_link(ListenerPid, Socket, Transport, Options) ->
    proc_lib:start_link(?MODULE,
                        init,
                        [ListenerPid, Socket, Transport, Options]).

%% @private
init(ListenerPid, Socket, Transport, _Options) ->
    %% Acknowledge process initialization.
    ok = proc_lib:init_ack({ok, self()}),

    %% Acknowledge the connection.
    ok = ranch:accept_ack(ListenerPid),

    %% Link to the socket.
    link(Socket),

    %% Set the socket modes.
    ok = inet:setopts(Socket, [{packet, 2}, {active, true}]),

    %% Generate the welcome message, encode it and transmit the message.
    send_message(Socket, Transport, {hello, node()}),

    %% Enter the gen_server loop.
    gen_server:enter_loop(?MODULE,
                          [],
                          #state{socket=Socket, transport=Transport}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([ListenerPid, Socket, Transport, Options]) ->
    init(ListenerPid, Socket, Transport, Options).

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
terminate(_Reason, #state{socket=Socket, transport=Transport}) ->
    Transport:close(Socket),
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_message({hello, Node},
               #state{socket=Socket, transport=Transport}=State) ->
    %% Connect the node with Distributed Erlang, just for now for
    %% control messaging in the test suite execution.
    case net_adm:ping(Node) of
        pong ->
            lager:info("Node ~p connected to ~p via disterl.",
                       [Node, node()]),
            {noreply, State};
        pang ->
            lager:info("Node could not be connected."),
            send_message(Socket, Transport, {error, pang}),
            {noreply, State}
    end;
handle_message(Message, State) ->
    partisan_peer_service_manager:receive_message(Message),
    {stop, normal, State}.

%% @private
send_message(Socket, Transport, Message) ->
    EncodedMessage = encode(Message),
    Transport:send(Socket, EncodedMessage).

%% @private
encode(Message) ->
    term_to_binary(Message).

%% @private
decode(Message) ->
    binary_to_term(Message).
