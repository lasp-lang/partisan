%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher Meiklejohn.  All Rights Reserved.
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

%% @doc Wrapper for peer connections that allows transparent usage of
%% plain TCP or TLS/SSL.
-module(partisan_peer_connection).

-export([
         accept/1,
         close/1,
         connect/3,
         connect/4,
         recv/2,
         recv/3,
         send/2,
         setopts/2,
         socket/1
        ]).

-type reason() :: closed | inet:posix().
-type options() :: [ssl:option()].
-record(connection, {
          socket :: gen_tcp:socket() | ssl:sslsocket(),
          transport :: gen_tcp | ssl,
          control :: inet | ssl,
          monotonic :: boolean()
         }).

-type connection() :: #connection{}.
-export_type([connection/0]).

%% @doc Wraps a TCP socket with the appropriate information for
%% transceiving on and controlling the socket later. If TLS/SSL is
%% enabled, this performs the socket upgrade/negotiation before
%% returning the wrapped socket.
-spec accept(gen_tcp:socket()) -> connection().
accept(TCPSocket) ->
    case tls_enabled() of
        true ->
            TLSOpts = tls_options(),
            %% as per http://erlang.org/doc/man/ssl.html#ssl_accept-1
            %% The listen socket is to be in mode {active, false} before telling the client
            %% that the server is ready to upgrade by calling this function, else the upgrade
            %% succeeds or does not succeed depending on timing.
            inet:setopts(TCPSocket, [{active, false}]),
            {ok, TLSSocket} = ssl:ssl_accept(TCPSocket, TLSOpts),
            %% restore the expected active once setting
            ssl:setopts(TLSSocket, [{active, once}]),
            #connection{socket = TLSSocket, transport = ssl, control = ssl, monotonic = false};
        _ ->
            #connection{socket = TCPSocket, transport = gen_tcp, control = inet, monotonic = false}
    end.

%% @see gen_tcp:send/2
%% @see ssl:send/2
-spec send(connection(), iodata()) -> ok | {error, reason()}.
send(#connection{socket = Socket, transport = Transport, monotonic = true}, Data) ->
    %% Get the current message queue length.
    {message_queue_len, MessageQueueLen} = process_info(self(), message_queue_len),

    %% Get last transmission time.
    LastTransmissionTime = get(last_transmission_time),

    %% Test for whether we should send or not.
    case monotonic_should_send(MessageQueueLen, LastTransmissionTime) of
        false ->
            ok;
        true ->
            send(Transport, Socket, Data)
    end;
send(#connection{socket = Socket, transport = Transport, monotonic = false}, Data) ->
    send(Transport, Socket, Data).

%% @see gen_tcp:recv/2
%% @see ssl:recv/2
-spec recv(connection(), integer()) -> {ok, iodata()} | {error, reason()}.
recv(Conn, Length) ->
    recv(Conn, Length, infinity).

%% @see gen_tcp:recv/3
%% @see ssl:recv/3
-spec recv(connection(), integer(), timeout()) -> {ok, iodata()} | {error, reason()}.
recv(#connection{socket = Socket, transport = Transport}, Length, Timeout) ->
    Transport:recv(Socket, Length, Timeout).

%% @see inet:setopts/2
%% @see ssl:setopts/2
-spec setopts(connection(), options()) -> ok | {error, inet:posix()}.
setopts(#connection{socket = Socket, control = Control}, Options) ->
    Control:setopts(Socket, Options).

%% @see gen_tcp:close/1
%% @see ssl:close/1
-spec close(connection()) -> ok.
close(#connection{socket = Socket, transport = Transport}) ->
    Transport:close(Socket).

%% @see gen_tcp:connect/3
%% @see ssl:connect/3
-spec connect(inet:socket_address() | inet:hostname(), inet:port_number(), options()) -> {ok, connection()} | {error, inet:posix()}.
connect(Address, Port, Options) ->
    connect(Address, Port, Options, infinity).

-spec connect(inet:socket_address() | inet:hostname(), inet:port_number(),  options(), timeout()) -> {ok, connection()} | {error, inet:posix()}.
connect(Address, Port, Options, Timeout) ->
    case tls_enabled() of
        true ->
            TLSOptions = tls_options(),
            do_connect(Address, Port, Options ++ TLSOptions, Timeout, ssl, ssl);
        _ ->
            do_connect(Address, Port, Options, Timeout, gen_tcp, inet)
    end.

%% @doc Returns the wrapped socket from within the connection.
-spec socket(connection()) -> gen_tcp:socket() | ssl:ssl_socket().
socket(Conn) ->
    Conn#connection.socket.

%% @private
do_connect(Address, Port, Options, Timeout, Transport, Control) ->
   case Transport:connect(Address, Port, Options, Timeout) of
       {ok, Socket} ->
           {ok, #connection{socket = Socket, transport = Transport, control = Control, monotonic = false}};
       Error ->
           Error
   end.

%% @private
tls_enabled() ->
    partisan_config:get(tls).

%% @private
tls_options() ->
    partisan_config:get(tls_options).

%% @private
monotonic_now() ->
    erlang:monotonic_time(millisecond).

%% @private
send(Transport, Socket, Data) ->
    %% Update last transmission time.
    put(last_transmission_time, monotonic_now()),

    %% Transmit the data on the socket.
    Transport:send(Socket, Data).

%% Determine if we should transmit:
%%
%% If there's another message in the queue, we can skip 
%% sending this message.  However, if the arrival rate of 
%% messages is too high, we risk starvation where
%% we may never send.  Therefore, we must force a transmission 
%% after a given period with no transmissions.
%%
%% @private
monotonic_should_send(MessageQueueLen, LastTransmissionTime) ->
    case length(MessageQueueLen) > 0 of
        true ->
            %% Messages in queue; conditional send.
            NowTime = monotonic_now(),

            Diff = abs(NowTime - LastTransmissionTime),

            SendWindow = partisan_config:get(send_window, 1000),

            Diff > SendWindow;
        false ->
            %% No messages in queue; transmit.
            true
    end.