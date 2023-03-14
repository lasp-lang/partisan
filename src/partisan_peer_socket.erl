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

%% -----------------------------------------------------------------------------
%% @doc Wrapper that allows transparent usage of plain TCP or TLS socket
%% for peer connections.
%%
%% This module also implements the monotonic channel functionality.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_peer_socket).

%% this macro only exists in OTP-21 and above, where ssl_accept/2 is deprecated
-ifdef(OTP_RELEASE).
-define(ssl_accept(TCPSocket, TLSOpts), ssl:handshake(TCPSocket, TLSOpts)).
-else.
-define(ssl_accept(TCPSocket, TLSOpts), ssl:ssl_accept(TCPSocket, TLSOpts)).
-endif.


-record(partisan_peer_socket, {
    socket              :: gen_tcp:socket() | ssl:sslsocket() | socket:socket(),
    transport           :: gen_tcp | ssl,
    control             :: inet | ssl,
    monotonic = false   :: boolean()
}).

-type t()               :: #partisan_peer_socket{}.
-type reason()          :: closed | inet:posix().
-type options()         :: [gen_tcp:option()] | map().


-export_type([t/0]).


-export([accept/1]).
-export([close/1]).
-export([connect/3]).
-export([connect/4]).
-export([connect/5]).
-export([recv/2]).
-export([recv/3]).
-export([send/2]).
-export([setopts/2]).
-export([socket/1]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Wraps a TCP socket with the appropriate information for
%% transceiving on and controlling the socket later. If TLS/SSL is
%% enabled, this performs the socket upgrade/negotiation before
%% returning the wrapped socket.
%% @end
%% -----------------------------------------------------------------------------
-spec accept(gen_tcp:socket()) -> t().

accept(TCPSocket) ->
    case tls_enabled() of
        true ->
            TLSOpts = partisan_config:get(tls_server_options),
            %% as per http://erlang.org/doc/man/ssl.html#ssl_accept-1
            %% The listen socket is to be in mode {active, false} before
            %% telling the client that the server is ready to upgrade by
            %% calling this function, else the upgrade succeeds or does not
            %% succeed depending on timing.
            inet:setopts(TCPSocket, [{active, false}]),
            {ok, TLSSocket} = ?ssl_accept(TCPSocket, TLSOpts),
            %% restore the expected active once setting
            ssl:setopts(TLSSocket, [{active, once}]),
            #partisan_peer_socket{
                socket = TLSSocket,
                transport = ssl,
                control = ssl
            };
        _ ->
            #partisan_peer_socket{
                socket = TCPSocket,
                transport = gen_tcp,
                control = inet
            }
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @see gen_tcp:send/2
%% @see ssl:send/2
%% @end
%% -----------------------------------------------------------------------------
-spec send(t(), iodata()) -> ok | {error, reason()}.

send(#partisan_peer_socket{monotonic = false} = Conn, Data) ->
    Socket = Conn#partisan_peer_socket.socket,
    Transport = Conn#partisan_peer_socket.transport,
    send(Transport, Socket, Data);

send(#partisan_peer_socket{monotonic = true} = Conn, Data) ->
    Socket = Conn#partisan_peer_socket.socket,
    Transport = Conn#partisan_peer_socket.transport,

    %% Get the current process message queue length.
    {message_queue_len, MQLen} = process_info(self(), message_queue_len),

    %% Get last transmission time from process dictionary
    Time = get(last_transmission_time),

    %% Test for whether we should send or not.
    case monotonic_should_send(MQLen, Time) of
        false ->
            ok;
        true ->
            %% Update last transmission time on process dictionary
            put(last_transmission_time, monotonic_now()),
            send(Transport, Socket, Data)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @see gen_tcp:recv/2
%% @see ssl:recv/2
%% @end
%% -----------------------------------------------------------------------------
-spec recv(t(), integer()) -> {ok, iodata()} | {error, reason()}.

recv(Conn, Length) ->
    recv(Conn, Length, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @see gen_tcp:recv/3
%% @see ssl:recv/3
%% @end
%% -----------------------------------------------------------------------------
-spec recv(t(), integer(), timeout()) ->
    {ok, iodata()} | {error, reason()}.

recv(#partisan_peer_socket{socket = Socket, transport = Transport}, Length, Timeout) ->
    Transport:recv(Socket, Length, Timeout).


%% -----------------------------------------------------------------------------
%% @doc
%% @see inet:setopts/2
%% @see ssl:setopts/2
%% @end
%% -----------------------------------------------------------------------------
-spec setopts(t(), options()) -> ok | {error, inet:posix()}.

setopts(#partisan_peer_socket{} = Connection, Options) when is_map(Options) ->
    setopts(Connection, maps:to_list(Options));

setopts(#partisan_peer_socket{socket = Socket, control = Control}, Options) ->
    Control:setopts(Socket, Options).


%% -----------------------------------------------------------------------------
%% @doc
%% @see gen_tcp:close/1
%% @see ssl:close/1
%% @end
%% -----------------------------------------------------------------------------
-spec close(t()) -> ok.

close(#partisan_peer_socket{socket = Socket, transport = Transport}) ->
    Transport:close(Socket).


%% -----------------------------------------------------------------------------
%% @doc
%% @see gen_tcp:connect/3
%% @see ssl:connect/3
%% @end
%% -----------------------------------------------------------------------------
-spec connect(
    inet:socket_address() | inet:hostname(), inet:port_number(), options()) ->
    {ok, t()} | {error, inet:posix()}.

connect(Address, Port, Options) ->
    connect(Address, Port, Options, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec connect(
    inet:socket_address() | inet:hostname(),
    inet:port_number(),
    options(),
    timeout()) ->
    {ok, t()} | {error, inet:posix()}.

connect(Address, Port, Options, Timeout) ->
    connect(Address, Port, Options, Timeout, #{}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec connect(
    inet:socket_address() | inet:hostname(),
    inet:port_number(),
    options(),
    timeout(),
    map() | list()) -> {ok, t()} | {error, inet:posix()}.

connect(Address, Port, Options, Timeout, PartisanOptions)
when is_list(PartisanOptions) ->
    connect(Address, Port, Options, Timeout, maps:from_list(PartisanOptions));

connect(Address, Port, Options0, Timeout, PartisanOptions)
when is_map(PartisanOptions) ->
    Options = connection_options(Options0),

    case tls_enabled() of
        true ->
            TLSOptions = partisan_config:get(tls_client_options),
            do_connect(
                Address,
                Port,
                Options ++ TLSOptions,
                Timeout,
                ssl,
                ssl,
                PartisanOptions
            );
        _ ->
            do_connect(
                Address,
                Port,
                Options,
                Timeout,
                gen_tcp,
                inet,
                PartisanOptions
            )
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the wrapped socket from within the connection.
%% @end
%% -----------------------------------------------------------------------------
-spec socket(t()) -> gen_tcp:socket() | ssl:sslsocket().
socket(Conn) ->
    Conn#partisan_peer_socket.socket.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
do_connect(Address, Port, ConnectOpts, Timeout, Transport, Control, Opts) ->
   Monotonic = maps:get(monotonic, Opts, false),

   case Transport:connect(Address, Port, ConnectOpts, Timeout) of
       {ok, Socket} ->
            Connection = #partisan_peer_socket{
                socket = Socket,
                transport = Transport,
                control = Control,
                monotonic = Monotonic
            },
           {ok, Connection};
       Error ->
           Error
   end.


%% @private
connection_options(Options) when is_map(Options) ->
    connection_options(maps:to_list(Options));

connection_options(Options) when is_list(Options) ->
    Options ++ [{nodelay, true}].


%% @private
tls_enabled() ->
    partisan_config:get(tls).


%% @private
monotonic_now() ->
    erlang:monotonic_time(millisecond).

%% @private
send(Transport, Socket, Data) ->
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
    case MessageQueueLen > 0 of
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
