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
-include("partisan_logger.hrl").
-include("partisan_peer_socket.hrl").

-behaviour(acceptor).
-behaviour(gen_server).

%% acceptor api
-export([acceptor_init/3,
         acceptor_continue/3,
         acceptor_terminate/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    socket      ::  partisan_peer_socket:t(),
    channel     ::  partisan:channel() | undefined,
    ref         ::  reference()
}).

-type state_t() :: #state{}.

acceptor_init(_SockName, LSocket, []) ->
    %% monitor listen socket to gracefully close when it closes
    MRef = monitor(port, LSocket),
    {ok, MRef}.

acceptor_continue(_PeerName, Socket0, MRef) ->
    put({?MODULE, ingress_delay}, partisan_config:get(ingress_delay, 0)),
    Socket = partisan_peer_socket:accept(Socket0),
    send_message(Socket, {hello, partisan:node()}),
    gen_server:enter_loop(?MODULE, [], #state{socket = Socket, ref = MRef}).

acceptor_terminate(Reason, _) ->
    %% Something went wrong. Either the acceptor_pool is terminating
    %% or the accept failed.
    exit(Reason).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init(_) ->
    {stop, acceptor}.


handle_call(Req, _, State) ->
    {stop, {bad_call, Req}, State}.


handle_cast(Req, State) ->
    {stop, {bad_cast, Req}, State}.


handle_info({Tag, _RawSocket, Data}, State=#state{socket = Socket})
when ?DATA_MSG(Tag) ->
    Decoded = binary_to_term(Data),

    ?LOG_TRACE("Received data from socket: ~p", [Decoded]),

    case get({?MODULE, ingress_delay}) of
        0 ->
            ok;
        Other ->
            timer:sleep(Other)
    end,

    handle_message(binary_to_term(Data), State),
    ok = partisan_peer_socket:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({Tag, _RawSocket, Reason}, State=#state{socket=Socket})
when ?ERROR_MSG(Tag) ->
    ?LOG_ERROR(#{
        description => "Connection socket error, closing",
        socket => Socket,
        reason => Reason
    }),
    {stop, Reason, State};

handle_info({Tag, _RawSocket}, State) when ?CLOSED_MSG(Tag) ->
    ?LOG_TRACE(
        "Connection socket ~p has been remotely closed",
        [State#state.socket]
    ),

    {stop, normal, State};

handle_info({'DOWN', MRef, port, _, _}, #state{ref=MRef} = State) ->
    %% Listen socket closed
    {stop, normal, State};

handle_info(_, State) ->
    {noreply, State}.


terminate(_, State) ->
    ok = partisan_peer_socket:close(State#state.socket),
    ok.


-spec code_change(term() | {down, term()}, state_t(), term()) ->
    {ok, state_t()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%% =============================================================================
%% PRIVATE
%% =============================================================================



handle_message({hello, Node, Channel}, #state{socket = Socket}) ->
    %% Get our tag, if set.
    Tag = partisan_config:get(tag, undefined),

    %% Store node and channel in the process dictionary.
    put({?MODULE, peer}, Node),
    put({?MODULE, channel}, Channel),

    %% Connect the node with Distributed Erlang, just for now for
    %% control messaging in the test suite execution.
    case maybe_connect_disterl(Node) of
        ok ->
            %% Send our state to the remote service, in case they want
            %% it to bootstrap.
            Manager = partisan_peer_service:manager(),
            {ok, LocalState} = Manager:get_local_state(),
            send_message(Socket, {state, Tag, LocalState}),
            ok;
        error ->
            ?LOG_INFO(#{description => "Node could not be connected."}),
            send_message(Socket, {hello, {error, pang}}),
            ok
    end;

handle_message(Message, _State) ->
    Peer = get({?MODULE, peer}),
    Channel = get({?MODULE, channel}),

    ?LOG_TRACE("Received message from peer ~p: ~p", [Peer, Message]),

    Manager = partisan_peer_service:manager(),
    Manager:receive_message(Peer, Channel, Message),

    ?LOG_TRACE("Dispatched ~p to manager.", [Message]),

    ok.

%% @private
send_message(Socket, Message) ->
    EncodedMessage = erlang:term_to_binary(Message),
    partisan_peer_socket:send(Socket, EncodedMessage).


%% @private
maybe_connect_disterl(Node) ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            case net_adm:ping(Node) of
                pong ->
                    ok;
                pang ->
                    error
            end;
        false ->
            ok
    end.

