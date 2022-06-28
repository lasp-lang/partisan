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


-include("partisan.hrl").
-include("partisan_logger.hrl").
-include("partisan_peer_socket.hrl").


-export([start_link/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {socket, listen_addr, channel, from, peer}).

-type state_t() :: #state{}.

%% Macros.
-define(TIMEOUT, 1000).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(node_spec(), listen_addr(), channel(), pid()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Peer, ListenAddr, Channel, From) ->
    gen_server:start_link(?MODULE, [Peer, ListenAddr, Channel, From], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([iolist()]) -> {ok, state_t()}.
init([Peer, ListenAddr, Channel, From]) ->
    case connect(ListenAddr, Channel) of
        {ok, Socket} ->
            %% For debugging, store information in the process dictionary.
            put({?MODULE, from}, From),
            put({?MODULE, listen_addr}, ListenAddr),
            put({?MODULE, channel}, Channel),
            put({?MODULE, peer}, Peer),
            put({?MODULE, egress_delay}, partisan_config:get(egress_delay, 0)),

            {ok, #state{from=From, listen_addr=ListenAddr, channel=Channel, socket=Socket, peer=Peer}};
        Error ->
            ?LOG_TRACE(
                "Pid ~p is unable to connect to ~p due to ~p",
                [self(), Peer, Error]
            ),
            {stop, normal}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({send_message, Message}, _From, #state{channel=_Channel, socket=Socket}=State) ->
    case get({?MODULE, egress_delay}) of
        0 ->
            ok;
        Other ->
            timer:sleep(Other)
    end,

    case partisan_peer_socket:send(Socket, encode(Message)) of
        ok ->
            ?LOG_TRACE("Dispatched message: ~p", [Message]),

            {reply, ok, State};
        Error ->
            ?LOG_DEBUG("Message ~p failed to send: ~p", [Message, Error]),
            {reply, Error, State}
    end;
handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.

-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.
%% @private
handle_cast({send_message, Message}, #state{channel=_Channel, socket=Socket}=State) ->
    ?LOG_TRACE("Received cast: ~p", [Message]),

    case get({?MODULE, egress_delay}) of
        0 ->
            ok;
        Other ->
            timer:sleep(Other)
    end,

    case partisan_peer_socket:send(Socket, encode(Message)) of
        ok ->
            ?LOG_TRACE("Dispatched message: ~p", [Message]),
            ok;
        Error ->
            ?LOG_INFO(#{
                description => "Failed to send message",
                message => Message,
                error => Error
            })
    end,
    {noreply, State};
handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),
    {noreply, State}.

%% @private
-spec handle_info(term(), state_t()) -> {noreply, state_t()}.
handle_info({Tag, _Socket, Data}, State0) when ?DATA_MSG(Tag) ->
    ?LOG_TRACE("Received info message at ~p: ~p", [self(), decode(Data)]),
    handle_message(decode(Data), State0);
handle_info({Tag, _Socket}, #state{peer = Peer} = State) when ?CLOSED_MSG(Tag) ->
    ?LOG_TRACE("Connection to ~p has been closed for pid ~p", [Peer, self()]),

    {stop, normal, State};
handle_info(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled info event", event => Event}),
    {noreply, State}.

%% @private
-spec terminate(term(), state_t()) -> term().
terminate(Reason, #state{socket=Socket}) ->
    ?LOG_TRACE("Process ~p terminating for reason ~p...", [self(), Reason]),
    ok = partisan_peer_socket:close(Socket),
    ok.

%% @private
-spec code_change(term() | {down, term()}, state_t(), term()) ->
    {ok, state_t()}.
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
connect(Node, Channel) when is_atom(Node) ->
    ListenAddrs = rpc:call(Node, partisan_config, get, [listen_addrs]),
    case length(ListenAddrs) > 0 of
        true ->
            ListenAddr = hd(ListenAddrs),
            connect(ListenAddr, Channel);
        _ ->
            {error, no_listen_addr}
    end;

%% @doc Connect to remote peer.
%%      Only use the first listen address.
connect(#{ip := Address, port := Port}, Channel) ->
    Monotonic = case Channel of
        {monotonic, _} ->
            true;
        _ ->
            false
    end,

    SocketOptions = [binary, {active, true}, {packet, 4}, {keepalive, true}],
    PartisanOptions = [{monotonic, Monotonic}],

    case partisan_peer_socket:connect(Address, Port, SocketOptions, ?TIMEOUT, PartisanOptions) of
        {ok, Socket} ->
            {ok, Socket};
        {error, Error} ->
            %% TODO LOG HERE
            {error, Error}
    end.

%% @private
decode(Message) ->
    binary_to_term(Message).

%% @private
handle_message({state, Tag, LocalState}, #state{}=State) ->

    #state{
        peer = Peer,
        channel = Channel,
        from = From
    } = State,

    %% Notify peer service manager we are done.
    case LocalState of
        %% TODO: Anything using a three tuple will be caught here.
        %% TODO: This format is specific to the HyParView manager.
        {state, _Active, Epoch} ->
            From ! {connected, Peer, Channel, Tag, Epoch, LocalState};
        _Other ->
            From ! {connected, Peer, Channel, Tag, LocalState}
    end,

    {noreply, State};

handle_message({hello, Node}, #state{peer=Peer, socket=Socket}=State) ->
    #{name := PeerName} = Peer,

    case Node of
        PeerName ->
            Message = {hello, partisan:node()},

            case partisan_peer_socket:send(Socket, default_encode(Message)) of
                ok ->
                    ok;
                Error ->
                    ?LOG_INFO(#{
                        description => "Failed to send hello message to node",
                        node => Node,
                        error => Error
                    })
            end,

            {noreply, State};
        _ ->
            %% If the peer isn't who it should be, abort.
            ?LOG_ERROR(#{
                description => "Unexpected peer, aborting",
                got => Node,
                expected => Peer
            }),
            {stop, {unexpected_peer, Node, Peer}, State}
    end;

handle_message(Message, State) ->
    ?LOG_WARNING(#{
        description => "Received invalid message",
        message => Message
    }),
    {stop, normal, State}.

%% @private
encode(Message) ->
    partisan_util:term_to_iolist(Message).

%% @private
default_encode(Message) ->
    term_to_binary(Message).
