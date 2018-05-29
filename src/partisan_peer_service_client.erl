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

-include("partisan.hrl").
-include("partisan_peer_connection.hrl").

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

            {ok, #state{from=From, listen_addr=ListenAddr, channel=Channel, socket=Socket, peer=Peer}};
        Error ->
            lager:warning("unable to connect to ~p due to ~p",
                          [Peer, Error]),
            {stop, normal}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

%% @private
handle_call({send_message, Message}, _From, #state{channel=_Channel, socket=Socket}=State) ->
    case partisan_peer_connection:send(Socket, encode(Message)) of
        ok ->
            {reply, ok, State};
        Error ->
            lager:info("Message failed to send: ~p", [Error]),
            {reply, Error, State}
    end;
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {reply, ok, State}.

-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.
%% @private
handle_cast({send_message, Message}, #state{channel=_Channel, socket=Socket}=State) ->
    case partisan_peer_connection:send(Socket, encode(Message)) of
        ok ->
            ok;
        Error ->
            lager:info("Message failed to send: ~p", [Error])
    end,
    {noreply, State};
handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), state_t()) -> {noreply, state_t()}.
handle_info({Tag, _Socket, Data}, State0) when ?DATA_MSG(Tag) ->
    handle_message(decode(Data), State0);
handle_info({Tag, _Socket}, #state{peer = _Peer} = State) when ?CLOSED_MSG(Tag) ->
    % lager:info("connection to ~p has been closed", [Peer]),
    {stop, normal, State};
handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), state_t()) -> term().
terminate(_Reason, #state{socket=Socket}) ->
    ok = partisan_peer_connection:close(Socket),
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

    case partisan_peer_connection:connect(Address, Port, SocketOptions, ?TIMEOUT, PartisanOptions) of
        {ok, Socket} ->
            {ok, Socket};
        {error, Error} ->
            {error, Error}
    end.

%% @private
decode(Message) ->
    binary_to_term(Message).

%% @private
handle_message({state, Tag, LocalState},
               #state{peer=Peer, from=From, socket=Socket}=State) ->
    %% Extract name for readability.
    #{name := Name} = Peer,

    %% If gossip on the client side is disabled, the server will never learn
    %% about the clients presence and connect to it, because that's 
    %% what normally happens in the gossip path.
    %%
    %% Therefore, send an explicit message back to the server telling it
    %% to initiate a connect to the client until it's membership has
    %% been updated by the external membership oracle.  This is required
    %% because typically, the external oracle will require partisan for
    %% node to node communication.
    %%
    case partisan_config:get(initiate_reverse, false) of
        false ->
            ok;
        true ->
            lager:info("Notifying ~p to connect to us at ~p at pid ~p", 
                       [Name, partisan_peer_service_manager:mynode(), self()]),

            Message = {connect, self(), partisan_peer_service_manager:myself()},

            case partisan_peer_connection:send(Socket, default_encode(Message)) of
                ok ->
                    ok;
                Error ->
                    lager:info("failed to send hello message to node ~p due to ~p",
                               [Peer, Error])
            end
    end,

    %% Notify peer service manager we are done.
    case LocalState of
        {state, _Active, Epoch} ->
            From ! {connected, Peer, Tag, Epoch, LocalState};
        _Other ->
            From ! {connected, Peer, Tag, LocalState}
    end,

    {noreply, State};
handle_message({hello, Node}, #state{peer=Peer, socket=Socket}=State) ->
    % lager:info("sending hello to ~p", [Node]),

    #{name := PeerName} = Peer,

    case Node of
        PeerName ->
            Message = {hello, partisan_peer_service_manager:mynode()},

            case partisan_peer_connection:send(Socket, default_encode(Message)) of
                ok ->
                    ok;
                Error ->
                    lager:info("failed to send hello message to node ~p due to ~p",
                               [Node, Error])
            end,

            {noreply, State};
        _ ->
            %% If the peer isn't who it should be, abort.
            lager:error("Peer ~p isn't ~p.", [Node, Peer]),
            {stop, {unexpected_peer, Node, Peer}, State}
    end;

handle_message(Message, State) ->
    lager:info("Invalid message: ~p", [Message]),
    {stop, normal, State}.

%% @private
encode(Message) ->
    partisan_util:term_to_iolist(Message).

%% @private
default_encode(Message) ->
    term_to_binary(Message).
