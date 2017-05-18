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

-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {socket, from, peer}).

-type state_t() :: #state{}.

%% Macros.
-define(TIMEOUT, 1000).

-include("partisan.hrl").
-include("partisan_peer_connection.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start and link to calling process.
-spec start_link(node_spec(), pid()) -> {ok, pid()} | ignore | error().
start_link(Peer, From) ->
    gen_server:start_link(?MODULE, [Peer, From], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([iolist()]) -> {ok, state_t()}.
init([Peer, From]) ->
    case connect(Peer) of
        {ok, Socket} ->
            {ok, #state{from=From, socket=Socket, peer=Peer}};
        _Error ->
            {stop, normal}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

%% @private
handle_call({send_message, Message}, _From, #state{socket=Socket}=State) ->
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
handle_cast({send_message, Message}, #state{socket=Socket}=State) ->
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
handle_info({Tag, _Socket}, State) when ?CLOSED_MSG(Tag) ->
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
connect(Peer) when is_atom(Peer) ->
    %% Bootstrap with disterl.
    PeerPort = rpc:call(Peer,
                        partisan_config,
                        get,
                        [peer_port, ?PEER_PORT]),
    connect({Peer, {127, 0, 0, 1}, PeerPort});

%% @doc Connect to remote peer.
connect({_Name, Address, Port}) ->
    Options = [binary, {active, true}, {packet, 4}, {keepalive, true}],
    case partisan_peer_connection:connect(Address, Port, Options, ?TIMEOUT) of
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
               #state{peer=Peer, from=From}=State) ->
    case LocalState of
        {state, _Active, Epoch} ->
            From ! {connected, Peer, Tag, Epoch, LocalState};
        _ ->
            From ! {connected, Peer, Tag, LocalState}
    end,
    {noreply, State};
handle_message({hello, _Node}, #state{socket=Socket}=State) ->
    Message = {hello, node()},
    ok = partisan_peer_connection:send(Socket, encode(Message)),
    {noreply, State};
handle_message(Message, State) ->
    lager:info("Invalid message: ~p", [Message]),
    {stop, normal, State}.

%% @private
encode(Message) ->
    term_to_binary(Message).
