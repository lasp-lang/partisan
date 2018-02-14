%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(partisan_peer_service_libp2p_client).
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

-record(state, {stream, listen_addr, channel, from, peer}).

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
        {ok, Stream} ->
            %% For debugging, store information in the process dictionary.
            put({?MODULE, from}, From),
            put({?MODULE, listen_addr}, ListenAddr),
            put({?MODULE, channel}, Channel),
            put({?MODULE, peer}, Peer),

            {ok, #state{from=From, listen_addr=ListenAddr, channel=Channel, stream=Stream, peer=Peer}};
        Error ->
            lager:error("unable to connect to ~p due to ~p",
                        [Peer, Error]),
            {stop, normal}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

%% @private
handle_call({send_message, Message}, _From, #state{channel=_Channel, stream=Stream}=State) ->
    case send(Stream, encode(Message)) of
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
handle_cast({send_message, Message}, #state{channel=_Channel, stream=Stream}=State) ->
    case send(Stream, encode(Message)) of
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
handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), state_t()) -> term().
terminate(_Reason, #state{stream=Stream}) ->
    ok = libp2p_connection:close(Stream),
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
        false ->
            {error, no_listen_addr}
    end;

%% @doc Connect to remote peer.
%%      Only use the first listen address.
connect(#{ip := Address, port := Port}, Channel) ->
    ToListenAddr = "/ip4/" ++ inet:ntoa(Address) ++ "/tcp/" ++ integer_to_list(Port),

    %% Get our information.
    #{listen_addrs := ListenAddrs} = partisan_peer_service_manager:myself(),
    #{ip := MyAddress, port := MyPort} = hd(ListenAddrs),
    OurListenAddr = "/ip4/" ++ inet:ntoa(MyAddress) ++ "/tcp/" ++ integer_to_list(MyPort),

    %% Start local swarm, which will reuse existing swarm if available.
    case libp2p_swarm:start(OurListenAddr) of
        {ok, FromSwarm} ->
            case libp2p_swarm:dial(FromSwarm, ToListenAddr, atom_to_list(Channel)) of
                {ok, Stream} ->
                    {ok, Stream};
                {error, Error} ->
                    {error, Error}
            end;
        InitError ->
            InitError
    end.

%% @private
encode(Message) ->
    partisan_util:term_to_iolist(Message).

%% @private
send(Stream, Message) ->
    libp2p_connection:send(Stream, Message).