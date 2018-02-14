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

-module(partisan_peer_service_libp2p_framed_stream).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(libp2p_framed_stream).

-export([init/3, 
         handle_info/3, 
         handle_data/3, 
         handle_call/4, 
         handle_cast/3]).

-record(state, {
          type,
          peer,
          from:: pid(),
          path=undefined :: binary() | undefined,
          connection :: libp2p_connection:connection()
         }).

init(server, Connection, [Path]) ->
    lager:info("libp2p: server connected: ~p", [Connection]),
    
    %% Get our tag, if set.
    Tag = partisan_config:get(tag, undefined),

    %% Send our state to the remote service, incase they want
    %% it to bootstrap.
    Manager = partisan_peer_service:manager(),
    {ok, LocalState} = Manager:get_local_state(),

    ok = libp2p_framed_stream:send(Connection, encode({state, Tag, LocalState})),

    {ok, #state{type=server, connection=Connection, path=Path}};
init(client, Connection, [Peer, From]) ->
    lager:info("libp2p: client connected: ~p", [Connection]),
    {ok, #state{type=client, peer=Peer, from=From, connection=Connection}}.

%% @private
handle_data(_, Data, State=#state{type=Type}) ->
    Decoded = decode(Data),
    lager:info("libp2p: ~p received ~p", [Type, Decoded]),
    handle_message(Decoded, State).

%% @private
handle_call(_, Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_cast(_, Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(_, {send, Message}, #state{connection=Connection}=State) ->
    ok = libp2p_framed_stream:send(Connection, encode(Message)),
    {noreply, State};
handle_info(_, Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
encode(Message) ->
    partisan_util:term_to_iolist(Message).

%% @private
decode(Bin) ->
    binary_to_term(Bin).

%% @private
handle_message({state, Tag, LocalState}, #state{peer=Peer, from=From}=State) ->
    case LocalState of
        {state, _Active, Epoch} ->
            lager:debug("got local state from peer ~p, informing ~p that we're connected",
                       [Peer, From]),
            From ! {connected, Peer, Tag, Epoch, LocalState};
        _ ->
            From ! {connected, Peer, Tag, LocalState}
    end,
    {noresp, State};
handle_message(Message, State) ->
    Manager = partisan_peer_service:manager(),
    Manager:receive_message(Message),
    {noresp, State}.