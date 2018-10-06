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
-include("partisan_peer_connection.hrl").

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
          socket :: partisan_peer_connection:connection(),
          ref :: reference()
         }).

-type state_t() :: #state{}.

acceptor_init(_SockName, LSocket, []) ->
    %% monitor listen socket to gracefully close when it closes
    MRef = monitor(port, LSocket),
    {ok, MRef}.

acceptor_continue(_PeerName, Socket0, MRef) ->
    put({?MODULE, ingress_delay}, partisan_config:get(ingress_delay, 0)),
    Socket = partisan_peer_connection:accept(Socket0),
    send_message(Socket, {hello, partisan_peer_service_manager:mynode()}),
    gen_server:enter_loop(?MODULE, [], #state{socket=Socket, ref=MRef}).

acceptor_terminate(Reason, _) ->
    %% Something went wrong. Either the acceptor_pool is terminating
    %% or the accept failed.
    exit(Reason).

%% gen_server api

init(_) ->
    {stop, acceptor}.

handle_call(Req, _, State) ->
    {stop, {bad_call, Req}, State}.

handle_cast(Req, State) ->
    {stop, {bad_cast, Req}, State}.

handle_info({Tag, _RawSocket, Data}, State=#state{socket=Socket}) when ?DATA_MSG(Tag) ->
    case partisan_config:get(tracing, ?TRACING) of
        true ->
            lager:info("Recevied data from socket: ~p", [decode(Data)]);
        false ->
            ok
    end,

    case get({?MODULE, ingress_delay}) of
        0 ->
            ok;
        Other ->
            timer:sleep(Other)
    end,
    handle_message(decode(Data), State),
    ok = partisan_peer_connection:setopts(Socket, [{active, once}]),
    {noreply, State};
handle_info({Tag, _RawSocket, Reason}, State=#state{socket=Socket}) when ?ERROR_MSG(Tag) ->
    lager:error("Connection socket ~p errored out, closing", [Socket]),
    {stop, Reason, State};
handle_info({Tag, _RawSocket}, State=#state{socket=Socket}) when ?CLOSED_MSG(Tag) ->
    lager:info("Connection socket ~p has been remotely closed", [Socket]),
    {stop, normal, State};
handle_info({'DOWN', MRef, port, _, _}, State=#state{ref=MRef}) ->
    %% Listen socket closed
    {stop, normal, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, state_t(), term()) ->
                         {ok, state_t()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_message({hello, Node},
               #state{socket=Socket}) ->
    %% Get our tag, if set.
    Tag = partisan_config:get(tag, undefined),

    %% Store node in the process dictionary.
    put({?MODULE, peer}, Node),

    %% Connect the node with Distributed Erlang, just for now for
    %% control messaging in the test suite execution.
    case maybe_connect_disterl(Node) of
        ok ->
            %% Send our state to the remote service, incase they want
            %% it to bootstrap.
            Manager = partisan_peer_service:manager(),
            {ok, LocalState} = Manager:get_local_state(),
            % lager:info("got hello reply from ~p, sending local state", [Node]),
            send_message(Socket, {state, Tag, LocalState}),
            ok;
        error ->
            lager:info("Node could not be connected."),
            send_message(Socket, {hello, {error, pang}}),
            ok
    end;
handle_message(Message, _State) ->
    Peer = get({?MODULE, peer}),

    case partisan_config:get(tracing, ?TRACING) of
        true ->
            lager:info("Received message from peer ~p: ~p", [Peer, Message]);
        false ->
            ok
    end,

    Manager = partisan_peer_service:manager(),
    Manager:receive_message(Peer, Message),

    case partisan_config:get(tracing, ?TRACING) of
        true ->
            lager:info("Dispatched ~p to manager.", [Message]);
        false ->
            ok
    end,

    ok.

%% @private
send_message(Socket, Message) ->
    EncodedMessage = encode(Message),
    partisan_peer_connection:send(Socket, EncodedMessage).

%% @private
encode(Message) ->
    term_to_binary(Message).

%% @private
decode(Message) ->
    binary_to_term(Message).

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

