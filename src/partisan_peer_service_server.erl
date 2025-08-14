%% =============================================================================
%% SPDX-FileCopyrightText: 2016 Christopher Meiklejohn
%% SPDX-FileCopyrightText: 2021 - 2025 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(partisan_peer_service_server).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(acceptor).
-behaviour(gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").
-include("partisan_peer_socket.hrl").

-record(state, {
    socket                  ::  partisan_peer_socket:t(),
    peer_node               ::  node(),
    channel                 ::  partisan:channel(),
    ref                     ::  reference(),
    ping_idle_timeout       ::  non_neg_integer(),
    ping_tref               ::  optional(partisan_remote_ref:r()),
    ping_retry              ::  optional(partisan_retry:t()),
    ping_id                 ::  optional(partisan:reference())
}).

-type state_t() :: #state{}.


%% Acceptor callbacks
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



%% =============================================================================
%% ACCEPTOR CALLBACKS
%% =============================================================================



acceptor_init(_SockName, LSocket, []) ->
    %% monitor listen socket to gracefully close when it closes
    MRef = monitor(port, LSocket),
    {ok, MRef}.


acceptor_continue(_PeerName, Socket0, MRef) ->
    put({?MODULE, ingress_delay}, partisan_config:get(ingress_delay, 0)),
    Socket = partisan_peer_socket:accept(Socket0),
    send_message(Socket, {hello, partisan:node()}),
    State0 = #state{socket = Socket, ref = MRef},
    State = maybe_enable_ping(
        State0,
        partisan_config:get(connection_ping, #{})
    ),
    gen_server:enter_loop(?MODULE, [], State).


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


handle_info({Tag, _RawSocket, Data}, #state{} = State)
when ?DATA_MSG(Tag) ->
    Msg = binary_to_term(Data),
    ?LOG_TRACE("Received data from socket: ~p", [Msg]),
    ok = maybe_delay(),
    ok = reset_socket_opts(State),
    handle_inbound(Msg, State);

handle_info({Tag, _RawSocket, Reason}, #state{} = State)
when ?ERROR_MSG(Tag) ->
    ?LOG_ERROR(#{
        description => "Connection socket error, closing",
        socket => State#state.socket,
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

handle_info(
    {timeout, Ref, ping_idle_timeout}, #state{ping_tref = Ref} = State) ->
    ?LOG_INFO(#{
        description => "Connection idle, sending ping",
        peer_node => State#state.peer_node,
        channel => State#state.channel,
        attempt => partisan_retry:count(State#state.ping_retry)
    }),
    maybe_send_ping(State);


handle_info({timeout, Ref, ping_timeout}, #state{ping_tref = Ref} = State) ->
    ?LOG_INFO(#{
        description => "Ping timeout, retrying",
        attempts => partisan_retry:count(State#state.ping_retry)
    }),
    maybe_send_ping(State);

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



handle_inbound({hello, Node, Channel}, #state{} = State0) ->
    %% Get our tag, if set.
    Tag = partisan_config:get(tag, undefined),

    %% Store node and channel in the process dictionary.
    put({?MODULE, peer}, Node),
    put({?MODULE, channel}, Channel),

    State = State0#state{peer_node = Node},

    %% Connect the node with Distributed Erlang, just for now for
    %% control messaging in the test suite execution.
    case maybe_connect_disterl(Node) of
        ok ->
            %% Send our state to the remote service, in case they want
            %% it to bootstrap.
            Manager = partisan_peer_service:manager(),
            {ok, LocalState} = Manager:get_local_state(),
            send_message(State#state.socket, {state, Tag, LocalState}),
            ok;

        error ->
            ?LOG_INFO(#{description => "Node could not be connected."}),
            send_message(State#state.socket, {hello, {error, pang}}),
            ok
    end,
    {noreply, reset_ping(State)};


handle_inbound(#ping{from = Node} = Ping, #state{peer_node = Node} = State) ->
    ok = send_pong(State, Ping),
    {noreply, reset_ping(State)};

handle_inbound(#ping{} = Ping, #state{} = State) ->
        ?LOG_WARNING(#{
        description => "Received invalid ping message",
        message => Ping
    }),
    {noreply, State};

handle_inbound(
    #pong{from = Node, id = Id, timestamp = Ts},
    #state{peer_node = Node, ping_id = Id} = State) ->
    ok = telemetry:execute(
        [partisan, connection, server, hearbeat],
        #{latency => erlang:system_time(millisecond) - Ts},
        #{
            node => partisan:node(),
            channel => State#state.channel,
            socket => State#state.socket,
            peer_node => Node
        }
    ),
    {noreply, reset_ping(State)};

handle_inbound(#pong{} = Pong, #state{} = State) ->
    {stop, {invalid_ping_response, Pong}, State};

handle_inbound(Message, State) ->
    PeerNode = get({?MODULE, peer_node}),
    Channel = get({?MODULE, channel}),
    Manager = partisan_peer_service:manager(),
    _ = Manager:receive_message(PeerNode, Channel, Message),
    ?LOG_TRACE("Dispatched ~p to manager.", [Message]),
    {noreply, reset_ping(State)}.



%% @private
reset_socket_opts(State) ->
    partisan_peer_socket:setopts(State#state.socket, [{active, once}]).


%% @private
maybe_delay() ->
    case get({?MODULE, ingress_delay}) of
        0 ->
            ok;

        Other ->
            timer:sleep(Other)
    end.


%% @private
send_message(Socket, Message) ->
    Data = erlang:term_to_iovec(Message),
    send_data(Socket, Data).

send_data(Socket, Data) ->
    partisan_peer_socket:send(Socket, Data).


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


%% =============================================================================
%% PRIVATE: KEEP ALIVE PING
%% =============================================================================


%% @private
maybe_enable_ping(State, #{enabled := true} = PingOpts) ->
    IdleTimeout = maps:get(idle_timeout, PingOpts),
    Timeout = maps:get(timeout, PingOpts),
    Attempts = maps:get(max_attempts, PingOpts),

    Retry = partisan_retry:init(
        ping_timeout,
        #{
            deadline => 0, % disable, use max_retries only
            interval => Timeout,
            max_retries => Attempts,
            backoff_enabled => false
        }
    ),

    State#state{
        ping_idle_timeout = IdleTimeout,
        ping_id = partisan:make_ref(),
        ping_retry = Retry
    };

maybe_enable_ping(#{enabled := false}, State) ->
    State.


%% @private
reset_ping(State) ->
    %% The client ping idle_timeout is the same as ours,
    %% so we offset the server timeout to avoid synchronization.
    reset_ping(State, trunc(State#state.ping_idle_timeout * 0.25)).


%% @private
reset_ping(#state{ping_retry = undefined} = State, _Offset) ->
    State;

reset_ping(#state{ping_tref = undefined} = State, Offset) ->
    Time = State#state.ping_idle_timeout + Offset,
    Ref = erlang:start_timer(Time, self(), ping_idle_timeout),
    State#state{ping_tref = Ref};

reset_ping(#state{} = State, Offset) ->
    _ = erlang:cancel_timer(State#state.ping_tref),
    {_, Retry} = partisan_retry:succeed(State#state.ping_retry),
    Time = State#state.ping_idle_timeout + Offset,
    Ref = erlang:start_timer(Time, self(), ping_idle_timeout),
    State#state{
        ping_retry = Retry,
        ping_tref = Ref
    }.


%% @private
maybe_send_ping(#state{ping_idle_timeout = undefined} = State) ->
    {noreply, State};

maybe_send_ping(#state{} = State) ->
    {Result, Retry} = partisan_retry:fail(State#state.ping_retry),
    maybe_send_ping(Result, State#state{ping_retry = Retry}).


%% @private
maybe_send_ping(Limit, State)
when Limit == deadline orelse Limit == max_retries ->
    {stop, {shutdown, ping_timeout}, State};

maybe_send_ping(_Time, #state{} = State0) ->
    State = send_ping(State0),
    {noreply, State}.



%% @private
send_ping(State) ->
    Ref = partisan:make_ref(),
    Ping = #ping{
        from = partisan:node(),
        id = Ref,
        timestamp = erlang:system_time(millisecond)
    },
    ok = send_message(State#state.socket, Ping),

    %% We schedule the next retry
    Tref = partisan_retry:fire(State#state.ping_retry),
    State#state{ping_id = Ref, ping_tref = Tref}.

%% @private
send_pong(State, #ping{id = Id, timestamp = Ts}) ->
    Pong = #pong{
        from = partisan:node(),
        id = Id,
        timestamp = Ts
    },
    send_message(State#state.socket, Pong).