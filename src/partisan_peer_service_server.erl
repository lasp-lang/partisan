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
    socket :: partisan_peer_socket:t(),
    %% The authentication phase of the connection (ADR-000005):
    %% - `undefined': the phase is off (`peer_auth_methods' is empty).
    %% - `{pending, Negotiator, TimerRef}': the peer has not authenticated.
    %%   `handle_info/2' gives every frame to `partisan_peer_auth' and to
    %%   nothing else.
    %% - a map: the phase succeeded.
    auth ::
        undefined
        | {pending, partisan_peer_auth:server(), reference()}
        | partisan_peer_auth:authn(),
    %% Whether `partisan_peer_auth:enter_pending/0' counts this connection.
    counted = false :: boolean(),
    %% The bytes of a frame of the phase that is not whole yet. During the
    %% phase the socket is raw and this process takes the frames itself.
    buffer = <<>> :: binary(),
    peer_node :: node(),
    channel :: partisan:channel(),
    ref :: reference(),
    %% `undefined' when pings are disabled for this connection — both modules
    %% have a `#state{ping_idle_timeout = undefined}' clause in
    %% `maybe_send_ping/1' that relies on it.
    ping_idle_timeout :: optional(non_neg_integer()),
    %% A local timer reference from `erlang:start_timer/3' (directly, or via
    %% `partisan_retry:fire/1' which also returns one). NOT a remote reference:
    %% it is passed to `erlang:cancel_timer/1', which only accepts a local
    %% `reference()'.
    ping_tref :: optional(reference()),
    ping_retry :: optional(partisan_retry:t()),
    ping_id :: optional(partisan:any_reference())
}).

-type state_t() :: #state{}.

%% Acceptor callbacks
-export([
    acceptor_init/3,
    acceptor_continue/3,
    acceptor_terminate/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% =============================================================================
%% ACCEPTOR CALLBACKS
%% =============================================================================

acceptor_init(_SockName, LSocket, []) ->
    %% monitor listen socket to gracefully close when it closes
    MRef = monitor(port, LSocket),
    {ok, MRef}.

acceptor_continue(_PeerName, Socket0, MRef) ->
    put({?MODULE, ingress_delay}, partisan_config:get(ingress_delay, 0)),
    %% One read of the method list decides everything about this connection.
    Methods = partisan_peer_auth:methods(),
    Counted = enter_pending(Methods, Socket0),

    %% The socket is passive (see `partisan_acceptor_socket'), so it has read
    %% nothing. It takes its framing here, before TLS takes the socket over:
    %% a TLS socket inherits the framing.
    Framing = partisan_peer_auth:initial_socket_opts(Methods),
    Socket =
        try
            ok = inet:setopts(Socket0, Framing),
            partisan_peer_socket:accept(Socket0)
        catch
            Class:Reason ->
                %% The peer closed, or the TLS handshake failed.
                _ = Counted andalso partisan_peer_auth:leave_pending(),
                _ = (try gen_tcp:close(Socket0) catch _:_ -> ok end),
                case {Class, Reason} of
                    {exit, _} -> exit(Reason);
                    _ -> exit(normal)
                end
        end,

    State0 = start_handshake(
        Methods, #state{socket = Socket, ref = MRef, counted = Counted}
    ),
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

handle_info(
    {Tag, _RawSocket, Data}, #state{auth = {pending, Neg, TRef}} = State
) when
    ?DATA_MSG(Tag)
->
    %% The peer has not authenticated. The socket is raw: this process takes
    %% the frame out of the bytes, which bounds its size, and gives it to the
    %% negotiator. Nothing is decoded here and the bytes reach no other code.
    Socket = State#state.socket,
    Bytes = <<(State#state.buffer)/binary, Data/binary>>,

    case partisan_peer_auth:deframe(Bytes) of
        more ->
            ok = reset_socket_opts(State),
            {noreply, State#state{buffer = Bytes}};
        {ok, Frame, <<>>} ->
            case partisan_peer_auth:server_handle(Frame, Neg) of
                {ok, Reply, Authn} ->
                    _ = erlang:cancel_timer(TRef),
                    ok = auth_telemetry(ok, Authn),
                    %% The socket takes the framing of the existing protocol
                    %% before the result goes out: the peer sends `hello'
                    %% only after it has read the result.
                    Framing = partisan_peer_auth:established_socket_opts(),
                    maybe
                        ok ?= partisan_peer_socket:setopts(Socket, Framing),
                        ok ?= send_data(Socket, Reply),
                        ok ?= reset_socket_opts(State),
                        {noreply,
                            leave_pending(State#state{
                                auth = Authn, buffer = <<>>
                            })}
                    else
                        {error, _} ->
                            {stop, normal, State}
                    end;
                {error, Reason, Reply} ->
                    auth_failed(Reason, Reply, State)
            end;
        {ok, _Frame, _Rest} ->
            %% Bytes after the response. An honest peer sends nothing before
            %% it has read the result.
            auth_failed(trailing_bytes, partisan_peer_auth:error_frame(), State);
        {error, Reason} ->
            %% The length of the frame is over the limit. The frame itself is
            %% not read.
            auth_failed(Reason, partisan_peer_auth:error_frame(), State)
    end;
handle_info(
    {timeout, TRef, auth_timeout}, #state{auth = {pending, _, TRef}} = State
) ->
    ok = auth_telemetry(error, timeout),
    ?LOG_DEBUG(#{description => "Peer authentication timed out, closing"}),
    {stop, normal, State};
handle_info(
    {Tag, _RawSocket, _Reason}, #state{auth = {pending, _, _}} = State
) when
    ?ERROR_MSG(Tag)
->
    %% For example a frame over the limit. The peer has not authenticated, so
    %% this is no more than a debug event: an error log per connection would
    %% let any host flood the log.
    ok = auth_telemetry(error, socket_error),
    {stop, normal, State};
handle_info({Tag, _RawSocket, Data}, #state{} = State) when
    ?DATA_MSG(Tag)
->
    Msg = binary_to_term(Data),
    ?LOG_TRACE("Received data from socket: ~p", [Msg]),
    ok = maybe_delay(),
    ok = reset_socket_opts(State),
    handle_inbound(Msg, State);
handle_info({Tag, _RawSocket, Reason}, #state{} = State) when
    ?ERROR_MSG(Tag)
->
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
handle_info({'DOWN', MRef, port, _, _}, #state{ref = MRef} = State) ->
    %% Listen socket closed
    {stop, normal, State};
handle_info(
    {timeout, Ref, ping_idle_timeout}, #state{ping_tref = Ref} = State
) ->
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
    _ = leave_pending(State),
    ok = partisan_peer_socket:close(State#state.socket),
    ok.

-spec code_change(term() | {down, term()}, state_t(), term()) ->
    {ok, state_t()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

handle_inbound({hello, Node, Channel}, #state{auth = Auth} = State) ->
    case authenticated_as(Node, Auth) of
        true ->
            case is_atom(Node) andalso partisan_peer_auth:is_allowed(Node) of
                true ->
                    handle_hello(Node, Channel, State);
                false ->
                    ?LOG_WARNING(#{
                        description =>
                            "Peer is not in peer_auth_allowed_nodes, closing",
                        presented => Node
                    }),
                    {stop, normal, State}
            end;
        false ->
            %% The peer authenticated under one name and now presents another.
            ?LOG_WARNING(#{
                description =>
                    "Peer presented a name it did not authenticate, closing",
                authenticated => Auth,
                presented => Node
            }),
            {stop, normal, State}
    end;
handle_inbound(Message, #state{auth = Auth, peer_node = undefined} = State) when
    is_map(Auth)
->
    %% The peer authenticated and sent another frame before `hello'. A
    %% connecting node sends `hello' first (see `partisan_peer_service_client').
    %% Without it the name the peer authenticated under is not checked, so
    %% the frame does not reach the manager.
    ?LOG_WARNING(#{
        description => "Peer sent a frame before hello, closing",
        authenticated => Auth,
        frame => element_or_term(Message)
    }),
    {stop, normal, State};
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
    #state{peer_node = Node, ping_id = Id} = State
) ->
    Measurements = maps:merge(
        #{latency => erlang:system_time(millisecond) - Ts},
        partisan_peer_socket:telemetry_stats(State#state.socket)
    ),

    ok = partisan_telemetry:execute(
        [partisan, connection, server, heartbeat],
        Measurements,
        #{
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
%% The phase failed: the peer gets the error result, and the connection ends.
auth_failed(Reason, Reply, State) ->
    ok = auth_telemetry(error, Reason),
    ?LOG_DEBUG(#{
        description => "Peer authentication failed, closing",
        reason => Reason
    }),
    _ = send_data(State#state.socket, partisan_peer_auth:enframe(Reply)),
    {stop, normal, State}.

%% @private
%% The tag of a frame, for a log line that does not hold the whole frame.
element_or_term(Term) when is_tuple(Term), tuple_size(Term) > 0 ->
    element(1, Term);
element_or_term(_) ->
    other.

%% @private
%% Counts the connection as one that has not authenticated, when the phase is
%% on. Over `peer_auth_max_pending' the node closes the connection at once.
enter_pending([], _Socket) ->
    false;
enter_pending(_Methods, Socket) ->
    case partisan_peer_auth:enter_pending() of
        ok ->
            true;
        {error, limit} ->
            ok = auth_telemetry(error, too_many_pending),
            _ = (try gen_tcp:close(Socket) catch _:_ -> ok end),
            exit(normal)
    end.

%% @private
leave_pending(#state{counted = true} = State) ->
    ok = partisan_peer_auth:leave_pending(),
    State#state{counted = false};
leave_pending(State) ->
    State.

%% @private
%% Sends the first frame of the connection: `hello' when the authentication
%% phase is off, as before the phase existed, and the challenge otherwise.
%% Then lets the socket deliver.
start_handshake(Methods, #state{socket = Socket} = State0) ->
    State = start_handshake_(Methods, State0),
    _ = partisan_peer_socket:setopts(Socket, [{active, once}]),
    State.

start_handshake_([], #state{socket = Socket} = State) ->
    send_message(Socket, {hello, partisan:node()}),
    State;
start_handshake_(Methods, #state{socket = Socket} = State) ->
    Binding =
        case partisan_peer_socket:channel_binding(Socket, server) of
            {ok, Value} ->
                Value;
            {error, _} ->
                %% The socket is gone, or cannot give its binding.
                _ = leave_pending(State),
                _ = partisan_peer_socket:close(Socket),
                exit(normal)
        end,
    {Challenge, Neg} = partisan_peer_auth:server_init(
        Methods, partisan:node(), Binding
    ),
    TRef = erlang:start_timer(
        partisan_peer_auth:timeout(), self(), auth_timeout
    ),
    _ = send_data(Socket, partisan_peer_auth:enframe(Challenge)),
    State#state{auth = {pending, Neg, TRef}}.

%% @private
auth_telemetry(Result, Detail) ->
    Metadata =
        case Result of
            ok -> maps:merge(#{result => ok}, maps:with([method, channel_binding], Detail));
            error -> #{result => error, reason => Detail}
        end,
    partisan_telemetry:execute(
        [partisan, connection, server, auth], #{count => 1}, Metadata
    ).

%% @private
%% Whether a peer may present `Node' in `hello'. A peer that authenticated
%% under a name presents that name. With the phase off, or `anonymous', no
%% name was authenticated and any name passes, as before the phase existed.
authenticated_as(_Node, undefined) ->
    true;
authenticated_as(_Node, #{method := anonymous}) ->
    true;
authenticated_as(Node, #{peer := Peer}) ->
    is_atom(Node) andalso atom_to_binary(Node, utf8) =:= Peer.

%% @private
handle_hello(Node, Channel, #state{} = State0) ->
    %% Get our tag, if set.
    Tag = partisan_config:get(tag, undefined),

    %% Store node and channel in the process dictionary.
    %% The key is read back as `{?MODULE, peer_node}' in `handle_inbound/2'
    %% to forward incoming messages with the correct origin node — naming
    %% them differently silently broke interposition (every Node arg ended
    %% up as `undefined').
    put({?MODULE, peer_node}, Node),
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
            % disable, use max_retries only
            deadline => 0,
            interval => Timeout,
            max_retries => Attempts
            %% No `backoff' key => fixed-interval retry (see partisan_retry).
        }
    ),

    State#state{
        ping_idle_timeout = IdleTimeout,
        ping_id = partisan:make_ref(),
        ping_retry = Retry
    };
maybe_enable_ping(State, #{enabled := false}) ->
    State.

%% @private
reset_ping(#state{ping_retry = undefined} = State) ->
    %% Pings are disabled.
    State;
reset_ping(State) ->
    %% The client ping idle_timeout is the same as ours,
    %% so we offset the server timeout to avoid synchronization.
    reset_ping(State, trunc(State#state.ping_idle_timeout * 0.25)).

%% @private
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
maybe_send_ping(Limit, State) when
    Limit == deadline orelse Limit == max_retries
->
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
