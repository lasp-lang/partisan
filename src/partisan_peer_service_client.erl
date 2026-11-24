%% =============================================================================
%% SPDX-FileCopyrightText: 2016 Christopher Meiklejohn
%% SPDX-FileCopyrightText: 2021 - 2025 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================

-module(partisan_peer_service_client).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").
-include("partisan_peer_socket.hrl").

-define(REPORT, #{
    channel => get({?MODULE, channel}),
    channel_opts => get({?MODULE, channel_opts}),
    listen_addr => get({?MODULE, listen_addr}),
    peer => get({?MODULE, peer})
}).
-define(REPORT(Report),
    maps:merge(Report, ?REPORT)
).

-record(state, {
    socket :: optional(partisan_peer_socket:t()),
    %% The authentication phase of the connection (ADR-000005):
    %% - `undefined': the socket is not connected.
    %% - `{pending, Negotiator}': the phase has not completed.
    %%   `handle_info/2' gives every frame to `partisan_peer_auth'.
    %% - a map: the phase completed.
    auth ::
        undefined
        | {pending, partisan_peer_auth:client()}
        | partisan_peer_auth:authn(),
    %% The messages the manager gave this process before the handshake
    %% completed, newest first. The handshake is complete when this process
    %% has sent its `hello'. Until then the peer gets no message: it has not
    %% authenticated, and the accepting node takes `hello' first.
    %%
    %% `undefined' when the process holds nothing back: after the handshake,
    %% and always when `peer_auth_methods' is empty, where a message goes out
    %% at once as before the phase existed.
    outbox :: optional([{cast, iodata()} | {call, gen_server:from(), iodata()}]),
    %% Whether the socket is raw: during the phase of a node with a non-empty
    %% `peer_auth_methods', which takes the frames out of the bytes itself.
    raw = false :: boolean(),
    %% The bytes of a frame of the phase that is not whole yet.
    buffer = <<>> :: binary(),
    %% Stops a handshake that does not complete. `undefined' when
    %% `peer_auth_methods' is empty: such a node waits for `hello' without a
    %% limit, as before.
    handshake_tref :: optional(reference()),
    listen_addr :: partisan:listen_addr(),
    channel :: partisan:channel(),
    channel_opts :: partisan:channel_opts(),
    %% `undefined' until the socket is connected (see `connect_socket/1').
    encoding_opts :: optional(list()),
    from :: pid(),
    peer :: partisan:node_spec(),
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

-type state() :: #state{}.

%% Macros.
-define(TIMEOUT, 1000).

%% API
-export([start_link/5]).
-export([start_link/6]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_continue/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% =============================================================================
%% API
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc Start and link to calling process.
%% If the process is started and can get a connection it returns `{ok, pid()}'.
%% Otherwise if it fails with `{error, Reason :: any()}'.
%% @end
%% -----------------------------------------------------------------------------
-spec start_link(
    Peer :: partisan:node_spec(),
    ListenAddr :: partisan:listen_addr(),
    Channel :: partisan:channel(),
    ChannelOpts :: partisan:channel_opts(),
    From :: pid()
) ->
    {ok, pid()} | ignore | {error, Reason :: any()}.

start_link(Peer, ListenAddr, Channel, ChannelOpts, From) ->
    start_link(Peer, ListenAddr, Channel, ChannelOpts, From, #{}).

%% -----------------------------------------------------------------------------
%% @doc Same as {@link start_link/5} but with options.
%%
%% With `#{async => true}' the call returns `{ok, pid()}' before the socket is
%% connected, so the caller is not blocked for up to the connect timeout. The
%% client then connects and, on success, sends `{client_connected, pid()}' to
%% `From'. On failure it exits with reason `normal', which a linked `From'
%% that traps exits receives as `{'EXIT', pid(), normal}'.
%%
%% The caller must not store the pid in {@link partisan_peer_connections}
%% before `{client_connected, pid()}' arrives: until then the process has no
%% socket. The message is sent before the client reads from the socket, so it
%% always precedes the `connected' message of the handshake.
%% @end
%% -----------------------------------------------------------------------------
-spec start_link(
    Peer :: partisan:node_spec(),
    ListenAddr :: partisan:listen_addr(),
    Channel :: partisan:channel(),
    ChannelOpts :: partisan:channel_opts(),
    From :: pid(),
    Opts :: #{async => boolean()}
) ->
    {ok, pid()} | ignore | {error, Reason :: any()}.

start_link(Peer, ListenAddr, Channel, ChannelOpts, From, Opts) ->
    gen_server:start_link(
        ?MODULE, [Peer, ListenAddr, Channel, ChannelOpts, From, Opts], []
    ).

%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================

-spec init(Args :: list()) ->
    {ok, state()} | {ok, state(), {continue, connect}} | {stop, normal}.

init([Peer, ListenAddr, Channel, ChannelOpts, From, Opts]) ->
    State = #state{
        from = From,
        listen_addr = ListenAddr,
        channel = Channel,
        channel_opts = ChannelOpts,
        peer = Peer
    },

    case maps:get(async, Opts, false) of
        true ->
            {ok, State, {continue, connect}};
        false ->
            case connect_socket(State) of
                {ok, Connected} ->
                    {ok, Connected};
                {error, _} ->
                    {stop, normal}
            end
    end.

-spec handle_continue(connect, state()) ->
    {noreply, state()} | {stop, normal, state()}.

handle_continue(connect, #state{from = From} = State0) ->
    case connect_socket(State0) of
        {ok, State} ->
            From ! {client_connected, self()},
            {noreply, State};
        {error, _} ->
            {stop, normal, State0}
    end.

-spec handle_call(term(), {pid(), term()}, state()) ->
    {reply, term(), state()}.

handle_call({Tag, Arg}, From, #state{outbox = Outbox} = State) when
    (Tag == send_message orelse Tag == send_encoded), is_list(Outbox)
->
    %% The handshake has not completed: the message waits in the outbox. A
    %% manager can hold this process as a connection from the moment the
    %% socket connects. The caller gets its reply when the message goes out.
    Entry = {call, From, outbox_data(Tag, Arg, State)},
    {noreply, State#state{outbox = [Entry | Outbox]}};
handle_call({send_message, Msg}, _From, #state{} = State) ->
    maybe_egress_delay(),

    Data = partisan_util:encode(Msg, State#state.encoding_opts),

    case send_data(State#state.socket, Data) of
        ok ->
            ?LOG_TRACE("Dispatched message: ~p", [Msg]),
            {reply, ok, State};
        Error ->
            ?LOG_DEBUG("Message ~p failed to send: ~p", [Msg, Error]),
            {reply, Error, State}
    end;
handle_call({send_encoded, Data}, _From, #state{} = State) ->
    %% `Data' was already encoded by the calling process — see
    %% `partisan_util:channel_encode_opts/1'. This process is a pure socket
    %% pump for this path.
    maybe_egress_delay(),

    case send_data(State#state.socket, Data) of
        ok ->
            {reply, ok, State};
        Error ->
            ?LOG_DEBUG("Encoded message failed to send: ~p", [Error]),
            {reply, Error, State}
    end;
handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.

handle_cast({Tag, Arg}, #state{outbox = Outbox} = State) when
    (Tag == send_message orelse Tag == send_encoded), is_list(Outbox)
->
    %% See the same clause of `handle_call/3'.
    Entry = {cast, outbox_data(Tag, Arg, State)},
    {noreply, State#state{outbox = [Entry | Outbox]}};
handle_cast({send_message, Msg}, #state{} = State) ->
    ?LOG_TRACE("Received cast: ~p", [Msg]),

    maybe_egress_delay(),

    Data = partisan_util:encode(Msg, State#state.encoding_opts),

    case send_data(State#state.socket, Data) of
        ok ->
            ?LOG_TRACE("Dispatched message: ~p", [Msg]),
            ok;
        Error ->
            ?LOG_ERROR(#{
                description => "Failed to send message",
                data => Msg,
                error => Error
            })
    end,
    {noreply, State};
handle_cast({send_encoded, Data}, #state{} = State) ->
    %% Pre-encoded by the calling process (see the `send_encoded' clause of
    %% `handle_call/3'). Encoding in the caller keeps serialisation — and
    %% compression, when enabled — off this process, so a connection does not
    %% serialise the CPU cost of every message sent to its peer. It also keeps
    %% the term out of this mailbox: refc binaries are not copied.
    maybe_egress_delay(),

    case send_data(State#state.socket, Data) of
        ok ->
            ok;
        Error ->
            ?LOG_ERROR(#{
                description => "Failed to send encoded message",
                error => Error
            })
    end,
    {noreply, State};
handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),
    {noreply, State}.

-spec handle_info(term(), state()) ->
    {noreply, state()} | {stop, normal, state()}.

handle_info(
    {Tag, _Socket, Data}, #state{auth = {pending, _}, raw = true} = State
) when
    ?DATA_MSG(Tag), is_binary(Data)
->
    %% The phase has not completed and the socket is raw: this process takes
    %% the frame out of the bytes, which bounds its size.
    Bytes = <<(State#state.buffer)/binary, Data/binary>>,

    case partisan_peer_auth:deframe(Bytes) of
        more ->
            ok = reset_socket_opts(State),
            {noreply, State#state{buffer = Bytes}};
        {ok, Frame, <<>>} ->
            handle_auth_frame(Frame, State#state{buffer = <<>>});
        {ok, _Frame, _Rest} ->
            %% Bytes after the frame. An honest server sends its next frame
            %% only after it has read this node's answer.
            auth_failed(trailing_bytes, State);
        {error, Reason} ->
            auth_failed(Reason, State)
    end;
handle_info(
    {Tag, _Socket, Data}, #state{auth = {pending, _}} = State
) when
    ?DATA_MSG(Tag), is_binary(Data)
->
    %% A node with an empty `peer_auth_methods': the socket frames as the
    %% existing protocol does, and `Data' is a whole frame.
    handle_auth_frame(Data, State);
handle_info(
    {timeout, TRef, auth_timeout}, #state{handshake_tref = TRef} = State
) ->
    %% The phase did not complete, or the server did not send `hello' after
    %% it.
    ?LOG_WARNING(
        ?REPORT(#{description => "Peer handshake timed out, closing"})
    ),
    {stop, {shutdown, {auth_failed, timeout}}, State};
handle_info({Tag, _Socket, Data}, State) when
    ?DATA_MSG(Tag), is_binary(Data)
->
    Msg = binary_to_term(Data),
    ?LOG_TRACE("Received tcp data at ~p: ~p", [self(), Msg]),
    ok = reset_socket_opts(State),
    handle_inbound(Msg, State);
handle_info({Tag, _Socket, Reason}, #state{} = State) when ?ERROR_MSG(Tag) ->
    %% For example a frame over the socket's limit. A TLS socket reports the
    %% error and stays open, so the process closes it. The reason of a TLS
    %% `invalid_packet' carries the frame, which stays out of the exit reason.
    Error =
        case Reason of
            {Name, _} when is_atom(Name) -> Name;
            _ -> Reason
        end,
    {stop, {shutdown, {socket_error, Error}}, State};
handle_info({Tag, _Socket}, #state{} = State) when ?CLOSED_MSG(Tag) ->
    ?LOG_TRACE(
        "Connection to ~p has been closed for pid ~p",
        [State#state.peer, self()]
    ),
    {stop, normal, State};
handle_info(
    {timeout, Ref, ping_idle_timeout}, #state{ping_tref = Ref} = State
) ->
    ?LOG_INFO(#{
        description => "Connection idle, sending ping",
        peer => State#state.peer,
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
handle_info(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled info event", event => Event}),
    {noreply, State}.

-spec terminate(term(), state()) -> term().

terminate(_, #state{socket = undefined}) ->
    %% An async client whose connection attempt failed: nothing was opened.
    ok;
terminate(Reason, #state{} = State) when
    Reason == normal orelse
        Reason == shutdown orelse
        (is_tuple(Reason) andalso element(1, Reason) == shutdown)
->
    ok = fail_outbox(State),
    ?LOG_INFO(
        ?REPORT(#{
            description => "Connection closed",
            reason => Reason
        })
    ),
    ok = close_socket(State#state.socket);
terminate(Reason, #state{} = State) ->
    ok = fail_outbox(State),
    ?LOG_ERROR(
        ?REPORT(#{
            description => "Connection closed",
            reason => Reason
        })
    ),
    close_socket(State#state.socket).

-spec code_change(term() | {down, term()}, state(), term()) ->
    {ok, state()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Connects the socket for a client built by `init/1' and completes its state.
%% Shared by the synchronous path (`init/1') and the async one
%% (`handle_continue/2').
connect_socket(#state{} = State0) ->
    #state{
        peer = Peer,
        listen_addr = ListenAddr,
        channel = Channel,
        channel_opts = ChannelOpts
    } = State0,

    %% One read of the method list decides everything about this connection.
    Methods = partisan_peer_auth:methods(),

    T0 = erlang:monotonic_time(millisecond),
    Result =
        maybe
            ok ?= allowed(maps:get(name, Peer)),
            {ok, Socket0} ?= connect(ListenAddr, Channel, ChannelOpts, Methods),
            {ok, Binding0} ?= channel_binding(Methods, Socket0),
            {ok, Socket0, Binding0}
        end,
    Latency = erlang:monotonic_time(millisecond) - T0,

    partisan_telemetry:execute(
        [partisan, connection, client, connect],
        #{latency => Latency},
        #{
            peer_node => maps:get(name, Peer),
            channel => Channel,
            listen_addr => ListenAddr,
            result => element(1, Result)
        }
    ),

    case Result of
        {ok, Socket, Binding} ->
            ?LOG_INFO(#{
                description => "Connection established",
                peer => Peer,
                listen_addr => ListenAddr,
                channel => Channel
            }),

            %% For debugging, store information in the process dictionary.
            put({?MODULE, from}, State0#state.from),
            put({?MODULE, listen_addr}, ListenAddr),
            put({?MODULE, channel}, Channel),
            put({?MODULE, channel_opts}, ChannelOpts),
            put({?MODULE, peer}, Peer),
            put({?MODULE, egress_delay}, partisan_config:get(egress_delay, 0)),

            Neg = partisan_peer_auth:client_init(
                Methods, partisan:node(), maps:get(name, Peer), Binding
            ),
            {TRef, Outbox} =
                case Methods of
                    [] ->
                        {undefined, undefined};
                    _ ->
                        Timer = erlang:start_timer(
                            partisan_peer_auth:timeout(), self(), auth_timeout
                        ),
                        {Timer, []}
                end,

            State1 = State0#state{
                auth = {pending, Neg},
                raw = Methods =/= [],
                handshake_tref = TRef,
                outbox = Outbox,
                encoding_opts = partisan_util:channel_encode_opts(ChannelOpts),
                socket = Socket
            },

            State = maybe_enable_ping(
                State1, partisan_config:get(connection_ping, #{})
            ),
            {ok, State};
        {error, Reason} = Error ->
            ?LOG_TRACE(#{
                description => "Unable to establish connection with peer",
                peer => Peer,
                listen_addr => ListenAddr,
                channel => Channel,
                reason => Reason
            }),
            Error
    end.

%% @doc Test harness specific.
%%
%% If we're running a local test, we have to use the same IP address for
%% every bind operation, but a different port instead of the standard
%% port.
%%
connect(Node, Channel, ChannelOpts, Methods) when
    is_atom(Node), is_atom(Channel), is_map(ChannelOpts)
->
    %% Used for testing when connect_disterl is enabled
    partisan_config:get(connect_disterl, false) orelse
        error(disterl_not_enabled),

    case rpc:call(Node, partisan_config, get, [listen_addrs]) of
        {badrpc, Reason} ->
            {error, Reason};
        [] ->
            {error, no_listen_addr};
        [ListenAddr | _] ->
            %% Use first address
            connect(ListenAddr, Channel, ChannelOpts, Methods)
    end;
connect(#{ip := Address, port := Port}, Channel, ChannelOpts, Methods) when
    is_atom(Channel), is_map(ChannelOpts)
->
    SocketOpts = [
        binary,
        {active, once},
        {keepalive, true}
        %% Raw when the phase is on, where this process takes the frames
        %% itself. `authenticated/2' sets the framing of the existing
        %% protocol.
        | partisan_peer_auth:initial_socket_opts(Methods)
    ],

    Opts = [{monotonic, maps:get(monotonic, ChannelOpts, false)}],

    Result = partisan_peer_socket:connect(
        Address, Port, SocketOpts, ?TIMEOUT, Opts
    ),

    case Result of
        {ok, Socket} ->
            {ok, Socket};
        {error, Reason} when ?IS_INET_POSIX(Reason) ->
            %% TODO We do not want to log here becuase this can be high frequency
            %% so what we should do is store the latest error on the
            %% partisan_peer_connections as a status that is cleanedup when we
            %% establish the connection
            {error, Reason};
        {error, Reason} ->
            ?LOG_ERROR(#{
                description => "Error while trying to establish connection",
                channel => Channel,
                reason => Reason,
                ip => Address,
                port => Port
            }),
            {error, Reason}
    end.

%% @private
%% The channel binding for the authentication phase. A node with an empty
%% method list makes no proof and needs none. A socket that cannot give its
%% binding is closed: the phase does not continue without it.
channel_binding([], _Socket) ->
    {ok, none};
channel_binding(_Methods, Socket) ->
    case partisan_peer_socket:channel_binding(Socket, client) of
        {ok, _} = OK ->
            OK;
        {error, _} = Error ->
            _ = partisan_peer_socket:close(Socket),
            Error
    end.

%% @private
%% Gives a frame to the negotiator while the phase has not completed.
handle_auth_frame(Frame, #state{auth = {pending, Neg}} = State) ->
    case partisan_peer_auth:client_handle(Frame, Neg) of
        {reply, Reply, Neg1} ->
            maybe
                ok ?= send_auth_frame(Reply, State),
                ok ?= reset_socket_opts(State),
                {noreply, State#state{auth = {pending, Neg1}}}
            else
                {error, Reason} ->
                    {stop, {shutdown, Reason}, State}
            end;
        {ok, Authn} ->
            %% The server authenticated and sends no `hello': its challenge
            %% carried its name, which the negotiator checked. This node
            %% sends its `hello' now.
            send_hello(authenticated(Authn, State));
        {ok, Authn, Msg} ->
            %% The server runs no phase: `Msg' is its `hello'.
            handle_inbound(Msg, authenticated(Authn, State));
        {error, Reason} ->
            auth_failed(Reason, State)
    end.

%% @private
auth_failed(Reason, State) ->
    ?LOG_WARNING(
        ?REPORT(#{
            description => "Peer authentication failed, closing",
            reason => Reason
        })
    ),
    {stop, {shutdown, {auth_failed, Reason}}, State}.

%% @private
%% A raw socket does not add the length of a frame.
send_auth_frame(Frame, #state{raw = true, socket = Socket}) ->
    send_control(Socket, partisan_peer_auth:enframe(Frame));
send_auth_frame(Frame, #state{raw = false, socket = Socket}) ->
    send_control(Socket, Frame).

%% @private
%% Sends this node's `hello', which completes the handshake.
send_hello(#state{socket = Socket, peer = #{name := Node}} = State) ->
    Msg = {hello, partisan:node(), State#state.channel},

    case send_control(Socket, erlang:term_to_iovec(Msg)) of
        ok ->
            {noreply, reset_ping(handshake_complete(State))};
        {error, Reason} = Error ->
            ?LOG_INFO(#{
                description => "Failed to send hello message to node",
                node => Node,
                reason => Reason
            }),
            {stop, Error, State}
    end.

%% @private
%% The gate of `net_kernel:allow/1': this node does not dial a node that is
%% not in `peer_auth_allowed_nodes'.
allowed(Node) ->
    case partisan_peer_auth:is_allowed(Node) of
        true -> ok;
        false -> {error, not_allowed}
    end.

%% @private
%% The bytes of a message that waits in the outbox. `send_message' is encoded
%% here, as it is when it goes out at once.
outbox_data(send_message, Msg, State) ->
    partisan_util:encode(Msg, State#state.encoding_opts);
outbox_data(send_encoded, Data, _State) ->
    Data.

%% @private
%% The handshake is complete: this process has sent its `hello'. The messages
%% that waited go out in the order they came, after `hello' on the wire.
handshake_complete(#state{outbox = Outbox, socket = Socket} = State) ->
    _ =
        is_reference(State#state.handshake_tref) andalso
            erlang:cancel_timer(State#state.handshake_tref),
    lists:foreach(
        fun
            ({cast, Data}) ->
                _ = send_data(Socket, Data);
            ({call, From, Data}) ->
                gen_server:reply(From, send_data(Socket, Data))
        end,
        case Outbox of
            undefined -> [];
            _ -> lists:reverse(Outbox)
        end
    ),
    State#state{outbox = undefined, handshake_tref = undefined}.

%% @private
%% The connection ends before the handshake completed: each caller that waits
%% for a reply gets an error, and does not wait for its own timeout.
fail_outbox(#state{outbox = Outbox}) when is_list(Outbox) ->
    lists:foreach(
        fun
            ({call, From, _}) -> gen_server:reply(From, {error, closed});
            ({cast, _}) -> ok
        end,
        Outbox
    );
fail_outbox(_) ->
    ok.

%% @private
%% Sends a frame of the handshake. It goes out on a monotonic channel too.
send_control(Socket, Data) ->
    partisan_peer_socket:send_control(Socket, Data).

%% @private
send_message(Socket, Message) ->
    send_data(Socket, erlang:term_to_iovec(Message)).

%% @private
send_data(undefined, _) ->
    {error, no_socket};
send_data(Socket, Data) ->
    partisan_peer_socket:send(Socket, Data).

%% @private
%% Test-only knob (`egress_delay') used by the fault-injection harness to slow
%% a connection down. Kept in the process dictionary and read on every send.
maybe_egress_delay() ->
    case get({?MODULE, egress_delay}) of
        0 ->
            ok;
        undefined ->
            ok;
        Other ->
            timer:sleep(Other)
    end.

%% @private
close_socket(undefined) ->
    ok;
close_socket(Socket) ->
    partisan_peer_socket:close(Socket).

%% @private
handle_inbound({state, Tag, LocalState}, #state{} = State) ->
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

    {noreply, reset_ping(State)};
handle_inbound({hello, Node}, #state{peer = #{name := Node}} = State) ->
    %% The `hello' of a server that runs no phase.
    send_hello(State);
handle_inbound({hello, A}, #state{peer = #{name := B}} = State) when
    A =/= B
->
    %% Peer isn't who it should be, abort.
    ?LOG_ERROR(#{
        description => "Unexpected peer, aborting",
        got => A,
        expected => B
    }),
    {stop, {unexpected_peer, A, B}, State};
handle_inbound(
    #ping{from = Node} = Ping, #state{peer = #{name := Node}} = State
) ->
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
    #state{peer = #{name := Node}, ping_id = Id} = State
) ->
    Measurements = maps:merge(
        #{latency => erlang:system_time(millisecond) - Ts},
        partisan_peer_socket:telemetry_stats(State#state.socket)
    ),

    ok = partisan_telemetry:execute(
        [partisan, connection, client, heartbeat],
        Measurements,
        #{
            channel => State#state.channel,
            listen_addr => State#state.listen_addr,
            socket => State#state.socket,
            peer_node => Node
        }
    ),
    {noreply, reset_ping(State)};
handle_inbound(#pong{} = Pong, #state{} = State) ->
    {stop, {invalid_ping_response, Pong}, State};
handle_inbound(Msg, State) ->
    ?LOG_WARNING(#{
        description => "Received invalid message",
        message => Msg
    }),
    {stop, normal, State}.

%% @private
%% Completes the authentication phase: the socket takes the framing of the
%% existing protocol and delivers again.
authenticated(Authn, #state{auth = {pending, _}} = State) ->
    Opts = [{active, once} | partisan_peer_auth:established_socket_opts()],
    _ = partisan_peer_socket:setopts(State#state.socket, Opts),
    State#state{auth = Authn, raw = false}.

%% @private
reset_socket_opts(State) ->
    partisan_peer_socket:setopts(State#state.socket, [{active, once}]).

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
        ping_retry = Retry
    };
maybe_enable_ping(State, #{enabled := false}) ->
    State.

%% @private
reset_ping(#state{ping_retry = undefined} = State) ->
    State;
reset_ping(#state{ping_tref = undefined} = State) ->
    Time = State#state.ping_idle_timeout,
    Ref = erlang:start_timer(Time, self(), ping_idle_timeout),
    State#state{ping_tref = Ref};
reset_ping(#state{} = State) ->
    _ = erlang:cancel_timer(State#state.ping_tref),
    {_, Retry} = partisan_retry:succeed(State#state.ping_retry),
    Time = State#state.ping_idle_timeout,
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
