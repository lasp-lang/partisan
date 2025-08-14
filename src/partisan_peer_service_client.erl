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

-define(REPORT,
    #{
        channel => get({?MODULE, channel}),
        channel_opts => get({?MODULE, channel_opts}),
        listen_addr => get({?MODULE, listen_addr}),
        peer => get({?MODULE, peer})
    }
).
-define(REPORT(Report),
    maps:merge(Report, ?REPORT)
).


-record(state, {
    socket                  ::  optional(partisan_peer_socket:t()),
    listen_addr             ::  partisan:listen_addr(),
    channel                 ::  partisan:channel(),
    channel_opts            ::  partisan:channel_opts(),
    encoding_opts           ::  list(),
    from                    ::  pid(),
    peer                    ::  partisan:node_spec(),
    ping_idle_timeout       ::  non_neg_integer(),
    ping_tref               ::  optional(partisan_remote_ref:r()),
    ping_retry              ::  optional(partisan_retry:t()),
    ping_id                 ::  optional(partisan:reference())
}).

-type state() :: #state{}.

%% Macros.
-define(TIMEOUT, 1000).

%% API
-export([start_link/5]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
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
    From :: pid()) ->
    {ok, pid()} | ignore | {error, Reason :: any()}.

start_link(Peer, ListenAddr, Channel, ChannelOpts, From) ->
    gen_server:start_link(
        ?MODULE, [Peer, ListenAddr, Channel, ChannelOpts, From], []
    ).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================




-spec init(Args :: list()) -> {ok, state()} | {stop, Reason :: any()}.

init([Peer, ListenAddr, Channel, ChannelOpts, From]) ->
    case connect(ListenAddr, Channel, ChannelOpts) of
        {ok, Socket} ->
            ?LOG_INFO(#{
                description => "Connection established",
                peer => Peer,
                listen_addr => ListenAddr,
                channel => Channel
            }),

            %% For debugging, store information in the process dictionary.
            EgressDelay =  partisan_config:get(egress_delay, 0),

            put({?MODULE, from}, From),
            put({?MODULE, listen_addr}, ListenAddr),
            put({?MODULE, channel}, Channel),
            put({?MODULE, channel_opts}, ChannelOpts),
            put({?MODULE, peer}, Peer),
            put({?MODULE, egress_delay}, EgressDelay),

            EncodeOpts =
                case maps:get(compression, ChannelOpts, false) of
                    true ->
                        [compressed];

                    N when N >= 0, N =< 9 ->
                        [{compressed, N}];
                    _ ->
                        []
                end,

            State0 = #state{
                from = From,
                listen_addr = ListenAddr,
                channel = Channel,
                channel_opts = ChannelOpts,
                encoding_opts = EncodeOpts,
                socket = Socket,
                peer = Peer
            },


            State = maybe_enable_ping(
                State0, partisan_config:get(connection_ping, #{})
            ),
            {ok, State};

        {error, Reason} ->
            ?LOG_TRACE(#{
                description => "Unable to establish connection with peer",
                peer => Peer,
                listen_addr => ListenAddr,
                channel => Channel,
                reason => Reason
            }),
            %% We use shutdown to avoid a crash report
            {stop, normal}
    end.


-spec handle_call(term(), {pid(), term()}, state()) ->
    {reply, term(), state()}.


handle_call({send_message, Msg}, _From, #state{} = State) ->
    case get({?MODULE, egress_delay}) of
        0 ->
            ok;
        Other ->
            timer:sleep(Other)
    end,

    Data = partisan_util:encode(Msg, State#state.encoding_opts),

    case send_data(State#state.socket, Data) of
        ok ->
            ?LOG_TRACE("Dispatched message: ~p", [Msg]),
            {reply, ok, State};
        Error ->
            ?LOG_DEBUG("Message ~p failed to send: ~p", [Msg, Error]),
            {reply, Error, State}
    end;

handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.


-spec handle_cast(term(), state()) -> {noreply, state()}.

handle_cast({send_message, Msg}, #state{} = State) ->
    ?LOG_TRACE("Received cast: ~p", [Msg]),

    case get({?MODULE, egress_delay}) of
        0 ->
            ok;
        Other ->
            timer:sleep(Other)
    end,

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

handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),
    {noreply, State}.


-spec handle_info(term(), state()) ->
    {noreply, state()} | {stop, normal, state()}.

handle_info({Tag, _Socket, Data}, State)
when ?DATA_MSG(Tag), is_binary(Data) ->
    Msg = binary_to_term(Data),
    ?LOG_TRACE("Received tcp data at ~p: ~p", [self(), Msg]),
    ok = reset_socket_opts(State),
    handle_inbound(Msg, State);

handle_info({Tag, _Socket}, #state{} = State) when ?CLOSED_MSG(Tag) ->
    ?LOG_TRACE(
        "Connection to ~p has been closed for pid ~p",
        [State#state.peer, self()]
    ),
    {stop, normal, State};

handle_info(
    {timeout, Ref, ping_idle_timeout}, #state{ping_tref = Ref} = State) ->
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

terminate(Reason, #state{} = State)
when Reason == normal orelse
     Reason == shutdown orelse
     (is_tuple(Reason) andalso element(1, Reason) == shutdown) ->
    ?LOG_INFO(?REPORT(#{
        description => "Connection closed",
        reason => Reason
    })),
    ok = close_socket(State#state.socket);

terminate(Reason, #state{} = State) ->
    ?LOG_ERROR(?REPORT(#{
        description => "Connection closed",
        reason => Reason
    })),
    close_socket(State#state.socket).


-spec code_change(term() | {down, term()}, state(), term()) ->
    {ok, state()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @doc Test harness specific.
%%
%% If we're running a local test, we have to use the same IP address for
%% every bind operation, but a different port instead of the standard
%% port.
%%
connect(Node, Channel, ChannelOpts)
when is_atom(Node), is_atom(Channel), is_map(ChannelOpts) ->
    %% Used for testing when connect_disterl is enabled
    partisan_config:get(connect_disterl, false)
        orelse error(disterl_not_enabled),

    case rpc:call(Node, partisan_config, get, [listen_addrs]) of
        {badrpc, Reason} ->
            {error, Reason};

        [] ->
            {error, no_listen_addr};

        [ListenAddr|_] ->
            %% Use first address
            connect(ListenAddr, Channel, ChannelOpts)
    end;

connect(#{ip := Address, port := Port}, Channel, ChannelOpts)
when is_atom(Channel), is_map(ChannelOpts) ->
    SocketOpts = [
        binary,
        {active, once},
        {packet, 4},
        {keepalive, true}
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
send_message(Socket, Message) ->
    send_data(Socket, erlang:term_to_iovec(Message)).


%% @private
send_data(undefined, _) ->
    {error, no_socket};

send_data(Socket, Data) ->
    partisan_peer_socket:send(Socket, Data).


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
    Socket = State#state.socket,
    Msg = {hello, partisan:node(), State#state.channel},

    case send_message(Socket, Msg) of
        ok ->
            {noreply, reset_ping(State)};

        {error, Reason} = Error ->
            ?LOG_INFO(#{
                description => "Failed to send hello message to node",
                node => Node,
                reason => Reason
            }),
            {stop, Error, State}
    end;

handle_inbound({hello, A}, #state{peer = #{name := B}} = State)
when A =/= B ->
    %% Peer isn't who it should be, abort.
    ?LOG_ERROR(#{
        description => "Unexpected peer, aborting",
        got => A,
        expected => B
    }),
    {stop, {unexpected_peer, A, B}, State};

handle_inbound(
    #ping{from = Node} = Ping, #state{peer = #{name := Node}} = State) ->
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
    #state{peer = #{name := Node}, ping_id = Id} = State) ->
    ok = telemetry:execute(
        [partisan, connection, client, hearbeat],
        #{latency => erlang:system_time(millisecond) - Ts},
        #{
            node => partisan:node(),
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
            deadline => 0, % disable, use max_retries only
            interval => Timeout,
            max_retries => Attempts,
            backoff_enabled => false
        }
    ),

    State#state{
        ping_idle_timeout = IdleTimeout,
        ping_retry = Retry
    };

maybe_enable_ping(#{enabled := false}, State) ->
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

