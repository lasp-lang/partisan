%%-------------------------------------------------------------------
%%
%% Copyright (c) 2016, James Fish <james@fishcakez.com>
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%-------------------------------------------------------------------
%% @doc This module provides a `gen_tcp' acceptor behaviour for use with
%% `acceptor_pool'. There are three callbacks.
%%
%% Before accepting a socket `acceptor_init/3':
%% ```
%% -callback acceptor_init(SockName, LSock, Args) ->
%%     {ok, State} | {ok, State, TimeoutOrHib} | ignore | {error, Reason} when
%%       SockName :: acceptor_pool:name(),
%%       LSock :: gen_tcp:socket(),
%%       Args :: term(),
%%       State :: term(),
%%       TimeoutOrHib :: timeout() | hibernate,
%%       Reason :: term().
%% '''
%% `SockName' is the `inet:sockname/1' of the listen socket, which may be
%% `{{0,0,0,0}, Port}' if bound to all ip addresses. `LSock' is the listen
%% socket and `Args' is the argument from the `acceptor_pool:acceptor_spec/0'
%% in the `acceptor_pool'. This callback should do any setup required before
%% trying to accept on the socket.
%%
%% To be able to gracefully close open connections it is recommended for an
%% acceptor process to `monitor(port, LSock)' and gracefully close on receiving
%% the DOWN messages. This can be combined with the listen socket being shut
%% down before the `acceptor_pool' in the supervisor tree (e.g. after the
%% `acceptor_pool' in a `rest_for_one' or `one_for_all') and the
%% `acceptor_pool' defining the `grace' specification.
%%
%% To accept on the socket for `Timeout' timeout return
%% `{ok, State, Timeout :: timeout()}' with `{ok, State}' and
%% `{ok, State, hibernate}' being equivalent to `{ok, State, infinity}' wit the
%% later also causing the process to hibernate before trying to accept a
%% connection. `State' will be passed to either `acceptor_continue/3' or
%% `acceptor_continue/2'.
%%
%% To ignore this process and try again with another process return `ignore', or
%% if an error occurs `{error, term()}'. Start errors always count towards the
%% restart intensity of the `acceptor_pool', even with a `restart' of
%% `temporary' but `ignore' does not.
%%
%% Once a socket is accepted `acceptor_continue/3':
%% ```
%% -callback acceptor_continue(PeerName, Sock, State) -> no_return() when
%%      PeerName :: acceptor_pool:name(),
%%      Sock :: gen_tcp:socket(),
%%      State :: term().
%% '''
%% `PeerName' the `inet:peername/1' of the accepted socket `Sock' and `State' is
%% the state returned by `acceptor_init/3'. The callback module now has full
%% control of the process and should enter its main loop, perhaps with
%% `gen_statem:enter_loop/6'.
%%
%% It can be a long wait for a socket and so it possible for the `State' to have
%% been created using an old version of the module. During the wait the process
%% is hidden from the supervision tree and so hidden from the relup system.
%% However it is possible to `load' both `acceptor' and the acceptor callback
%% module during an appup with a soft post purge because neither module is on
%% the call stack.
%%
%% If accepting a socket fails `acceptor_terminate/2':
%% ```
%% -callback acceptor_terminate(Reason, State) -> any() when
%%      Reason :: {shutdown, timeout | closed | system_limit | inet:posix()} |
%%                 term(),
%%       State :: term().
%% '''
%% `Reason' is the reason for termination, and the exit reason of the process.
%% If a socket error caused the termination the reason is
%% `{shutdown, timeout | closed | system_limit | inet:posix()}'. Otherwise the
%% reason is an exit signal from the `acceptor_pool'.
%%
%% `State' is the state returned by `acceptor_init/3'.
-module(acceptor).
-behaviour(acceptor_loop).

%% public api

-export([spawn_opt/6]).

%% private api

-export([init_it/7]).

%% acceptor_loop api

-export([acceptor_continue/3]).
-export([acceptor_terminate/3]).

-type option() :: {spawn_opt, [proc_lib:spawn_option()]}.

-export_type([option/0]).

-type data() :: #{
    module => module(),
    state => term(),
    socket_module => module(),
    socket => gen_tcp:socket(),
    ack => reference()
}.

-callback acceptor_init(SockName, LSock, Args) ->
    {ok, State} | {ok, State, TimeoutOrHib} | ignore | {error, Reason}
when
    SockName :: acceptor_pool:name(),
    LSock :: gen_tcp:socket(),
    Args :: term(),
    State :: term(),
    TimeoutOrHib :: timeout() | hibernate,
    Reason :: term().

-callback acceptor_continue(PeerName, Sock, State) -> no_return() when
    PeerName :: acceptor_pool:name(),
    Sock :: gen_tcp:socket(),
    State :: term().

-callback acceptor_terminate(Reason, State) -> any() when
    Reason ::
        {shutdown, timeout | closed | system_limit | inet:posix()}
        | term(),
    State :: term().

%% public api

%% @private
-spec spawn_opt(Mod, SockMod, SockName, LSock, Args, Opts) -> {Pid, Ref} when
    Mod :: module(),
    SockMod :: module(),
    SockName :: acceptor_pool:name(),
    LSock :: gen_tcp:socket(),
    Args :: term(),
    Opts :: [option()],
    Pid :: pid(),
    Ref :: reference().
spawn_opt(Mod, SockMod, SockName, LSock, Args, Opts) ->
    AckRef = make_ref(),
    SArgs = [self(), AckRef, Mod, SockMod, SockName, LSock, Args],
    Pid = proc_lib:spawn_opt(?MODULE, init_it, SArgs, spawn_options(Opts)),
    {Pid, AckRef}.

%% private api

%% @private
-ifdef(OTP_RELEASE).
init_it(Parent, AckRef, Mod, SockMod, SockName, LSock, Args) ->
    _ = put('$initial_call', {Mod, init, 3}),
    try Mod:acceptor_init(SockName, LSock, Args) of
        Result ->
            handle_init(Result, Mod, SockMod, LSock, Parent, AckRef, SockName)
    catch
        throw:Result ->
            handle_init(Result, Mod, SockMod, LSock, Parent, AckRef, SockName);
        error:Reason:Stacktrace ->
            exit({Reason, Stacktrace})
    end.
-else.
init_it(Parent, AckRef, Mod, SockMod, SockName, LSock, Args) ->
    _ = put('$initial_call', {Mod, init, 3}),
    try Mod:acceptor_init(SockName, LSock, Args) of
        Result ->
            handle_init(Result, Mod, SockMod, LSock, Parent, AckRef, SockName)
    catch
        throw:Result ->
            handle_init(Result, Mod, SockMod, LSock, Parent, AckRef, SockName);
        error:Reason ->
            exit({Reason, erlang:get_stacktrace()})
    end.
-endif.

%% acceptor_loop api

%% @private
-spec acceptor_continue({ok, Sock} | {error, Reason}, Parent, Data) ->
    no_return()
when
    Sock :: gen_tcp:socket(),
    Reason :: timeout | closed | system_limit | inet:posix(),
    Parent :: pid(),
    Data :: data().
acceptor_continue({ok, Sock}, Parent, #{socket := LSock} = Data) ->
    % As done by prim_inet:accept/2
    OptNames = [active, nodelay, keepalive, delay_send, priority, tos],
    case inet:getopts(LSock, OptNames) of
        {ok, Opts} ->
            success(Sock, Opts, Parent, Data);
        {error, Reason} ->
            gen_tcp:close(Sock),
            failure(Reason, Parent, Data)
    end;
acceptor_continue({error, Reason}, Parent, Data) ->
    case is_connection_error(Reason) of
        true -> conn_failure(Reason, Parent, Data);
        false -> failure(Reason, Parent, Data)
    end.

%% @private
%% @doc Errors that describe the connection being accepted rather than the
%% listening socket.
%%
%% A peer that sends RST between the SYN and our `accept' surfaces here: on
%% darwin as `einval', elsewhere conventionally as `econnaborted'. Neither says
%% anything about the listener's health.
is_connection_error(einval) -> true;
is_connection_error(econnaborted) -> true;
is_connection_error(closed) -> true;
is_connection_error(_) -> false.

%% @private
-spec acceptor_terminate(Reason, Parent, Data) -> no_return() when
    Reason :: term(),
    Parent :: pid(),
    Data :: data().
acceptor_terminate(Reason, _, Data) ->
    terminate(Reason, Data).

%% internal

spawn_options(Opts) ->
    case lists:keyfind(spawn_options, 1, Opts) of
        {_, SpawnOpts} -> [link | SpawnOpts];
        false -> [link]
    end.

handle_init({ok, State}, Mod, SockMod, LSock, Parent, AckRef, SockName) ->
    handle_init(
        {ok, State, infinity}, Mod, SockMod, LSock, Parent, AckRef, SockName
    );
handle_init(
    {ok, State, Timeout}, Mod, SockMod, LSock, Parent, AckRef, SockName
) ->
    Data = #{
        module => Mod,
        state => State,
        socket_module => SockMod,
        socket => LSock,
        ack => AckRef,
        sockname => SockName
    },
    % Use another module to accept so can reload this module.
    acceptor_loop:accept(LSock, Timeout, Parent, ?MODULE, Data);
handle_init(ignore, _, _, _, Parent, AckRef, _) ->
    _ = Parent ! {'IGNORE', self(), AckRef},
    exit(normal);
handle_init(Other, _, _, _, _, _, _) ->
    handle_init(Other).

handle_init({error, Reason}) ->
    exit(Reason);
handle_init(Other) ->
    exit({bad_return_value, Other}).

success(Sock, Opts, Parent, Data) ->
    case inet:peername(Sock) of
        {ok, PeerName} ->
            AcceptMsg = accept_message(Sock, PeerName, Data),
            _ = Parent ! AcceptMsg,
            continue(Sock, Opts, PeerName, Data);
        {error, Reason} ->
            %% `Sock' is the accepted socket, so this describes one connection
            %% and never the listener. `'ACCEPT'' has not been sent yet, so the
            %% pool still counts this process as accepting: it must be
            %% cancelled, otherwise `acceptor_pool:acceptor_exit/3' treats the
            %% exit as a start error and charges it to the pool's restart
            %% intensity. See `conn_failure/3'.
            gen_tcp:close(Sock),
            conn_failure(Reason, Parent, Data)
    end.

accept_message(Sock, PeerName, #{
    ack := AckRef,
    sockname := {{0, 0, 0, 0}, _}
}) ->
    case inet:sockname(Sock) of
        {ok, SockName} ->
            {'ACCEPT', self(), AckRef, SockName, PeerName};
        _ ->
            {'ACCEPT', self(), AckRef, PeerName}
    end;
accept_message(_, PeerName, #{ack := AckRef}) ->
    {'ACCEPT', self(), AckRef, PeerName}.

continue(Sock, Opts, PeerName, Data) ->
    #{socket_module := SockMod, module := Mod, state := State} = Data,
    _ = inet_db:register_socket(Sock, SockMod),
    case inet:setopts(Sock, Opts) of
        ok ->
            Mod:acceptor_continue(PeerName, Sock, State);
        {error, Reason} ->
            %% `Sock' is the accepted socket, so this describes one connection
            %% and never the listener. This is the exact call that returns
            %% `{error, einval}' on darwin when the peer closed between accept
            %% and setup. See `conn_failure/2'.
            gen_tcp:close(Sock),
            conn_failure(Reason, Data)
    end.

%% @private
%% @doc Terminates this acceptor after a per-connection error, leaving the
%% listening socket alone.
%%
%% `failure/2' below deliberately kills `LSock' with `exit(LSock, Reason)' so
%% the socket's owner restarts it. That is the right response to a broken
%% listener and the wrong one to a connection that died between `accept' and
%% setup: a port is linked to its owner, so killing it also killed
%% `partisan_acceptor_socket', whose supervisor restarted it into the same
%% condition until it reached its restart intensity and shut the subtree down,
%% leaving the node unable to accept at all.
%%
%% Reaching that state took nothing more exotic than a peer connecting and
%% immediately disconnecting — which partisan does whenever it drops a
%% duplicate or rejected connection, and which any TCP health check or port
%% scan does too. Verified on OTP 28.5/darwin, where the post-accept
%% `inet:setopts/2' returns `{error, einval}' in that race; covered by
%% `partisan_listener_resilience_test'.
-spec conn_failure(timeout | closed | inet:posix(), pid(), data()) ->
    no_return().
conn_failure(Reason, Parent, #{ack := AckRef} = Data) ->
    _ = Parent ! {'CANCEL', self(), AckRef},
    conn_failure(Reason, Data).

-spec conn_failure(timeout | closed | inet:posix(), data()) -> no_return().
conn_failure(_Reason, Data) ->
    %% Normally, not `{shutdown, Reason}': the acceptor pool counts an abnormal
    %% acceptor exit against its restart intensity, so aborting connections
    %% would take the pool down and the socket owner with it — the same outcome
    %% by a longer route. Exiting normally lets the pool simply replace this
    %% acceptor, which is what `partisan_peer_socket:accept/1' already does for
    %% a failed TLS handshake.
    terminate(normal, Data).

-spec failure(timeout | closed | system_limit | inet:posix(), pid(), data()) ->
    no_return().
failure(Reason, Parent, #{ack := AckRef} = Data) ->
    _ = Parent ! {'CANCEL', self(), AckRef},
    failure(Reason, Data).

-spec failure(timeout | closed | system_limit | inet:posix(), data()) ->
    no_return().
failure(timeout, Data) ->
    terminate({shutdown, timeout}, Data);
failure(Reason, #{socket := LSock} = Data) ->
    Ref = monitor(port, LSock),
    exit(LSock, Reason),
    receive
        {'DOWN', Ref, _, _, _} -> terminate({shutdown, Reason}, Data)
    end.

-spec terminate(any(), data()) -> no_return().
-ifdef(OTP_RELEASE).
terminate(Reason, #{module := Mod, state := State} = Data) ->
    try Mod:acceptor_terminate(Reason, State) of
        _ -> terminated(Reason, Data)
    catch
        throw:_ -> terminated(Reason, Data);
        exit:NReason -> terminated(NReason, Data);
        error:NReason:Stacktrace -> terminated({NReason, Stacktrace}, Data)
    end.
-else.
terminate(Reason, #{module := Mod, state := State} = Data) ->
    try Mod:acceptor_terminate(Reason, State) of
        _ -> terminated(Reason, Data)
    catch
        throw:_ -> terminated(Reason, Data);
        exit:NReason -> terminated(NReason, Data);
        error:NReason -> terminated({NReason, erlang:get_stacktrace()}, Data)
    end.
-endif.

terminated(normal, _) ->
    exit(normal);
terminated(shutdown, _) ->
    exit(shutdown);
terminated({shutdown, _} = Shutdown, _) ->
    exit(Shutdown);
terminated(Reason, #{module := Mod, state := State}) ->
    Msg =
        "** Acceptor ~p terminating~n"
        "** When acceptor state == ~p~n"
        "** Reason for termination ==~n** ~p~n",
    error_logger:format(Msg, [{self(), Mod}, State]),
    exit(Reason).
