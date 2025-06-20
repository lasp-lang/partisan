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
%% @doc This module provides a `gen_tcp' acceptor pool supervisor. An
%% `acceptor_pool' must define the `acceptor_pool' behaviour callback, which is
%% very similar to the `supervisor' behaviour except all children must be
%% `acceptor' proccesses and only the map terms are supported.
%% The only callback is `init/1':
%% ```
%%-callback init(Args) -> {ok, {PoolFlags, [AcceptorSpec, ...]}} | ignore when
%%    Args :: any(),
%%    PoolFlags :: pool_flags(),
%%    AcceptorSpec :: acceptor_spec().
%% '''
%% The `pool_flags()' are the `intensity' and `period' pairs as in as
%% `supervisor:sup_flags/0' and have the equivalent behaviour.
%%
%% There must be list of a single `acceptor_spec()', just as there can only be a
%% single `supervisor:child_spec/0' when the `strategy' is `simple_one_for_one'.
%% The map pairs are the same as `supervisor:child_spec/0' except that `start''s
%% value is of form:
%% ```
%% {AcceptorMod :: module(), AcceptorArg :: term(), Opts :: [acceptor:option()]}
%% '''
%% `AcceptorMod' is an `acceptor' callback module that will be called with
%% argument `AcceptorArg', and spawned with acceptor options `Opts'.
%%
%% There is an additional `grace' key, that has a `timeout()' value. This is
%% the time in milliseconds that the acceptor pool will wait for children to
%% exit before starting to shut them down when terminating. This allows
%% connections to be gracefully closed.
%%
%% To start accepting connections using the `acceptor_pool' call
%% `accept_socket/3'.
%%
%% @see supervisor
%% @see acceptor
-module(acceptor_pool).

-behaviour(gen_server).

%% public api

-export([start_link/2,
         start_link/3,
         accept_socket/3,
         which_sockets/1]).

%% supervisor api

-export([which_children/1,
         count_children/1]).

%% gen_server api

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([code_change/3]).
-export([format_status/2]).
-export([terminate/2]).

-type pool() :: pid() | atom() | {atom(), node()} | {via, module(), any()} |
                {global, any()}.

-type pool_flags() :: #{intensity => non_neg_integer(),
                        period => pos_integer()}.

-type acceptor_spec() :: #{id := term(),
                           start := {module(), any(), [acceptor:option()]},
                           restart => transient | temporary,
                           shutdown => timeout() | brutal_kill,
                           grace => timeout(),
                           type => worker | supervisor,
                           modules => [module()] | dynamic}.

-type name() ::
    {inet:ip_address(), inet:port_number()} | inet:returned_non_ip_address().

-export_type([pool/0,
              pool_flags/0,
              acceptor_spec/0,
              name/0]).

-record(state, {name,
                mod :: module(),
                args :: any(),
                id :: term(),
                start :: {module(), any(), [acceptor:option()]},
                restart :: transient | temporary,
                shutdown :: timeout() | brutal_kill,
                grace :: timeout(),
                type :: worker | supervisor,
                modules :: [module()] | dynamic,
                intensity :: non_neg_integer(),
                period :: pos_integer(),
                restarts = queue:new() :: queue:queue(integer()),
                sockets = #{} :: #{reference() =>
                                   {module(), name(), gen_tcp:socket()}},
                acceptors = #{} :: #{pid() =>
                                     {reference(), name(), reference()}},
                conns = #{} :: #{pid() => {name(), name(), reference()}}}).

-callback init(Args) -> {ok, {PoolFlags, [AcceptorSpec, ...]}} | ignore when
      Args :: any(),
      PoolFlags :: pool_flags(),
      AcceptorSpec :: acceptor_spec().

%% public api

%% @doc Start an `acceptor_pool' with callback module `Module' and argument
%% `Args'.
%%
%% @see start_link/3
-spec start_link(Module, Args) -> {ok, Pid} | ignore | {error, Reason} when
      Module :: module(),
      Args :: any(),
      Pid :: pid(),
      Reason :: any().
start_link(Module, Args) ->
    gen_server:start_link(?MODULE, {self, Module, Args}, []).

%% @doc Start an `acceptor_pool' with name `Name', callback module `Module' and
%% argument `Args'.
%%
%% This function is equivalent to `supervisor:start_link/3' except starts an
%% `acceptor_pool' instead of a `supervisor'.
%%
%% @see supervisor:start_link/3
-spec start_link(Name, Module, Args) ->
    {ok, Pid} | ignore | {error, Reason} when
      Name :: {local, atom()} | {via, module, any()} | {global, any()},
      Module :: module(),
      Args :: any(),
      Pid :: pid(),
      Reason :: any().
start_link(Name, Module, Args) ->
    gen_server:start_link(Name, ?MODULE, {Name, Module, Args}, []).

%% @doc Ask `acceptor_pool' `Pool' to accept on the listen socket `Sock' with
%% `Acceptors' number of acceptors.
%%
%% Returns `{ok, Ref}' on success or `{error, Reason}' on failure. If acceptors
%% fail to accept connections an exit signal is sent `Sock'.
-spec accept_socket(Pool, Sock, Acceptors) -> {ok, Ref} | {error, Reason} when
      Pool :: pool(),
      Sock :: gen_tcp:socket(),
      Acceptors :: pos_integer(),
      Ref :: reference(),
      Reason :: inet:posix().
accept_socket(Pool, Sock, Acceptors)
  when is_port(Sock), is_integer(Acceptors), Acceptors > 0 ->
    case gen_server:call(Pool, {accept_socket, Sock, Acceptors}, infinity) of
        {ok, SockRef, SockPid} ->
            ok = gen_tcp:controlling_process(Sock, SockPid),
            {ok, SockRef};
        {error, _}=E ->
            E
    end.

%% @doc List the listen sockets being used by the `acceptor_pool'.
-spec which_sockets(Pool) -> [{SockModule, SockName, Sock, Ref}] when
      Pool :: pool(),
      SockModule :: module(),
      SockName :: name(),
      Sock :: gen_tcp:socket(),
      Ref :: reference().
which_sockets(Pool) ->
    gen_server:call(Pool, which_sockets, infinity).

%% @doc List the children of the `acceptor_pool'.
%%
%% This function is equivalent to `supervisor:which_children/1' except that the
%% peer name, listen socket name and a unique reference are combined with the
%% `id' from `init/1'.
%%
%% Processes that are waiting for a socket are not included.
%%
%% @see supervisor:which_children/1.
-spec which_children(Pool) -> [{Id, Child, Type, Modules}] when
      Pool :: pool(),
      Id ::
        {term(), PeerName :: name(), SockName :: name(), Ref :: reference()},
      Child :: pid(),
      Type :: worker | supervisor,
      Modules :: [module()] | dynamic.
which_children(Pool) ->
    gen_server:call(Pool, which_children, infinity).

%% @doc Count the children of the `acceptor_pool'.
%%
%% Processes that are waiting for a socket are not included in `active'.
%%
%% @see supervisor:count_children/1.
-spec count_children(Pool) -> Counts when
      Pool :: pool(),
      Counts :: [{spec | active | workers | supervisors, non_neg_integer()}].
count_children(Pool) ->
    gen_server:call(Pool, count_children, infinity).

%% gen_server api

%% @private
init({self, Mod, Args}) ->
    init({{self(), Mod}, Mod, Args});
init({Name, Mod, Args}) ->
    _ = process_flag(trap_exit, true),
    try Mod:init(Args) of
        Res ->
            init(Name, Mod, Args, Res)
    catch
        throw:Res ->
            init(Name, Mod, Args, Res)
    end.

%% @private
handle_call({accept_socket, Sock, NumAcceptors}, _, State) ->
    SockRef = monitor(port, Sock),
    case socket_info(Sock) of
        {ok, SockInfo} ->
            NState = start_acceptors(SockRef, SockInfo, NumAcceptors, State),
            {reply, {ok, SockRef, self()}, NState};
        {error, _} = Error ->
            demonitor(SockRef, [flush]),
            {reply, Error, State}
    end;
handle_call(which_sockets, _, #state{sockets=Sockets} = State) ->
    Reply = [{SockMod, SockName, Sock, SockRef} ||
             {SockRef, {SockMod, SockName, Sock}} <- maps:to_list(Sockets)],
    {reply, Reply, State};
handle_call(which_children, _, State) ->
    #state{conns=Conns, id=Id, type=Type, modules=Modules} = State,
    Children = [{{Id, PeerName, SockName, Ref}, Pid, Type, Modules} ||
                {Pid, {PeerName, SockName, Ref}} <- maps:to_list(Conns)],
    {reply, Children, State};
handle_call(count_children, _, State) ->
    {reply, count(State), State}.

%% @private
handle_cast(Req, State) ->
    {stop, {bad_cast, Req}, State}.

%% @private
handle_info({'EXIT', Conn, Reason}, State) ->
    handle_exit(Conn, Reason, State);
handle_info({'ACCEPT', Pid, AcceptRef, PeerName}, State) ->
    #state{acceptors=Acceptors, conns=Conns} = State,
    case maps:take(Pid, Acceptors) of
        {{SockRef, ListenSockName, AcceptRef}, NAcceptors} ->
            NAcceptors2 = start_acceptor(SockRef, NAcceptors, State),
            NConns = Conns#{Pid => {PeerName, ListenSockName, AcceptRef}},
            {noreply, State#state{acceptors=NAcceptors2, conns=NConns}};
        error ->
            {noreply, State}
    end;
handle_info({'ACCEPT', Pid, AcceptRef, SockName, PeerName}, State) ->
    #state{acceptors=Acceptors, conns=Conns} = State,
    case maps:take(Pid, Acceptors) of
        %% we ignore the sock name from the acceptor, because it could
        %% be {0,0,0,0}, and we want to know which interface the
        %% socket was opened on.
        {{SockRef, _ListenSockName, AcceptRef}, NAcceptors} ->
            NAcceptors2 = start_acceptor(SockRef, NAcceptors, State),
            NConns = Conns#{Pid => {PeerName, SockName, AcceptRef}},
            {noreply, State#state{acceptors=NAcceptors2, conns=NConns}};
        error ->
            {noreply, State}
    end;
handle_info({'CANCEL', Pid, AcceptRef}, #state{acceptors=Acceptors} = State) ->
    case Acceptors of
        #{Pid := {SockRef, SockName, AcceptRef}} ->
            NAcceptors = start_acceptor(SockRef, Acceptors, State),
            PidInfo = {undefined, SockName, AcceptRef},
            {noreply, State#state{acceptors=NAcceptors#{Pid := PidInfo}}};
        _ ->
            {noreply, State}
    end;
handle_info({'IGNORE', Pid, AcceptRef}, #state{acceptors=Acceptors} = State) ->
    case maps:take(Pid, Acceptors) of
        {{SockRef, _, AcceptRef}, NAcceptors} ->
            NAcceptors2 = start_acceptor(SockRef, NAcceptors, State),
            {noreply, State#state{acceptors=NAcceptors2}};
        error ->
            {noreply, State}
    end;
handle_info({'DOWN', SockRef, port, _, _}, #state{sockets=Sockets} = State) ->
    {noreply, State#state{sockets=maps:remove(SockRef, Sockets)}};
handle_info(Msg, #state{name=Name} = State) ->
    error_logger:error_msg("~p received unexpected message: ~p~n", [Name, Msg]),
    {noreply, State}.

%% @private
code_change(_, #state{mod=Mod, args=Args} = State, _) ->
    try Mod:init(Args) of
        Result       -> change_init(Result, State)
    catch
        throw:Result -> change_init(Result, State)
    end.

%% @private
format_status(terminate, [_, State]) ->
    State;
format_status(_, [_, #state{mod=Mod} = State]) ->
    [{data, [{"State", State}]}, {supervisor, [{"Callback", Mod}]}].

%% @private
terminate(_, State) ->
    #state{conns=Conns, acceptors=Acceptors, grace=Grace, shutdown=Shutdown,
           restart=Restart, name=Name, id=Id, start={AMod, _, _},
           type=Type} = State,
    Pids = maps:keys(Acceptors) ++ maps:keys(Conns),
    MRefs = maps:from_list([{monitor(process, Pid), Pid} || Pid <- Pids]),
    Timer = grace_timer(Grace, Shutdown),
    Reports = await_down(Timer, Restart, MRefs),
    terminate_report(Name, Id, AMod, Restart, Shutdown, Type, Reports).

%% internal

init(Name, Mod, Args, {ok, {#{} = Flags, [#{} = Spec]}}) ->
    case validate_config(Flags, Spec) of
        ok ->
            % Same defaults as supervisor
            Intensity = maps:get(intensity, Flags, 1),
            Period = maps:get(period, Flags, 5),
            Id = maps:get(id, Spec),
            {AMod, _, _} = Start = maps:get(start, Spec),
            Restart = maps:get(restart, Spec, temporary),
            Type = maps:get(type, Spec, worker),
            Shutdown = maps:get(shutdown, Spec, shutdown_default(Type)),
            Grace = maps:get(grace, Spec, 0),
            Modules = maps:get(modules, Spec, [AMod]),
            State = #state{name=Name, mod=Mod, args=Args, id=Id, start=Start,
                           restart=Restart, shutdown=Shutdown, grace=Grace,
                           type=Type, modules=Modules, intensity=Intensity,
                           period=Period},
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end;
init(_, _, _, ignore) ->
    ignore;
init(_, Mod, _, Other) ->
    {stop, {bad_return, {Mod, init, Other}}}.

validate_config(Flags, Spec) ->
    case validate_flags(Flags) of
        ok                 -> validate_spec(Spec);
        {error, _} = Error -> Error
    end.

validate_flags(Flags) ->
    validate(fun validate_flag/2, Flags).

validate(Validate, Map) ->
    maps:fold(fun(Key, Value, ok)           -> Validate(Key, Value);
                 (_, _, {error, _} = Error) -> Error
              end, ok, Map).

validate_flag(intensity, Intensity)
  when is_integer(Intensity), Intensity >= 0 ->
    ok;
validate_flag(period, Period)
  when is_integer(Period), Period > 0 ->
    ok;
validate_flag(Key, Value) ->
    {error, {bad_flag, {Key, Value}}}.

validate_spec(Spec) ->
    case {maps:is_key(id, Spec), maps:is_key(start, Spec)} of
        {true, true} -> validate(fun validate_spec/2, Spec);
        {false, _}   -> {error, {missing_spec, id}};
        {_, false}   -> {error, {missing_spec, start}}
    end.

validate_spec(id, _) ->
    ok;
validate_spec(start, {AMod, _, Opts}) when is_atom(AMod), is_list(Opts) ->
    ok;
validate_spec(restart, Restart)
  when Restart == transient; Restart == temporary ->
    ok;
validate_spec(shutdown, Shutdown) when is_integer(Shutdown), Shutdown >= 0 ->
    ok;
validate_spec(shutdown, Shutdown)
  when Shutdown == infinity; Shutdown == brutal_kill ->
    ok;
validate_spec(grace, Grace) when is_integer(Grace), Grace >= 0 ->
    ok;
validate_spec(type, Type) when Type == worker; Type == supervisor ->
    ok;
validate_spec(modules, Modules) when is_list(Modules); Modules == dynamic ->
    ok;
validate_spec(Key, Value) ->
    {error, {bad_spec, {Key, Value}}}.

shutdown_default(worker)     -> 5000;
shutdown_default(supervisor) -> infinity.

count(#state{conns=Conns, acceptors=Acceptors, type=Type}) ->
    Active = maps:size(Conns),
    Size = Active + maps:size(Acceptors),
    case Type of
        worker ->
            [{specs, 1}, {active, Active}, {supervisors, 0}, {workers, Size}];
        supervisor ->
            [{specs, 1}, {active, Active}, {supervisors, Size}, {workers, 0}]
    end.

socket_info(Sock) ->
    case inet_db:lookup_socket(Sock) of
        {ok, SockMod}      -> socket_info(SockMod, Sock);
        {error, _} = Error -> Error
    end.

socket_info(SockMod, Sock) ->
    case inet:sockname(Sock) of
        {ok, SockName}     -> {ok, {SockMod, SockName, Sock}};
        {error, _} = Error -> Error
    end.

start_acceptors(SockRef, SockInfo, NumAcceptors, State) ->
    #state{sockets=Sockets, acceptors=Acceptors} = State,
    NState = State#state{sockets=Sockets#{SockRef => SockInfo}},
    NAcceptors = start_loop(SockRef, NumAcceptors, Acceptors, NState),
    NState#state{acceptors=NAcceptors}.

start_loop(_, 0, Acceptors, _) ->
    Acceptors;
start_loop(SockRef, N, Acceptors, State) ->
    start_loop(SockRef, N-1, start_acceptor(SockRef, Acceptors, State), State).

start_acceptor(SockRef, Acceptors,
               #state{sockets=Sockets, start={Mod, Args, Opts}}) ->
    case Sockets of
        #{SockRef := {SockMod, SockName, Sock}} ->
            {Pid, AcceptRef} =
                acceptor:spawn_opt(Mod, SockMod, SockName, Sock, Args, Opts),
            Acceptors#{Pid => {SockRef, SockName, AcceptRef}};
        _ ->
            Acceptors
    end.

handle_exit(Pid, Reason, #state{conns=Conns} = State) ->
    case maps:take(Pid, Conns) of
        {SockInfo, NConns} ->
            child_exit(Pid, Reason, SockInfo, State#state{conns=NConns});
        error ->
            acceptor_exit(Pid, Reason, State)
    end.

% TODO: Send supervisor_reports like a supervisor
child_exit(_, normal, _, State) ->
    {noreply, State};
child_exit(_, shutdown, _, State) ->
    {noreply, State};
child_exit(_, {shutdown, _}, _, State) ->
    {noreply, State};
child_exit(Pid, Reason, SockInfo, #state{restart=temporary} = State) ->
    report(child_terminated, Pid, Reason, SockInfo, State),
    {noreply, State};
child_exit(Pid, Reason, SockInfo, #state{restart=transient} = State) ->
    report(child_terminated, Pid, Reason, SockInfo, State),
    add_restart(State).

report(Context, Pid, Reason, {PeerName, SockName, Ref}, State) ->
    #state{name=Name, id=Id, start={AMod, _, _}, restart=Restart,
           shutdown=Shutdown, type=Type} = State,
    Report = [{supervisor, Name},
              {errorContext, Context},
              {reason, Reason},
              {offender, [{pid, Pid},
                          {id, {Id, PeerName, SockName, Ref}},
                          {mfargs, {AMod, acceptor_init, undefined}},
                          {restart_type, Restart},
                          {shutdown, Shutdown},
                          {child_type, Type}]}],
    error_logger:error_report(supervisor_report, Report).

report(Context, Reason, State) ->
    SockInfo = {undefined, undefined, undefined},
    report(Context, undefined, Reason, SockInfo, State).

acceptor_exit(Pid, Reason, #state{acceptors=Acceptors} = State) ->
    case maps:take(Pid, Acceptors) of
        {{SockRef, SockName, AcceptRef}, NAcceptors}
          when SockRef /= undefined ->
            SockInfo = {undefined, SockName, AcceptRef},
            report(start_error, Pid, Reason, SockInfo, State),
            restart_acceptor(SockRef, NAcceptors, State);
        {{undefined, _, _} = SockInfo, NAcceptors} ->
            % Received 'CANCEL' due to accept timeout or error. If accept error
            % we are waiting for listen socket 'DOWN' to cancel accepting and
            % don't want accept errors to bring down pool. The acceptor will
            % have sent exit signal to listen socket, hopefully isolating the
            % pool from a bad listen socket. With acceptor_terminate/2
            % crash the max intensity can still be reached.
            NState = State#state{acceptors=NAcceptors},
            child_exit(Pid, Reason, SockInfo, NState);
        error ->
            {noreply, State}
    end.

restart_acceptor(SockRef, Acceptors, State) ->
    case add_restart(State) of
        {noreply, NState} ->
            NAcceptors = start_acceptor(SockRef, Acceptors, NState),
            {noreply, NState#state{acceptors=NAcceptors}};
        {stop, Reason,NState} ->
            {stop, Reason, NState#state{acceptors=Acceptors}}
    end.

add_restart(State) ->
    #state{intensity=Intensity, period=Period, restarts=Restarts} = State,
    Now = erlang:monotonic_time(1),
    NRestarts = drop_restarts(Now - Period, queue:in(Now, Restarts)),
    NState = State#state{restarts=NRestarts},
    case queue:len(NRestarts) of
        Len when Len =< Intensity ->
            {noreply, NState};
        Len when Len > Intensity ->
            report(shutdown, reached_max_restart_intennsity, NState),
            {stop, shutdown, NState}
    end.

drop_restarts(Stale, Restarts) ->
    % Just inserted Now and Now > Stale so get/1 and drop/1 always succeed
    case queue:get(Restarts) of
        Time when Time >= Stale -> Restarts;
        Time when Time < Stale  -> drop_restarts(Stale, queue:drop(Restarts))
    end.

change_init(Result, State) ->
    #state{name=Name, mod=Mod, args=Args, restarts=Restarts, sockets=Sockets,
           acceptors=Acceptors, conns=Conns} = State,
    case init(Name, Mod, Args, Result) of
        {ok, NState} ->
            {ok, NState#state{restarts=Restarts, sockets=Sockets,
                              acceptors=Acceptors, conns=Conns}};
        ignore ->
            {ok, State};
        {stop, Reason} ->
            {error, Reason}
    end.

grace_timer(infinity, _) ->
    make_ref();
grace_timer(Grace, Shutdown) ->
    erlang:start_timer(Grace, self(), {shutdown, Shutdown}).

await_down(Timer, Restart, MRefs) ->
    await_down(Timer, grace, Restart, dict:new(), #{}, MRefs).

await_down(_, _, _, Reports, _, MRefs) when MRefs == #{} ->
    Reports;
await_down(Timer, Status, Restart, Reports, Exits, MRefs) ->
    receive
        {'EXIT', Pid, Reason} ->
            NExits = Exits#{Pid => Reason},
            await_down(Timer, Status, Restart, Reports, NExits, MRefs);
        {'DOWN', MRef, process, _, Reason} ->
            {NReports, NExits, NMRefs} =
                down(MRef, Reason, Status, Restart, Reports, Exits, MRefs),
            await_down(Timer, Status, Restart, NReports, NExits, NMRefs);
        {timeout, Timer, Msg} ->
            {NTimer, NStatus} = down_timeout(Msg, Status, MRefs),
            await_down(NTimer, NStatus, Restart, Reports, Exits, MRefs);
        _ ->
            await_down(Timer, Status, Restart, Reports, Exits, MRefs)
    end.

down_timeout({shutdown, infinity}, grace, MRefs) ->
    exits(shutdown, MRefs),
    {make_ref(), shutdown};
down_timeout({shutdown, brutal_kill}, grace, MRefs) ->
    exits(kill, MRefs),
    {make_ref(), brutal_kill};
down_timeout({shutdown, Shutdown}, grace, MRefs) ->
    exits(shutdown, MRefs),
    {erlang:start_timer(Shutdown, self(), brutal_kill), shutdown};
down_timeout(brutal_kill, shutdown, MRefs) ->
    exits(kill, MRefs),
    {make_ref(), shutdown}.

exits(Reason, MRefs) ->
    _ = [exit(Pid, Reason) || Pid <- maps:values(MRefs)],
    ok.

down(MRef, Reason, Status, Restart, Reports, Exits, MRefs) ->
    case maps:take(MRef, MRefs) of
        {Pid, NMRefs} ->
            {NReason, NExits} = check_reason(Pid, Reason, Exits),
            NReports = handle_down(NReason, Status, Restart, Reports),
            {NReports, NExits, NMRefs};
        _ ->
            {Reports, Exits, MRefs}
    end.

check_reason(Pid, noproc, Exits) ->
    case maps:take(Pid, Exits) of
        {_, _} = Result -> Result;
        error           -> {noproc, Exits}
    end;
check_reason(Pid, Reason, Exits) ->
    {Reason, maps:remove(Pid, Exits)}.

handle_down(Reason, Status, Restart, Reports) ->
    case down_action(Reason, Status, Restart) of
        ignore -> Reports;
        report -> dict:update_counter(Reason, 1, Reports)
    end.

down_action(normal, _, _)             -> ignore;
down_action(shutdown, _, _)           -> ignore;
down_action({shutdown, _}, _, _)      -> ignore;
down_action(killed, brutal_kill, _)   -> ignore;
down_action(_, _, _)                  -> report.

terminate_report(Name, Id, AMod, Restart, Shutdown, Type, Reports) ->
    ReportAll = fun(Reason, Count, Acc) ->
                        Offender = [{pid, Count},
                                    {id, {Id, undefined, undefined, undefined}},
                                    {mfargs, {AMod, acceptor_init, undefined}},
                                    {restart_type, Restart},
                                    {shutdown, Shutdown},
                                    {child_type, Type}],
                        Report = [{supervisor, Name},
                                  {errorContext, shutdown_error},
                                  {reason, Reason},
                                  {offender, Offender}],
                        error_logger:error_report(supervisor_report, Report),
                        Acc
                end,
    dict:fold(ReportAll, ok, Reports).
