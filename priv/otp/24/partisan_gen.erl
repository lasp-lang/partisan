%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2021. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%

%% -----------------------------------------------------------------------------
%% @doc This module is an adaptation of Erlang's `gen' module which
%% implements the really generic stuff of the generic standard behaviours (e.g.
%% gen_server, gen_fsm).
%%
%% It replaces all instances of `erlang:send/2` and `erlang:monitor/2` with
%% their Partisan counterparts.
%%
%% It maintains the `gen' API with the following exceptions:
%%
%% <ul>
%% <li>`call/3` - the 3th argument accepts a timeout value but also a list of
%% options containing any of the following: `{timeout, timeout()}' | `{channel,
%% partisan:channel()}'.</li>
%% <li>`send_request/4` - the 4th argument is a list of
%% options containing any of the following: `{channel,
%% partisan:channel()}'.</li>
%% </ul>
%%
%% <strong>NOTICE:</strong>
%% At the moment this only works for `partisan_pluggable_peer_service_manager'.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_gen).

-include_lib("partisan/include/partisan_logger.hrl").

% -compile({inline,[get_node/1]}).

%% PARTISAN EXTENSIONS
-export([send_request/4]).
-export([erase_opts/0]).
-export([get_opts/0]).
-export([set_opts/1]).


%%%-----------------------------------------------------------------
%%% This module implements the really generic stuff of the generic
%%% standard behaviours (e.g. gen_server, gen_fsm).
%%%
%%% The standard behaviour should export init_it/6.
%%%-----------------------------------------------------------------
-export([start/5, start/6, debug_options/2, hibernate_after/1,
     name/1, unregister_name/1, get_proc_name/1, get_parent/0,
     call/3, call/4, reply/2,
         send_request/3, wait_response/2,
         receive_response/2, check_response/2,
         stop/1, stop/3]).

-export([init_it/6, init_it/7]).

-export([format_status_header/2]).

-define(default_timeout, 5000).

%%-----------------------------------------------------------------

-type linkage()    :: 'monitor' | 'link' | 'nolink'.
-type emgr_name()  :: {'local', atom()}
                    | {'global', term()}
                    | {'via', Module :: module(), Name :: term()}.

-type start_ret()  :: {'ok', pid()} | {'ok', {pid(), reference()}} | 'ignore' | {'error', term()}.

-type debug_flag() :: 'trace' | 'log' | 'statistics' | 'debug'
                    | {'logfile', string()}.
-type option()     :: {'timeout', timeout()}
            | {'debug', [debug_flag()]}
            | {'hibernate_after', timeout()}
            | {'spawn_opt', [partisan_proc_lib:spawn_option()]}
            | {'channel', partisan:channel()}.
-type options()    :: [option()].

-type server_ref() :: pid() | atom() | {atom(), node()}
                    | {global, term()} | {via, module(), term()}.

-type request_id() :: reference() | partisan_remote_ref:r().

%%-----------------------------------------------------------------
%% Starts a generic process.
%% start(GenMod, LinkP, Mod, Args, Options)
%% start(GenMod, LinkP, Name, Mod, Args, Options)
%%    GenMod = atom(), callback module implementing the 'real' fsm
%%    LinkP = link | nolink
%%    Name = {local, atom()} | {global, term()} | {via, atom(), term()}
%%    Args = term(), init arguments (to Mod:init/1)
%%    Options = [{timeout, Timeout} | {debug, [Flag]} | {spawn_opt, OptionList}]
%%      Flag = trace | log | {logfile, File} | statistics | debug
%%          (debug == log && statistics)
%% Returns: {ok, Pid} | ignore |{error, Reason} |
%%          {error, {already_started, Pid}} |
%%    The 'already_started' is returned only if Name is given
%%-----------------------------------------------------------------

-spec start(module(), linkage(), emgr_name(), module(), term(), options()) ->
    start_ret().

start(GenMod, LinkP, Name, Mod, Args, Options) ->
    case where(Name) of
    undefined ->
        do_spawn(GenMod, LinkP, Name, Mod, Args, Options);
    Pid ->
        {error, {already_started, Pid}}
    end.

-spec start(module(), linkage(), module(), term(), options()) -> start_ret().

start(GenMod, LinkP, Mod, Args, Options) ->
    do_spawn(GenMod, LinkP, Mod, Args, Options).

%%-----------------------------------------------------------------
%% Spawn the process (and link) maybe at another node.
%% If spawn without link, set parent to ourselves 'self'!!!
%%-----------------------------------------------------------------
do_spawn(GenMod, link, Mod, Args, Options) ->
    Time = timeout(Options),
    partisan_proc_lib:start_link(?MODULE, init_it,
            [GenMod, self(), self(), Mod, Args, Options],
            Time,
            spawn_opts(Options));
do_spawn(GenMod, monitor, Mod, Args, Options) ->
    Time = timeout(Options),
    Ret = partisan_proc_lib:start_monitor(?MODULE, init_it,
                                 [GenMod, self(), self(), Mod, Args, Options],
                                 Time,
                                 spawn_opts(Options)),
   monitor_return(Ret);
do_spawn(GenMod, _, Mod, Args, Options) ->
    Time = timeout(Options),
    partisan_proc_lib:start(?MODULE, init_it,
           [GenMod, self(), 'self', Mod, Args, Options],
           Time,
           spawn_opts(Options)).

do_spawn(GenMod, link, Name, Mod, Args, Options) ->
    Time = timeout(Options),
    partisan_proc_lib:start_link(?MODULE, init_it,
            [GenMod, self(), self(), Name, Mod, Args, Options],
            Time,
            spawn_opts(Options));
do_spawn(GenMod, monitor, Name, Mod, Args, Options) ->
    Time = timeout(Options),
    Ret = partisan_proc_lib:start_monitor(?MODULE, init_it,
                                 [GenMod, self(), self(), Name, Mod, Args, Options],
                                 Time,
                                 spawn_opts(Options)),
    monitor_return(Ret);
do_spawn(GenMod, _, Name, Mod, Args, Options) ->
    Time = timeout(Options),
    partisan_proc_lib:start(?MODULE, init_it,
           [GenMod, self(), 'self', Name, Mod, Args, Options],
           Time,
           spawn_opts(Options)).


%%
%% Adjust monitor returns for OTP gen behaviours...
%%
%% If an OTP behaviour is introduced that 'init_ack's
%% other results, this has code has to be moved out
%% into all behaviours as well as adjusted...
%%
monitor_return({{ok, Pid}, Mon}) when is_pid(Pid), is_reference(Mon) ->
    %% Successful start_monitor()...
    {ok, {Pid, Mon}};
monitor_return({{ok, Pid}, Mon}) ->
    %% Partisan pid case of the previous clause
    case partisan:is_pid(Pid) andalso partisan:is_reference(Mon) of
        true ->
            {ok, {Pid, Mon}};
        false ->
            %% Simulate same error we would have if this the previous clause
            error(function_clause)
    end;
monitor_return({Error, Mon}) when is_reference(Mon) ->
    %% Failure; wait for spawned process to terminate
    %% and release resources, then return the error...
    receive
        {'DOWN', Mon, process, _Pid, _Reason} ->
            ok
    end,
    Error;
monitor_return({Error, Mon}) ->
    %% Partisan reference case of the previous clause
    case partisan:is_reference(Mon) of
        true ->
            receive
                {'DOWN', Mon, process, _Pid, _Reason} ->
                    ok
            end,
            Error;
        false ->
            %% Simulate same error we would have if this the previous clause
            error(function_clause)
    end.

%%-----------------------------------------------------------------
%% Initiate the new process.
%% Register the name using the Rfunc function
%% Calls the Mod:init/Args function.
%% Finally an acknowledge is sent to Parent and the main
%% loop is entered.
%%-----------------------------------------------------------------
init_it(GenMod, Starter, Parent, Mod, Args, Options) ->
    init_it2(GenMod, Starter, Parent, self(), Mod, Args, Options).

init_it(GenMod, Starter, Parent, Name, Mod, Args, Options) ->
    case register_name(Name) of
    true ->
        init_it2(GenMod, Starter, Parent, Name, Mod, Args, Options);
    {false, Pid} ->
        partisan_proc_lib:init_ack(Starter, {error, {already_started, Pid}})
    end.

init_it2(GenMod, Starter, Parent, Name, Mod, Args, Options) ->
    ok = set_opts(Options), % partisan addition
    GenMod:init_it(Starter, Parent, Name, Mod, Args, Options).

%%-----------------------------------------------------------------
%% Makes a synchronous call to a generic process.
%% Request is sent to the Pid, and the response must be
%% {Tag, Reply}.
%%-----------------------------------------------------------------

%%% New call function which uses the new monitor BIF
%%% call(ServerId, Label, Request)

call(Process, Label, Request) ->
    call(Process, Label, Request, ?default_timeout).

%% Partisan extension
call(Process, Label, Request, Opts0) when is_list(Opts0) ->
    case lists:keytake(timeout, 1, Opts0) of
        {value, {timeout, Timeout}, Opts} ->
            set_opts(Opts),
            call(Process, Label, Request, Timeout);
        false ->
            set_opts(Opts0),
            call(Process, Label, Request, ?default_timeout)
    end;
%% Optimize a common case.
call(Process, Label, Request, Timeout) when is_pid(Process),
  Timeout =:= infinity orelse is_integer(Timeout) andalso Timeout >= 0 ->
    do_call(Process, Label, Request, Timeout);
call(Process, Label, Request, Timeout)
  when Timeout =:= infinity; is_integer(Timeout), Timeout >= 0 ->
    case partisan_remote_ref:is_local_pid(Process) of
        true ->
            %% Optimisation
            Pid = partisan_remote_ref:to_term(Process),
            do_call(Pid, Label, Request, Timeout);
        false ->
            Fun = fun(Arg) -> do_call(Arg, Label, Request, Timeout) end,
            do_for_proc(Process, Fun)
    end.

-dialyzer({no_improper_lists, do_call/4}).

do_call(Process, Label, Request, infinity)
  when (is_pid(Process)
        andalso (node(Process) == node()))
       orelse (element(2, Process) == node()
               andalso is_atom(element(1, Process))
               andalso (tuple_size(Process) =:= 2)) ->
    Mref = erlang:monitor(process, Process),
    %% Local without timeout; no need to use alias since we unconditionally
    %% will wait for either a reply or a down message which corresponds to
    %% the process being terminated (as opposed to 'noconnection')...
    Process ! {Label, {self(), Mref}, Request},
    receive
        {Mref, Reply} ->
            erlang:demonitor(Mref, [flush]),
            {ok, Reply};
        {'DOWN', Mref, _, _, Reason} ->
            exit(Reason)
    end;
do_call(Process, Label, Request, Timeout)
when is_reference(Process) andalso node(Process) == node() ->
    Mref = erlang:monitor(process, Process, [{alias,demonitor}]),

    Tag = [alias | Mref],

    %% OTP-24:
    %% Using alias to prevent responses after 'noconnection' and timeouts.
    %% We however still may call nodes responding via process identifier, so
    %% we still use 'noconnect' on send in order to try to send on the
    %% monitored connection, and not trigger a new auto-connect.
    %%
    erlang:send(Process, {Label, {self(), Tag}, Request}, [noconnect]),

    receive
        {[alias | Mref], Reply} ->
            erlang:demonitor(Mref, [flush]),
            {ok, Reply};
        {'DOWN', Mref, _, _, noconnection} ->
            Node = get_node(Process),
            exit({nodedown, Node});
        {'DOWN', Mref, _, _, Reason} ->
            exit(Reason)
    after Timeout ->
            erlang:demonitor(Mref, [flush]),
            receive
                {[alias | Mref], Reply} ->
                    {ok, Reply}
            after 0 ->
                    exit(timeout)
            end
    end;

do_call(Process, Label, Request, Timeout) ->
    %% Partisan case
    %% For remote process do_send_request will use Tag = [alias | Mref]
    %% to comply with OTP although we do not support remote aliases
    Mref = do_send_request(Process, Label, Request),

    %% Wait for reply.
    receive
        {[alias | Mref], Reply} ->
            partisan:demonitor(Mref, [flush]),
            {ok, Reply};
        {'DOWN', Mref, _, _, noconnection} ->
            Node = get_node(Process),
            exit({nodedown, Node});
        {'DOWN', Mref, _, _, Reason} ->
            exit(Reason)
    after Timeout ->
        partisan:demonitor(Mref, [flush]),
        ?LOG_DEBUG(#{
            description => "Timed out at while waiting for response to message",
            message => Request
        }),
        receive
            {[alias | Mref], Reply} ->
                {ok, Reply}
        after 0 ->
                exit(timeout)
        end
    end.

get_node(Process) ->
    %% We trust the arguments to be correct, i.e
    %% Process is either a local or remote pid,
    %% or a {Name, Node} tuple (of atoms) and in this
    %% case this node (node()) _is_ distributed and Node =/= node().
    case Process of
    {_S, N} when is_atom(N) ->
        N;
    _ when is_pid(Process) ->
        node(Process);
    _ ->
        %% We assume process is partisan_remote_ref
        partisan:node(Process)
    end.


-spec send_request(Name::server_ref(), Label::term(), Request::term()) -> request_id().
send_request(Process, Label, Request) when is_pid(Process) ->
    do_send_request(Process, Label, Request);
send_request(Process, Label, Request) ->
    Fun = fun(Pid) -> do_send_request(Pid, Label, Request) end,
    try do_for_proc(Process, Fun)
    catch exit:Reason ->
            %% Make send_request async and fake a down message
            Ref = partisan:make_ref(),
            self() ! {'DOWN', Ref, process, Process, Reason},
            Ref
    end.

%% Partisan addition
-spec send_request(Name::server_ref(), Label::term(), Request::term(), Opts :: list()) -> request_id().
send_request(Process, Label, Request, Opts) ->
    set_opts(Opts),
    send_request(Process, Label, Request).

-dialyzer({no_improper_lists, do_send_request/3}).

do_send_request(Process, Label, Request)
when is_pid(Process) orelse is_atom(Process) ->
    Mref = erlang:monitor(process, Process, [{alias, demonitor}]),
    erlang:send(Process, {Label, {self(), [alias|Mref]}, Request}, [noconnect]),
    Mref;
do_send_request({ServerRef, Node} = Process, Label, Request) ->
    case Node == partisan:node() of
        true ->
            do_send_request(ServerRef, Label, Request);
        false ->
            %% Monitor request and message are delivered using the same channel
            Opts = get_opts(),
            Mref = partisan:monitor(process, Process, Opts),
            Message = {Label, {partisan:self(), [alias|Mref]}, Request},
            ?LOG_DEBUG(#{
                description => "Sending message",
                destination => Process,
                message => Message
            }),
            partisan:forward_message(Node, ServerRef, Message, Opts),
            Mref
    end;
do_send_request(Process, Label, Request) ->
    %% Process is a partisan remote pid or name
    case partisan:is_local(Process) of
        true ->
            %% Local process
            LocalProcess = partisan_remote_ref:to_term(Process),
            do_send_request(LocalProcess, Label, Request);
        false ->
            %% Monitor request and message are delivered using the same channel
            Opts = get_opts(),
            Mref = partisan:monitor(process, Process, Opts),
            Message = {Label, {partisan:self(), [alias|Mref]}, Request},
            ?LOG_DEBUG(#{
                description => "Sending message",
                destination => Process,
                message => Message
            }),
            partisan:forward_message(Process, Message, Opts),
            Mref
    end.




%%
%% Wait for a reply to the client.
%% Note: if timeout is returned monitors are kept.

-spec wait_response(RequestId::request_id(), timeout()) ->
        {reply, Reply::term()} | 'timeout' | {error, {term(), server_ref()}}.
wait_response(Mref, Timeout) when is_reference(Mref) ->
    receive
        {[alias|Mref], Reply} ->
            erlang:demonitor(Mref, [flush]),
            {reply, Reply};
        {'DOWN', Mref, _, Object, Reason} ->
            {error, {Reason, Object}}
    after Timeout ->
            timeout
    end;
wait_response(Mref, Timeout) ->
    %% MRef is a partisan ref
    receive
        {[alias|Mref], Reply} ->
            partisan:demonitor(Mref, [flush]),
            {reply, Reply};
        {'DOWN', Mref, _, Object, Reason} ->
            {error, {Reason, Object}}
    after Timeout ->
            timeout
    end.

-spec receive_response(RequestId::request_id(), timeout()) ->
    {reply, Reply::term()} | 'timeout' | {error, {term(), server_ref()}}.
receive_response(Mref, Timeout) ->
    receive
        {[alias|Mref], Reply} ->
            partisan:demonitor(Mref, [flush]),
            {reply, Reply};
        {'DOWN', Mref, _, Object, Reason} ->
            {error, {Reason, Object}}
    after Timeout ->
            partisan:demonitor(Mref, [flush]),
            receive
                {[alias|Mref], Reply} ->
                    {reply, Reply}
            after 0 ->
                    timeout
            end
    end.

-spec check_response(Msg::term(), Mref::request_id()) ->
    {reply, Reply::term()} | 'no_reply' | {error, {term(), server_ref()}}.
check_response(Msg, Mref) ->
case Msg of
    {[alias|Mref], Reply} ->
        partisan:demonitor(Mref, [flush]),
        {reply, Reply};
    {'DOWN', Mref, _, Object, Reason} ->
        {error, {Reason, Object}};
        _ ->
            no_reply
    end.

%%
%% Send a reply to the client.
%%
reply({_To, [alias|Alias] = Tag}, Reply) when is_reference(Alias) ->
    Alias ! {Tag, Reply}, ok;
reply({_To, [[alias|Alias] | _] = Tag}, Reply) when is_reference(Alias) ->
    Alias ! {Tag, Reply}, ok;
%% reply({To, Tag}, Reply) ->
%%     try To ! {Tag, Reply}, ok catch _:_ -> ok end.
reply({To, Tag}, Reply) ->
    try partisan:forward_message(To, {Tag, Reply}) catch _:_ -> ok end.


%%-----------------------------------------------------------------
%% Synchronously stop a generic process
%%-----------------------------------------------------------------
stop(Process) ->
    stop(Process, normal, infinity).

stop(Process, Reason, Timeout)
  when Timeout =:= infinity; is_integer(Timeout), Timeout >= 0 ->
    Fun = fun(Pid) -> partisan_proc_lib:stop(Pid, Reason, Timeout) end,
    do_for_proc(Process, Fun).

%%-----------------------------------------------------------------
%% Map different specifications of a process to either Pid or
%% {Name,Node}. Execute the given Fun with the process as only
%% argument.
%% -----------------------------------------------------------------

%% Local or remote by pid
do_for_proc(Pid, Fun) when is_pid(Pid) ->
    Fun(Pid);
%% Local by name
do_for_proc(Name, Fun) when is_atom(Name) ->
    case whereis(Name) of
    Pid when is_pid(Pid) ->
        Fun(Pid);
    undefined ->
        exit(noproc)
    end;
%% Global by name
do_for_proc(Process, Fun)
  when ((tuple_size(Process) == 2 andalso element(1, Process) == global)
    orelse
      (tuple_size(Process) == 3 andalso element(1, Process) == via)) ->
    case where(Process) of
    Pid when is_pid(Pid) ->
        Node = node(Pid),
        try Fun(Pid)
        catch
        exit:{nodedown, Node} ->
            %% A nodedown not yet detected by global,
            %% pretend that it was.
            exit(noproc)
        end;
    undefined ->
        exit(noproc)
    end;
%% Local by name in disguise
%% do_for_proc({Name, Node}, Fun) when Node =:= node() ->
%%     do_for_proc(Name, Fun);
%% Remote by name
do_for_proc({Name, Node} = Process, Fun) when is_atom(Node) ->

    case partisan:node() of
        Node ->
            %% Local by name in disguise
            do_for_proc(Name, Fun);
        nonode@nohost ->
            exit({nodedown, Node});
        _ ->
            Fun(Process)
    end;

do_for_proc(Process, Fun) ->
    partisan_remote_ref:is_pid(Process) orelse error(function_clause),
    Fun(Process).


%%%-----------------------------------------------------------------
%%%  Misc. functions.
%%%-----------------------------------------------------------------
where({global, Name}) -> global:whereis_name(Name);
where({via, Module, Name}) -> Module:whereis_name(Name);
where({local, Name})  -> whereis(Name).

register_name({local, Name} = LN) ->
    try register(Name, self()) of
    true -> true
    catch
    error:_ ->
        {false, where(LN)}
    end;
register_name({global, Name} = GN) ->
    case global:register_name(Name, self()) of
    yes -> true;
    no -> {false, where(GN)}
    end;
register_name({via, Module, Name} = GN) ->
    case Module:register_name(Name, self()) of
    yes ->
        true;
    no ->
        {false, where(GN)}
    end.

name({local,Name}) -> Name;
name({global,Name}) -> Name;
name({via,_, Name}) -> Name;
name(Pid) when is_pid(Pid) -> Pid.

unregister_name({local,Name}) ->
    try unregister(Name) of
    _ -> ok
    catch
    _:_ -> ok
    end;
unregister_name({global,Name}) ->
    _ = global:unregister_name(Name),
    ok;
unregister_name({via, Mod, Name}) ->
    _ = Mod:unregister_name(Name),
    ok;
unregister_name(Pid) when is_pid(Pid) ->
    ok.

get_proc_name(Pid) when is_pid(Pid) ->
    Pid;
get_proc_name({local, Name}) ->
    case process_info(self(), registered_name) of
    {registered_name, Name} ->
        Name;
    {registered_name, _Name} ->
        exit(process_not_registered);
    [] ->
        exit(process_not_registered)
    end;
get_proc_name({global, Name}) ->
    case global:whereis_name(Name) of
    undefined ->
        exit(process_not_registered_globally);
    Pid when Pid =:= self() ->
        Name;
    _Pid ->
        exit(process_not_registered_globally)
    end;
get_proc_name({via, Mod, Name}) ->
    case Mod:whereis_name(Name) of
    undefined ->
        exit({process_not_registered_via, Mod});
    Pid when Pid =:= self() ->
        Name;
    _Pid ->
        exit({process_not_registered_via, Mod})
    end.

get_parent() ->
    case get('$ancestors') of
    [Parent | _] when is_pid(Parent) ->
        Parent;
    [Parent | _] when is_atom(Parent) ->
        name_to_pid(Parent);
    _ ->
        exit(process_was_not_started_by_partisan_proc_lib)
    end.

name_to_pid(Name) ->
    case whereis(Name) of
    undefined ->
        case global:whereis_name(Name) of
        undefined ->
            exit(could_not_find_registered_name);
        Pid ->
            Pid
        end;
    Pid ->
        Pid
    end.


timeout(Options) ->
    case lists:keyfind(timeout, 1, Options) of
    {_,Time} ->
        Time;
    false ->
        infinity
    end.

spawn_opts(Options) ->
    case lists:keyfind(spawn_opt, 1, Options) of
    {_,Opts} ->
        Opts;
    false ->
        []
    end.

hibernate_after(Options) ->
    case lists:keyfind(hibernate_after, 1, Options) of
        {_,HibernateAfterTimeout} ->
            HibernateAfterTimeout;
        false ->
            infinity
    end.

debug_options(Name, Opts) ->
    case lists:keyfind(debug, 1, Opts) of
    {_,Options} ->
        try partisan_sys:debug_options(Options)
        catch _:_ ->
            error_logger:format(
              "~tp: ignoring erroneous debug options - ~tp~n",
              [Name,Options]),
            []
        end;
    false ->
        []
    end.

format_status_header(TagLine, Pid) when is_pid(Pid) ->
    lists:concat([TagLine, " ", pid_to_list(Pid)]);
format_status_header(TagLine, RegName) when is_atom(RegName) ->
    lists:concat([TagLine, " ", RegName]);
format_status_header(TagLine, Name) ->
    {TagLine, Name}.






%% =============================================================================
%% PARTISAN PUBLIC
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc Returns opts from the calling process' dictionary.
%% The returned list is guaranteed to have a value for key `channel'.
%% @end
%% -----------------------------------------------------------------------------
-spec get_opts() -> [{channel, partisan:channel()}].

get_opts() ->
    case erlang:get(partisan_gen_opts) of
        undefined ->
            Channel = partisan_config:default_channel(),
            [{channel, Channel}];
        Opts ->
            Opts
    end.



%% -----------------------------------------------------------------------------
%% @doc Set partisan options in the process dictionary of the calling process. These options are used by the partisan OTP behaviour either to set the options during init or when sending a request to a process implementing the behaviours.
%% This library uses these approach to
%% @end
%% -----------------------------------------------------------------------------
-spec set_opts([{atom(), any()}]) -> ok.

set_opts(Options) when is_list(Options) ->
    PartisanOpts =
        case lists:keyfind(channel, 1, Options) of
            {channel, _} = Opt ->
                [Opt];
            false ->
                Channel = partisan_config:default_channel(),
                [{channel, Channel}]
        end,
    _ = erlang:put(partisan_gen_opts, PartisanOpts),
    ok.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec erase_opts() -> ok.

erase_opts() ->
    _ = erlang:erase(partisan_gen_opts),
    ok.
