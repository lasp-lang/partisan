%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Christopher S. Meiklejohn.  All Rights Reserved.
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

%% -----------------------------------------------------------------------------
%% @doc Structural AST patches applied AFTER the mechanical rewrite done by
%% partisan_otp_rewrite:transform/2. Replaces, appends, or adds functions and
%% exports in the already-rewritten forms for gen_server and gen modules.
%%
%% Because the patches are injected into code that has already been rewritten,
%% all patch source strings use partisan names directly (e.g. partisan:node(),
%% partisan:send(), partisan_gen:get_opts()).
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_otp_patches).

-export([apply_patches/2]).

%% =============================================================================
%% API
%% =============================================================================

%% @doc Apply all structural patches for OrigModule to the given forms.
apply_patches(OrigModule, Forms) ->
    Patches = patches(OrigModule),
    lists:foldl(fun apply_patch/2, Forms, Patches).

%% =============================================================================
%% INTERNAL: Patch dispatch
%% =============================================================================

patches(gen_server) ->
    gen_server_patches();
patches(gen) ->
    gen_patches();
patches(proc_lib) ->
    proc_lib_patches();
patches(sys) ->
    [];
patches(supervisor) ->
    supervisor_patches(partisan_gen_transform:otp_major_version());
patches(_) ->
    [].

%% =============================================================================
%% INTERNAL: gen_server patches
%% =============================================================================

gen_server_patches() ->
    [
        {replace, do_send, 2, do_send_source()},
        {replace, client_stacktrace, 1, client_stacktrace_source()},
        {append_clause, cast, 2, cast_catch_all_source()},
        {add_function, cast, 3, cast_3_source()},
        {add_function, send_request, 3, send_request_3_source()},
        {add_export, [{cast, 3}, {send_request, 3}]}
    ].

do_send_source() ->
    "do_send(Dest, Msg) when is_pid(Dest) orelse is_reference(Dest) ->\n"
    "    try erlang:send(Dest, Msg)\n"
    "    catch error:_ -> ok\n"
    "    end,\n"
    "    ok;\n"
    "do_send({Name, Node}, Msg) ->\n"
    "    case Node =:= partisan:node() of\n"
    "        true -> do_send(Name, Msg);\n"
    "        false -> partisan:forward_message(Node, Name, Msg, partisan_gen:get_opts())\n"
    "    end;\n"
    "do_send(Dest, Msg) ->\n"
    "    partisan:send(Dest, Msg, partisan_gen:get_opts()).\n".

client_stacktrace_source() ->
    "client_stacktrace(undefined) ->\n"
    "    undefined;\n"
    "client_stacktrace({From, _Tag}) ->\n"
    "    client_stacktrace(From);\n"
    "client_stacktrace(From) when is_pid(From), node(From) =:= node() ->\n"
    "    case process_info(From, [current_stacktrace, registered_name]) of\n"
    "        undefined ->\n"
    "            {From, dead};\n"
    "        [{current_stacktrace, Stacktrace}, {registered_name, []}] ->\n"
    "            {From, {From, Stacktrace}};\n"
    "        [{current_stacktrace, Stacktrace}, {registered_name, Name}] ->\n"
    "            {From, {Name, Stacktrace}}\n"
    "    end;\n"
    "client_stacktrace(From) when is_pid(From) ->\n"
    "    {From, remote};\n"
    "client_stacktrace(From) ->\n"
    "    try\n"
    "        client_stacktrace(partisan_remote_ref:to_term(From))\n"
    "    catch\n"
    "        error:badarg ->\n"
    "            {From, remote}\n"
    "    end.\n".

cast_catch_all_source() ->
    "cast(PartisanDest, Request) ->\n"
    "    do_cast(PartisanDest, Request).\n".

cast_3_source() ->
    "cast(ServerRef, Request, Opts) when is_list(Opts) ->\n"
    "    partisan_gen:set_opts(Opts),\n"
    "    cast(ServerRef, Request).\n".

send_request_3_source() ->
    "send_request(Name, Request, Opts) ->\n"
    "    partisan_gen:send_request(Name, '$gen_call', Request, Opts).\n".

%% =============================================================================
%% INTERNAL: gen patches
%% =============================================================================

gen_patches() ->
    [
        {replace, do_send_request, 3, do_send_request_source()},
        {replace, reply, 2, reply_source()},
        {replace, monitor_return, 1, monitor_return_source()},
        {replace, init_it2, 7, init_it2_source()},
        {replace, get_node, 1, get_node_source()},
        {replace, do_for_proc, 2, do_for_proc_source()},
        {replace, call, 4, call_4_source()},
        {replace, do_call, 4, do_call_source()},
        {replace, get_parent, 0, get_parent_source()},
        {add_function, set_opts, 1, set_opts_source()},
        {add_function, get_opts, 0, get_opts_0_source()},
        {add_function, get_opts, 1, get_opts_1_source()},
        {add_function, erase_opts, 0, erase_opts_source()},
        {add_function, send_request, 4, gen_send_request_4_source()},
        {add_export, [
            {set_opts, 1},
            {get_opts, 0},
            {get_opts, 1},
            {erase_opts, 0},
            {send_request, 4}
        ]}
    ].

do_send_request_source() ->
    "do_send_request(Process, Label, Request)\n"
    "when is_pid(Process) orelse is_atom(Process) ->\n"
    "    Mref = erlang:monitor(process, Process, [{alias, demonitor}]),\n"
    "    erlang:send(Process, {Label, {self(), [alias|Mref]}, Request}, [noconnect]),\n"
    "    Mref;\n"
    "do_send_request({ServerRef, Node} = Process, Label, Request) ->\n"
    "    case Node == partisan:node() of\n"
    "        true ->\n"
    "            do_send_request(ServerRef, Label, Request);\n"
    "        false ->\n"
    "            %% The `{alias, demonitor}' option is required so the receiver\n"
    "            %% can reply via `[alias|Mref]'. Without it, the reply has no\n"
    "            %% delivery target and the caller hangs until the monitor fires.\n"
    "            Opts = partisan_gen:get_opts(),\n"
    "            Mref = partisan:monitor(\n"
    "                process, Process, [{alias, demonitor} | Opts]\n"
    "            ),\n"
    "            Message = {Label, {partisan:self(), [alias|Mref]}, Request},\n"
    "            partisan:forward_message(Node, ServerRef, Message, Opts),\n"
    "            Mref\n"
    "    end;\n"
    "do_send_request(Process, Label, Request) ->\n"
    "    case partisan:is_local(Process) of\n"
    "        true ->\n"
    "            LocalProcess = partisan_remote_ref:to_term(Process),\n"
    "            do_send_request(LocalProcess, Label, Request);\n"
    "        false ->\n"
    "            Opts = partisan_gen:get_opts(),\n"
    "            %% Same `{alias, demonitor}' requirement as the {Name, Node}\n"
    "            %% clause above — the receiver replies via `[alias|Mref]'.\n"
    "            Mref = partisan:monitor(\n"
    "                process, Process, [{alias, demonitor} | Opts]\n"
    "            ),\n"
    "            Message = {Label, {partisan:self(), [alias|Mref]}, Request},\n"
    "            partisan:forward_message(Process, Message, Opts),\n"
    "            Mref\n"
    "    end.\n".

reply_source() ->
    "reply({_To, [alias|Alias] = Tag}, Reply) when is_reference(Alias) ->\n"
    "    Alias ! {Tag, Reply}, ok;\n"
    "reply({_To, [[alias|Alias] | _] = Tag}, Reply) when is_reference(Alias) ->\n"
    "    Alias ! {Tag, Reply}, ok;\n"
    "reply({To, Tag}, Reply) ->\n"
    "    try partisan:forward_message(To, {Tag, Reply}) catch _:_ -> ok end.\n".

monitor_return_source() ->
    "monitor_return({{ok, Pid}, Mon}) when is_pid(Pid), is_reference(Mon) ->\n"
    "    {ok, {Pid, Mon}};\n"
    "monitor_return({{ok, Pid}, Mon}) ->\n"
    "    case partisan:is_pid(Pid) andalso partisan:is_reference(Mon) of\n"
    "        true -> {ok, {Pid, Mon}};\n"
    "        false -> error(function_clause)\n"
    "    end;\n"
    "monitor_return({Error, Mon}) ->\n"
    "    case partisan:is_reference(Mon) of\n"
    "        true ->\n"
    "            receive\n"
    "                {'DOWN', Mon, process, _Pid, _Reason} -> ok\n"
    "            end,\n"
    "            Error;\n"
    "        false ->\n"
    "            error(function_clause)\n"
    "    end.\n".

init_it2_source() ->
    "init_it2(GenMod, Starter, Parent, Name, Mod, Args, Options) ->\n"
    "    ok = set_opts(Options),\n"
    "    GenMod:init_it(Starter, Parent, Name, Mod, Args, Options).\n".

get_node_source() ->
    "get_node(Process) ->\n"
    "    case Process of\n"
    "        {_S, N} when is_atom(N) -> N;\n"
    "        _ when is_pid(Process) -> node(Process);\n"
    "        _ -> partisan:node(Process)\n"
    "    end.\n".

do_for_proc_source() ->
    "do_for_proc(Pid, Fun) when is_pid(Pid) ->\n"
    "    Fun(Pid);\n"
    "do_for_proc(Name, Fun) when is_atom(Name) ->\n"
    "    case whereis(Name) of\n"
    "        Pid when is_pid(Pid) -> Fun(Pid);\n"
    "        undefined -> exit(noproc)\n"
    "    end;\n"
    "do_for_proc(Process, Fun)\n"
    "  when ((tuple_size(Process) == 2 andalso element(1, Process) == global)\n"
    "        orelse\n"
    "        (tuple_size(Process) == 3 andalso element(1, Process) == via)) ->\n"
    "    case where(Process) of\n"
    "        Pid when is_pid(Pid) ->\n"
    "            Node = node(Pid),\n"
    "            try Fun(Pid)\n"
    "            catch\n"
    "                exit:{nodedown, Node} -> exit(noproc)\n"
    "            end;\n"
    "        undefined ->\n"
    "            exit(noproc)\n"
    "    end;\n"
    "do_for_proc({Name, Node} = Process, Fun) when is_atom(Node) ->\n"
    "    case partisan:node() of\n"
    "        'nonode@nohost' -> exit({nodedown, Node});\n"
    "        Node -> do_for_proc(Name, Fun);\n"
    "        _Peer -> Fun(Process)\n"
    "    end;\n"
    "do_for_proc(ProcessRef, Fun) ->\n"
    "    partisan_remote_ref:is_pid(ProcessRef) orelse error(function_clause),\n"
    "    Fun(ProcessRef).\n".

call_4_source() ->
    "call(Process, Label, Request, Opts0) when is_list(Opts0) ->\n"
    "    case lists:keytake(timeout, 1, Opts0) of\n"
    "        {value, {timeout, Timeout}, Opts} ->\n"
    "            set_opts(Opts),\n"
    "            call(Process, Label, Request, Timeout);\n"
    "        false ->\n"
    "            set_opts(Opts0),\n"
    "            call(Process, Label, Request, 5000)\n"
    "    end;\n"
    "call(Process, Label, Request, Timeout) when is_pid(Process),\n"
    "  Timeout =:= infinity orelse is_integer(Timeout) andalso Timeout >= 0 ->\n"
    "    do_call(Process, Label, Request, Timeout);\n"
    "call(Process, Label, Request, Timeout)\n"
    "  when Timeout =:= infinity; is_integer(Timeout), Timeout >= 0 ->\n"
    "    case partisan_remote_ref:is_local_pid(Process) of\n"
    "        true ->\n"
    "            Pid = partisan_remote_ref:to_term(Process),\n"
    "            do_call(Pid, Label, Request, Timeout);\n"
    "        false ->\n"
    "            Fun = fun(Arg) -> do_call(Arg, Label, Request, Timeout) end,\n"
    "            do_for_proc(Process, Fun)\n"
    "    end.\n".

do_call_source() ->
    "do_call(Process, _Label, _Request, _Timeout) when Process =:= self() ->\n"
    "    exit(calling_self);\n"
    "do_call(Process, Label, Request, infinity)\n"
    "  when (is_pid(Process) andalso (node(Process) == node()))\n"
    "       orelse (element(2, Process) == node()\n"
    "              andalso is_atom(element(1, Process))\n"
    "              andalso (tuple_size(Process) =:= 2)) ->\n"
    "    Mref = erlang:monitor(process, Process),\n"
    "    Process ! {Label, {self(), Mref}, Request},\n"
    "    receive\n"
    "        {Mref, Reply} ->\n"
    "            erlang:demonitor(Mref, [flush]),\n"
    "            {ok, Reply};\n"
    "        {'DOWN', Mref, _, _, Reason} ->\n"
    "            exit(Reason)\n"
    "    end;\n"
    "do_call(ProcessRef, Label, Request, Timeout) ->\n"
    "    Mref = do_send_request(ProcessRef, Label, Request),\n"
    "    receive\n"
    "        {[alias|Mref], Reply} ->\n"
    "            partisan:demonitor(Mref, [flush]),\n"
    "            {ok, Reply};\n"
    "        {'DOWN', Mref, _, _, noconnection} ->\n"
    "            Node = get_node(ProcessRef),\n"
    "            exit({nodedown, Node});\n"
    "        {'DOWN', Mref, _, _, Reason} ->\n"
    "            exit(Reason)\n"
    "    after Timeout ->\n"
    "        partisan:demonitor(Mref, [flush]),\n"
    "        receive\n"
    "            {[alias|Mref], Reply} ->\n"
    "                {ok, Reply}\n"
    "        after 0 ->\n"
    "                exit(timeout)\n"
    "        end\n"
    "    end.\n".

get_parent_source() ->
    "get_parent() ->\n"
    "    case get('$ancestors') of\n"
    "        [Parent | _] when is_pid(Parent) ->\n"
    "            Parent;\n"
    "        [Parent | _] when is_atom(Parent) ->\n"
    "            name_to_pid(Parent);\n"
    "        _ ->\n"
    "            exit(process_was_not_started_by_proc_lib)\n"
    "    end.\n".

set_opts_source() ->
    "set_opts(Options) when is_list(Options) ->\n"
    "    PartisanOpts =\n"
    "        case lists:keyfind(channel, 1, Options) of\n"
    "            {channel, _} = Opt -> [Opt];\n"
    "            false ->\n"
    "                Channel = partisan_config:default_channel(),\n"
    "                [{channel, Channel}]\n"
    "        end,\n"
    "    _ = erlang:put(partisan_gen_opts, PartisanOpts),\n"
    "    ok.\n".

get_opts_0_source() ->
    "get_opts() ->\n"
    "    case erlang:get(partisan_gen_opts) of\n"
    "        undefined ->\n"
    "            Channel = partisan_config:default_channel(),\n"
    "            [{channel, Channel}];\n"
    "        Opts ->\n"
    "            Opts\n"
    "    end.\n".

get_opts_1_source() ->
    "get_opts(Opts) ->\n"
    "    lists:keymerge(1, get_opts(), lists:keysort(1, Opts)).\n".

erase_opts_source() ->
    "erase_opts() ->\n"
    "    _ = erlang:erase(partisan_gen_opts),\n"
    "    ok.\n".

gen_send_request_4_source() ->
    "send_request(Process, Label, Request, Opts) ->\n"
    "    set_opts(Opts),\n"
    "    send_request(Process, Label, Request).\n".

%% =============================================================================
%% INTERNAL: proc_lib patches
%% =============================================================================

proc_lib_patches() ->
    [
        {replace, stop, 3, proc_lib_stop_source()},
        {add_function, do_stop, 2, proc_lib_do_stop_source()},
        {replace, proc_info, 2, proc_lib_proc_info_source()}
    ].

proc_lib_stop_source() ->
    %% Mirror OTP's `proc_lib:stop/3' with the `partisan_sys' atom. `Reason'
    %% in the catch patterns is a LITERAL match against the argument (bound in
    %% the enclosing scope), so only same-reason exits from `partisan_sys:\n"
    %% terminate' are treated as success — any other wrapped exit (e.g.
    %% `{nodedown, Node}') propagates verbatim via the `exit:Reason1` clause,
    %% matching OTP's `{'EXIT', {{nodedown, Node}, _}}' contract.
    "stop(Process, Reason, Timeout) ->\n"
    "    Mref = partisan:monitor(process, Process),\n"
    "    T0 = erlang:monotonic_time(millisecond),\n"
    "    StopTimeout = fun(infinity) -> infinity;\n"
    "                     (T) -> max(0, T - (erlang:monotonic_time(millisecond) - T0))\n"
    "                  end,\n"
    "    Remaining = try partisan_sys:terminate(Process, Reason, Timeout) of\n"
    "        ok -> StopTimeout(Timeout)\n"
    "    catch\n"
    "        exit:{noproc, {partisan_sys, terminate, _}} ->\n"
    "            partisan:demonitor(Mref, [flush]),\n"
    "            exit(noproc);\n"
    "        exit:{timeout, {partisan_sys, terminate, _}} ->\n"
    "            partisan:demonitor(Mref, [flush]),\n"
    "            exit(timeout);\n"
    "        exit:{Reason, {partisan_sys, terminate, _}} ->\n"
    "            StopTimeout(Timeout);\n"
    "        exit:Reason1 ->\n"
    "            partisan:demonitor(Mref, [flush]),\n"
    "            exit(Reason1)\n"
    "    end,\n"
    "    receive\n"
    "        {'DOWN', Mref, _, _, Reason} ->\n"
    "            ok;\n"
    "        {'DOWN', Mref, _, _, Reason2} ->\n"
    "            exit(Reason2)\n"
    "    after Remaining ->\n"
    "        partisan:demonitor(Mref, [flush]),\n"
    "        exit(timeout)\n"
    "    end.\n".

proc_lib_do_stop_source() ->
    "do_stop(Process, Reason) ->\n"
    "    fun() ->\n"
    "        Mref = partisan:monitor(process, Process),\n"
    "        ok = partisan_sys:terminate(Process, Reason, infinity),\n"
    "        receive\n"
    "            {'DOWN', Mref, _, _, ExitReason} ->\n"
    "                exit(ExitReason)\n"
    "        end\n"
    "    end.\n".

proc_lib_proc_info_source() ->
    "proc_info(Pid, Item) when node(Pid) =:= node() ->\n"
    "    process_info(Pid, Item);\n"
    "proc_info(Pid, Item) ->\n"
    "    case partisan_config:get(connect_disterl) of\n"
    "        true ->\n"
    "            case lists:member(node(Pid), nodes()) of\n"
    "                true ->\n"
    "                    check(rpc:call(node(Pid), erlang, process_info, [Pid, Item]));\n"
    "                _ ->\n"
    "                    hidden\n"
    "            end;\n"
    "        false ->\n"
    "            MyNode = partisan:node(),\n"
    "            TheirNode = partisan:node(Pid),\n"
    "            case TheirNode =:= MyNode of\n"
    "                true ->\n"
    "                    partisan:process_info(Pid, Item);\n"
    "                false ->\n"
    "                    case lists:member(TheirNode, partisan:nodes()) of\n"
    "                        true ->\n"
    "                            Result = partisan_rpc:call(\n"
    "                                TheirNode, partisan, process_info, [Pid, Item]\n"
    "                            ),\n"
    "                            check(Result);\n"
    "                        _ ->\n"
    "                            hidden\n"
    "                    end\n"
    "            end\n"
    "    end.\n".

%% =============================================================================
%% INTERNAL: supervisor patches
%% =============================================================================

supervisor_patches(OtpVsn) ->
    [
        {replace, do_start_child, 3, sup_do_start_child_source()},
        {replace, do_start_child_i, 3, sup_do_start_child_i_source()},
        {replace, handle_call, 3, sup_handle_call_source(OtpVsn)},
        {replace, handle_start_child, 2, sup_handle_start_child_source()},
        {replace, restarting, 1, sup_restarting_source()},
        {replace, do_terminate, 2, sup_do_terminate_source()},
        {replace, terminate_dynamic_children, 1,
            sup_terminate_dynamic_children_source()},
        {replace, find_child, 2, sup_find_child_source()},
        {replace, find_child_and_args, 2, sup_find_child_and_args_source()},
        {replace, unlink_flush, 2, sup_unlink_flush_source()},
        {replace, shutdown, 1, sup_shutdown_source()},
        {replace, count_child, 2, sup_count_child_source()},
        %% Child type validation: accept both `supervisor` and
        %% `partisan_gen_supervisor` since the rewrite renames the atom but
        %% users pass `supervisor` in their child specs.
        {replace, validChildType, 1, sup_validChildType_source()},
        {replace, do_check_childspec, 2, sup_do_check_childspec_source()}
        %% NOTE: format_log_multi/2 and format_log_single/2 are NOT patched.
        %% The mechanical rewrite handles the atom renames in pattern matches
        %% ({supervisor,progress} → {partisan_gen_supervisor,progress}).
        %% The format strings ("Supervisor: ", "    supervisor: ~tp~n") are
        %% human-readable labels that should stay as-is.
    ].

%% OTP 28 supervisor: do_start_child/3 has case guards `when is_pid(Pid)`.
%% Move the is_pid check to body context using partisan:is_pid/1.
sup_do_start_child_source() ->
    "do_start_child(SupName, Child, Report) ->\n"
    "    #child{mfargs = {M, F, Args}} = Child,\n"
    "    case do_start_child_i(M, F, Args) of\n"
    "        {ok, Pid} when Pid =:= undefined ->\n"
    "            {ok, undefined};\n"
    "        {ok, Pid} ->\n"
    "            case partisan:is_pid(Pid) of\n"
    "                true ->\n"
    "                    NChild = Child#child{pid = Pid},\n"
    "                    report_progress(NChild, SupName, Report),\n"
    "                    {ok, Pid};\n"
    "                false ->\n"
    "                    {error, {bad_return, {M, F, Args}, {ok, Pid}}}\n"
    "            end;\n"
    "        {ok, Pid, Extra} ->\n"
    "            case partisan:is_pid(Pid) of\n"
    "                true ->\n"
    "                    NChild = Child#child{pid = Pid},\n"
    "                    report_progress(NChild, SupName, Report),\n"
    "                    {ok, Pid, Extra};\n"
    "                false ->\n"
    "                    {error, {bad_return, {M, F, Args}, {ok, Pid, Extra}}}\n"
    "            end;\n"
    "        Other ->\n"
    "            Other\n"
    "    end.\n".

%% OTP 28 supervisor: do_start_child_i/3 has case guards `when is_pid(Pid)`.
sup_do_start_child_i_source() ->
    "do_start_child_i(M, F, A) ->\n"
    "    case catch apply(M, F, A) of\n"
    "        {ok, Pid} when Pid =:= undefined ->\n"
    "            {ok, undefined};\n"
    "        {ok, Pid} ->\n"
    "            case partisan:is_pid(Pid) of\n"
    "                true -> {ok, Pid};\n"
    "                false -> {error, {bad_return, {M, F, A}, {ok, Pid}}}\n"
    "            end;\n"
    "        {ok, Pid, Extra} ->\n"
    "            case partisan:is_pid(Pid) of\n"
    "                true -> {ok, Pid, Extra};\n"
    "                false -> {error, {bad_return, {M, F, A}, {ok, Pid, Extra}}}\n"
    "            end;\n"
    "        ignore ->\n"
    "            {ok, undefined};\n"
    "        {error, Error} ->\n"
    "            {error, Error};\n"
    "        What ->\n"
    "            {error, What}\n"
    "    end.\n".

%% OTP 28 supervisor: handle_call/3 uses `not is_pid(Id)` in function clause
%% guards. Replace with body-level check via partisan:is_pid/1.
%% NOTE: After mechanical rewrite, atoms are already renamed
%% (supervisor -> partisan_gen_supervisor, gen_server -> partisan_gen_server).
%% The patch source uses the REWRITTEN names.
sup_handle_call_source(OtpVsn) ->
    %% The handle_call replacement is written for OTP 28+ which uses
    %% hibernate_after_action(State) as the 4th reply element.
    %% For OTP 27, we strip it out since supervisor replies are 3-tuples.
    Source = sup_handle_call_source_28(),
    case OtpVsn >= 28 of
        true ->
            Source;
        false ->
            %% Remove hibernate_after_action(State) from all reply tuples.
            %% Handles both same-line (", hibernate_after_action(State)")
            %% and next-line (",\n     hibernate_after_action(State)") patterns.
            {ok, Re} = re:compile(",\\s*hibernate_after_action\\(State\\)"),
            re:replace(Source, Re, "", [global, {return, list}])
    end.

sup_handle_call_source_28() ->
    "handle_call({start_child, EArgs}, _From, State)\n"
    "    when State#state.strategy =:= simple_one_for_one ->\n"
    "    Child = get_dynamic_child(State),\n"
    "    #child{mfargs = {M, F, A}} = Child,\n"
    "    Args = A ++ EArgs,\n"
    "    case do_start_child_i(M, F, Args) of\n"
    "        {ok, undefined} ->\n"
    "            {reply, {ok, undefined}, State, hibernate_after_action(State)};\n"
    "        {ok, Pid} ->\n"
    "            NState = dyn_store(Pid, Args, State),\n"
    "            {reply, {ok, Pid}, NState, hibernate_after_action(State)};\n"
    "        {ok, Pid, Extra} ->\n"
    "            NState = dyn_store(Pid, Args, State),\n"
    "            {reply, {ok, Pid, Extra}, NState, hibernate_after_action(State)};\n"
    "        What ->\n"
    "            {reply, What, State, hibernate_after_action(State)}\n"
    "    end;\n"
    "handle_call({start_child, ChildSpec}, _From, State) ->\n"
    "    case check_childspec(ChildSpec, State#state.auto_shutdown) of\n"
    "        {ok, Child} ->\n"
    "            {Resp, NState} = handle_start_child(Child, State),\n"
    "            {reply, Resp, NState, hibernate_after_action(State)};\n"
    "        What ->\n"
    "            {reply, {error, What}, State, hibernate_after_action(State)}\n"
    "    end;\n"
    "handle_call({terminate_child, Id}, _From, State)\n"
    "    when State#state.strategy =:= simple_one_for_one ->\n"
    "    case partisan:is_pid(Id) of\n"
    "        true ->\n"
    "            case find_child(Id, State) of\n"
    "                {ok, Child} ->\n"
    "                    do_terminate(Child, State#state.name),\n"
    "                    {reply, ok, del_child(Child, State),\n"
    "                     hibernate_after_action(State)};\n"
    "                error ->\n"
    "                    {reply, {error, not_found}, State,\n"
    "                     hibernate_after_action(State)}\n"
    "            end;\n"
    "        false ->\n"
    "            {reply, {error, simple_one_for_one}, State,\n"
    "             hibernate_after_action(State)}\n"
    "    end;\n"
    "handle_call({terminate_child, Id}, _From, State) ->\n"
    "    case find_child(Id, State) of\n"
    "        {ok, Child} ->\n"
    "            do_terminate(Child, State#state.name),\n"
    "            {reply, ok, del_child(Child, State),\n"
    "             hibernate_after_action(State)};\n"
    "        error ->\n"
    "            {reply, {error, not_found}, State,\n"
    "             hibernate_after_action(State)}\n"
    "    end;\n"
    "handle_call({restart_child, _Id}, _From, State)\n"
    "    when State#state.strategy =:= simple_one_for_one ->\n"
    "    {reply, {error, simple_one_for_one}, State,\n"
    "     hibernate_after_action(State)};\n"
    "handle_call({restart_child, Id}, _From, State) ->\n"
    "    case find_child(Id, State) of\n"
    "        {ok, Child} when Child#child.pid =:= undefined ->\n"
    "            case do_start_child(State#state.name, Child, debug_report) of\n"
    "                {ok, Pid} ->\n"
    "                    NState = set_pid(Pid, Id, State),\n"
    "                    {reply, {ok, Pid}, NState,\n"
    "                     hibernate_after_action(State)};\n"
    "                {ok, Pid, Extra} ->\n"
    "                    NState = set_pid(Pid, Id, State),\n"
    "                    {reply, {ok, Pid, Extra}, NState,\n"
    "                     hibernate_after_action(State)};\n"
    "                Error ->\n"
    "                    {reply, Error, State, hibernate_after_action(State)}\n"
    "            end;\n"
    "        {ok, #child{pid = {restarting, _}}} ->\n"
    "            {reply, {error, restarting}, State,\n"
    "             hibernate_after_action(State)};\n"
    "        {ok, _} ->\n"
    "            {reply, {error, running}, State,\n"
    "             hibernate_after_action(State)};\n"
    "        _ ->\n"
    "            {reply, {error, not_found}, State,\n"
    "             hibernate_after_action(State)}\n"
    "    end;\n"
    "handle_call({delete_child, _Id}, _From, State)\n"
    "    when State#state.strategy =:= simple_one_for_one ->\n"
    "    {reply, {error, simple_one_for_one}, State,\n"
    "     hibernate_after_action(State)};\n"
    "handle_call({delete_child, Id}, _From, State) ->\n"
    "    case find_child(Id, State) of\n"
    "        {ok, Child} when Child#child.pid =:= undefined ->\n"
    "            NState = remove_child(Id, State),\n"
    "            {reply, ok, NState, hibernate_after_action(State)};\n"
    "        {ok, #child{pid = {restarting, _}}} ->\n"
    "            {reply, {error, restarting}, State,\n"
    "             hibernate_after_action(State)};\n"
    "        {ok, _} ->\n"
    "            {reply, {error, running}, State,\n"
    "             hibernate_after_action(State)};\n"
    "        _ ->\n"
    "            {reply, {error, not_found}, State,\n"
    "             hibernate_after_action(State)}\n"
    "    end;\n"
    "handle_call({get_childspec, Id}, _From, State) ->\n"
    "    case find_child(Id, State) of\n"
    "        {ok, Child} ->\n"
    "            {reply, {ok, child_to_spec(Child)}, State,\n"
    "             hibernate_after_action(State)};\n"
    "        error ->\n"
    "            {reply, {error, not_found}, State,\n"
    "             hibernate_after_action(State)}\n"
    "    end;\n"
    "handle_call(which_children, _From, State)\n"
    "    when State#state.strategy =:= simple_one_for_one ->\n"
    "    #child{child_type = CT, modules = Mods} = get_dynamic_child(State),\n"
    "    Reply = dyn_map(\n"
    "        fun({restarting, _}) -> {undefined, restarting, CT, Mods};\n"
    "           (Pid) -> {undefined, Pid, CT, Mods}\n"
    "        end, State),\n"
    "    {reply, Reply, State, hibernate_after_action(State)};\n"
    "handle_call(which_children, _From, State) ->\n"
    "    Resp = children_to_list(\n"
    "        fun(Id, #child{pid = {restarting, _}, child_type = ChildType,\n"
    "                       modules = Mods}) ->\n"
    "                {Id, restarting, ChildType, Mods};\n"
    "           (Id, #child{pid = Pid, child_type = ChildType,\n"
    "                       modules = Mods}) ->\n"
    "                {Id, Pid, ChildType, Mods}\n"
    "        end, State#state.children),\n"
    "    {reply, Resp, State, hibernate_after_action(State)};\n"
    "handle_call({which_child, Id}, _From, State)\n"
    "    when State#state.strategy =:= simple_one_for_one ->\n"
    "    case partisan:is_pid(Id) of\n"
    "        true ->\n"
    "            Result = case find_dynamic_child(Id, State) of\n"
    "                {ok, #child{pid = {restarting, _}, child_type = CT,\n"
    "                            modules = Mods}} ->\n"
    "                    {ok, {undefined, restarting, CT, Mods}};\n"
    "                {ok, #child{pid = Id, child_type = CT, modules = Mods}} ->\n"
    "                    {ok, {undefined, Id, CT, Mods}};\n"
    "                error ->\n"
    "                    {error, not_found}\n"
    "            end,\n"
    "            {reply, Result, State, hibernate_after_action(State)};\n"
    "        false ->\n"
    "            {reply, {error, simple_one_for_one}, State,\n"
    "             hibernate_after_action(State)}\n"
    "    end;\n"
    "handle_call({which_child, Id}, _From, State) ->\n"
    "    Result = case find_child(Id, State) of\n"
    "        {ok, #child{pid = {restarting, _}, child_type = CT,\n"
    "                    modules = Mods}} ->\n"
    "            {ok, {Id, restarting, CT, Mods}};\n"
    "        {ok, #child{pid = Pid, child_type = CT, modules = Mods}} ->\n"
    "            {ok, {Id, Pid, CT, Mods}};\n"
    "        error ->\n"
    "            {error, not_found}\n"
    "    end,\n"
    "    {reply, Result, State, hibernate_after_action(State)};\n"
    "handle_call(count_children, _From,\n"
    "            #state{dynamic_restarts = Restarts} = State)\n"
    "    when State#state.strategy =:= simple_one_for_one ->\n"
    "    #child{child_type = CT} = get_dynamic_child(State),\n"
    "    Sz = dyn_size(State),\n"
    "    Active = Sz - Restarts,\n"
    "    Reply = case CT of\n"
    "        supervisor ->\n"
    "            [{specs, 1}, {active, Active}, {supervisors, Sz}, {workers, 0}];\n"
    "        partisan_gen_supervisor ->\n"
    "            [{specs, 1}, {active, Active}, {supervisors, Sz}, {workers, 0}];\n"
    "        worker ->\n"
    "            [{specs, 1}, {active, Active}, {supervisors, 0}, {workers, Sz}]\n"
    "    end,\n"
    "    {reply, Reply, State, hibernate_after_action(State)};\n"
    "handle_call(count_children, _From, State) ->\n"
    "    {Specs, Active, Supers, Workers} =\n"
    "        children_fold(\n"
    "            fun(_Id, Child, Counts) -> count_child(Child, Counts) end,\n"
    "            {0, 0, 0, 0}, State#state.children),\n"
    "    Reply = [{specs, Specs}, {active, Active},\n"
    "             {supervisors, Supers}, {workers, Workers}],\n"
    "    {reply, Reply, State, hibernate_after_action(State)}.\n".

%% handle_start_child/2: guard `is_pid(OldChild#child.pid)` → body check.
sup_handle_start_child_source() ->
    "handle_start_child(Child, State) ->\n"
    "    case find_child(Child#child.id, State) of\n"
    "        error ->\n"
    "            case do_start_child(State#state.name, Child, debug_report) of\n"
    "                {ok, undefined}\n"
    "                    when Child#child.restart_type =:= temporary ->\n"
    "                    {{ok, undefined}, State};\n"
    "                {ok, Pid} ->\n"
    "                    {{ok, Pid},\n"
    "                     save_child(Child#child{pid = Pid}, State)};\n"
    "                {ok, Pid, Extra} ->\n"
    "                    {{ok, Pid, Extra},\n"
    "                     save_child(Child#child{pid = Pid}, State)};\n"
    "                {error, {already_started, _Pid} = What} ->\n"
    "                    {{error, What}, State};\n"
    "                {error, What} ->\n"
    "                    {{error, {What, Child}}, State}\n"
    "            end;\n"
    "        {ok, OldChild} ->\n"
    "            case partisan:is_pid(OldChild#child.pid) of\n"
    "                true ->\n"
    "                    {{error, {already_started, OldChild#child.pid}}, State};\n"
    "                false ->\n"
    "                    {{error, already_present}, State}\n"
    "            end\n"
    "    end.\n".

%% restarting/1: guard `is_pid(Pid)` → body check.
sup_restarting_source() ->
    "restarting(Pid) ->\n"
    "    case partisan:is_pid(Pid) of\n"
    "        true -> {restarting, Pid};\n"
    "        false -> Pid\n"
    "    end.\n".

%% do_terminate/2: guard `is_pid(Child#child.pid)` → body check.
sup_do_terminate_source() ->
    "do_terminate(Child, SupName) ->\n"
    "    case partisan:is_pid(Child#child.pid) of\n"
    "        true ->\n"
    "            case shutdown(Child) of\n"
    "                ok ->\n"
    "                    ok;\n"
    "                {error, OtherReason} ->\n"
    "                    case logger:allow(error, partisan_gen_supervisor) of\n"
    "                        true ->\n"
    "                            apply(logger, macro_log,\n"
    "                                [#{mfa => {partisan_gen_supervisor,\n"
    "                                           do_terminate, 2},\n"
    "                                   line => 0,\n"
    "                                   file => \"supervisor.erl\"},\n"
    "                                 error,\n"
    "                                 #{label => {partisan_gen_supervisor,\n"
    "                                             shutdown_error},\n"
    "                                   report =>\n"
    "                                       [{partisan_gen_supervisor, SupName},\n"
    "                                        {errorContext, shutdown_error},\n"
    "                                        {reason, OtherReason},\n"
    "                                        {offender,\n"
    "                                         extract_child(Child)}]},\n"
    "                                 #{domain => [otp, sasl],\n"
    "                                   report_cb =>\n"
    "                                       fun partisan_gen_supervisor:format_log/2,\n"
    "                                   logger_formatter =>\n"
    "                                       #{title => \"SUPERVISOR REPORT\"},\n"
    "                                   error_logger =>\n"
    "                                       #{tag => error_report,\n"
    "                                         type => supervisor_report,\n"
    "                                         report_cb =>\n"
    "                                             fun partisan_gen_supervisor:format_log/1}}]);\n"
    "                        false ->\n"
    "                            ok\n"
    "                    end\n"
    "            end,\n"
    "            ok;\n"
    "        false ->\n"
    "            ok\n"
    "    end.\n".

%% terminate_dynamic_children/1: fun clause guard `is_pid(P)` → body check.
%% Also uses exit/2 and monitor/2 which are handled by auto-import rewrites,
%% but we need to handle the is_pid guard in the fun.
sup_terminate_dynamic_children_source() ->
    "terminate_dynamic_children(State) ->\n"
    "    Child = get_dynamic_child(State),\n"
    "    Pids = dyn_fold(\n"
    "        fun(P, Acc) ->\n"
    "            case partisan:is_pid(P) of\n"
    "                true ->\n"
    "                    Mon = partisan:monitor(process, P),\n"
    "                    case Child#child.shutdown of\n"
    "                        brutal_kill -> partisan:exit(P, kill);\n"
    "                        _ -> partisan:exit(P, shutdown)\n"
    "                    end,\n"
    "                    Acc#{{P, Mon} => true};\n"
    "                false ->\n"
    "                    Acc\n"
    "            end\n"
    "        end, #{}, State),\n"
    "    TRef = case Child#child.shutdown of\n"
    "        brutal_kill -> undefined;\n"
    "        infinity -> undefined;\n"
    "        Time -> erlang:start_timer(Time, self(), kill)\n"
    "    end,\n"
    "    Sz = maps:size(Pids),\n"
    "    EStack = wait_dynamic_children(Child, Pids, Sz, TRef, #{}),\n"
    "    maps:foreach(\n"
    "        fun(Reason, Ls) ->\n"
    "            case logger:allow(error, partisan_gen_supervisor) of\n"
    "                true ->\n"
    "                    apply(logger, macro_log,\n"
    "                        [#{mfa => {partisan_gen_supervisor,\n"
    "                                   terminate_dynamic_children, 1},\n"
    "                           line => 0,\n"
    "                           file => \"supervisor.erl\"},\n"
    "                         error,\n"
    "                         #{label => {partisan_gen_supervisor,\n"
    "                                     shutdown_error},\n"
    "                           report =>\n"
    "                               [{partisan_gen_supervisor,\n"
    "                                 State#state.name},\n"
    "                                {errorContext, shutdown_error},\n"
    "                                {reason, Reason},\n"
    "                                {offender,\n"
    "                                 extract_child(\n"
    "                                     Child#child{pid = Ls})}]},\n"
    "                         #{domain => [otp, sasl],\n"
    "                           report_cb =>\n"
    "                               fun partisan_gen_supervisor:format_log/2,\n"
    "                           logger_formatter =>\n"
    "                               #{title => \"SUPERVISOR REPORT\"},\n"
    "                           error_logger =>\n"
    "                               #{tag => error_report,\n"
    "                                 type => supervisor_report,\n"
    "                                 report_cb =>\n"
    "                                     fun partisan_gen_supervisor:format_log/1}}]);\n"
    "                false ->\n"
    "                    ok\n"
    "            end\n"
    "        end, EStack).\n".

%% find_child/2: guard `is_pid(Pid)` → body check.
sup_find_child_source() ->
    "find_child(Pid, State)\n"
    "    when State#state.strategy =:= simple_one_for_one ->\n"
    "    case partisan:is_pid(Pid) of\n"
    "        true ->\n"
    "            case find_dynamic_child(Pid, State) of\n"
    "                error ->\n"
    "                    case find_dynamic_child(restarting(Pid), State) of\n"
    "                        error ->\n"
    "                            case partisan:is_process_alive(Pid) of\n"
    "                                true -> error;\n"
    "                                false -> {ok, get_dynamic_child(State)}\n"
    "                            end;\n"
    "                        Other ->\n"
    "                            Other\n"
    "                    end;\n"
    "                Other ->\n"
    "                    Other\n"
    "            end;\n"
    "        false ->\n"
    "            find_child(Pid, State#state{strategy = undefined})\n"
    "    end;\n"
    "find_child(Id, #state{children = {_Ids, Db}}) ->\n"
    "    maps:find(Id, Db).\n".

%% find_child_and_args/2: guard `is_pid(Pid)` → body check.
sup_find_child_and_args_source() ->
    "find_child_and_args(Pid, State)\n"
    "    when State#state.strategy =:= simple_one_for_one ->\n"
    "    case find_dynamic_child(Pid, State) of\n"
    "        {ok, #child{mfargs = {M, F, _}} = Child} ->\n"
    "            {ok, Args} = dyn_args(Pid, State),\n"
    "            {ok, Child#child{mfargs = {M, F, Args}}};\n"
    "        error ->\n"
    "            error\n"
    "    end;\n"
    "find_child_and_args(Pid, State) ->\n"
    "    case partisan:is_pid(Pid) of\n"
    "        true -> find_child_by_pid(Pid, State);\n"
    "        false ->\n"
    "            #state{children = {_Ids, Db}} = State,\n"
    "            maps:find(Pid, Db)\n"
    "    end.\n".

%% unlink_flush/2: add catch-all for non-native pids (partisan remote refs).
sup_unlink_flush_source() ->
    "unlink_flush(Pid, noproc) when is_pid(Pid) ->\n"
    "    {links, Ls} = process_info(self(), links),\n"
    "    Timeout = case lists:member(Pid, Ls) of\n"
    "        true -> infinity;\n"
    "        false -> 0\n"
    "    end,\n"
    "    receive\n"
    "        {'EXIT', Pid, ExitReason} -> ExitReason\n"
    "    after Timeout -> child_process_unlinked\n"
    "    end;\n"
    "unlink_flush(Pid, ExitReason) when is_pid(Pid) ->\n"
    "    unlink(Pid),\n"
    "    receive\n"
    "        {'EXIT', Pid, _} -> ok\n"
    "    after 0 -> ok\n"
    "    end,\n"
    "    ExitReason;\n"
    "unlink_flush(_, _) ->\n"
    "    normal.\n".

%% shutdown/1: uses exit/2 and monitor/2 in body context. The auto-import
%% rewrite handles exit/2 → partisan:exit/2 and monitor/2 → partisan:monitor/2.
%% But we need to explicitly use partisan: calls since this is a patch
%% (patches bypass rewrite).
sup_shutdown_source() ->
    "shutdown(#child{pid = Pid, shutdown = brutal_kill} = Child) ->\n"
    "    Mon = partisan:monitor(process, Pid),\n"
    "    partisan:exit(Pid, kill),\n"
    "    receive\n"
    "        {'DOWN', Mon, process, Pid, Reason0} ->\n"
    "            case unlink_flush(Pid, Reason0) of\n"
    "                killed -> ok;\n"
    "                shutdown\n"
    "                    when not (Child#child.restart_type =:= permanent) -> ok;\n"
    "                {shutdown, _}\n"
    "                    when not (Child#child.restart_type =:= permanent) -> ok;\n"
    "                normal\n"
    "                    when not (Child#child.restart_type =:= permanent) -> ok;\n"
    "                Reason -> {error, Reason}\n"
    "            end\n"
    "    end;\n"
    "shutdown(#child{pid = Pid, shutdown = Time} = Child) ->\n"
    "    Mon = partisan:monitor(process, Pid),\n"
    "    partisan:exit(Pid, shutdown),\n"
    "    receive\n"
    "        {'DOWN', Mon, process, Pid, Reason0} ->\n"
    "            case unlink_flush(Pid, Reason0) of\n"
    "                shutdown -> ok;\n"
    "                {shutdown, _}\n"
    "                    when not (Child#child.restart_type =:= permanent) -> ok;\n"
    "                normal\n"
    "                    when not (Child#child.restart_type =:= permanent) -> ok;\n"
    "                Reason -> {error, Reason}\n"
    "            end\n"
    "    after Time ->\n"
    "        partisan:exit(Pid, kill),\n"
    "        receive\n"
    "            {'DOWN', Mon, process, Pid, Reason0} ->\n"
    "                case unlink_flush(Pid, Reason0) of\n"
    "                    shutdown -> ok;\n"
    "                    {shutdown, _}\n"
    "                        when not (Child#child.restart_type =:= permanent) -> ok;\n"
    "                    normal\n"
    "                        when not (Child#child.restart_type =:= permanent) -> ok;\n"
    "                    Reason -> {error, Reason}\n"
    "                end\n"
    "        end\n"
    "    end.\n".

%% count_child/2: uses `is_pid(Pid) andalso is_process_alive(Pid)` in body.
%% The auto-import rewrite would handle this, but since this is body context
%% we need to use partisan: calls explicitly in the patch.
sup_count_child_source() ->
    "count_child(#child{pid = Pid, child_type = worker},\n"
    "            {Specs, Active, Supers, Workers}) ->\n"
    "    case partisan:is_pid(Pid) andalso partisan:is_process_alive(Pid) of\n"
    "        true -> {Specs + 1, Active + 1, Supers, Workers + 1};\n"
    "        false -> {Specs + 1, Active, Supers, Workers + 1}\n"
    "    end;\n"
    "count_child(#child{pid = Pid, child_type = CT},\n"
    "            {Specs, Active, Supers, Workers})\n"
    "        when CT =:= supervisor; CT =:= partisan_gen_supervisor ->\n"
    "    case partisan:is_pid(Pid) andalso partisan:is_process_alive(Pid) of\n"
    "        true -> {Specs + 1, Active + 1, Supers + 1, Workers};\n"
    "        false -> {Specs + 1, Active, Supers + 1, Workers}\n"
    "    end.\n".

%% validChildType/1: accept both `supervisor` and `partisan_gen_supervisor`.
%% Users pass `type => supervisor` in child specs, but the rewrite renames the
%% atom `supervisor` to `partisan_gen_supervisor` in the code. We must accept
%% both to maintain API compatibility.
sup_validChildType_source() ->
    "validChildType(supervisor) -> true;\n"
    "validChildType(partisan_gen_supervisor) -> true;\n"
    "validChildType(worker) -> true;\n"
    "validChildType(What) -> throw({invalid_child_type, What}).\n".

%% do_check_childspec/2: The mechanical rewrite changes pattern matches on
%% `#{type := supervisor}` to `#{type := partisan_gen_supervisor}`. We need
%% to handle both atoms. This is a full replacement of the function.
sup_do_check_childspec_source() ->
    %% Normalize the `supervisor' atom to `partisan_gen_supervisor' (instead
    %% of the other direction): downstream guards in this module have been
    %% rewritten to `=:= partisan_gen_supervisor', so the stored value must
    %% match. `get_childspec' will also return `partisan_gen_supervisor',
    %% which is what the (rewritten) test source expects.
    "do_check_childspec(#{restart := RestartType, type := ChildType0} = ChildSpec,\n"
    "                   AutoShutdown) ->\n"
    "    ChildType = case ChildType0 of\n"
    "        supervisor -> partisan_gen_supervisor;\n"
    "        Other0 -> Other0\n"
    "    end,\n"
    "    Id = case ChildSpec of\n"
    "        #{id := I} -> I;\n"
    "        _ -> throw(missing_id)\n"
    "    end,\n"
    "    Func = case ChildSpec of\n"
    "        #{start := F} -> F;\n"
    "        _ -> throw(missing_start)\n"
    "    end,\n"
    "    validId(Id),\n"
    "    validFunc(Func),\n"
    "    validRestartType(RestartType),\n"
    "    Significant = case ChildSpec of\n"
    "        #{significant := Signf} -> Signf;\n"
    "        _ -> false\n"
    "    end,\n"
    "    validSignificant(Significant, RestartType, AutoShutdown),\n"
    "    validChildType(ChildType),\n"
    "    Shutdown = case ChildSpec of\n"
    "        #{shutdown := S} -> S;\n"
    "        _ when ChildType =:= worker -> 5000;\n"
    "        _ when ChildType =:= partisan_gen_supervisor;\n"
    "                 ChildType =:= supervisor -> infinity\n"
    "    end,\n"
    "    validShutdown(Shutdown),\n"
    "    Mods = case ChildSpec of\n"
    "        #{modules := Ms} -> Ms;\n"
    "        _ -> {M, _, _} = Func, [M]\n"
    "    end,\n"
    "    validMods(Mods),\n"
    "    {ok, #child{id = Id, mfargs = Func, restart_type = RestartType,\n"
    "               significant = Significant, shutdown = Shutdown,\n"
    "               child_type = ChildType, modules = Mods}}.\n".

%% =============================================================================
%% INTERNAL: Patch application
%% =============================================================================

%% Apply a single patch to the list of forms.
apply_patch({replace, FunName, Arity, Source}, Forms) ->
    NewForm = parse_function(Source),
    replace_function(FunName, Arity, NewForm, Forms);
apply_patch({add_function, _FunName, _Arity, Source}, Forms) ->
    NewForm = parse_function(Source),
    insert_before_eof(NewForm, Forms);
apply_patch({add_export, FunArities}, Forms) ->
    add_exports(FunArities, Forms);
apply_patch({append_clause, FunName, Arity, Source}, Forms) ->
    NewForm = parse_function(Source),
    {function, _, _, _, NewClauses} = NewForm,
    append_clauses(FunName, Arity, NewClauses, Forms).

%% =============================================================================
%% INTERNAL: Form manipulation helpers
%% =============================================================================

%% Parse an Erlang function source string into an abstract form.
parse_function(Source) ->
    {ok, Tokens, _} = erl_scan:string(Source),
    {ok, Form} = erl_parse:parse_form(Tokens),
    Form.

%% Replace a function by name and arity with a new form.
replace_function(FunName, Arity, NewForm, Forms) ->
    lists:map(
        fun
            ({function, _Anno, Name, Ar, _Clauses}) when
                Name =:= FunName, Ar =:= Arity
            ->
                NewForm;
            (Other) ->
                Other
        end,
        Forms
    ).

%% Insert a form before the eof tuple, or append if no eof is found.
insert_before_eof(NewForm, Forms) ->
    case lists:reverse(Forms) of
        [{eof, _} = Eof | Rest] ->
            lists:reverse([Eof, NewForm | Rest]);
        _ ->
            Forms ++ [NewForm]
    end.

%% Add function name/arity pairs to the first export attribute found.
add_exports(FunArities, Forms) ->
    ExportEntries = [{FN, FA} || {FN, FA} <- FunArities],
    add_exports_to_first(ExportEntries, Forms, false).

add_exports_to_first(_Entries, [], _Found) ->
    [];
add_exports_to_first(
    Entries, [{attribute, Anno, export, Existing} | Rest], false
) ->
    NewExport = Existing ++ Entries,
    [
        {attribute, Anno, export, NewExport}
        | add_exports_to_first(Entries, Rest, true)
    ];
add_exports_to_first(Entries, [Form | Rest], Found) ->
    [Form | add_exports_to_first(Entries, Rest, Found)].

%% Append clauses to an existing function matched by name and arity.
append_clauses(FunName, Arity, NewClauses, Forms) ->
    lists:map(
        fun
            ({function, Anno, Name, Ar, Clauses}) when
                Name =:= FunName, Ar =:= Arity
            ->
                {function, Anno, Name, Ar, Clauses ++ NewClauses};
            (Other) ->
                Other
        end,
        Forms
    ).
