%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1999-2022. All Rights Reserved.
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
%%

-module(partisan_monitor_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([init_per_suite/1, end_per_suite/1]).

-export([
    all/0,
    suite/0,
    groups/0,
    case_1/1,
    case_1a/1,
    case_2/1,
    case_2a/1,
    mon_e_1/1,
    demon_e_1/1,
    demon_1/1,
    demon_2/1,
    demon_3/1,
    demonitor_flush/1,
    gh_5225_demonitor_alias/1,
    local_remove_monitor/1,
    remote_remove_monitor/1,
    mon_1/1,
    mon_2/1,
    large_exit/1,
    list_cleanup/1,
    mixer/1,
    named_down/1,
    otp_5827/1,
    monitor_time_offset/1,
    monitor_tag_storage/1,
    unexpected_alias_at_demonitor_gh5310/1,
    down_on_alias_gh5310/1,
    monitor_3_noproc_gh6185/1,
    exactly_one_down_on_race/1,
    channel_bound_down/1,
    caller_gc/1
]).

-export([y2/1, g/1, g0/0, g1/0, large_exit_sub/1]).

suite() ->
    [
        {ct_hooks, [ts_install_cth]},
        {timetrap, {minutes, 15}}
    ].

all() ->
    [
        case_1,
        case_1a,
        case_2,
        case_2a,
        mon_e_1,
        demon_e_1,
        demon_1,
        mon_1,
        mon_2,
        demon_2,
        demon_3,
        demonitor_flush,
        gh_5225_demonitor_alias,
        {group, remove_monitor},
        large_exit,
        list_cleanup,
        mixer,
        named_down,
        otp_5827,
        monitor_time_offset,
        monitor_tag_storage,
        unexpected_alias_at_demonitor_gh5310,
        down_on_alias_gh5310,
        monitor_3_noproc_gh6185,
        exactly_one_down_on_race,
        channel_bound_down,
        caller_gc
    ].

groups() ->
    [{remove_monitor, [], [local_remove_monitor, remote_remove_monitor]}].

init_per_suite(Config) ->
    %% we start disterl and partisan at runner so that we can join the peers
    partisan_support:start_disterl(),
    erlang:is_alive() orelse ct:fail("Runner not in distribution mode"),
    application:ensure_all_started(partisan),
    Config.

end_per_suite(_Config) ->
    %% case proplists:get_value(node, Config) of
    %% undefined ->
    %%     ok;
    %% N ->
    %%     partisan_support_otp:stop_node(N)
    %% end,
    %% ok.

    % stop all peers
    partisan_support_otp:stop_all_nodes(),
    % stop partisan at runner
    application:stop(partisan),
    ok.

%% A monitors B, B kills A and then exits (yielded core dump)
case_1(Config) when is_list(Config) ->
    process_flag(trap_exit, true),
    spawn_link(?MODULE, g0, []),
    receive
        _ -> ok
    end,
    ok.

%% A monitors B, B kills A and then exits (yielded core dump)
case_1a(Config) when is_list(Config) ->
    process_flag(trap_exit, true),
    spawn_link(?MODULE, g1, []),
    receive
        _ -> ok
    end,
    ok.

g0() ->
    B = spawn(?MODULE, g, [self()]),
    partisan:monitor(process, B),
    B ! ok,
    receive
        ok -> ok
    end,
    ok.

g1() ->
    {B, _} = spawn_monitor(?MODULE, g, [self()]),
    B ! ok,
    receive
        ok -> ok
    end,
    ok.

g(Parent) ->
    receive
        ok -> ok
    end,
    partisan:exit(Parent, foo),
    ok.

%% A monitors B, B demonitors A (yielded core dump)
case_2(Config) when is_list(Config) ->
    B = spawn(?MODULE, y2, [self()]),
    R = partisan:monitor(process, B),
    B ! R,
    receive
        true -> ok;
        Other -> ct:fail({rec, Other})
    end,
    expect_down(R, B, normal),
    ok.

%% A monitors B, B demonitors A (yielded core dump)
case_2a(Config) when is_list(Config) ->
    {B, R} = spawn_monitor(?MODULE, y2, [self()]),
    B ! R,
    receive
        true -> ok;
        Other -> ct:fail({rec, Other})
    end,
    expect_down(R, B, normal),
    ok.

y2(Parent) ->
    R =
        receive
            T -> T
        end,
    Parent ! (catch partisan:demonitor(R)),
    ok.

expect_down(Ref, P) ->
    receive
        {'DOWN', PRef, process, PP, Reason} = Signal ->
            (PRef =:= Ref orelse partisan:is_local_reference(PRef, Ref)) andalso
                (PP =:= P orelse
                    (is_pid(P) andalso
                        partisan:is_local_pid(PP, P)) orelse
                    (is_pid(PP) andalso
                        partisan:is_local_pid(P, PP)) orelse
                    same_name(P, PP)) orelse
                ct:fail([{rec, Signal}, {args, [Ref, P]}]),
            Reason;
        Other ->
            ct:fail([{rec, Other}, {args, [Ref, P]}])
    end.

expect_down(Ref, P, Reason) ->
    receive
        {'DOWN', PRef, process, PP, Reason} = Signal ->
            (PRef =:= Ref orelse partisan:is_local_reference(PRef, Ref)) andalso
                (PP =:= P orelse
                    (is_pid(P) andalso
                        partisan:is_local_pid(PP, P)) orelse
                    (is_pid(PP) andalso
                        partisan:is_local_pid(P, PP)) orelse
                    same_name(P, PP)) orelse
                ct:fail([{rec, Signal}, {args, [Ref, P, Reason]}]),
            ok;
        Other ->
            ct:fail([{rec, Other}, {args, [Ref, P, Reason]}])
    end.

%% `P' may be given as `{Name, Node}' while the signal carries the encoded
%% remote name reference. `partisan_remote_ref:to_term/1' decodes local terms
%% only and raises `badarg' on a reference belonging to another node, so the
%% two halves are compared through the accessors that do work on remote refs.
same_name({Name, Node}, PP) when is_atom(Name), is_atom(Node) ->
    partisan_remote_ref:is_name(PP, Name) andalso
        partisan_remote_ref:node(PP) == Node;
same_name(_, _) ->
    false.

expect_no_msg() ->
    receive
        Msg ->
            ct:fail({msg, Msg})
    after 0 ->
        ok
    end.

%%% Error cases for monitor/2

mon_e_1(Config) when is_list(Config) ->
    {ok, N} = partisan_support_otp:start_node(hej),
    partisan_support:cluster(N),
    timer:sleep(2000),
    mon_error(plutt, self()),
    mon_error(process, [bingo]),
    mon_error(process, {rex, N, junk}),
    mon_error(process, 1),

    ok = partisan_support_otp:stop_node(N),
    ok.

%%% We would also like to have a test case that tries to monitor something
%%% on an R5 node, but this isn't possible to do systematically.
%%%
%%% Likewise against an R6 node, which is not capable of monitoring
%%% by name, which gives a badarg on the R7 node at the call to
%%% partisan:monitor(process, {Name, Node}). This has been tested
%%% manually at least once.

mon_error(Type, Item) ->
    case catch partisan:monitor(Type, Item) of
        {'EXIT', _} ->
            ok;
        Other ->
            ct:fail({err, Other})
    end.

%%% Error cases for demonitor/1

demon_e_1(Config) when is_list(Config) ->
    {ok, N} = partisan_support_otp:start_node(hej),
    partisan_support:cluster(N),
    timer:sleep(2000),
    demon_error(plutt, badarg),
    demon_error(1, badarg),

    %% Demonitor with ref created at other node
    R1 = rpc:call(N, erlang, make_ref, []),
    demon_error(R1, badarg),

    %% Demonitor with ref created at wrong monitor link end
    P0 = self(),
    P2 = spawn(
        fun() ->
            P0 ! {self(), ref, partisan:monitor(process, P0)},
            receive
                {P0, stop} -> ok
            end
        end
    ),
    receive
        {P2, ref, R2} ->
            true = partisan:demonitor(R2),
            P2 ! {self(), stop};
        Other2 ->
            ct:fail({rec, Other2})
    end,

    ok = partisan_support_otp:stop_node(N),
    ok.

demon_error(Ref, Reason) ->
    case catch partisan:demonitor(Ref) of
        {'EXIT', {Reason, _}} ->
            ok;
        Other ->
            ct:fail({err, Other})
    end.

%%% No-op cases for demonitor/1

demon_1(Config) when is_list(Config) ->
    true = partisan:demonitor(make_ref()),
    ok.

%%% Cases for demonitor/1

demon_2(Config) when is_list(Config) ->
    R1 = partisan:monitor(process, self()),
    true = partisan:demonitor(R1),
    %% Extra demonitor
    true = partisan:demonitor(R1),
    expect_no_msg(),

    %% Normal 'DOWN'
    P2 = spawn(timer, sleep, [1]),
    R2 = partisan:monitor(process, P2),
    case expect_down(R2, P2) of
        normal -> ok;
        noproc -> ok;
        BadReason -> ct:fail({bad_reason, BadReason})
    end,

    %% OTP-5772
    %     %% 'DOWN' before demonitor
    %     P3 = spawn(timer, sleep, [100000]),
    %     R3 = partisan:monitor(process, P3),
    %     exit(P3, frop),
    %     partisan:demonitor(R3),
    %     expect_down(R3, P3, frop),

    %% Demonitor before 'DOWN'
    P4 = spawn(timer, sleep, [100000]),
    R4 = partisan:monitor(process, P4),
    partisan:demonitor(R4),
    partisan:exit(P4, frop),
    expect_no_msg(),

    ok.

%% Distributed case for demonitor/1 (OTP-3499)
demon_3(Config) when is_list(Config) ->
    {ok, N} = partisan_support_otp:start_node(hej),
    partisan_support:cluster(N),
    timer:sleep(2000),

    %% 'DOWN' before demonitor
    P2 = partisan:spawn(N, timer, sleep, [100000]),
    R2 = partisan:monitor(process, P2),
    ok = partisan_support_otp:stop_node(N),
    true = partisan:demonitor(R2),
    expect_down(R2, P2, noconnection),

    {ok, N2} = partisan_support_otp:start_node(hej),
    partisan_support:cluster(N2),
    timer:sleep(2000),
    %% Demonitor before 'DOWN'
    P3 = partisan:spawn(N2, timer, sleep, [100000]),
    R3 = partisan:monitor(process, P3),
    true = partisan:demonitor(R3),
    ok = partisan_support_otp:stop_node(N2),
    expect_no_msg(),
    ok.

demonitor_flush(Config) when is_list(Config) ->
    {'EXIT', {badarg, _}} = (catch partisan:demonitor(make_ref(), flush)),
    {'EXIT', {badarg, _}} = (catch partisan:demonitor(make_ref(), [flus])),
    {'EXIT', {badarg, _}} = (catch partisan:demonitor(x, [flush])),
    {ok, N} = partisan_support_otp:start_node(demonitor_flush),
    partisan_support:cluster(N),
    timer:sleep(2000),

    ok = demonitor_flush_test(N),
    ok = partisan_support_otp:stop_node(N),
    ok = demonitor_flush_test(node()).

demonitor_flush_test(Node) ->
    P = partisan:spawn(Node, timer, sleep, [100000]),
    %% A DOWN for a local target is delivered by the VM and names the raw pid;
    %% for a remote one `partisan_monitor' fabricates it and names the remote
    %% reference. Both stand for `P', and this runs against both a remote node
    %% and the local one, so the receives below accept either form.
    Raw = raw(P),
    M1 = partisan:monitor(process, P),
    M2 = partisan:monitor(process, P),
    M3 = partisan:monitor(process, P),
    M4 = partisan:monitor(process, P),
    true = partisan:demonitor(M1, [flush, flush]),
    partisan:exit(P, bang),
    receive
        {'DOWN', M2, process, D2, bang} when D2 == P; D2 == Raw -> ok
    end,
    receive
    after 100 -> ok
    end,
    true = partisan:demonitor(M3, [flush]),
    true = partisan:demonitor(M4, []),
    receive
        {'DOWN', M4, process, D4, bang} when D4 == P; D4 == Raw -> ok
    end,
    receive
        {'DOWN', M, _, _, _} = DM when
            M == M1;
            M == M3
        ->
            ct:fail({unexpected_down_message, DM})
    after 100 ->
        ok
    end.

gh_5225_demonitor_alias(Config) when is_list(Config) ->
    %% Demonitor using a reference that was an active alias, but not an
    %% active monitor, used to crash the runtime system.
    Alias = alias(),
    partisan:demonitor(Alias),
    partisan:demonitor(Alias, [flush]),
    {Pid, MonAlias1} = spawn_opt(
        fun() ->
            ok
        end,
        [{monitor, [{alias, explicit_unalias}]}]
    ),
    receive
        {'DOWN', MonAlias1, process, Pid, normal} -> ok
    end,
    partisan:demonitor(MonAlias1),
    partisan:demonitor(MonAlias1, [flush]),
    MonAlias2 = partisan:monitor(process, Pid, [{alias, explicit_unalias}]),
    receive
        {'DOWN', MonAlias2, process, Pid, noproc} -> ok
    end,
    partisan:demonitor(MonAlias2),
    partisan:demonitor(MonAlias2, [flush]),
    ok.

-define(RM_MON_GROUPS, 100).
-define(RM_MON_GPROCS, 100).

local_remove_monitor(Config) when is_list(Config) ->
    Gs = generate(
        fun() -> start_remove_monitor_group(node()) end,
        ?RM_MON_GROUPS
    ),
    {True, False} = lists:foldl(
        fun(G, {T, F}) ->
            receive
                {rm_mon_res, G, {GT, GF}} ->
                    {T + GT, F + GF}
            end
        end,
        {0, 0},
        Gs
    ),
    erlang:display({local_remove_monitor, True, False}),
    {comment,
        "True = " ++ integer_to_list(True) ++ "; False = " ++
            integer_to_list(False)}.

remote_remove_monitor(Config) when is_list(Config) ->
    {ok, N} = partisan_support_otp:start_node(demonitor_flush),
    partisan_support:cluster(N),
    timer:sleep(2000),

    Gs = generate(
        fun() -> start_remove_monitor_group(N) end,
        ?RM_MON_GROUPS
    ),
    {True, False} = lists:foldl(
        fun(G, {T, F}) ->
            receive
                {rm_mon_res, G, {GT, GF}} ->
                    {T + GT, F + GF}
            end
        end,
        {0, 0},
        Gs
    ),
    erlang:display({remote_remove_monitor, True, False}),
    ok = partisan_support_otp:stop_node(N),
    {comment,
        "True = " ++ integer_to_list(True) ++ "; False = " ++
            integer_to_list(False)}.

start_remove_monitor_group(Node) ->
    Master = self(),
    spawn_link(
        fun() ->
            Ms = generate(
                fun() ->
                    P = partisan:spawn(Node, fun() -> ok end),
                    partisan:monitor(process, P)
                end,
                ?RM_MON_GPROCS
            ),
            Res = lists:foldl(
                fun(M, {T, F}) ->
                    case partisan:demonitor(M, [info]) of
                        true ->
                            receive
                                {'DOWN', M, _, _, _} ->
                                    exit(down_msg_found)
                            after 0 ->
                                ok
                            end,
                            {T + 1, F};
                        false ->
                            receive
                                {'DOWN', M, _, _, _} ->
                                    ok
                            after 0 ->
                                exit(no_down_msg_found)
                            end,
                            {T, F + 1}
                    end
                end,
                {0, 0},
                Ms
            ),
            Master ! {rm_mon_res, self(), Res}
        end
    ).

%%% Cases for monitor/2

mon_1(Config) when is_list(Config) ->
    %% Normal case
    P2 = spawn(timer, sleep, [1]),
    R2 = partisan:monitor(process, P2),
    case expect_down(R2, P2) of
        normal -> ok;
        noproc -> ok;
        BadReason -> ct:fail({bad_reason, BadReason})
    end,
    {P2A, R2A} = spawn_monitor(timer, sleep, [1]),
    expect_down(R2A, P2A, normal),

    %% 'DOWN' with other reason
    P3 = spawn(timer, sleep, [100000]),
    R3 = partisan:monitor(process, P3),
    partisan:exit(P3, frop),
    expect_down(R3, P3, frop),
    {P3A, R3A} = spawn_monitor(timer, sleep, [100000]),
    partisan:exit(P3A, frop),
    expect_down(R3A, P3A, frop),

    %% Monitor fails because process is dead
    R4 = partisan:monitor(process, P3),
    expect_down(R4, P3, noproc),

    %% Normal case (named process)
    P5 = start_jeeves(jeeves),
    R5 = partisan:monitor(process, jeeves),
    tell_jeeves(P5, stop),
    expect_down(R5, {jeeves, node()}, normal),

    %% 'DOWN' with other reason and node explicit activation
    P6 = start_jeeves(jeeves),
    R6 = partisan:monitor(process, {jeeves, node()}),
    tell_jeeves(P6, {exit, frop}),
    expect_down(R6, {jeeves, node()}, frop),

    %% Monitor (named process) fails because process is dead
    R7 = partisan:monitor(process, {jeeves, node()}),
    expect_down(R7, {jeeves, node()}, noproc),

    ok.

%% Distributed cases for monitor/2
mon_2(Config) when is_list(Config) ->
    {ok, N1} = partisan_support_otp:start_node(?FUNCTION_NAME),
    partisan_support:cluster(N1),
    timer:sleep(2000),

    %% Normal case
    P2 = partisan:spawn(N1, timer, sleep, [4000]),
    R2 = partisan:monitor(process, P2),
    expect_down(R2, P2, normal),

    %% 'DOWN' with other reason
    P3 = partisan:spawn(N1, timer, sleep, [100000]),
    R3 = partisan:monitor(process, P3),
    partisan:exit(P3, frop),
    expect_down(R3, P3, frop),

    %% Monitor fails because process is dead
    R4 = partisan:monitor(process, P3),
    expect_down(R4, P3, noproc),

    %% Other node goes down
    P5 = partisan:spawn(N1, timer, sleep, [100000]),
    R5 = partisan:monitor(process, P5),

    ok = partisan_support_otp:stop_node(N1),
    expect_down(R5, P5, noconnection),

    %% Monitor fails because other node is dead
    P6 = partisan:spawn(N1, timer, sleep, [100000]),
    R6 = partisan:monitor(process, P6),
    R6_Reason = expect_down(R6, P6),
    true = (R6_Reason == noconnection) orelse (R6_Reason == noproc),

    %% Start a new node that can load code in this module
    PA = filename:dirname(code:which(?MODULE)),
    {ok, N2} = partisan_support_otp:start_node(hej2, [
        {node_config, [{args, "-pa " ++ PA}]}
    ]),
    partisan_support:cluster(N2),
    timer:sleep(2000),

    %% Normal case (named process)
    P7 = start_jeeves({jeeves, N2}),
    R7 = partisan:monitor(process, {jeeves, N2}),
    tell_jeeves(P7, stop),
    expect_down(R7, {jeeves, N2}, normal),

    %% 'DOWN' with other reason (named process)
    P8 = start_jeeves({jeeves, N2}),
    R8 = partisan:monitor(process, {jeeves, N2}),
    tell_jeeves(P8, {exit, frop}),
    expect_down(R8, {jeeves, N2}, frop),

    %% Monitor (named process) fails because process is dead
    R9 = partisan:monitor(process, {jeeves, N2}),
    expect_down(R9, {jeeves, N2}, noproc),

    %% Other node goes down (named process)
    _P10 = start_jeeves({jeeves, N2}),
    R10 = partisan:monitor(process, {jeeves, N2}),

    ok = partisan_support_otp:stop_node(N2),

    expect_down(R10, {jeeves, N2}, noconnection),

    %% Monitor (named process) fails because other node is dead
    R11 = partisan:monitor(process, {jeeves, N2}),
    expect_down(R11, {jeeves, N2}, noconnection),

    ok.

%%% Large exit reason. Crashed first attempt to release R5B.

large_exit(Config) when is_list(Config) ->
    f(100),
    ok.

f(0) ->
    ok;
f(N) ->
    f(),
    f(N - 1).

f() ->
    S0 = {big, tuple, with, [list, 4563784278]},
    S = {S0, term_to_binary(S0)},
    P = spawn(?MODULE, large_exit_sub, [S]),
    R = partisan:monitor(process, P),
    P ! hej,
    receive
        {'DOWN', R, process, P, X} ->
            io:format(" -> ~p~n", [X]),
            if
                X == S ->
                    ok;
                true ->
                    ct:fail({X, S})
            end;
        Other ->
            io:format(" -> ~p~n", [Other]),
            exit({answer, Other})
    end.

large_exit_sub(S) ->
    receive
        _X -> ok
    end,
    exit(S).

%%% Testing of monitor link list cleanup
%%% by using erlang:process_info(self(), monitors)
%%% and      erlang:process_info(self(), monitored_by)

list_cleanup(Config) when is_list(Config) ->
    %% Every assertion below is in remote-reference space: that is the one
    %% vocabulary both monitor bookkeepings share. See `monitors/1'.
    P0 = partisan:self(),
    M = node(),
    PA = filename:dirname(code:which(?MODULE)),
    true = register(master_bertie, self()),
    Bertie = pref({master_bertie, M}),
    JeevesM = pref({jeeves, M}),

    %% Normal local case, monitor and demonitor
    P1 = start_jeeves(jeeves),
    {[], []} = monitors(),
    expect_jeeves(P1, monitors, {monitors, {[], []}}),
    R1a = partisan:monitor(process, P1),
    {[{process, P1}], []} = monitors(),
    expect_jeeves(P1, monitors, {monitors, {[], [P0]}}),
    true = partisan:demonitor(R1a),
    expect_no_msg(),
    {[], []} = monitors(),
    expect_jeeves(P1, monitors, {monitors, {[], []}}),
    %% Remonitor named and try again, now exiting the monitored process
    R1b = partisan:monitor(process, jeeves),
    {[{process, JeevesM}], []} = monitors(),
    expect_jeeves(P1, monitors, {monitors, {[], [P0]}}),
    tell_jeeves(P1, stop),
    expect_down(R1b, {jeeves, node()}, normal),
    {[], []} = monitors(),

    %% Slightly weird local case - the monitoring process crashes
    P2 = start_jeeves(jeeves),
    {[], []} = monitors(),
    expect_jeeves(P2, monitors, {monitors, {[], []}}),
    {monitor_process, _R2} =
        ask_jeeves(P2, {monitor_process, master_bertie}),
    {[], [P2]} = monitors(),
    expect_jeeves(P2, monitors, {monitors, {[{process, Bertie}], []}}),
    tell_jeeves(P2, {exit, frop}),
    timer:sleep(2000),
    {[], []} = monitors(),

    %% Start a new node that can load code in this module
    {ok, J} = partisan_support_otp:start_node(jeeves, [
        {node_config, [{args, "-pa " ++ PA}]}
    ]),
    partisan_support:cluster(J),
    timer:sleep(2000),
    JeevesJ = pref({jeeves, J}),

    %% Normal remote case, monitor and demonitor
    P3 = start_jeeves({jeeves, J}),
    {[], []} = monitors(),
    expect_jeeves(P3, monitors, {monitors, {[], []}}),
    R3a = partisan:monitor(process, P3),
    {[{process, P3}], []} = monitors(),
    expect_jeeves(P3, monitors, {monitors, {[], [P0]}}),
    true = partisan:demonitor(R3a),
    expect_no_msg(),
    {[], []} = monitors(),
    expect_jeeves(P3, monitors, {monitors, {[], []}}),
    %% Remonitor named and try again, now exiting the monitored process
    R3b = partisan:monitor(process, {jeeves, J}),
    {[{process, JeevesJ}], []} = monitors(),
    expect_jeeves(P3, monitors, {monitors, {[], [P0]}}),
    tell_jeeves(P3, stop),
    expect_down(R3b, {jeeves, J}, normal),
    {[], []} = monitors(),

    %% Slightly weird remote case - the monitoring process crashes
    P4 = start_jeeves({jeeves, J}),
    {[], []} = monitors(),
    expect_jeeves(P4, monitors, {monitors, {[], []}}),
    {monitor_process, _R4} =
        ask_jeeves(P4, {monitor_process, {master_bertie, M}}),
    {[], [P4]} = monitors(),
    expect_jeeves(P4, monitors, {monitors, {[{process, Bertie}], []}}),
    tell_jeeves(P4, {exit, frop}),
    timer:sleep(2000),
    {[], []} = monitors(),

    %% Now, the monitoring remote node crashes
    P5 = start_jeeves({jeeves, J}),
    {[], []} = monitors(),
    expect_jeeves(P5, monitors, {monitors, {[], []}}),
    {monitor_process, _R5} =
        ask_jeeves(P5, {monitor_process, P0}),
    {[], [P5]} = monitors(),
    expect_jeeves(P5, monitors, {monitors, {[{process, P0}], []}}),
    partisan_support_otp:stop_node(J),
    timer:sleep(4000),
    {[], []} = monitors(),

    true = unregister(master_bertie),
    ok.

%%% Mixed internal and external monitors

mixer(Config) when is_list(Config) ->
    %% As in `list_cleanup/1', assertions are in remote-reference space so that
    %% native and Partisan monitors are counted in one vocabulary.
    Me = partisan:self(),
    PA = filename:dirname(code:which(?MODULE)),
    NN = [j0, j1, j2],
    NL0 = [
        begin
            {ok, J} = partisan_support_otp:start_node(X, [
                {node_config, [{args, "-pa " ++ PA}]}
            ]),
            partisan_support:cluster(J),
            timer:sleep(2000),
            J
        end
     || X <- NN
    ],
    NL1 = lists:duplicate(2, node()) ++ NL0,
    Perm = perm(NL1),
    lists:foreach(
        fun(NL) ->
            Js = [start_jeeves({[], M}) || M <- (NL ++ NL)],
            [ask_jeeves(P, {monitor_process, Me}) || P <- Js],
            {[], MB} = monitors(),
            MBL = lists:sort(MB),
            JsL = lists:sort(Js),
            MBL = JsL,
            [tell_jeeves(P, {exit, flaff}) || P <- Js],
            wait_for_m([], [], 200)
        end,
        Perm
    ),
    lists:foreach(
        fun(NL) ->
            Js = [start_jeeves({[], M}) || M <- (NL ++ NL)],
            Rs = [
                begin
                    {monitor_process, Ref} = ask_jeeves(
                        P, {monitor_process, Me}
                    ),
                    {P, Ref}
                end
             || P <- Js
            ],
            {[], MB} = monitors(),
            MBL = lists:sort(MB),
            JsL = lists:sort(Js),
            MBL = JsL,
            [ask_jeeves(P, {demonitor, Ref}) || {P, Ref} <- Rs],
            wait_for_m([], [], 200),
            [tell_jeeves(P, {exit, flaff}) || P <- Js]
        end,
        Perm
    ),
    lists:foreach(
        fun(NL) ->
            Js = [start_jeeves({[], M}) || M <- (NL ++ NL)],
            [ask_jeeves(P, {monitor_process, Me}) || P <- Js],
            [partisan:monitor(process, P) || P <- Js],
            {Mons, MB} = monitors(),
            MBL = lists:sort(MB),
            JsL = lists:sort(Js),
            MBL = JsL,
            ML = lists:sort([P || {process, P} <- Mons]),
            ML = JsL,
            [
                begin
                    tell_jeeves(P, {exit, flaff}),
                    expect_down_from(P)
                end
             || P <- Js
            ],
            wait_for_m([], [], 200)
        end,
        Perm
    ),
    lists:foreach(
        fun(NL) ->
            Js = [start_jeeves({[], M}) || M <- (NL ++ NL)],
            Rs = [
                begin
                    {monitor_process, Ref} = ask_jeeves(
                        P, {monitor_process, Me}
                    ),
                    {P, Ref}
                end
             || P <- Js
            ],
            R2s = [{P, partisan:monitor(process, P)} || P <- Js],
            {Mons, MB} = monitors(),
            MBL = lists:sort(MB),
            JsL = lists:sort(Js),
            MBL = JsL,
            ML = lists:sort([P || {process, P} <- Mons]),
            ML = JsL,
            [ask_jeeves(P, {demonitor, Ref}) || {P, Ref} <- Rs],
            wait_for_m(lists:sort(Mons), [], 200),
            [partisan:demonitor(Ref) || {_P, Ref} <- R2s],
            wait_for_m([], [], 200),
            [tell_jeeves(P, {exit, flaff}) || P <- Js]
        end,
        Perm
    ),
    [partisan_support_otp:stop_node(K) || K <- NL0],
    ok.

%% Test that DOWN message for a named monitor isn't
%%  delivered until name has been unregistered
named_down(Config) when is_list(Config) ->
    Name = list_to_atom(
        atom_to_list(?MODULE) ++
            "-named_down-" ++
            integer_to_list(erlang:system_time(second)) ++
            "-" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    Prio = process_flag(priority, high),
    %% Spawn a bunch of high prio cpu bound processes to prevent
    %% normal prio processes from terminating during the next
    %% 500 ms...
    Self = self(),
    spawn_opt(
        fun() ->
            WFun = fun
                (F, hej) -> F(F, hopp);
                (F, hopp) -> F(F, hej)
            end,
            NoSchedulers = erlang:system_info(schedulers_online),
            lists:foreach(
                fun(_) ->
                    spawn_opt(
                        fun() ->
                            WFun(
                                WFun,
                                hej
                            )
                        end,
                        [
                            {priority, high},
                            link
                        ]
                    )
                end,
                lists:seq(1, NoSchedulers)
            ),
            receive
            after 500 -> ok
            end,
            unlink(Self),
            exit(bang)
        end,
        [{priority, high}, link]
    ),
    NamedProc = spawn_link(fun() ->
        receive
        after infinity -> ok
        end
    end),
    ?assertEqual(true, register(Name, NamedProc)),
    unlink(NamedProc),
    Mon = partisan:monitor(process, Name),
    partisan:exit(NamedProc, bang),
    receive
        {'DOWN', Mon, _, _, bang} -> ok
    after 3000 -> ?assert(false)
    end,
    ?assertEqual(true, register(Name, self())),
    ?assertEqual(true, unregister(Name)),
    process_flag(priority, Prio),
    ok.

otp_5827(Config) when is_list(Config) ->
    %% Make a pid with the same nodename but with another creation
    [CreEnd | RPTail] =
        lists:reverse(binary_to_list(term_to_binary(self()))),
    NewCreEnd =
        case CreEnd of
            0 -> 1;
            1 -> 2;
            _ -> CreEnd - 1
        end,
    OtherCreationPid =
        binary_to_term(list_to_binary(lists:reverse([NewCreEnd | RPTail]))),
    %% If the bug is present partisan:monitor(process, OtherCreationPid)
    %% will hang...
    Parent = self(),
    Ok = make_ref(),
    spawn(fun() ->
        Mon = partisan:monitor(process, OtherCreationPid),
        % Should get the DOWN message right away
        receive
            {'DOWN', Mon, process, OtherCreationPid, noproc} ->
                Parent ! Ok
        end
    end),
    receive
        Ok ->
            ok
    after 1000 ->
        ct:fail("partisan:monitor/2 hangs")
    end.

monitor_time_offset(Config) when is_list(Config) ->
    {ok, Node} = start_node(Config, "+C single_time_warp"),
    %% The workers run on `Node' and are addressed by partisan reference, so
    %% both directions go through `partisan:send/2'; a raw `!' to a reference
    %% raises `badarg'.
    Me = partisan:self(),
    PMs = lists:map(
        fun(_) ->
            Pid = partisan:spawn(
                Node,
                fun() ->
                    check_monitor_time_offset(Me)
                end
            ),
            {Pid, partisan:monitor(process, Pid)}
        end,
        lists:seq(1, 100)
    ),
    lists:foreach(
        fun({P, _M}) ->
            partisan:send(P, check_no_change_message)
        end,
        PMs
    ),
    lists:foreach(
        fun({P, M}) ->
            receive
                {no_change_message_received, P} ->
                    ok;
                {'DOWN', M, process, P, Reason} ->
                    ct:fail(Reason)
            end
        end,
        PMs
    ),
    preliminary = rpc:call(Node, erlang, system_flag, [time_offset, finalize]),
    lists:foreach(
        fun({P, M}) ->
            receive
                {change_messages_received, P} ->
                    partisan:demonitor(M, [flush]);
                {'DOWN', M, process, P, Reason} ->
                    ct:fail(Reason)
            end
        end,
        PMs
    ),
    stop_node(Node),
    ok.

check_monitor_time_offset(Leader) ->
    Mon1 = partisan:monitor(time_offset, clock_service),
    Mon2 = partisan:monitor(time_offset, clock_service),
    Mon3 = partisan:monitor(time_offset, clock_service),
    Mon4 = partisan:monitor(time_offset, clock_service),

    partisan:demonitor(Mon2, [flush]),

    Mon5 = partisan:monitor(time_offset, clock_service),
    Mon6 = partisan:monitor(time_offset, clock_service),
    Mon7 = partisan:monitor(time_offset, clock_service),

    receive
        check_no_change_message -> ok
    end,
    receive
        {'CHANGE', _, time_offset, clock_service, _} ->
            exit(unexpected_change_message_received)
    after 0 ->
        partisan:send(Leader, {no_change_message_received, partisan:self()})
    end,
    receive
    after 100 -> ok
    end,
    partisan:demonitor(Mon4, [flush]),
    receive
        {'CHANGE', Mon3, time_offset, clock_service, _} ->
            ok
    end,
    receive
        {'CHANGE', Mon6, time_offset, clock_service, _} ->
            ok
    end,
    partisan:demonitor(Mon5, [flush]),
    receive
        {'CHANGE', Mon7, time_offset, clock_service, _} ->
            ok
    end,
    receive
        {'CHANGE', Mon1, time_offset, clock_service, _} ->
            ok
    end,
    receive
        {'CHANGE', _, time_offset, clock_service, _} ->
            exit(unexpected_change_message_received)
    after 1000 ->
        ok
    end,
    partisan:send(Leader, {change_messages_received, partisan:self()}).

monitor_tag_storage(Config) when is_list(Config) ->
    process_flag(priority, max),
    %% WHITEBOX:
    %%
    %% There are three scenarios we want to test. The receiver of the
    %% DOWN message with tag:
    %% * has on-heap message queue data enabled and can allocate
    %%   DOWN message on the heap
    %% * has on-heap message queue data enabled and cannot allocate
    %%   DOWN message on the heap, i.e. the message will be allocated
    %%   in a heap fragment
    %% * has off-heap message queue data enabled, i.e. the message
    %%   will be allocated in a combined message/heap fragment.

    %%
    %% Testing the two on heap message queue data scenarios. Initially
    %% there will be room on the heap, but eventually DOWN messages
    %% will be placed in heap fragments.
    %%
    ok = monitor_tag_storage_test(on_heap),

    %%
    %% Testing the off heap message queue data scenarios.
    %%
    ok = monitor_tag_storage_test(off_heap).

monitor_tag_storage_test(MQD) ->
    Len = 1000,
    Tag = make_ref(),
    Parent = self(),
    Ps = lists:map(
        fun(_) ->
            spawn_opt(
                fun() ->
                    receive
                    after infinity -> ok
                    end
                end,
                [link, {priority, high}]
            )
        end,
        lists:seq(1, Len)
    ),
    {Recvr, RMon} = spawn_opt(
        fun() ->
            lists:foreach(
                fun(P) ->
                    partisan:monitor(
                        process,
                        P,
                        [{tag, Tag}]
                    )
                end,
                Ps
            ),
            Parent ! {ready, self()},
            receive
                {continue, Parent} -> ok
            end,
            garbage_collect(),
            Msgs = receive_tagged_down_msgs(Tag, []),
            Len = length(Msgs),
            garbage_collect(),
            id(Msgs)
        end,
        [link, monitor, {message_queue_data, MQD}]
    ),
    receive
        {ready, Recvr} -> ok
    end,
    lists:foreach(
        fun(P) ->
            unlink(P),
            partisan:exit(P, bang)
        end,
        Ps
    ),
    wait_until(fun() ->
        {message_queue_len, Len} ==
            process_info(
                Recvr,
                message_queue_len
            )
    end),
    {messages, Msgs} = process_info(Recvr, messages),
    lists:foreach(
        fun(Msg) ->
            {Tag, _Mon, process, _Pid, bang} = Msg
        end,
        Msgs
    ),
    garbage_collect(),
    id(Msgs),
    Recvr ! {continue, self()},
    receive
        {'DOWN', RMon, process, Recvr, Reason} ->
            normal = Reason
    end,
    ok.

receive_tagged_down_msgs(Tag, Msgs) ->
    receive
        {Tag, _Mon, process, _Pid, bang} = Msg ->
            receive_tagged_down_msgs(Tag, [Msg | Msgs])
    after 0 ->
        Msgs
    end.

unexpected_alias_at_demonitor_gh5310(Config) when is_list(Config) ->
    %% The demonitor operation erroneously behaved as if the
    %% monitor had been created using the {alias, explicit_unalias}
    %% option...
    Pid = spawn_link(fun() ->
        receive
            {alias, Alias} ->
                Alias ! {hello_via_alias, self()}
        end
    end),
    Mon = partisan:monitor(process, Pid),
    AliasMon = partisan:monitor(process, Pid, [{alias, reply_demonitor}]),
    partisan:demonitor(AliasMon, [flush]),
    Pid ! {alias, AliasMon},
    receive
        {'DOWN', Mon, process, Pid, normal} ->
            ok
    end,
    receive
        {hello_via_alias, Pid} ->
            ct:fail(unexpected_message_via_alias)
    after 0 ->
        ok
    end.

down_on_alias_gh5310(Config) when is_list(Config) ->
    %% Could only occur when the internal monitor structure was transformed
    %% into an alias structure during the demonitor() operation, the target
    %% terminated before the demonitor signal reached it, and the exit reason
    %% wasn't an immediate. We test with both immediate and compound exit
    %% reason just to make sure we don't introduce the bug in the immediate
    %% case at a later time...
    process_flag(scheduler, 1),
    {DeMonSched, TermSched} =
        case erlang:system_info(schedulers) of
            1 -> {1, 1};
            2 -> {1, 2};
            _ -> {2, 3}
        end,
    lists:foreach(
        fun(N) ->
            ImmedExitReason =
                case N rem 2 of
                    0 -> true;
                    _ -> false
                end,
            down_on_alias_gh5310_test(
                ImmedExitReason,
                DeMonSched,
                TermSched
            )
        end,
        lists:seq(1, 200)
    ),
    ok.

down_on_alias_gh5310_test(ImmedExitReason, DeMonSched, TermSched) ->
    Go = make_ref(),
    Done = make_ref(),
    Parent = self(),
    TermPid = spawn_opt(
        fun() ->
            Parent ! {ready, self()},
            receive
                Go ->
                    if
                        ImmedExitReason == true ->
                            exit(bye);
                        true ->
                            exit(Go)
                    end
            end
        end,
        [{scheduler, TermSched}]
    ),
    DeMonPid = spawn_opt(
        fun() ->
            AliasMon = partisan:monitor(
                process,
                TermPid,
                [{alias, explicit_unalias}]
            ),
            Parent ! {ready, self()},
            receive
                Go -> ok
            end,
            partisan:demonitor(AliasMon, [flush]),
            busy_wait_until(fun() ->
                not is_process_alive(TermPid)
            end),
            receive
                {'DOWN', AliasMon, process, _, _} = DownMsg ->
                    exit({unexpected_msg, DownMsg})
            after 0 ->
                Parent ! Done
            end
        end,
        [{scheduler, DeMonSched}, link]
    ),
    receive
        {ready, TermPid} -> ok
    end,
    receive
        {ready, DeMonPid} -> ok
    end,
    erlang:yield(),
    TermPid ! Go,
    DeMonPid ! Go,
    receive
        Done -> ok
    end.

monitor_3_noproc_gh6185(Config) when is_list(Config) ->
    %% `erts_test_utils' lives in OTP's erts/emulator/test tree, which
    %% `fetch_otp_test_sources.sh' does not pull (it fetches lib/stdlib/test
    %% only), so it is absent from an installed release. Skip rather than
    %% report a spurious `undef' failure.
    case code:ensure_loaded(erts_test_utils) of
        {module, _} ->
            monitor_3_noproc_gh6185_cases();
        {error, _} ->
            {skip,
                "erts_test_utils is not available; see fetch_otp_test_sources.sh"}
    end.

monitor_3_noproc_gh6185_cases() ->
    monitor_3_noproc_gh6185_test(false, false),
    monitor_3_noproc_gh6185_test(true, false),
    monitor_3_noproc_gh6185_test(false, true),
    monitor_3_noproc_gh6185_test(true, true),
    monitor_3_noproc_gh6185_exit_test(false, false),
    monitor_3_noproc_gh6185_exit_test(true, false),
    monitor_3_noproc_gh6185_exit_test(false, true),
    monitor_3_noproc_gh6185_exit_test(true, true).

monitor_3_noproc_gh6185_test(AliasTest, TagTest) ->
    NodeName = node(),
    UN = undefined_name_gh6185,
    UNN = {UN, NodeName},
    undefined = whereis(UN),

    {AliasOpt, CheckAlias} =
        case AliasTest of
            false ->
                {[], fun(_NotAnAlias) -> ok end};
            true ->
                {[{alias, explicit_unalias}], fun(Alias) ->
                    AMsg1 = make_ref(),
                    OMsg1 = make_ref(),
                    Alias ! AMsg1,
                    self() ! OMsg1,
                    receive
                        OMsg1 -> ok
                    end,
                    receive
                        AMsg1 -> ok
                    after 0 -> ct:fail(missing_alias_message)
                    end,
                    unalias(Alias),
                    AMsg2 = make_ref(),
                    OMsg2 = make_ref(),
                    Alias ! AMsg2,
                    self() ! OMsg2,
                    receive
                        OMsg2 -> ok
                    end,
                    receive
                        AMsg2 -> ct:fail(unexpected_alias_message)
                    after 0 -> ok
                    end
                end}
        end,

    TagFun =
        case TagTest of
            false ->
                fun() ->
                    {'DOWN', []}
                end;
            true ->
                fun() ->
                    Tag = make_ref(),
                    {Tag, [{tag, Tag}]}
                end
        end,

    %% not registered process...
    {Tag1, TagOpt1} = TagFun(),
    M1 = partisan:monitor(process, UN, AliasOpt ++ TagOpt1),
    receive
        {Tag1, M1, process, UNN, noproc} ->
            ok;
        ID1 when element(2, ID1) == M1 ->
            ct:fail({invalid_down, ID1})
    after 100 ->
        ct:fail(missing_down)
    end,
    CheckAlias(M1),

    {Tag2, TagOpt2} = TagFun(),
    M2 = partisan:monitor(process, UNN, AliasOpt ++ TagOpt2),
    receive
        {Tag2, M2, process, UNN, noproc} ->
            ok;
        ID2 when element(2, ID2) == M2 ->
            ct:fail({invalid_down, ID2})
    after 100 ->
        ct:fail(missing_down)
    end,
    CheckAlias(M2),

    %% Not registered port...
    {Tag3, TagOpt3} = TagFun(),
    M3 = partisan:monitor(port, UN, AliasOpt ++ TagOpt3),
    receive
        {Tag3, M3, port, UNN, noproc} ->
            ok;
        ID3 when element(2, ID3) == M3 ->
            ct:fail({invalid_down, ID3})
    after 100 ->
        ct:fail(missing_down)
    end,
    CheckAlias(M3),

    {Tag4, TagOpt4} = TagFun(),
    M4 = partisan:monitor(port, UNN, AliasOpt ++ TagOpt4),
    receive
        {Tag4, M4, port, UNN, noproc} ->
            ok;
        ID4 when element(2, ID4) == M4 ->
            ct:fail({invalid_down, ID4})
    after 100 ->
        ct:fail(missing_down)
    end,
    CheckAlias(M4),

    OldCreation =
        case erlang:system_info(creation) of
            Creation when Creation =< 4 -> 16#ffffffff;
            Creation -> Creation - 1
        end,

    %% Process of old incarnation...
    Pid = erts_test_utils:mk_ext_pid({NodeName, OldCreation}, 4711, 17),
    {Tag5, TagOpt5} = TagFun(),
    M5 = partisan:monitor(process, Pid, AliasOpt ++ TagOpt5),
    receive
        {Tag5, M5, process, Pid, noproc} ->
            ok;
        ID5 when element(2, ID5) == M5 ->
            ct:fail({invalid_down, ID5})
    after 100 ->
        ct:fail(missing_down)
    end,
    CheckAlias(M5),

    %% Port of old incarnation...
    Prt = erts_test_utils:mk_ext_port({NodeName, OldCreation}, 4711),
    {Tag6, TagOpt6} = TagFun(),
    M6 = partisan:monitor(port, Prt, AliasOpt ++ TagOpt6),
    receive
        {Tag6, M6, port, Prt, noproc} ->
            ok;
        ID6 when element(2, ID6) == M6 ->
            ct:fail({invalid_down, ID6})
    after 100 ->
        ct:fail(missing_down)
    end,
    CheckAlias(M6),

    ok.

monitor_3_noproc_gh6185_exit_test(AliasTest, TagTest) ->
    %%
    %% Testing that we handle these quite unusual monitors correct
    %% in case the monotoring process dies right after setting up
    %% the monitor. We cannot check any results, but we might hit
    %% asserts, crashes, or memory leaks if any bugs exist...
    %%

    NodeName = node(),
    UN = undefined_name_gh6185,
    UNN = {UN, NodeName},
    undefined = whereis(UN),

    AliasOpt =
        case AliasTest of
            false -> [];
            true -> [{alias, explicit_unalias}]
        end,

    TagOpt =
        case TagTest of
            false -> [];
            true -> [{tag, make_ref()}]
        end,

    %% not registered process...
    {P1, M1} = spawn_monitor(fun() ->
        erlang:yield(),
        _ = partisan:monitor(process, UN, AliasOpt ++ TagOpt),
        exit(bang)
    end),
    receive
        {'DOWN', M1, process, P1, bang} -> ok
    end,
    {P2, M2} = spawn_monitor(fun() ->
        erlang:yield(),
        _ = partisan:monitor(process, UNN, AliasOpt ++ TagOpt),
        exit(bang)
    end),
    receive
        {'DOWN', M2, process, P2, bang} -> ok
    end,

    %% Not registered port...
    {P3, M3} = spawn_monitor(fun() ->
        erlang:yield(),
        _ = partisan:monitor(port, UN, AliasOpt ++ TagOpt),
        exit(bang)
    end),
    receive
        {'DOWN', M3, process, P3, bang} -> ok
    end,
    {P4, M4} = spawn_monitor(fun() ->
        erlang:yield(),
        _ = partisan:monitor(port, UNN, AliasOpt ++ TagOpt),
        exit(bang)
    end),
    receive
        {'DOWN', M4, process, P4, bang} -> ok
    end,

    OldCreation =
        case erlang:system_info(creation) of
            Creation when Creation =< 4 -> 16#ffffffff;
            Creation -> Creation - 1
        end,

    %% Process of old incarnation...
    {P5, M5} = spawn_monitor(fun() ->
        Pid = erts_test_utils:mk_ext_pid(
            {NodeName, OldCreation},
            4711,
            17
        ),
        erlang:yield(),
        _ = partisan:monitor(process, Pid, AliasOpt ++ TagOpt),
        exit(bang)
    end),
    receive
        {'DOWN', M5, process, P5, bang} -> ok
    end,

    %% Port of old incarnation...
    {P6, M6} = spawn_monitor(fun() ->
        Prt = erts_test_utils:mk_ext_port(
            {NodeName, OldCreation},
            4711
        ),
        erlang:yield(),
        _ = partisan:monitor(port, Prt, AliasOpt ++ TagOpt),
        exit(bang)
    end),
    receive
        {'DOWN', M6, process, P6, bang} -> ok
    end,
    ok.

%% =============================================================================
%% CHANNEL / F1 (at-most-once) / F4 (caller GC) REGRESSION TESTS
%%
%% These are whitebox: they read the ETS tables owned by partisan_monitor and
%% inject signals into the monitor server. The table names and row layouts are
%% kept in sync with the ?PROC_MON_OUT / ?NODE_TYPE_MON macros and the
%% #partisan_proc_mon_out{} / #partisan_node_type_mon{} records in
%% partisan_monitor.erl.
%% =============================================================================

-define(PROC_MON_OUT_TAB, partisan_proc_mon_out).
-define(PROC_MON_IN_TAB, partisan_proc_mon_in).
-define(NODE_TYPE_MON_TAB, partisan_node_type_mon).

%% F1 regression gate.
%%
%% Monitoring a remote process on a channel must deliver EXACTLY ONE DOWN,
%% even when the monitored process exits at the same time the node/channel
%% goes down. Two paths can produce that signal for the same reference — the
%% direct delivery and the nodedown/channeldown fabrication — and both claim
%% the proc_mon_out entry atomically, so at most one wins.
exactly_one_down_on_race(Config) when is_list(Config) ->
    {ok, N} = partisan_support_otp:start_node(?FUNCTION_NAME),
    partisan_support:cluster(N),
    timer:sleep(2000),

    %% Monitor several long-lived remote processes on the (default) channel.
    %% Several processes give the race more chances to manifest in one node
    %% lifecycle.
    Ps = [partisan:spawn(N, timer, sleep, [100000]) || _ <- lists:seq(1, 10)],
    Refs = [partisan:monitor(process, P) || P <- Ps],

    %% Each process exits, then we immediately drop the node. This races the
    %% per-process DOWN relayed on the channel against the nodedown-driven
    %% fabrication running on our monitoring server.
    _ = [partisan:exit(P, kill) || P <- Ps],
    ok = partisan_support_otp:stop_node(N),

    %% Every reference must see EXACTLY ONE DOWN.
    Counts = collect_down_counts(Refs, 20000, 2000),
    lists:foreach(
        fun(R) ->
            case maps:get(R, Counts, 0) of
                1 -> ok;
                C -> ct:fail({expected_exactly_one_down, R, C})
            end
        end,
        Refs
    ),
    ok.

%% Channel-bound DOWN.
%%
%% A monitor bound to a channel must fire a DOWN with reason `noconnection'
%% when *that* channel goes down, even if the node stays reachable on other
%% channels. A single-channel test cluster cannot drop one channel without
%% dropping the whole node, so we drive the `channeldown' signal straight
%% into the monitor server (the same message the peer-service manager's
%% channel-down callback would send).
channel_bound_down(Config) when is_list(Config) ->
    {ok, N} = partisan_support_otp:start_node(?FUNCTION_NAME),
    partisan_support:cluster(N),
    timer:sleep(2000),

    P = partisan:spawn(N, timer, sleep, [100000]),
    R = partisan:monitor(process, P),

    %% Bookkeeping for the monitor exists (bound to the default channel).
    [_] = ets:lookup(?PROC_MON_OUT_TAB, R),

    %% The monitor's channel goes down while the node is otherwise still up.
    Server = whereis(partisan_monitor),
    true = is_pid(Server),
    Server ! {channeldown, N, undefined},

    %% Exactly one DOWN with reason noconnection, and the bookkeeping is gone.
    receive
        {'DOWN', R, process, _, noconnection} -> ok
    after 5000 ->
        ct:fail(missing_channel_down)
    end,
    ok = wait_until_true(
        fun() -> ets:lookup(?PROC_MON_OUT_TAB, R) =:= [] end, 5000
    ),
    %% No duplicate for the same reference.
    receive
        {'DOWN', R, process, _, _} = Dup ->
            ct:fail({unexpected_duplicate_down, Dup})
    after 500 ->
        ok
    end,

    ok = partisan_support_otp:stop_node(N),
    ok.

%% F4 regression gate.
%%
%% A local caller that installs monitors and then dies must have all of its
%% monitor state reclaimed by the server (otherwise an unbounded ETS leak and
%% a growing dead-pid fan-out). Here the caller subscribes to node status
%% (node_type_mon) and monitors a remote process (proc_mon_out); after it
%% dies both must be gone.
caller_gc(Config) when is_list(Config) ->
    {ok, N} = partisan_support_otp:start_node(?FUNCTION_NAME),
    partisan_support:cluster(N),
    timer:sleep(2000),

    P = partisan:spawn(N, timer, sleep, [100000]),
    Self = self(),

    %% Baseline count of proc_mon_in rows on the *monitored* node N, before we
    %% install our monitor. Used to prove the remote half is reclaimed too.
    InBefore = count_proc_mon_in_on(N),

    Caller = spawn(fun() ->
        ok = partisan:monitor_nodes(true, []),
        _ = partisan:monitor(process, P),
        Self ! {ready, self()},
        receive
            stop -> ok
        end
    end),
    receive
        {ready, Caller} -> ok
    after 5000 ->
        ct:fail(caller_not_ready)
    end,

    %% The caller's local rows are present (and the server has had time to
    %% install its own monitor on the caller via the monitor_caller cast), AND
    %% the monitored node N has installed the companion proc_mon_in row + its
    %% native erlang:monitor.
    ok = wait_until_true(
        fun() ->
            count_node_type_mon(Caller) >= 1 andalso
                count_proc_mon_out(Caller) >= 1 andalso
                count_proc_mon_in_on(N) > InBefore
        end,
        5000
    ),

    %% Caller dies.
    exit(Caller, kill),

    %% The server reclaims every row the caller owned locally AND — the B1
    %% remote-leak fix — asks N to drop its half, so N's proc_mon_in row and
    %% native monitor are released. Without the remote demonitor in
    %% purge_caller/1, count_proc_mon_in_on(N) would stay above the baseline
    %% forever and this wait would time out.
    ok = wait_until_true(
        fun() ->
            count_node_type_mon(Caller) =:= 0 andalso
                count_proc_mon_out(Caller) =:= 0 andalso
                count_proc_mon_in_on(N) =:= InBefore
        end,
        5000
    ),

    ok = partisan_support_otp:stop_node(N),
    ok.

%% Count node_type_mon rows owned by Caller.
%% Row layout: {partisan_node_type_mon, {Pid, Hash}, NodeType, NodedownReason}.
count_node_type_mon(Caller) ->
    ets:select_count(?NODE_TYPE_MON_TAB, [
        {{partisan_node_type_mon, {Caller, '_'}, '_', '_'}, [], [true]}
    ]).

%% Count proc_mon_out rows owned by Caller.
%% Row layout: {partisan_proc_mon_out, Ref, Monitored, Monitor, Channel, Tag}.
%% The arity must track the record in `partisan_monitor' — a short pattern
%% matches nothing and silently reports zero rows for every caller.
count_proc_mon_out(Caller) ->
    ets:select_count(?PROC_MON_OUT_TAB, [
        {{partisan_proc_mon_out, '_', '_', Caller, '_', '_'}, [], [true]}
    ]).

%% Count proc_mon_in rows on a remote (monitored) node. Used to prove the
%% remote half of a monitor is reclaimed when the local caller dies.
count_proc_mon_in_on(Node) ->
    case rpc:call(Node, ets, info, [?PROC_MON_IN_TAB, size]) of
        Size when is_integer(Size) -> Size;
        _ -> 0
    end.

%% Bounded poll until Fun() returns true; fails on timeout.
wait_until_true(Fun, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_until_true_loop(Fun, Deadline).

wait_until_true_loop(Fun, Deadline) ->
    case (catch Fun()) of
        true ->
            ok;
        _ ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    timer:sleep(100),
                    wait_until_true_loop(Fun, Deadline);
                false ->
                    ct:fail(wait_until_true_timeout)
            end
    end.

%% Tally DOWN messages per reference. Wait up to FirstTimeout ms for every
%% reference to see at least one DOWN, then drain an extra Grace ms to catch
%% any duplicate. Returns a map Ref => Count.
collect_down_counts(Refs, FirstTimeout, Grace) ->
    RefSet = maps:from_list([{R, true} || R <- Refs]),
    Init = maps:from_list([{R, 0} || R <- Refs]),
    Deadline = erlang:monotonic_time(millisecond) + FirstTimeout,
    Counts1 = collect_until_all(RefSet, Init, length(Refs), Deadline),
    GraceDeadline = erlang:monotonic_time(millisecond) + Grace,
    collect_grace(RefSet, Counts1, GraceDeadline).

collect_until_all(RefSet, Counts, NRefs, Deadline) ->
    Seen = length([1 || {_, C} <- maps:to_list(Counts), C >= 1]),
    case Seen >= NRefs of
        true ->
            Counts;
        false ->
            Remaining = Deadline - erlang:monotonic_time(millisecond),
            case Remaining =< 0 of
                true ->
                    Counts;
                false ->
                    receive
                        {'DOWN', Ref, process, _, _} when
                            is_map_key(Ref, RefSet)
                        ->
                            collect_until_all(
                                RefSet,
                                maps:update_with(
                                    Ref, fun(C) -> C + 1 end, Counts
                                ),
                                NRefs,
                                Deadline
                            )
                    after Remaining ->
                        Counts
                    end
            end
    end.

collect_grace(RefSet, Counts, Deadline) ->
    Remaining = Deadline - erlang:monotonic_time(millisecond),
    case Remaining =< 0 of
        true ->
            Counts;
        false ->
            receive
                {'DOWN', Ref, process, _, _} when is_map_key(Ref, RefSet) ->
                    collect_grace(
                        RefSet,
                        maps:update_with(Ref, fun(C) -> C + 1 end, Counts),
                        Deadline
                    )
            after Remaining ->
                Counts
            end
    end.

%%
%% ...
%%

id(X) -> X.

busy_wait_until(Fun) ->
    case catch Fun() of
        true ->
            ok;
        _ ->
            busy_wait_until(Fun)
    end.

wait_until(Fun) ->
    case catch Fun() of
        true ->
            ok;
        _ ->
            receive
            after 100 -> ok
            end,
            wait_until(Fun)
    end.

wait_for_m(_, _, 0) ->
    exit(monitor_wait_timeout);
wait_for_m(Monitors, MonitoredBy, N) ->
    {M0, MB0} = monitors(),
    case lists:sort(M0) of
        Monitors ->
            case lists:sort(MB0) of
                MonitoredBy ->
                    ok;
                _ ->
                    receive
                    after 100 -> ok
                    end,
                    wait_for_m(Monitors, MonitoredBy, N - 1)
            end;
        _ ->
            receive
            after 100 -> ok
            end,
            wait_for_m(Monitors, MonitoredBy, N - 1)
    end.

% All permutations of a list...
perm([]) ->
    [];
perm([X]) ->
    [[X]];
perm(List) ->
    perm([], List, []).

perm(_, [], Acc) ->
    Acc;
perm(Pre, [El | Post], Acc) ->
    Res = [[El | X] || X <- perm(Pre ++ Post)],
    perm(Pre ++ [El], Post, Res ++ Acc).

%%% Our butler for named process monitor tests

jeeves(Parent, Name, Ref) ->
    %% when is_pid(Parent), (is_atom(Name) or (Name =:= [])), is_reference(Ref) ->
    %%io:format("monitor_SUITE:jeeves(~p, ~p)~n", [Parent, Name]),
    case Name of
        Atom when is_atom(Atom) ->
            register(Name, self());
        [] ->
            ok
    end,
    partisan:send(Parent, {partisan:self(), Ref}),
    jeeves_loop(Parent).

jeeves_loop(Parent) ->
    %% `Parent' is a partisan remote reference, not a pid, so replies go
    %% through `partisan:send/2' and identify this process with
    %% `partisan:self/0' — the form the caller matches on. A raw `!' to a
    %% reference raises `badarg'.
    receive
        {Parent, monitors} ->
            reply(Parent, {monitors, monitors()}),
            jeeves_loop(Parent);
        {Parent, {monitor_process, P}} ->
            reply(
                Parent, {monitor_process, catch partisan:monitor(process, P)}
            ),
            jeeves_loop(Parent);
        {Parent, {demonitor, Ref}} ->
            reply(Parent, {demonitor, catch partisan:demonitor(Ref)}),
            jeeves_loop(Parent);
        {Parent, stop} ->
            ok;
        {Parent, {exit, Reason}} ->
            exit(Reason);
        Other ->
            io:format("~p:jeeves_loop received ~p~n", [?MODULE, Other])
    end.

start_jeeves({Name, Node}) when
    (is_atom(Name) or (Name =:= [])), is_atom(Node)
->
    Parent = partisan:self(),
    Ref = partisan:make_ref(),
    Pid = partisan:spawn(Node, fun() -> jeeves(Parent, Name, Ref) end),
    receive
        {Pid, Ref} ->
            ok;
        Other ->
            ct:fail({rec, Other})
    end,
    Pid;
start_jeeves(Name) when is_atom(Name) ->
    start_jeeves({Name, partisan:node()}).

reply(Parent, Response) ->
    partisan:send(Parent, {partisan:self(), Response}).

tell_jeeves(Pid, What) ->
    partisan:send(Pid, {partisan:self(), What}).

ask_jeeves(Pid, Request) ->
    partisan:send(Pid, {partisan:self(), Request}),
    receive
        {Pid, Response} ->
            Response;
        Other ->
            ct:fail({rec, Other})
    end.

expect_jeeves(Pid, Request, Response) ->
    partisan:send(Pid, {partisan:self(), Request}),
    receive
        {Pid, Response} ->
            ok;
        Other ->
            ct:fail({rec, Other})
    end.

%% Whether monitor bookkeeping is reclaimed cannot be read from
%% `erlang:process_info(_, monitors | monitored_by)' alone: Partisan registers
%% a native monitor when the target is local, but a monitor on a *remote*
%% process lives in `partisan_monitor''s own tables and never appears in
%% `process_info/2'. So we report the union of both mechanisms in a single
%% vocabulary — remote references, the form `partisan:spawn/2' and
%% `partisan:self/0' hand back. A `{[], []}' assertion therefore means
%% "nothing leaked in either bookkeeping".
monitors() ->
    monitors(self()).

monitors(Pid) when is_pid(Pid) ->
    {monitors, Monitors} = process_info(Pid, monitors),
    {monitored_by, MonitoredBy} = process_info(Pid, monitored_by),
    Srv = whereis(partisan_monitor),
    {
        [normalise_monitor(M) || M <- Monitors] ++ proc_mon_out(Pid),
        %% `partisan_monitor' natively monitors both the local callers of
        %% `partisan:monitor/2' (so it can reclaim their rows) and the local
        %% targets of a remote monitor (so it can forward the DOWN). Neither is
        %% a monitor the test asked for; the second is reported instead by
        %% `proc_mon_in/1', naming the remote process that actually holds it.
        [pref(M) || M <- MonitoredBy, M =/= Srv] ++ proc_mon_in(Pid)
    }.

%% A DOWN for a local target is delivered by `erlang:monitor/2' and names the
%% raw pid; one for a remote target is fabricated by `partisan_monitor' and
%% names the remote reference. Accept whichever form applies to `P'.
expect_down_from(P) ->
    Raw = raw(P),
    receive
        {'DOWN', _, process, D, _} when D == P; D == Raw ->
            ok
    end.

raw(RemoteRef) ->
    case partisan_remote_ref:is_local(RemoteRef) of
        true -> partisan_remote_ref:to_term(RemoteRef);
        false -> RemoteRef
    end.

normalise_monitor({process, Target}) ->
    {process, pref(Target)};
normalise_monitor(Other) ->
    Other.

%% Canonical remote-reference form of a monitor target, so the two bookkeepings
%% compare equal: `process_info/2' names local targets as a pid or
%% `{Name, Node}', `partisan_monitor' holds them already encoded.
pref(Pid) when is_pid(Pid) ->
    partisan_remote_ref:from_term(Pid);
pref({Name, Node}) when is_atom(Name), is_atom(Node) ->
    partisan_remote_ref:from_term(Name, Node);
pref(Name) when is_atom(Name) ->
    partisan_remote_ref:from_term(Name, node());
pref(RemoteRef) ->
    RemoteRef.

%% Remote processes `Pid' monitors. Tuple shape is
%% `#partisan_proc_mon_out{ref, monitored, monitor, channel, tag}' — see
%% `partisan_monitor'. A field reorder makes these selects return nothing,
%% which fails the assertions rather than passing them silently.
proc_mon_out(Pid) ->
    select(partisan_proc_mon_out, [
        {
            {partisan_proc_mon_out, '_', '$1', Pid, '_', '_'},
            [],
            [{{process, '$1'}}]
        }
    ]).

%% Remote processes monitoring `Pid'. The monitored side is recorded as the pid
%% or, for a monitor established by name, the registered name.
proc_mon_in(Pid) ->
    Keys =
        case process_info(Pid, registered_name) of
            {registered_name, Name} when is_atom(Name) -> [Pid, Name];
            _ -> [Pid]
        end,
    select(
        partisan_proc_mon_in,
        [
            {{partisan_proc_mon_in, '_', Key, '$1', '_'}, [], ['$1']}
         || Key <- Keys
        ]
    ).

select(Tab, MatchSpec) ->
    case ets:whereis(Tab) of
        undefined -> [];
        Ref -> ets:select(Ref, MatchSpec)
    end.

generate(_Fun, 0) ->
    [];
generate(Fun, N) ->
    [Fun() | generate(Fun, N - 1)].

%% `Args' is a string of extra emulator flags, appended to the `args' string
%% inside `node_config' — not to the proplist itself, which would make it
%% improper and drop the flags. Returns `{ok, Node}'.
start_node(Config, Args) ->
    TestCase = proplists:get_value(testcase, Config),
    PA = filename:dirname(code:which(?MODULE)),
    ESTime = erlang:monotonic_time(1) + erlang:time_offset(1),
    Unique = erlang:unique_integer([positive]),
    Name = list_to_atom(
        atom_to_list(?MODULE) ++
            "-" ++
            atom_to_list(TestCase) ++
            "-" ++
            integer_to_list(ESTime) ++
            "-" ++
            integer_to_list(Unique)
    ),
    {ok, Node} = partisan_support_otp:start_node(Name, [
        {node_config, [{args, "-pa " ++ PA ++ " " ++ Args}]}
    ]),
    partisan_support:cluster(Node),
    timer:sleep(2000),
    {ok, Node}.

stop_node(Node) ->
    partisan_support_otp:stop_node(Node).
