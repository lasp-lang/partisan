%% -------------------------------------------------------------------
%%
%% Copyright (c) 2026 Alejandro M. Ramallo.  All Rights Reserved.
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
%% @doc Generates partisan-adapted versions of OTP test suites.
%%
%% Takes an OTP test suite source file (e.g., gen_server_SUITE.erl) and
%% applies the same mechanical AST rewrites used by partisan_otp_rewrite
%% to produce a partisan test suite (e.g., partisan_gen_server_SUITE.erl)
%% that tests the generated partisan modules.
%%
%% Helper modules (oc_server.erl, supervisor_1.erl, etc.) are also
%% transformed so they use partisan behaviours and calls.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_otp_test_gen).

-export([
    generate_suite/2,
    generate_helper/2,
    generate_rewritten_helper/2,
    generate_all_suites/1,
    otp_src_dir/0,
    peer_opts/1
]).


%% =============================================================================
%% API
%% =============================================================================


%% @doc Generate an adapted test suite from an OTP source file.
%% SourceFile is the path to the OTP SUITE .erl file.
%% OutDir is the directory to write the generated .beam file.
%% Returns {ok, NewModule} or {error, Reason}.
-spec generate_suite(file:filename(), file:filename()) ->
    {ok, module()} | {error, term()}.
generate_suite(SourceFile, OutDir) ->
    generate_module(SourceFile, OutDir, fun suite_module_name/1).


%% @doc Generate an adapted helper module from an OTP test helper source file.
%% Renames the module with a partisan_ prefix.
-spec generate_helper(file:filename(), file:filename()) ->
    {ok, module()} | {error, term()}.
generate_helper(SourceFile, OutDir) ->
    generate_module(SourceFile, OutDir, fun helper_module_name/1).


%% @doc Generate an adapted helper, keeping the original module name.
%% Applies the partisan rewrite (gen_server→partisan_gen_server, etc.)
%% but keeps the module name unchanged so existing references work.
-spec generate_rewritten_helper(file:filename(), file:filename()) ->
    {ok, module()} | {error, term()}.
generate_rewritten_helper(SourceFile, OutDir) ->
    generate_module(SourceFile, OutDir, fun(Name) -> Name end).


%% @doc Generate all test suites and helpers for the current OTP version.
%% Generates adapted SUITE beams and sets up data_dir directories with
%% the original OTP helper source files (callback modules don't need
%% renaming — they work as-is with partisan gen_server/supervisor).
-spec generate_all_suites(file:filename()) -> ok | {error, term()}.
generate_all_suites(OutDir) ->
    SrcDir = otp_src_dir(),
    TestDir = filename:join(SrcDir, "test"),

    Suites = [
        "gen_server_SUITE.erl",
        "supervisor_SUITE.erl",
        "gen_statem_SUITE.erl",
        "gen_event_SUITE.erl",
        "proc_lib_SUITE.erl",
        "sys_SUITE.erl"
    ],

    ok = filelib:ensure_dir(filename:join(OutDir, "dummy")),

    %% Generate adapted SUITE beams.
    SuiteResults = [{F, generate_suite(filename:join(TestDir, F), OutDir)}
                    || F <- Suites,
                       filelib:is_file(filename:join(TestDir, F))],

    %% Copy helper source files into data_dir directories.
    %% CT computes data_dir from the suite name: partisan_gen_server_SUITE_data/
    %% The helpers keep their ORIGINAL names since they are plain callback
    %% modules that don't need partisan-specific changes.
    ok = setup_data_dirs(TestDir, OutDir),

    %% Also compile and place standalone helper modules (supervisor_1, sys_sp1, etc.)
    %% that are referenced by the suites but not in data_dir.
    ok = compile_standalone_helpers(TestDir, OutDir),

    Errors = [{F, E} || {F, {error, E}} <- SuiteResults],
    Oks = [M || {_, {ok, M}} <- SuiteResults],

    case Errors of
        [] ->
            io:format("Generated ~p adapted test suites: ~p~n",
                      [length(Oks), Oks]),
            ok;
        _ ->
            io:format(standard_error,
                      "Errors generating test suites:~n~p~n", [Errors]),
            {error, Errors}
    end.


%% @doc Inject code path arguments into peer node options.
%% Called at runtime by the adapted test suites when starting peer nodes
%% via ?CT_PEER(). Ensures the peer node has partisan modules available.
%%
%% test_server:start_peer accepts either:
%%   - A list of arg strings (e.g., ["-pa", "/path"])
%%   - A map #{args => [...], name => ...}
%% ?CT_PEER() passes [] (empty args list).
%% ?CT_PEER(#{args => [...]}) passes a map.
-spec peer_opts(list() | map()) -> list() | map().
peer_opts(Opts) ->
    %% Build -pa args for all current code paths (absolute).
    PaArgs = lists:flatmap(
        fun(P) -> ["-pa", filename:absname(P)] end,
        lists:filter(fun filelib:is_dir/1, code:get_path())
    ),
    case Opts of
        M when is_map(M) ->
            ExistingArgs = maps:get(args, M, []),
            M#{args => PaArgs ++ ExistingArgs};
        L when is_list(L) ->
            %% L is a list of arg strings, merge with our paths.
            PaArgs ++ L
    end.


%% @doc Path to the OTP source directory for the running OTP version.
otp_src_dir() ->
    Vsn = erlang:system_info(otp_release),
    %% Try versioned dirs, fall back to generic
    Candidates = [
        filename:join(["otp_src", "otp_" ++ Vsn]),
        filename:join(["otp_src", "otp_" ++ Vsn ++ ".*"])
    ],
    case find_existing_dir(Candidates) of
        {ok, Dir} -> Dir;
        error ->
            %% Try with wildcard
            Pattern = "otp_src/otp_" ++ Vsn ++ "*",
            case filelib:wildcard(Pattern) of
                [Dir | _] -> Dir;
                [] -> error({otp_src_not_found, Vsn, Candidates})
            end
    end.


%% =============================================================================
%% Internal
%% =============================================================================


generate_module(SourceFile, OutDir, NameFun) ->
    case parse_source(SourceFile) of
        {ok, Forms} ->
            %% Get the original module name from the forms.
            OrigModule = get_module_name(Forms),
            NewModule = NameFun(OrigModule),

            %% For test suites and helpers, we can't use
            %% partisan_otp_rewrite:transform/2 directly because the
            %% module name (e.g., gen_server_SUITE) isn't in the rename
            %% map. Instead, we use a two-pass approach:
            %% 1. Rename the module to the new name first
            %% 2. Then use transform/2 with a dummy key that maps to itself
            %%    — but that won't work either. Instead, we add the test
            %%    module to the rename map temporarily by putting the forms
            %%    through with a known OTP module key set to the new name.
            %%
            %% Simplest correct approach: rename module first, then apply
            %% the rewrite using gen_server as the OrigModule key (it doesn't
            %% matter which OTP module we use — the rewrite engine only uses
            %% OrigModule for the module attribute rename, and we override
            %% that afterward).
            RenamedForms = rename_module(Forms, NewModule),

            %% Pick any OTP module key to drive the rewrite. The module
            %% attribute will be rewritten to the partisan name, but we've
            %% already set it to NewModule, and the rewrite's module attribute
            %% handler will overwrite it. We'll fix it again after.
            RewrittenForms = partisan_otp_rewrite:transform(gen_server, RenamedForms),

            %% The rewrite renamed the module to partisan_gen_server.
            %% Fix it back to our desired NewModule.
            RewrittenForms2 = rename_module(RewrittenForms, NewModule),

            %% Rename expanded ?MODULE atoms.
            %% When epp parsed the source, ?MODULE was expanded to the
            %% original name (e.g., gen_server_SUITE).
            RewrittenForms3 = lists:foldl(
                fun({Old, New}, Acc) -> rename_atom(Acc, Old, New) end,
                RewrittenForms2,
                [{OrigModule, NewModule} | test_helper_renames()]
            ),

            %% Undo the rpc→partisan_rpc rename for test suites.
            %% OTP test suites use rpc:call to talk to peer nodes via
            %% standard disterl.
            RewrittenForms4 = rename_atom(RewrittenForms3,
                                          partisan_rpc, rpc),

            %% Replace test_server:start_peer calls with our wrapper
            %% that injects code paths so peer nodes have partisan.
            RewrittenForms4b = replace_start_peer(RewrittenForms4),

            %% Strip ts_install_cth from suite/0 — it's OTP's internal
            %% CT hook that isn't available outside the OTP test framework.
            FinalForms = strip_ts_install_cth(RewrittenForms4b),

            %% Compile to beam.
            case compile:forms(FinalForms,
                               [binary, return_errors, debug_info,
                                {d, 'PARTISAN_TEST'}]) of
                {ok, NewModule, Binary} ->
                    write_beam(OutDir, NewModule, Binary);
                {ok, NewModule, Binary, _Warnings} ->
                    write_beam(OutDir, NewModule, Binary);
                {error, Errors, _Warnings} ->
                    {error, {compile_errors, SourceFile, Errors}}
            end;
        {error, _} = Err ->
            Err
    end.


parse_source(File) ->
    StdlibDir = code:lib_dir(stdlib),
    KernelDir = code:lib_dir(kernel),
    CTDir = code:lib_dir(common_test),
    epp:parse_file(File, [
        {includes, [
            filename:dirname(File),
            filename:join(StdlibDir, "include"),
            filename:join(KernelDir, "include"),
            filename:join(CTDir, "include")
        ]}
    ]).


get_module_name([{attribute, _, module, Mod} | _]) -> Mod;
get_module_name([_ | Rest]) -> get_module_name(Rest);
get_module_name([]) -> error(no_module_attribute).


rename_module([{attribute, Anno, module, _Old} | Rest], NewName) ->
    [{attribute, Anno, module, NewName} | Rest];
rename_module([Form | Rest], NewName) ->
    [Form | rename_module(Rest, NewName)];
rename_module([], _) ->
    [].


%% Renames for helper modules referenced by the test suites.
%% NOTE: Callback helper modules (oc_server, format_status_server, etc.)
%% are NOT renamed. They are plain callback modules that export init/1,
%% handle_call/3, etc. — they work as-is with partisan_gen_server.
%% Only the SUITE module names change (via the ?MODULE rename).
test_helper_renames() ->
    [].


%% Strip the ts_install_cth CT hook from suite/0 return value.
%% Replace test_server:start_peer(Opts, Mod, Fun) calls so peer nodes
%% get the code paths needed to load partisan modules.
%% Wraps Opts with partisan_otp_test_gen:peer_opts(Opts).
replace_start_peer(Forms) ->
    [replace_start_peer_form(F) || F <- Forms].

replace_start_peer_form({function, Anno, Name, Arity, Clauses}) ->
    {function, Anno, Name, Arity,
     [replace_start_peer_clause(C) || C <- Clauses]};
replace_start_peer_form(Other) ->
    Other.

replace_start_peer_clause({clause, Anno, Pats, Guards, Body}) ->
    {clause, Anno, Pats, Guards,
     [replace_start_peer_expr(E) || E <- Body]}.

%% Match: test_server:start_peer(Opts, Mod, Fun) →
%%        test_server:start_peer(partisan_otp_test_gen:peer_opts(Opts), Mod, Fun)
replace_start_peer_expr(
    {call, Anno,
     {remote, Anno2,
      {atom, Anno3, test_server},
      {atom, Anno4, start_peer}},
     [Opts | RestArgs]}) ->
    WrappedOpts = {call, Anno,
        {remote, Anno, {atom, Anno, partisan_otp_test_gen},
                       {atom, Anno, peer_opts}},
        [Opts]},
    {call, Anno,
     {remote, Anno2,
      {atom, Anno3, test_server},
      {atom, Anno4, start_peer}},
     [WrappedOpts | RestArgs]};
%% Recurse into compound expressions
replace_start_peer_expr({'case', Anno, Expr, Clauses}) ->
    {'case', Anno, replace_start_peer_expr(Expr),
     [replace_start_peer_clause(C) || C <- Clauses]};
replace_start_peer_expr({'try', Anno, Body, Cases, Catches, After}) ->
    {'try', Anno,
     [replace_start_peer_expr(E) || E <- Body],
     [replace_start_peer_clause(C) || C <- Cases],
     [replace_start_peer_clause(C) || C <- Catches],
     [replace_start_peer_expr(E) || E <- After]};
replace_start_peer_expr({block, Anno, Body}) ->
    {block, Anno, [replace_start_peer_expr(E) || E <- Body]};
replace_start_peer_expr({match, Anno, P, E}) ->
    {match, Anno, P, replace_start_peer_expr(E)};
replace_start_peer_expr(Other) ->
    Other.


%% OTP test suites reference this internal hook which isn't available
%% outside OTP's test framework. We remove the {ct_hooks, ...} tuple
%% from the suite/0 return list.
strip_ts_install_cth(Forms) ->
    lists:map(fun(Form) -> strip_cth_form(Form) end, Forms).

strip_cth_form({function, Anno, suite, 0, Clauses}) ->
    NewClauses = [strip_cth_clause(C) || C <- Clauses],
    {function, Anno, suite, 0, NewClauses};
strip_cth_form(Other) ->
    Other.

strip_cth_clause({clause, Anno, Pats, Guards, Body}) ->
    NewBody = [strip_cth_expr(E) || E <- Body],
    {clause, Anno, Pats, Guards, NewBody}.

%% Remove {ct_hooks, [ts_install_cth]} from a list (cons cells).
strip_cth_expr({cons, _, {tuple, _, [{atom, _, ct_hooks} | _]}, Tail}) ->
    strip_cth_expr(Tail);
strip_cth_expr({cons, Anno, Head, Tail}) ->
    {cons, Anno, Head, strip_cth_expr(Tail)};
strip_cth_expr(Other) ->
    Other.


%% Replace all occurrences of an atom in the AST.
%% Used to fix expanded ?MODULE references after renaming.
rename_atom(Forms, OldAtom, NewAtom) when is_list(Forms) ->
    [rename_atom(F, OldAtom, NewAtom) || F <- Forms];
rename_atom({atom, Anno, OldAtom}, OldAtom, NewAtom) ->
    {atom, Anno, NewAtom};
rename_atom(Tuple, OldAtom, NewAtom) when is_tuple(Tuple) ->
    list_to_tuple([rename_atom(E, OldAtom, NewAtom)
                   || E <- tuple_to_list(Tuple)]);
rename_atom(Other, _, _) ->
    Other.


%% Suite names: gen_server_SUITE → partisan_otp_gen_server_SUITE
%% Prefix with "otp_" to avoid collision with existing partisan-specific
%% test suites (test/partisan_gen_server_SUITE.erl, etc.).
suite_module_name(gen_server_SUITE) -> partisan_otp_gen_server_SUITE;
suite_module_name(supervisor_SUITE) -> partisan_otp_supervisor_SUITE;
suite_module_name(gen_statem_SUITE) -> partisan_otp_gen_statem_SUITE;
suite_module_name(gen_event_SUITE) -> partisan_otp_gen_event_SUITE;
suite_module_name(proc_lib_SUITE) -> partisan_otp_proc_lib_SUITE;
suite_module_name(sys_SUITE) -> partisan_otp_sys_SUITE;
suite_module_name(Other) ->
    list_to_atom("partisan_otp_" ++ atom_to_list(Other)).


%% Helper modules: keep the same name but prefix with partisan_
%% (some helpers like oc_server, supervisor_1, etc.)
helper_module_name(Mod) ->
    list_to_atom("partisan_" ++ atom_to_list(Mod)).


write_beam(OutDir, Module, Binary) ->
    File = filename:join(OutDir, atom_to_list(Module) ++ ".beam"),
    case file:write_file(File, Binary) of
        ok -> {ok, Module};
        {error, Reason} -> {error, {write_failed, File, Reason}}
    end.


%% Copy OTP helper source files into the data_dir directories that CT expects.
%% For partisan_gen_server_SUITE, data_dir = partisan_gen_server_SUITE_data/
setup_data_dirs(TestDir, OutDir) ->
    DataDirMappings = [
        %% {OrigDataDir, NewDataDir, Files}
        {"gen_server_SUITE_data",
         "partisan_otp_gen_server_SUITE_data",
         ["oc_server.erl", "format_status_server.erl"]},
        {"gen_statem_SUITE_data",
         "partisan_otp_gen_statem_SUITE_data",
         ["oc_statem.erl", "format_status_statem.erl"]},
        {"gen_event_SUITE_data",
         "partisan_otp_gen_event_SUITE_data",
         ["oc_event.erl"]}
    ],
    lists:foreach(
        fun({OrigSubDir, NewSubDir, Files}) ->
            DestDir = filename:join(OutDir, NewSubDir),
            ok = filelib:ensure_dir(filename:join(DestDir, "dummy")),
            lists:foreach(
                fun(File) ->
                    Src = filename:join([TestDir, OrigSubDir, File]),
                    Dst = filename:join(DestDir, File),
                    case filelib:is_file(Src) of
                        true -> {ok, _} = file:copy(Src, Dst);
                        false -> ok
                    end
                end, Files)
        end, DataDirMappings),
    ok.


%% Compile standalone helper modules (supervisor_1.erl, sys_sp1.erl, etc.)
%% that are referenced by the suites but live alongside them, not in data_dir.
%% These helpers use gen_server:start_link etc., so they need the partisan
%% rewrite applied. We keep the original module names since the SUITE code
%% references them by their original names.
compile_standalone_helpers(TestDir, OutDir) ->
    Helpers = [
        "supervisor_1.erl",
        "supervisor_2.erl",
        "supervisor_3.erl",
        "supervisor_4.erl",
        "supervisor_deadlock.erl",
        "sys_sp1.erl",
        "sys_sp2.erl",
        "dummy_h.erl",
        "dummy1_h.erl",
        "dummy_via.erl",
        "error_logger_forwarder.erl"
    ],
    lists:foreach(
        fun(File) ->
            Src = filename:join(TestDir, File),
            case filelib:is_file(Src) of
                true ->
                    %% Apply partisan rewrite to the helper but keep
                    %% the original module name (SUITE references it).
                    case generate_rewritten_helper(Src, OutDir) of
                        {ok, _Mod} -> ok;
                        {error, Reason} ->
                            io:format("Warning: failed to compile ~s: ~p~n",
                                      [File, Reason])
                    end;
                false ->
                    ok
            end
        end, Helpers),
    ok.


find_existing_dir([]) -> error;
find_existing_dir([Dir | Rest]) ->
    case filelib:is_dir(Dir) of
        true -> {ok, Dir};
        false -> find_existing_dir(Rest)
    end.
