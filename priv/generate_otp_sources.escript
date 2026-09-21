#!/usr/bin/env escript
%% -*- erlang -*-

%% -----------------------------------------------------------------------------
%% Pre-compile generation of partisan OTP module BEAM files.
%%
%% Why a pre-compile (and not a post-compile) hook?
%% ------------------------------------------------
%% When partisan and another partisan-using dep (e.g. plum_db) are both
%% direct dependencies of a top-level project (e.g. bondy), rebar3 may
%% start compiling plum_db before partisan's `post_hooks compile' has
%% had a chance to fire. plum_db then fails because
%% `partisan_gen_supervisor' does not yet exist.
%%
%% Why .beam (and not .erl source)?
%% --------------------------------
%% `erl_pp:form/1' silently strips the `erlang:' prefix from auto-
%% imported BIF calls (e.g. `erlang:spawn_opt' becomes `spawn_opt'). In
%% the rewritten `partisan_proc_lib' there are LOCAL functions named
%% `spawn_opt/2,3,4,5', so a bare `spawn_opt(...)' call resolves to the
%% local function — causing infinite recursion. Compiling forms
%% directly to .beam preserves the AST exactly and avoids the issue.
%%
%% The support modules (`partisan_otp_modules', `partisan_otp_rewrite',
%% `partisan_otp_patches', `partisan_gen_transform') have no inter-
%% include dependencies, so we compile them in-memory here and load
%% them before invoking the generator.
%% -----------------------------------------------------------------------------

main(_Args) ->
    SrcDir = "src",
    EbinDir = ebin_dir(),

    case filelib:is_dir(SrcDir) of
        true -> ok;
        false ->
            io:format(standard_error,
                "generate_otp_sources.escript: '~s' not found; "
                "expected to run from partisan's root directory.~n",
                [SrcDir]),
            halt(1)
    end,

    SupportModules = [
        "partisan_otp_modules.erl",
        "partisan_otp_rewrite.erl",
        "partisan_otp_patches.erl",
        "partisan_gen_transform.erl"
    ],

    lists:foreach(fun(File) -> compile_and_load(File, SrcDir) end,
                  SupportModules),

    ok = filelib:ensure_dir(filename:join(EbinDir, "dummy")),

    case partisan_gen_transform:generate_all(EbinDir) of
        ok ->
            Modules = partisan_gen_transform:modules(),
            io:format(
                "Generated ~p partisan OTP module beams into ~s/~n",
                [length(Modules), EbinDir]
            ),
            update_app_src(SrcDir),
            ok;
        {error, Errors} ->
            io:format(standard_error,
                "Failed to generate partisan OTP module beams:~n~p~n",
                [Errors]),
            halt(1)
    end.


%% Locate the ebin directory for partisan. Honours rebar3's
%% `REBAR_BUILD_DIR' env var when running as a dep, falling back to a
%% sensible local path when running from partisan's own repo.
ebin_dir() ->
    case os:getenv("REBAR_BUILD_DIR") of
        false ->
            filename:absname("ebin");
        BuildDir ->
            CheckoutsDir = filename:join(
                [BuildDir, "checkouts", "partisan", "ebin"]
            ),
            LibDir = filename:join(
                [BuildDir, "lib", "partisan", "ebin"]
            ),
            case filelib:is_dir(CheckoutsDir) of
                true -> CheckoutsDir;
                false -> LibDir
            end
    end.


%% Update partisan.app.src so the generated module names appear in the
%% modules list. rebar3 reads .app.src to produce the .app file in
%% ebin. Without this step, releases assembled in embedded mode cannot
%% load the generated modules.
update_app_src(SrcDir) ->
    AppSrcFile = filename:join(SrcDir, "partisan.app.src"),
    case file:consult(AppSrcFile) of
        {ok, [{application, partisan, Props}]} ->
            ExistingModules = proplists:get_value(modules, Props, []),
            GenModules = partisan_otp_modules:partisan_modules(),
            AllModules = lists:usort(ExistingModules ++ GenModules),
            case AllModules of
                ExistingModules ->
                    %% No change — leave the file's mtime alone so
                    %% rebar3's incremental build is happy.
                    ok;
                _ ->
                    NewProps = lists:keystore(
                        modules, 1, Props, {modules, AllModules}
                    ),
                    Content = io_lib:format(
                        "~p.~n", [{application, partisan, NewProps}]
                    ),
                    ok = file:write_file(AppSrcFile, Content)
            end;
        _ ->
            ok
    end.


compile_and_load(File, SrcDir) ->
    Path = filename:join(SrcDir, File),
    case compile:file(Path, [binary, return_errors, debug_info]) of
        {ok, Mod, Bin} ->
            load_or_fail(Mod, Path, Bin, File);
        {ok, Mod, Bin, _Warnings} ->
            load_or_fail(Mod, Path, Bin, File);
        {error, Errors, Warnings} ->
            fail("compiling", File, {Errors, Warnings})
    end.


load_or_fail(Mod, Path, Bin, File) ->
    case code:load_binary(Mod, Path, Bin) of
        {module, Mod} -> ok;
        {error, Reason} -> fail("loading", File, Reason)
    end.


fail(Action, File, Reason) ->
    io:format(standard_error,
        "generate_otp_sources.escript: error ~s ~s: ~p~n",
        [Action, File, Reason]),
    halt(1).
