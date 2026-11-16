#!/usr/bin/env escript
%% -*- erlang -*-

%% -----------------------------------------------------------------------------
%% Pre-compile generation of partisan OTP module SOURCES.
%%
%% Why a pre-compile (and not a post-compile) hook?
%% ------------------------------------------------
%% When partisan and another partisan-using dep are both
%% direct dependencies of a top-level project, rebar3 may
%% start compiling that dep before partisan's `post_hooks compile' has
%% had a chance to fire. The dep then fails because
%% `partisan_gen_supervisor' does not yet exist.
%%
%% Why .erl sources in `src/'?
%% ---------------------------
%% This script writes `partisan_gen_server.erl',
%% `partisan_gen_supervisor.erl', etc. as real source files into `src/'.
%% The build tool then compiles them with the rest of partisan, into the
%% `ebin' it builds partisan into: rebar3's own output directory, or the
%% one Mix gives `rebar3 bare compile'. So the `partisan_gen_*' beams
%% exist wherever partisan is built, before any downstream dep starts
%% compiling against them. `partisan_gen_transform:generate_sources/1'
%% prints the forms with `erl_prettypr', which keeps calls such as
%% `erlang:spawn_opt' qualified.
%%
%% The support modules (`partisan_otp_modules', `partisan_otp_rewrite',
%% `partisan_otp_patches', `partisan_gen_transform') have no inter-
%% include dependencies, so we compile them in-memory here and load
%% them before invoking the generator.
%% -----------------------------------------------------------------------------

main(_Args) ->
    SrcDir = "src",

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

    case partisan_gen_transform:generate_sources(SrcDir) of
        ok ->
            Modules = partisan_gen_transform:modules(),
            io:format(
                "Generated ~p partisan OTP module sources into ~s/~n",
                [length(Modules), SrcDir]
            ),
            update_app_src(SrcDir),
            ok;
        {error, Errors} ->
            io:format(standard_error,
                "Failed to generate partisan OTP module sources:~n~p~n",
                [Errors]),
            halt(1)
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
