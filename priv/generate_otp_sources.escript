#!/usr/bin/env escript
%% -*- erlang -*-

%% -----------------------------------------------------------------------------
%% Pre-compile generation of partisan OTP module SOURCES.
%%
%% Why a pre-compile (and not a post-compile) hook?
%% ------------------------------------------------
%% When partisan and another partisan-using dep (e.g. plum_db) are both
%% direct dependencies of a top-level project (e.g. bondy), rebar3 may
%% start compiling plum_db before partisan's `post_hooks compile' has
%% had a chance to fire. plum_db then fails because
%% `partisan_gen_supervisor' does not yet exist.
%%
%% This pre-compile script writes `partisan_gen_server.erl',
%% `partisan_gen_supervisor.erl', etc. as real source files into
%% `src/'. They are then compiled by rebar3 as part of partisan's normal
%% compile phase — so by the time downstream deps start compiling,
%% the `partisan_gen_*' beams already exist.
%%
%% The three support modules (`partisan_otp_rewrite',
%% `partisan_otp_patches', `partisan_gen_transform') have no inter-
%% include dependencies, so we compile them in-memory here and load them
%% before invoking the generator.
%% -----------------------------------------------------------------------------

main(_Args) ->
    SrcDir = "src",

    %% Sanity check: this script is meant to run from the partisan dep's
    %% root directory, where `src/' exists.
    case filelib:is_dir(SrcDir) of
        true -> ok;
        false ->
            io:format(standard_error,
                "generate_otp_sources.escript: '~s' not found; "
                "expected to run from partisan's root directory.~n",
                [SrcDir]),
            halt(1)
    end,

    %% Compile the three support modules in dependency-call order.
    %% Erlang modules can be compiled independently (calls are dynamic),
    %% so the only constraint is that each is loaded into the BEAM
    %% before `generate_sources/1' invokes it.
    SupportModules = [
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
            ok;
        {error, Errors} ->
            io:format(standard_error,
                "Failed to generate partisan OTP module sources:~n~p~n",
                [Errors]),
            halt(1)
    end.


compile_and_load(File, SrcDir) ->
    Path = filename:join(SrcDir, File),
    case compile:file(Path, [binary, return_errors]) of
        {ok, Mod, Bin} ->
            case code:load_binary(Mod, Path, Bin) of
                {module, Mod} ->
                    ok;
                {error, Reason} ->
                    fail("loading", File, Reason)
            end;
        {ok, Mod, Bin, _Warnings} ->
            case code:load_binary(Mod, Path, Bin) of
                {module, Mod} -> ok;
                {error, Reason} -> fail("loading", File, Reason)
            end;
        {error, Errors, Warnings} ->
            fail("compiling", File, {Errors, Warnings})
    end.


fail(Action, File, Reason) ->
    io:format(standard_error,
        "generate_otp_sources.escript: error ~s ~s: ~p~n",
        [Action, File, Reason]),
    halt(1).
