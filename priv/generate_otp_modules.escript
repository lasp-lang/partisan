#!/usr/bin/env escript
%% -*- erlang -*-
%%! +A0 -pa _build/default/lib/partisan/ebin -pa _build/test/lib/partisan/ebin -pa _build/node1/lib/partisan/ebin -pa _build/node2/lib/partisan/ebin -pa _build/node3/lib/partisan/ebin

%% -----------------------------------------------------------------------------
%% Compile-time generation of partisan OTP modules.
%%
%% Called as a rebar3 post-compile hook. At this point the three transform
%% modules (partisan_gen_transform, partisan_otp_rewrite, partisan_otp_patches)
%% have already been compiled into ebin/. This script loads them and uses them
%% to generate partisan versions of OTP modules (partisan_gen_server,
%% partisan_gen_supervisor, etc.) from the installed OTP source.
%% -----------------------------------------------------------------------------

main(Args) ->
    %% Find the ebin directory for partisan.
    %% When called as a rebar3 hook, the first arg may be the profile-specific
    %% ebin path. Otherwise detect from the code path.
    EbinDir = case Args of
        [Dir] when Dir =/= [] -> Dir;
        _ -> find_ebin_dir()
    end,

    %% Ensure the transform modules are loadable from the correct ebin.
    %% Add as first in path so it takes priority over stale -pa paths.
    true = code:add_patha(EbinDir),
    %% Also check the checkouts dir (rebar3 puts checkout dep beams there).
    case os:getenv("REBAR_BUILD_DIR") of
        false -> ok;
        BD ->
            CheckoutsEbin = filename:join([BD, "checkouts", "partisan", "ebin"]),
            code:add_patha(CheckoutsEbin)
    end,

    io:format("Generating partisan OTP modules into ~s~n", [EbinDir]),

    %% Generate all modules into the detected ebin directory.
    case partisan_gen_transform:generate_all(EbinDir) of
        ok ->
            Modules = partisan_gen_transform:modules(),
            io:format("Generated ~p partisan OTP modules: ~p~n",
                      [length(Modules), Modules]),
            %% Update the .app file to include the generated modules.
            %% This is necessary for release mode (embedded) where only
            %% modules listed in the .app file are loadable.
            update_app_file(EbinDir, Modules),
            ok;
        {error, Errors} ->
            io:format(standard_error,
                      "Failed to generate partisan OTP modules:~n~p~n",
                      [Errors]),
            halt(1)
    end.


update_app_file(EbinDir, OtpModules) ->
    AppFile = filename:join(EbinDir, "partisan.app"),
    case file:consult(AppFile) of
        {ok, [{application, partisan, Props}]} ->
            ExistingModules = proplists:get_value(modules, Props, []),
            %% Map OTP module names to partisan module names.
            RenameMap = #{
                gen => partisan_gen,
                proc_lib => partisan_proc_lib,
                sys => partisan_sys,
                gen_server => partisan_gen_server,
                gen_event => partisan_gen_event,
                gen_statem => partisan_gen_statem,
                gen_fsm => partisan_gen_fsm,
                supervisor => partisan_gen_supervisor
            },
            GenModules = [maps:get(M, RenameMap) || M <- OtpModules],
            AllModules = lists:usort(ExistingModules ++ GenModules),
            NewProps = lists:keystore(modules, 1, Props, {modules, AllModules}),
            Content = io_lib:format("~p.\n", [{application, partisan, NewProps}]),
            ok = file:write_file(AppFile, Content);
        _ ->
            %% .app file doesn't exist yet or is malformed; skip.
            ok
    end.


find_ebin_dir() ->
    %% rebar3 sets REBAR_BUILD_DIR during builds (e.g., "/path/_build/node1").
    %% Use it to find the correct ebin even when partisan is a checkout dep.
    case os:getenv("REBAR_BUILD_DIR") of
        false ->
            find_ebin_dir_fallback();
        BuildDir ->
            %% Checkout deps go to _build/PROFILE/checkouts/APP/ebin.
            %% Regular deps go to _build/PROFILE/lib/APP/ebin.
            %% Try checkouts first (higher priority in rebar3).
            CheckoutsDir = filename:join([BuildDir, "checkouts", "partisan", "ebin"]),
            LibDir = filename:join([BuildDir, "lib", "partisan", "ebin"]),
            case filelib:is_dir(CheckoutsDir) of
                true -> CheckoutsDir;
                false ->
                    ok = filelib:ensure_dir(filename:join(LibDir, "dummy")),
                    LibDir
            end
    end.

find_ebin_dir_fallback() ->
    case code:which(partisan_gen_transform) of
        non_existing ->
            Candidates = [
                "_build/default/lib/partisan/ebin",
                "_build/test/lib/partisan/ebin"
            ],
            case lists:filter(fun filelib:is_dir/1, Candidates) of
                [Dir | _] -> filename:absname(Dir);
                [] ->
                    io:format(standard_error,
                              "Cannot find partisan ebin directory~n", []),
                    halt(1)
            end;
        BeamPath ->
            filename:dirname(BeamPath)
    end.
