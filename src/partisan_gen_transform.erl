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
%% @doc Orchestrator module that ties together `partisan_otp_rewrite' and
%% `partisan_otp_patches' to generate partisan versions of OTP modules from
%% installed OTP beam files.
%%
%% Given an OTP module name (e.g. `gen_server'), this module extracts the
%% abstract code from the installed beam file, applies mechanical AST rewrites,
%% applies structural patches, compiles the result, and optionally loads it
%% into the VM.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_gen_transform).

-export([generate/1, generate_and_load/1, generate_all/0, generate_all/1]).
-export([generate_sources/1]).
-export([modules/0, otp_major_version/0]).


%% =============================================================================
%% API
%% =============================================================================


%% @doc Transform a single OTP module into its partisan equivalent.
%% Extracts abstract code, applies rewrites and patches, then compiles.
generate(OrigModule) ->
    case get_abstract_code(OrigModule) of
        {ok, Forms} ->
            PartisanModule = partisan_module(OrigModule),
            %% Step 1: Mechanical AST rewrites (module names, calls, atoms).
            RewrittenForms = partisan_otp_rewrite:transform(OrigModule, Forms),
            %% Step 2: Structural patches (adding/modifying functions).
            PatchedForms = partisan_otp_patches:apply_patches(
                OrigModule, RewrittenForms
            ),
            %% Step 3: Compile the patched forms.
            case compile:forms(
                PatchedForms, [binary, return_errors, deterministic, debug_info]
            ) of
                {ok, PartisanModule, Binary} ->
                    {ok, PartisanModule, Binary};
                {ok, PartisanModule, Binary, _Warnings} ->
                    {ok, PartisanModule, Binary};
                {error, Errors, _Warnings} ->
                    {error, {compile_errors, Errors}}
            end;
        {error, _} = Error ->
            Error
    end.


%% @doc Transform, compile, and load the module into the runtime.
generate_and_load(OrigModule) ->
    case generate(OrigModule) of
        {ok, PartisanModule, Binary} ->
            BeamFile = atom_to_list(PartisanModule) ++ ".beam",
            case code:load_binary(PartisanModule, BeamFile, Binary) of
                {module, PartisanModule} ->
                    ok;
                {error, Reason} ->
                    {error, {load_failed, Reason}}
            end;
        {error, _} = Error ->
            Error
    end.


%% @doc Generate all supported partisan OTP modules.
%% Generates each module in dependency order, writes the .beam file
%% to the ebin directory, and loads it into the VM.
%% Returns ok on success or {error, [{Module, Reason}]} on failure.
generate_all() ->
    generate_all(ebin_dir()).

%% @doc Generate all modules into the specified ebin directory.
%%
%% Writing the beam to `Dir' is best-effort. When this is called as the
%% runtime fallback from `partisan_app:start/2', the release's ebin is
%% typically read-only — we still want to load the binary into memory
%% because that is what makes the module callable for the running node.
%% Persistence to disk only saves a regeneration on the next cold start.
generate_all(Dir) ->
    _ = catch filelib:ensure_dir(filename:join(Dir, "dummy")),
    Results = lists:foldl(
        fun(Module, Acc) ->
            case generate(Module) of
                {ok, PartisanModule, Binary} ->
                    BeamFile = atom_to_list(PartisanModule) ++ ".beam",
                    %% Best-effort persistence; ignore write failures
                    %% (read-only release filesystem is fine).
                    _ = write_beam(Dir, PartisanModule, Binary),
                    case code:load_binary(
                        PartisanModule, BeamFile, Binary
                    ) of
                        {module, PartisanModule} ->
                            Acc;
                        {error, Reason} ->
                            [{Module, {load_failed, Reason}} | Acc]
                    end;
                {error, Reason} ->
                    [{Module, Reason} | Acc]
            end
        end,
        [],
        modules()
    ),
    case Results of
        [] -> ok;
        Errors -> {error, lists:reverse(Errors)}
    end.


%% @doc Returns the ordered list of modules to transform.
%% Order matters: gen and proc_lib must be generated before gen_server.
modules() -> [gen, proc_lib, sys, gen_server, gen_event, gen_statem, supervisor].


%% @doc Generate `.erl' source files for all OTP modules into `SrcDir'.
%%
%% Used by the pre-compile escript so the rewritten modules become real
%% Erlang source that rebar3 compiles as part of partisan's normal
%% compile phase. This guarantees the `partisan_gen_*' beams exist
%% before any downstream dependency (e.g. plum_db) starts compiling
%% against them.
-spec generate_sources(file:filename_all()) ->
    ok | {error, [{module(), term()}]}.

generate_sources(SrcDir) ->
    ok = filelib:ensure_dir(filename:join(SrcDir, "dummy")),
    Results = lists:foldl(
        fun(Module, Acc) ->
            case generate_source(Module) of
                {ok, PartisanModule, Source} ->
                    File = filename:join(
                        SrcDir, atom_to_list(PartisanModule) ++ ".erl"
                    ),
                    case file:write_file(File, Source) of
                        ok -> Acc;
                        {error, Reason} ->
                            [{Module, {write_failed, Reason}} | Acc]
                    end;
                {error, Reason} ->
                    [{Module, Reason} | Acc]
            end
        end,
        [],
        modules()
    ),
    case Results of
        [] -> ok;
        Errors -> {error, lists:reverse(Errors)}
    end.


%% @private Pretty-print the rewritten AST back to Erlang source text.
generate_source(OrigModule) ->
    case get_abstract_code(OrigModule) of
        {ok, Forms} ->
            PartisanModule = partisan_module(OrigModule),
            RewrittenForms = partisan_otp_rewrite:transform(OrigModule, Forms),
            PatchedForms = partisan_otp_patches:apply_patches(
                OrigModule, RewrittenForms
            ),
            Header = "%% This file is auto-generated by "
                     "partisan_gen_transform.\n"
                     "%% Do not edit by hand — your changes will be "
                     "overwritten\n"
                     "%% on the next compile.\n\n",
            %% `erl_pp:form/1' returns chardata that may contain unicode
            %% codepoints beyond latin-1, so use `unicode:characters_to_binary'
            %% rather than `iolist_to_binary'.
            Body = [erl_pp:form(F) || F <- PatchedForms],
            Source = unicode:characters_to_binary([Header | Body]),
            {ok, PartisanModule, Source};
        {error, _} = Error ->
            Error
    end.


%% @doc Returns the major OTP version as an integer (e.g. 27, 28).
-spec otp_major_version() -> non_neg_integer().
otp_major_version() ->
    list_to_integer(erlang:system_info(otp_release)).


%% =============================================================================
%% Internal: abstract code extraction
%% =============================================================================


%% Try the beam file first, fall back to source if abstract code is missing.
get_abstract_code(Module) ->
    case code:which(Module) of
        non_existing ->
            fallback_source(Module);
        BeamFile ->
            case beam_lib:chunks(BeamFile, [abstract_code]) of
                {ok, {Module, [{abstract_code, {raw_abstract_v1, Forms}}]}} ->
                    {ok, Forms};
                {ok, {Module, [{abstract_code, no_abstract_code}]}} ->
                    %% Beam file is stripped; try OTP source.
                    fallback_source(Module);
                {error, beam_lib, {missing_chunk, _, abstract_code}} ->
                    fallback_source(Module);
                {error, beam_lib, Reason} ->
                    {error, {beam_lib, Reason}}
            end
    end.


%% Try to parse the OTP source file directly. This handles stripped beams.
fallback_source(Module) ->
    StdlibDir = code:lib_dir(stdlib),
    SrcDir = filename:join(StdlibDir, "src"),
    File = filename:join(SrcDir, atom_to_list(Module) ++ ".erl"),
    case filelib:is_file(File) of
        true ->
            epp:parse_file(File, [
                {includes, [
                    filename:join(StdlibDir, "include"),
                    filename:join(code:lib_dir(kernel), "include")
                ]}
            ]);
        false ->
            {error, {source_not_found, File}}
    end.


%% =============================================================================
%% Internal: module rename map
%% =============================================================================


partisan_module(gen_server) -> partisan_gen_server;
partisan_module(gen) -> partisan_gen;
partisan_module(gen_event) -> partisan_gen_event;
partisan_module(gen_fsm) -> partisan_gen_fsm;
partisan_module(gen_statem) -> partisan_gen_statem;
partisan_module(supervisor) -> partisan_gen_supervisor;
partisan_module(proc_lib) -> partisan_proc_lib;
partisan_module(sys) -> partisan_sys.


%% =============================================================================
%% Internal: file helpers
%% =============================================================================


%% Write a compiled .beam binary to the given directory.
write_beam(Dir, Module, Binary) ->
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    File = filename:join(Dir, atom_to_list(Module) ++ ".beam"),
    file:write_file(File, Binary).


%% Locate the ebin directory for partisan.
%% `code:lib_dir/2' is deprecated since OTP 28; use `code:lib_dir/1' and
%% join the subdirectory ourselves.
ebin_dir() ->
    case code:lib_dir(partisan) of
        {error, _} ->
            %% During development/test, search rebar3 output dirs.
            Candidates = [
                "_build/default/lib/partisan/ebin",
                "_build/test/lib/partisan/ebin"
            ],
            case [D || D <- Candidates, filelib:is_dir(D)] of
                [Dir | _] -> filename:absname(Dir);
                [] -> filename:absname("_build/default/lib/partisan/ebin")
            end;
        LibDir ->
            filename:join(LibDir, "ebin")
    end.
