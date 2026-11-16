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
%% abstract code from the installed beam file, applies mechanical AST rewrites
%% and structural patches, and either writes the result as source for the
%% build to compile or compiles it and optionally loads it into the VM.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_gen_transform).

-export([generate/1, generate_and_load/1, generate_all/0, generate_all/1]).
-export([generate_sources/1, check_source/2]).
-export([modules/0, otp_major_version/0]).

%% =============================================================================
%% API
%% =============================================================================

%% @doc Transform a single OTP module into its partisan equivalent.
%% Extracts abstract code, applies rewrites and patches, then compiles.
generate(OrigModule) ->
    case patched_forms(OrigModule) of
        {ok, PartisanModule, PatchedForms} ->
            case
                compile:forms(
                    PatchedForms, [
                        binary, return_errors, deterministic, debug_info
                    ]
                )
            of
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
                    case
                        code:load_binary(
                            PartisanModule, BeamFile, Binary
                        )
                    of
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

%% @doc Generate the `.erl' source of every supported partisan OTP module into
%% `SrcDir'.
%%
%% The pre-compile escript writes them into partisan's `src/', so the build
%% tool compiles them with the rest of partisan, into the `ebin' it builds
%% partisan into, before any dependency of partisan compiles against them.
%%
%% The forms are printed with `erl_prettypr', which writes every remote call
%% with its module. `erl_pp' prints a remote call to an auto-imported BIF as a
%% local call, and `partisan_proc_lib' defines its own `spawn_opt/2,3,4,5', so
%% its calls to `erlang:spawn_opt' have to stay qualified to reach the BIF.
-spec generate_sources(file:filename_all()) ->
    ok | {error, [{module(), term()}]}.
generate_sources(SrcDir) ->
    ok = filelib:ensure_dir(filename:join(SrcDir, "dummy")),
    Header =
        "%% Generated by partisan_gen_transform from the installed OTP on every\n"
        "%% compile. Do not edit; changes are overwritten.\n\n",
    Results = lists:foldl(
        fun(Module, Acc) ->
            case patched_forms(Module) of
                {ok, PartisanModule, Forms} ->
                    File = filename:join(
                        SrcDir, atom_to_list(PartisanModule) ++ ".erl"
                    ),
                    %% OTP's `-file' attributes are dropped: they name OTP's
                    %% files and lines, but the layout below is the
                    %% printer's, so keeping them makes the compiler record
                    %% line numbers that match neither file.
                    Source = unicode:characters_to_binary([
                        Header,
                        erl_prettypr:format(
                            erl_syntax:form_list(without_file_attributes(Forms))
                        ),
                        "\n"
                    ]),
                    case file:write_file(File, Source) of
                        ok ->
                            case check_source_forms(File, Forms) of
                                ok ->
                                    Acc;
                                {error, Reason} ->
                                    [{Module, Reason} | Acc]
                            end;
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

%% @doc Check that the source in `File' parses to the forms this module
%% generates for `OrigModule'.
%%
%% `generate_sources/1' runs this check on every file it writes, so a printer
%% that renders a form differently on the installed OTP fails the build instead
%% of changing the module. Source positions are not compared.
-spec check_source(file:filename_all(), module()) -> ok | {error, term()}.
check_source(File, OrigModule) ->
    case patched_forms(OrigModule) of
        {ok, _PartisanModule, Forms} ->
            check_source_forms(File, Forms);
        {error, _} = Error ->
            Error
    end.

%% @doc Returns the ordered list of modules to transform.
%% The order is the table's own; see `partisan_otp_modules:substitutions/0'.
modules() ->
    partisan_otp_modules:otp_modules().

%% @doc Returns the major OTP version as an integer (e.g. 27, 28).
-spec otp_major_version() -> non_neg_integer().
otp_major_version() ->
    list_to_integer(erlang:system_info(otp_release)).

%% =============================================================================
%% Internal: abstract code extraction
%% =============================================================================

%% The partisan module's forms: the OTP module's abstract code with the
%% mechanical AST rewrites (module names, calls, atoms) and then the structural
%% patches (adding/modifying functions) applied.
patched_forms(OrigModule) ->
    case get_abstract_code(OrigModule) of
        {ok, Forms} ->
            PartisanModule = partisan_otp_modules:partisan_module(
                OrigModule
            ),
            RewrittenForms = partisan_otp_rewrite:transform(OrigModule, Forms),
            PatchedForms = partisan_otp_patches:apply_patches(
                OrigModule, RewrittenForms
            ),
            {ok, PartisanModule, quiet(PatchedForms)};
        {error, _} = Error ->
            Error
    end.

%% Adds a `-compile' attribute after the `-module' attribute that turns off
%% the warnings OTP's own code raises once it is compiled as part of partisan:
%% functions and types the rewrite leaves unused, and OTP 29's `catch'
%% deprecation. They are not actionable by whoever builds partisan.
quiet([{attribute, Anno, module, _} = Module | Forms]) ->
    Options = [
        nowarn_unused_function, nowarn_unused_type, nowarn_deprecated_catch
    ],
    [Module, {attribute, Anno, compile, Options} | Forms];
quiet([Form | Forms]) ->
    [Form | quiet(Forms)].

without_file_attributes(Forms) ->
    [
        Form
     || Form <- Forms,
        not (element(1, Form) =:= attribute andalso element(3, Form) =:= file)
    ].

%% Parses `File' the way the compiler will (`epp') and compares the result
%% with `Forms', form by form.
check_source_forms(File, Forms) ->
    case epp:parse_file(File, []) of
        {ok, Parsed} ->
            case first_difference(code_forms(Forms), code_forms(Parsed)) of
                none ->
                    ok;
                Difference ->
                    {error, {source_mismatch, File, Difference}}
            end;
        {error, Reason} ->
            {error, {source_unreadable, File, Reason}}
    end.

%% The forms without `-file' attributes and `eof', each with every node's
%% annotation blanked, so only the code is compared.
code_forms(Forms) ->
    [unlocate(Form) || Form <- without_file_attributes(Forms), not is_eof(Form)].

is_eof(Form) ->
    element(1, Form) =:= eof.

%% Every abstract-format node is a tuple tagged with an atom whose second
%% element is its annotation. A raw term inside an attribute that has the same
%% shape is blanked too, on both sides of the comparison.
unlocate(T) when is_tuple(T), tuple_size(T) >= 2, is_atom(element(1, T)) ->
    [Tag, _Anno | Rest] = tuple_to_list(T),
    list_to_tuple([Tag, 0 | [unlocate(E) || E <- Rest]]);
unlocate(T) when is_tuple(T) ->
    list_to_tuple([unlocate(E) || E <- tuple_to_list(T)]);
unlocate(L) when is_list(L) ->
    [unlocate(E) || E <- L];
unlocate(X) ->
    X.

first_difference([Form | Expected], [Form | Parsed]) ->
    first_difference(Expected, Parsed);
first_difference([], []) ->
    none;
first_difference([Form | _], [_ | _]) ->
    {differs, form_id(Form)};
first_difference([Form | _], []) ->
    {missing, form_id(Form)};
first_difference([], [Form | _]) ->
    {unexpected, form_id(Form)}.

form_id({function, _, Name, Arity, _}) -> {function, Name, Arity};
form_id({attribute, _, Name, _}) -> {attribute, Name};
form_id(Form) -> element(1, Form).

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
