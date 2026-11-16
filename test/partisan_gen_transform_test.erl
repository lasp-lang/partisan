%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Aaron Paterson
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests that the sources `partisan_gen_transform:generate_sources/1'
%% writes are the forms `partisan_gen_transform:generate/1' compiles.
%%
%% The build compiles the generated `.erl' files, so a printer that renders a
%% form differently changes the module. `erl_pp:form/1' prints a remote call to
%% an auto-imported BIF as a local call, which turns `erlang:spawn_opt/4' in
%% `partisan_proc_lib' into a call to its own `spawn_opt/4'. Each case prints a
%% module, parses it back, and compares the forms with source positions blanked.
%% @end
%% =============================================================================
-module(partisan_gen_transform_test).

-include_lib("eunit/include/eunit.hrl").


generated_sources_parse_to_generated_forms_test_() ->
    {setup,
        fun() ->
            Dir = filename:join(
                filename:dirname(code:which(?MODULE)), "generated_sources"
            ),
            ok = partisan_gen_transform:generate_sources(Dir),
            Dir
        end,
        fun(Dir) ->
            [
                {atom_to_list(Module),
                    ?_assertEqual(
                        compiled_forms(Module), parsed_forms(Dir, Module)
                    )}
             || Module <- partisan_gen_transform:modules()
            ]
        end}.


%% `generate_sources/1' checks every file it writes with `check_source/2'.
%% These cases aim to break that check. The first rewrites one
%% `erlang:spawn_opt(' call in `partisan_proc_lib' to the local call `erl_pp'
%% would print, a file that still compiles. The second removes the last
%% function. Neither covers a difference that is only in source positions,
%% which the check ignores by design.
check_source_rejects_a_different_module_test_() ->
    {setup,
        fun() ->
            Dir = filename:join(
                filename:dirname(code:which(?MODULE)), "checked_sources"
            ),
            ok = partisan_gen_transform:generate_sources(Dir),
            filename:join(Dir, "partisan_proc_lib.erl")
        end,
        fun(File) ->
            {ok, Source} = file:read_file(File),
            [
                {"the generated source passes",
                    ?_assertEqual(
                        ok, partisan_gen_transform:check_source(File, proc_lib)
                    )},
                {"a remote BIF call printed as a local call", fun() ->
                    {Start, Length} = binary:match(
                        Source, <<"erlang:spawn_opt(">>
                    ),
                    ok = file:write_file(File, [
                        binary:part(Source, 0, Start),
                        <<"spawn_opt(">>,
                        binary:part(
                            Source,
                            Start + Length,
                            byte_size(Source) - Start - Length
                        )
                    ]),
                    ?assertMatch(
                        {error, {source_mismatch, File, {differs, _}}},
                        partisan_gen_transform:check_source(File, proc_lib)
                    )
                end},
                {"a missing function", fun() ->
                    Forms = binary:split(
                        string:trim(Source, trailing), <<"\n\n">>, [global]
                    ),
                    ok = file:write_file(
                        File,
                        lists:join(<<"\n\n">>, lists:droplast(Forms))
                    ),
                    ?assertMatch(
                        {error, {source_mismatch, File, {missing, _}}},
                        partisan_gen_transform:check_source(File, proc_lib)
                    )
                end}
            ]
        end}.


%% =============================================================================
%% PRIVATE
%% =============================================================================


compiled_forms(Module) ->
    {ok, PartisanModule, Binary} = partisan_gen_transform:generate(Module),
    {ok, {PartisanModule, [{abstract_code, {raw_abstract_v1, Forms}}]}} =
        beam_lib:chunks(Binary, [abstract_code]),
    code_forms(Forms).


parsed_forms(Dir, Module) ->
    PartisanModule = partisan_otp_modules:partisan_module(Module),
    File = filename:join(Dir, atom_to_list(PartisanModule) ++ ".erl"),
    {ok, Forms} = epp:parse_file(File, []),
    code_forms(Forms).


%% The forms without `-file' attributes and `eof', each with every node's
%% annotation blanked, so only the code is compared.
code_forms(Forms) ->
    [
        unlocate(Form)
     || Form <- Forms,
        element(1, Form) =/= eof,
        not (element(1, Form) =:= attribute andalso element(3, Form) =:= file)
    ].


%% Every abstract-format node is a tuple tagged with an atom whose second
%% element is its annotation. Raw terms inside attributes are blanked the same
%% way on both sides of the comparison.
unlocate(T) when is_tuple(T), tuple_size(T) >= 2, is_atom(element(1, T)) ->
    [Tag, _Anno | Rest] = tuple_to_list(T),
    list_to_tuple([Tag, 0 | [unlocate(E) || E <- Rest]]);
unlocate(T) when is_tuple(T) ->
    list_to_tuple([unlocate(E) || E <- tuple_to_list(T)]);
unlocate(L) when is_list(L) ->
    [unlocate(E) || E <- L];
unlocate(X) ->
    X.
