%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests that `partisan_otp_patches:apply_patches/2' raises when the
%% target of a patch is absent from the OTP source, instead of leaving the
%% upstream code in place.
%%
%% Each case takes the installed `gen_statem' source, removes the target of one
%% kind of patch, and expects the error. This is the shape of an OTP release
%% that renames or removes a patched function. The `replace' case is the one
%% that matters most: an unpatched `send/2' still compiles, but it sends with
%% `erlang:send/2' over disterl.
%%
%% Not covered: `add_function' with a target that already exists. That is a
%% duplicate definition, and `compile:forms/2' rejects it later in
%% `partisan_gen_transform:generate/1'.
%% @end
%% =============================================================================
-module(partisan_otp_patches_test).

-include_lib("eunit/include/eunit.hrl").


unmodified_source_is_patched_test() ->
    Forms = forms(),
    ?assertNot(has_function(cast, 3, Forms)),
    Patched = partisan_otp_patches:apply_patches(gen_statem, Forms),
    ?assert(has_function(cast, 3, Patched)).

missing_replace_target_raises_test() ->
    ?assertError(
        {otp_patch_target_missing, gen_statem, {replace, {send, 2}}},
        partisan_otp_patches:apply_patches(
            gen_statem, without_function(send, 2, forms())
        )
    ).

missing_append_clause_target_raises_test() ->
    ?assertError(
        {otp_patch_target_missing, gen_statem, {append_clause, {cast, 2}}},
        partisan_otp_patches:apply_patches(
            gen_statem, without_function(cast, 2, forms())
        )
    ).

missing_export_attribute_raises_test() ->
    Forms = [F || F <- forms(), not is_export(F)],
    ?assertError(
        {otp_patch_target_missing, gen_statem, {add_export, [{cast, 3}]}},
        partisan_otp_patches:apply_patches(gen_statem, Forms)
    ).


%% =============================================================================
%% HELPERS
%% =============================================================================


%% The forms apply_patches/2 receives in a build: the OTP source after the
%% mechanical rewrite.
forms() ->
    StdlibDir = code:lib_dir(stdlib),
    File = filename:join([StdlibDir, "src", "gen_statem.erl"]),
    {ok, Forms} = epp:parse_file(File, [
        {includes, [
            filename:join(StdlibDir, "include"),
            filename:join(code:lib_dir(kernel), "include")
        ]}
    ]),
    partisan_otp_rewrite:transform(gen_statem, Forms).

without_function(Name, Arity, Forms) ->
    [F || F <- Forms, not is_function_form(Name, Arity, F)].

has_function(Name, Arity, Forms) ->
    lists:any(fun(F) -> is_function_form(Name, Arity, F) end, Forms).

is_function_form(Name, Arity, {function, _, Name, Arity, _}) -> true;
is_function_form(_, _, _) -> false.

is_export({attribute, _, export, _}) -> true;
is_export(_) -> false.
