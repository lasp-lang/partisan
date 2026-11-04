%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Wu Shuwen
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(partisan_config_test).

-include_lib("eunit/include/eunit.hrl").

get_with_opts_reads_proplist_test() ->
    ?assertEqual(value, partisan_config:get_with_opts(key, [{key, value}])).

get_with_opts_uses_config_default_for_missing_proplist_key_test() ->
    ?assertEqual(
        config_default,
        partisan_config:get_with_opts(key, [], config_default)
    ).

get_with_opts_reads_map_test() ->
    ?assertEqual(value, partisan_config:get_with_opts(key, #{key => value})).
