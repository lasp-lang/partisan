%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Wu Shuwen
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(partisan_retry_test).

-include_lib("eunit/include/eunit.hrl").

init_accepts_proplist_options_test() ->
    Retry = partisan_retry:init(retry, [{interval, 250}, {max_retries, 3}]),
    ?assertEqual(250, partisan_retry:get(Retry)),
    ?assertEqual(0, partisan_retry:count(Retry)).

init_accepts_map_options_test() ->
    Retry = partisan_retry:init(retry, #{interval => 250, max_retries => 3}),
    ?assertEqual(250, partisan_retry:get(Retry)),
    ?assertEqual(0, partisan_retry:count(Retry)).

init_proplist_keeps_first_property_priority_test() ->
    Retry = partisan_retry:init(retry, [
        {max_retries, 0}, {max_retries, 10}, {interval, 250}
    ]),
    ?assertEqual({max_retries, Retry}, partisan_retry:fail(Retry)).
