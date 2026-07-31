%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Guards the off-path handler split (ADR-000001, Phase 1). Drives the
%% single-node `partisan_plumtree_broadcast' server's receive path directly by
%% casting the same `{broadcast, ...}' frame a peer would deliver, and asserts:
%%
%% <ul>
%% <li>`claim/2' is atomic (dedupes a repeated id);</li>
%% <li>a novel broadcast is delivered to the handler (apply runs);</li>
%% <li>a slow handler apply does NOT block the broadcast server — an unrelated
%% call to the server still returns promptly while the apply sleeps off-path;</li>
%% <li>a legacy handler (only `merge/2', no `claim/2') still delivers via the
%% backward-compatible path.</li>
%% </ul>
%% @end
%% =============================================================================
-module(partisan_broadcast_offpath_test).

-include_lib("eunit/include/eunit.hrl").

-define(BCAST, partisan_plumtree_broadcast).
-define(HANDLER, partisan_bcast_test_handler).
-define(LEGACY, partisan_bcast_legacy_handler).

%% =============================================================================
%% FIXTURE
%% =============================================================================

offpath_test_() ->
    {timeout, 60,
        {foreach, fun setup/0, fun cleanup/1, [
            fun claim_is_atomic/0,
            fun novel_broadcast_is_delivered/0,
            fun slow_apply_does_not_block_server/0,
            fun legacy_merge_handler_still_delivers/0
        ]}}.

setup() ->
    stop_partisan(),
    {ok, _} = application:ensure_all_started(partisan),
    {ok, _} = ?HANDLER:start_link(),
    {ok, _} = ?LEGACY:start_link(),
    ok.

cleanup(_) ->
    catch ?HANDLER:stop(),
    catch ?LEGACY:stop(),
    stop_partisan().

%% =============================================================================
%% TESTS
%% =============================================================================

%% `claim/2' records-and-decides in one atomic step: the first call wins, the
%% second sees a duplicate.
claim_is_atomic() ->
    ?HANDLER:reset(),
    Id = {atomic, erlang:unique_integer()},
    ?assert(?HANDLER:claim(Id, Id)),
    ?assertNot(?HANDLER:claim(Id, Id)).

%% A received broadcast for a claim/2 handler is applied off-path.
novel_broadcast_is_delivered() ->
    ?HANDLER:reset(),
    ?HANDLER:set_notify(self()),
    ?HANDLER:set_delay(0),
    Id = {msg, erlang:unique_integer()},
    cast_broadcast(?HANDLER, Id),
    ?assertEqual(ok, wait_applied(Id, 5000)),
    ?assert(?HANDLER:applied(Id)).

%% The core regression guard: while the handler's apply sleeps (off-path, in the
%% handler process), the broadcast server must stay responsive. If the apply ran
%% inline on the server, this unrelated call would block for the whole delay.
slow_apply_does_not_block_server() ->
    ?HANDLER:reset(),
    ?HANDLER:set_notify(self()),
    ?HANDLER:set_delay(1500),
    Id = {slow, erlang:unique_integer()},
    T0 = erlang:monotonic_time(millisecond),
    cast_broadcast(?HANDLER, Id),
    _ = partisan_plumtree_broadcast:broadcast_members(5000),
    Elapsed = erlang:monotonic_time(millisecond) - T0,
    ?assert(Elapsed < 500),
    %% and the apply still completes off-path
    ?assertEqual(ok, wait_applied(Id, 5000)).

%% A handler that only implements merge/2 (no claim/2) keeps working via the
%% backward-compatible fallback.
legacy_merge_handler_still_delivers() ->
    ?LEGACY:reset(),
    ?LEGACY:set_notify(self()),
    Id = {legacy, erlang:unique_integer()},
    cast_broadcast(?LEGACY, Id),
    ?assertEqual(ok, wait_applied(Id, 5000)),
    ?assert(?LEGACY:applied(Id)).

%% =============================================================================
%% HELPERS
%% =============================================================================

%% Cast the frame a peer would deliver for a received broadcast (Round/Root/From
%% present). Root = From = this node keeps the tree work local (no eager peers).
cast_broadcast(Mod, Id) ->
    Node = partisan:node(),
    gen_server:cast(?BCAST, {broadcast, Id, Id, Mod, 1, Node, Node}).

wait_applied(Id, Timeout) ->
    receive
        {applied, Id} -> ok
    after Timeout ->
        {error, timeout}
    end.

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.
