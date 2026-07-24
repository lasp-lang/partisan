%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Guards the lock-free membership snapshot (PDDR-000001, Phase 2a): the
%% oracle seeds and maintains `partisan_membership', and `broadcast_members/0'
%% is answered from it.
%% @end
%% =============================================================================
-module(partisan_membership_test).

-include_lib("eunit/include/eunit.hrl").

snapshot_test_() ->
    {timeout, 30,
        {setup, fun setup/0, fun cleanup/1, fun(_) ->
            [
                ?_test(seeded_at_boot()),
                ?_test(self_is_a_member()),
                ?_test(broadcast_members_reads_snapshot()),
                ?_test(push_notifies_subscriber()),
                ?_test(unsubscribe_stops_notifications())
            ]
        end}}.

setup() ->
    stop_partisan(),
    {ok, _} = application:ensure_all_started(partisan),
    ok.

cleanup(_) ->
    stop_partisan().

%% The manager seeds the snapshot in init/1, so the version is bumped and the
%% member set is populated before any reader starts.
seeded_at_boot() ->
    ?assert(partisan_membership:version() >= 1),
    ?assertMatch([_ | _], partisan_membership:members()).

self_is_a_member() ->
    Names = partisan_membership:node_names(),
    ?assert(ordsets:is_element(partisan:node(), Names)),
    %% members/0 returns node specs
    ?assert(
        lists:all(
            fun
                (#{name := _}) -> true;
                (_) -> false
            end,
            partisan_membership:members()
        )
    ).

broadcast_members_reads_snapshot() ->
    ?assertEqual(
        partisan_membership:node_names(),
        partisan_peer_service:broadcast_members()
    ).

%% A subscriber receives an async `{partisan_membership, Members}' message on a
%% change. `set/1' accepts the node-spec list shape the managers pass (after
%% `sets:to_list' for the set-based managers).
push_notifies_subscriber() ->
    ok = partisan_membership:subscribe(),
    Specs = [#{name => 'x@h'}, #{name => 'y@h'}],
    ok = partisan_membership:set(Specs),
    ok = partisan_membership:notify(Specs),
    receive
        {partisan_membership, Got} ->
            ?assertEqual(Specs, Got),
            ?assertEqual([x@h, y@h], partisan_membership:node_names())
    after 2000 ->
        ?assert(false)
    end,
    ok = partisan_membership:unsubscribe().

%% After unsubscribe, no further notifications arrive.
unsubscribe_stops_notifications() ->
    ok = partisan_membership:subscribe(),
    ok = partisan_membership:unsubscribe(),
    ok = partisan_membership:notify([#{name => 'z@h'}]),
    receive
        {partisan_membership, _} -> ?assert(false)
    after 300 ->
        ok
    end.

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.
