%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(partisan_membership).

-include("partisan.hrl").

-moduledoc """
Lock-free snapshot of the cluster's membership, with a non-blocking
change-notification feed.

The peer service manager (the membership oracle) writes the current member set
here on every change, and readers — broadcast groups and application code —
consume it with lock-free ETS reads. This replaces fanning membership updates out
over `partisan_peer_service_events` (a `gen_event` bus), whose synchronous
`sync_notify` blocked the oracle and did not scale to many readers.

The backing table is a public, `read_concurrency` set created and owned by
`partisan_sup`, so it survives manager restarts.

Writers observe a simple ordering discipline: the member set is written before the
version is bumped, so a reader that sees a new `version/0` is guaranteed to read
the corresponding — or newer — members. A cheap reader polls `version/0` and
re-reads `members/0` only when it changes.

## Observing membership

- `subscribe/0` — receive a `{partisan_membership, Members}` message on every change.
- `members/0`, `node_names/0` — read the current member set directly.
- `version/0` — the monotonic version, for change detection.

`set/1` and `notify/1` are the writer side, called by the peer service managers;
application code does not call them.
""".

-export([set/1]).
-export([members/0]).
-export([node_names/0]).
-export([version/0]).
-export([notify/1]).
-export([subscribe/0]).
-export([subscribe/1]).
-export([unsubscribe/0]).
-export([unsubscribe/1]).

-doc """
Publishes the current member set and bumps the version.

Called by the peer service manager whenever membership changes. The member set is
written before the version, so a reader that observes the new `version/0` reads
members at least as new. Not for application code.
""".
-spec set([partisan:node_spec()]) -> ok.

set(Members) when is_list(Members) ->
    %% Members first, then version — so a reader seeing a new version always
    %% reads members that are at least as new.
    true = ets:insert(?PARTISAN_MEMBERS, {members, Members}),
    _ = ets:update_counter(?PARTISAN_MEMBERS, version, {2, 1}, {version, 0}),
    ok.

-doc """
Returns the current member set as node specs, with a lock-free read.
""".
-spec members() -> [partisan:node_spec()].

members() ->
    case ets:lookup(?PARTISAN_MEMBERS, members) of
        [{members, Members}] -> Members;
        [] -> []
    end.

-doc """
Returns the current member node names as an ordset, with a lock-free read.
""".
-spec node_names() -> ordsets:ordset(node()).

node_names() ->
    ordsets:from_list([Name || #{name := Name} <- members()]).

-doc """
Returns the current membership version.

The version increases monotonically; a change is the signal for a polling reader
to re-read `members/0`. Lock-free read.
""".
-spec version() -> non_neg_integer().

version() ->
    case ets:lookup(?PARTISAN_MEMBERS, version) of
        [{version, Version}] -> Version;
        [] -> 0
    end.

-doc """
Delivers an asynchronous `{partisan_membership, Members}` message to every
subscribed process.

Called by the peer service managers after `set/1` on a membership change. It is
fire-and-forget: it never blocks the caller (the oracle), and each subscriber
handles the change in its own process. Subscribers are local pids, so a dead one
is pruned here as it is found. Not for application code.
""".
-spec notify([partisan:node_spec()]) -> ok.

notify(Members) when is_list(Members) ->
    Subscribers = ets:select(
        ?PARTISAN_MEMBERS,
        [{{{subscriber, '$1'}, '_'}, [], ['$1']}]
    ),
    lists:foreach(
        fun(Pid) ->
            case is_process_alive(Pid) of
                true ->
                    Pid ! {partisan_membership, Members};
                false ->
                    ets:delete(?PARTISAN_MEMBERS, {subscriber, Pid})
            end
        end,
        Subscribers
    ).

-doc """
Subscribes the calling process to membership-change notifications.

The subscriber receives a `{partisan_membership, Members}` message on every
change, where `Members` is the new member set as node specs.
""".
-spec subscribe() -> ok.

subscribe() ->
    subscribe(self()).

-doc """
Subscribes `Pid` to membership-change notifications.
""".
-spec subscribe(pid()) -> ok.

subscribe(Pid) when is_pid(Pid) ->
    true = ets:insert(?PARTISAN_MEMBERS, {{subscriber, Pid}, ok}),
    ok.

-doc """
Removes the calling process's membership-change subscription.
""".
-spec unsubscribe() -> ok.

unsubscribe() ->
    unsubscribe(self()).

-doc """
Removes `Pid`'s membership-change subscription.
""".
-spec unsubscribe(pid()) -> ok.

unsubscribe(Pid) when is_pid(Pid) ->
    true = ets:delete(?PARTISAN_MEMBERS, {subscriber, Pid}),
    ok.
