%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(partisan_broadcast).

-include("partisan.hrl").

-moduledoc """
Public API for Partisan's epidemic-broadcast groups.

A **broadcast group** is a supervised process that owns a complete, private
epidemic-broadcast context — its own mailbox, spanning-tree state and
outstanding-lazy table — reading membership from the shared, lock-free
`partisan_membership` snapshot. Group identity is the handler module: each
broadcast handler runs in its own group, so independent gossip streams never
share a tree or a mailbox.

Groups are declared by the `broadcast_mods` and `broadcast_groups` configuration
keys and started at boot, and can also be created or retired at runtime.

## Entry points

- `broadcast/2` — disseminate a message to the group that hosts a handler module.
- `start_group/1`, `stop_group/1` — create or retire a group at runtime.
- `groups/0` — list the running groups.
""".

-export([broadcast/2]).
-export([start_group/1]).
-export([stop_group/1]).
-export([groups/0]).
-export([group_name/1]).
-export([child_specs/0]).

-define(GROUP_SUP, partisan_broadcast_group_sup).

%% =============================================================================
%% API
%% =============================================================================

-doc """
Disseminates `Broadcast` through the broadcast group that hosts handler `Mod`.

`Mod` names the handler module — which is also the group's identity — and the
payload sent to peers is derived from `Broadcast` by that handler's
`broadcast_data/1` callback. Returns `ok` once the broadcast has been handed to
the group; dissemination itself is asynchronous.
""".
-spec broadcast(any(), module()) -> ok.

broadcast(Broadcast, Mod) ->
    partisan_plumtree_broadcast:broadcast(Broadcast, Mod).

-doc """
Returns the registered name of the broadcast group that hosts handler `Mod`.

The name is a deterministic function of `Mod` and is identical on every node, so
a group can be addressed cluster-wide without a registry lookup.
""".
-spec group_name(module()) -> atom().

group_name(Mod) ->
    partisan_plumtree_broadcast:group_name(Mod).

-doc """
Starts a broadcast group at runtime.

`Arg` is either a handler module or a spec map:

```erlang
#{mods := [module()],
  name => atom(),
  engine => module(),
  lazy_tick_period => integer(),
  exchange_tick_period => integer()}
```

`engine` selects the tree engine (defaults to `partisan_plumtree_engine`; see
`partisan_broadcast_engine`). The call is idempotent: if the group is already
running it returns the existing process rather than starting a second one.
""".
-spec start_group(module() | map()) -> {ok, pid()} | {error, term()}.

start_group(Mod) when is_atom(Mod) ->
    start_group(#{mods => [Mod]});
start_group(#{mods := [_ | _]} = Spec) ->
    {Name, Opts} = normalise(Spec),
    case supervisor:start_child(?GROUP_SUP, childspec(Name, Opts)) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, already_present} ->
            supervisor:restart_child(?GROUP_SUP, Name);
        {error, _} = Error ->
            Error
    end.

-doc """
Stops and removes the broadcast group for `Arg`.

`Arg` is a handler module or a spec map (only the group name is read). Returns
`ok` whether or not a group was running under that name.
""".
-spec stop_group(module() | map()) -> ok | {error, term()}.

stop_group(Mod) when is_atom(Mod) ->
    stop_group_by_name(group_name(Mod));
stop_group(#{mods := [Mod | _]}) ->
    stop_group_by_name(group_name(Mod)).

-doc """
Returns the registered names of the currently running broadcast groups.
""".
-spec groups() -> [atom()].

groups() ->
    [Id || {Id, _, _, _} <- supervisor:which_children(?GROUP_SUP)].

-doc """
Returns the child specs for the broadcast groups declared by configuration.

`partisan_broadcast_group_sup` uses these to start the configured groups at boot.
""".
-spec child_specs() -> [supervisor:child_spec()].

child_specs() ->
    [childspec(Name, Opts) || {Name, Opts} <- configured_groups()].

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
stop_group_by_name(Name) ->
    _ = supervisor:terminate_child(?GROUP_SUP, Name),
    case supervisor:delete_child(?GROUP_SUP, Name) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} = Error -> Error
    end.

%% @private
%% Declared groups: `broadcast_mods' grouped by group name (Partisan's default
%% handlers share the legacy-named group), plus any explicit `broadcast_groups'.
configured_groups() ->
    Mods = partisan_config:get(broadcast_mods, []),
    ByName = lists:foldl(
        fun(Mod, Acc) ->
            Name = group_name(Mod),
            maps:update_with(Name, fun(Ms) -> [Mod | Ms] end, [Mod], Acc)
        end,
        #{},
        Mods
    ),
    FromMods = [
        {Name, (base_opts())#{mods => lists:usort(Ms)}}
     || {Name, Ms} <- maps:to_list(ByName)
    ],
    FromGroups = [
        normalise(Spec)
     || Spec <- partisan_config:get(broadcast_groups, [])
    ],
    %% Explicit broadcast_groups win on name collisions.
    Merged = maps:from_list(FromMods ++ FromGroups),
    maps:to_list(Merged).

%% @private
normalise(#{mods := [Mod | _] = Mods} = Spec) ->
    Name = maps:get(name, Spec, group_name(Mod)),
    %% Thread the tree-engine selector and its parameters (PDDR-000002/000004): a
    %% group may pin `engine => plumtree | thicket' plus any engine options
    %% (`max_load'/`fanout'/`trees' for Thicket). Absent ⇒ the shell defaults to
    %% Plumtree.
    Opts = maps:merge(
        base_opts(),
        maps:with(
            [
                lazy_tick_period,
                exchange_tick_period,
                engine,
                max_load,
                fanout,
                trees
            ],
            Spec
        )
    ),
    {Name, Opts#{mods => Mods}}.

%% @private
base_opts() ->
    #{
        lazy_tick_period =>
            partisan_config:get(lazy_tick_period, ?DEFAULT_LAZY_TICK_PERIOD),
        exchange_tick_period =>
            partisan_config:get(
                exchange_tick_period, ?DEFAULT_EXCHANGE_TICK_PERIOD
            )
    }.

%% @private
childspec(Name, Opts) ->
    #{
        id => Name,
        start => {partisan_plumtree_broadcast, start_link, [Name, Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [partisan_plumtree_broadcast]
    }.
