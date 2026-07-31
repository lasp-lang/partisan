%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(partisan_telemetry).

-moduledoc """
Thin wrapper around `telemetry:execute/3` used by every `[partisan, ...]` event
in the codebase (see `telemetry.md`).

It exists to stop two things from drifting as new events are added: every event
needs `node => partisan:node()` in its metadata, and a delta-counter event's
measurement (`count => 1`, meant to be summed into a rate) must never be
confused with a gauge's live value — see `count/2` vs `execute/3`.
""".

-export([execute/3]).
-export([count/2]).

-doc """
Emits a telemetry event, injecting `node => partisan:node()` into `Metadata`
(an explicit `node` key in `Metadata` wins). Use directly for gauges and any
event with its own measurement shape; use `count/2` for a pure delta counter.
""".
-spec execute(
    EventName :: [atom()],
    Measurements :: map(),
    Metadata :: map()
) -> ok.

execute(EventName, Measurements, Metadata) when
    is_list(EventName), is_map(Measurements), is_map(Metadata)
->
    telemetry:execute(
        EventName, Measurements, maps:merge(#{node => local_node()}, Metadata)
    ).

%% @private
%% `partisan:node()' reads the `name' config key with no default and raises
%% `badarg' if it is not yet set — reachable in practice, since some telemetry
%% (e.g. `[partisan, channel, configured]') can fire from `partisan_config:set/2'
%% itself, before a node has finished configuring its own name. Every telemetry
%% call site funnels through here, so this is the one place that needs to
%% tolerate that rather than every future event having to know about it.
%%
%% `undefined' rather than `erlang:node()' is deliberate: Partisan's configured
%% `name' can differ from the underlying distributed-Erlang node name, so
%% falling back to the latter would report a node identity that never
%% reappears in any later event once the real name is configured — a
%% consumer aggregating by `node' would see one non-matching, short-lived
%% label. An explicit "not known yet" cannot be misread as a real node.
local_node() ->
    try
        partisan:node()
    catch
        error:badarg -> undefined
    end.

-doc """
Emits a delta-counter event: `#{count => 1}`, meant to be summed over a window
into a rate. `count` is reserved for this meaning — a gauge's live value must
use a different measurement key (e.g. `size`, `total`, `value`).
""".
-spec count(EventName :: [atom()], Metadata :: map()) -> ok.

count(EventName, Metadata) when is_list(EventName), is_map(Metadata) ->
    execute(EventName, #{count => 1}, Metadata).
