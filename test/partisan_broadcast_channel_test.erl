%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests for per-group broadcast channel selection.
%%
%% A broadcast group's traffic used to ride whatever channel its *handler module*
%% named in `broadcast_channel/0', and nothing else. So putting a handler on a
%% dedicated channel meant editing that handler — impossible for one you do not
%% own, and awkward for one shared between deployments that want different
%% answers.
%%
%% A group may now declare `channel' in its spec. The declared channel wins; its
%% absence falls back to the handler callback, which is what every group did
%% before, so this is additive.
%%
%% The channel is deliberately a property of the **group**, not of an individual
%% `broadcast/2' call — see `partisan_plumtree_broadcast:send/4' for why: repair
%% traffic follows the tree, so a per-message channel would send a grafted
%% payload back on the group's channel rather than the message's, defeating the
%% separation exactly when the network is under stress.
%% @end
%% =============================================================================
-module(partisan_broadcast_channel_test).

-include_lib("eunit/include/eunit.hrl").

%% Used as a handler module by the tests below.
-export([broadcast_channel/0]).
-export([broadcast_data/1]).
-export([merge/2]).
-export([is_stale/1]).
-export([graft/1]).
-export([exchange/1]).

-define(BCAST, partisan_broadcast).
-define(HANDLER_CHANNEL, channel_from_the_handler).
-define(GROUP_CHANNEL, channel_from_the_group).

channel_test_() ->
    {timeout, 60,
        {foreach, fun setup/0, fun cleanup/1, [
            fun group_spec_carries_the_channel/0,
            fun group_spec_without_a_channel_omits_it/0,
            fun declared_channel_wins_over_the_handler_callback/0,
            fun handler_callback_is_used_when_the_group_declares_nothing/0,
            fun default_channel_when_neither_is_declared/0
        ]}}.

setup() ->
    stop_partisan(),
    {ok, _} = application:ensure_all_started(partisan),
    ok.

cleanup(_) ->
    stop_partisan().

stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.

%% =============================================================================
%% THE GROUP SPEC
%% =============================================================================

%% `channel' has to survive `normalise/1'. It is filtered through a `maps:with/2'
%% allow-list, so a key that is not in that list is dropped silently — which is
%% the failure mode this pins.
group_spec_carries_the_channel() ->
    {ok, Pid} = ?BCAST:start_group(#{
        mods => [?MODULE],
        channel => ?GROUP_CHANNEL
    }),
    ?assert(is_pid(Pid)),

    try
        ?assertEqual(
            ?GROUP_CHANNEL,
            partisan_plumtree_broadcast:group_channel(?BCAST:group_name(?MODULE))
        )
    after
        ?BCAST:stop_group(?MODULE)
    end.

%% Absent means "ask the handler" — not "use the default channel". The
%% distinction matters: `undefined' *is* the default channel's name, so storing
%% it eagerly would be indistinguishable from a group that had declared it, and
%% would silently override the handler callback.
group_spec_without_a_channel_omits_it() ->
    {ok, _} = ?BCAST:start_group(#{mods => [?MODULE]}),

    try
        ?assertEqual(
            undefined,
            partisan_plumtree_broadcast:group_channel(?BCAST:group_name(?MODULE))
        )
    after
        ?BCAST:stop_group(?MODULE)
    end.

%% =============================================================================
%% RESOLUTION
%% =============================================================================

declared_channel_wins_over_the_handler_callback() ->
    ?assertEqual(
        ?GROUP_CHANNEL,
        partisan_plumtree_broadcast:channel(?GROUP_CHANNEL, ?MODULE)
    ).

handler_callback_is_used_when_the_group_declares_nothing() ->
    ?assertEqual(
        ?HANDLER_CHANNEL,
        partisan_plumtree_broadcast:channel(undefined, ?MODULE)
    ).

%% A handler with no `broadcast_channel/0' and a group with no `channel' lands on
%% the default channel, exactly as before either mechanism existed.
default_channel_when_neither_is_declared() ->
    ?assertEqual(
        partisan:default_channel(),
        partisan_plumtree_broadcast:channel(
            undefined, partisan_broadcast_channel_test_no_callback
        )
    ).

%% =============================================================================
%% HANDLER CALLBACKS
%% =============================================================================

broadcast_channel() ->
    ?HANDLER_CHANNEL.

broadcast_data({Id, Payload}) ->
    {Id, Payload}.

merge(_Id, _Payload) ->
    true.

is_stale(_Id) ->
    false.

graft(_Id) ->
    {error, not_found}.

exchange(_Peer) ->
    ignore.
