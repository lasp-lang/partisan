%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests for wire encoding.
%%
%% A message is encoded either inside the connection process or in the calling
%% process and handed to the connection as `iodata()' (the `send_encoded' path
%% in `partisan_peer_service_client'). Both paths must put identical bytes on
%% the wire, because a peer decodes whatever arrives with a plain
%% `binary_to_term/1' (`partisan_peer_service_server:88') and has no way to tell
%% which path produced it.
%%
%% `partisan_util:channel_encode_opts/1' is the single source of truth for that
%% derivation — the connection caches it at init, the dispatch path computes it
%% per send — so these tests pin its behaviour for every shape the `compression'
%% channel option can take.
%% @end
%% =============================================================================
-module(partisan_encoding_test).

-include_lib("eunit/include/eunit.hrl").

%% -----------------------------------------------------------------------------
%% The `compression' channel option maps onto `erlang:term_to_iovec/2' options.
%% These cases mirror exactly what the connection process derived inline before
%% the derivation was extracted, including the fall-through for out-of-range and
%% non-integer values.
%% -----------------------------------------------------------------------------
channel_encode_opts_test_() ->
    [
        ?_assertEqual(
            [compressed],
            partisan_util:channel_encode_opts(#{compression => true})
        ),
        ?_assertEqual(
            [{compressed, 0}],
            partisan_util:channel_encode_opts(#{compression => 0})
        ),
        ?_assertEqual(
            [{compressed, 6}],
            partisan_util:channel_encode_opts(#{compression => 6})
        ),
        ?_assertEqual(
            [{compressed, 9}],
            partisan_util:channel_encode_opts(#{compression => 9})
        ),
        %% Out of range falls through to "no compression".
        ?_assertEqual(
            [], partisan_util:channel_encode_opts(#{compression => 10})
        ),
        ?_assertEqual(
            [], partisan_util:channel_encode_opts(#{compression => false})
        ),
        %% Absent option is the common case: every default channel.
        ?_assertEqual([], partisan_util:channel_encode_opts(#{}))
    ].

roundtrip_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        fun encodes_to_something_the_peer_can_decode/0,
        fun compression_does_not_change_the_decoded_term/0,
        fun dispatch_many_defers_every_peer_it_cannot_route/0,
        fun dispatch_many_defers_all_when_fast_forward_disabled/0,
        fun dispatch_many_defers_the_local_node/0
    ]}.

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

%% The exact shape the dispatch path encodes, decoded the way the receiving
%% side decodes it.
encodes_to_something_the_peer_can_decode() ->
    Term = forward_message_term(),
    Opts = partisan_util:channel_encode_opts(#{}),
    Data = partisan_util:encode(Term, Opts),
    ?assertEqual(Term, binary_to_term(iolist_to_binary(Data))).

%% A compressed channel must decode to the same term as an uncompressed one —
%% compression is a transport detail, not a semantic one.
compression_does_not_change_the_decoded_term() ->
    Term = forward_message_term(),

    Plain = partisan_util:encode(
        Term, partisan_util:channel_encode_opts(#{})
    ),
    Compressed = partisan_util:encode(
        Term, partisan_util:channel_encode_opts(#{compression => true})
    ),

    ?assertEqual(
        binary_to_term(iolist_to_binary(Plain)),
        binary_to_term(iolist_to_binary(Compressed))
    ).

%% `dispatch_many/4' is a fast path only. Every peer it cannot route must come
%% back in the deferred list so the caller sends to it the ordinary way —
%% otherwise a fan-out would silently drop peers.
dispatch_many_defers_every_peer_it_cannot_route() ->
    Peers = ['nonexistent1@nowhere', 'nonexistent2@nowhere'],

    Deferred = partisan_peer_connections:dispatch_many(
        Peers, some_group, {'$gen_cast', hello}, partisan:default_channel()
    ),

    %% No connections exist for these, so none may be silently swallowed.
    ?assertEqual(lists:sort(Peers), lists:sort(Deferred)).

%% `disable_fast_forward' exists to force traffic through the peer service
%% manager; the batch path must honour it by declining everything.
dispatch_many_defers_all_when_fast_forward_disabled() ->
    Old = partisan_config:get(disable_fast_forward, false),
    ok = partisan_config:set(disable_fast_forward, true),

    try
        Peers = ['a@nowhere', 'b@nowhere'],
        ?assertEqual(
            lists:sort(Peers),
            lists:sort(
                partisan_peer_connections:dispatch_many(
                    Peers,
                    some_group,
                    {'$gen_cast', hello},
                    partisan:default_channel()
                )
            )
        )
    after
        partisan_config:set(disable_fast_forward, Old)
    end.

%% The local node is a direct delivery, not a connection send.
dispatch_many_defers_the_local_node() ->
    Self = partisan:node(),

    ?assertEqual(
        [Self],
        partisan_peer_connections:dispatch_many(
            [Self], some_group, {'$gen_cast', hello}, partisan:default_channel()
        )
    ),

    %% And an empty peer list is a no-op.
    ?assertEqual(
        [],
        partisan_peer_connections:dispatch_many(
            [], some_group, {'$gen_cast', hello}, partisan:default_channel()
        )
    ).

%% A representative payload: this is literally what
%% `partisan_peer_connections:do_dispatch/5' encodes, with a `cast_message'
%% body of the kind the broadcast path produces.
forward_message_term() ->
    {forward_message, some_registered_name,
        {'$gen_cast', {broadcast, [1, 2, 3], <<"payload">>, #{k => v}}}}.
