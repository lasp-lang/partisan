%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests for Partisan's RPC path.
%%
%% These run against a single node. `partisan_rpc:call/5' forwards to the
%% registered `partisan_rpc_backend' on the target node, and
%% `partisan_pluggable_peer_service_manager:forward_message/4' short-circuits to
%% a local delivery when the target node is this node — so a self-directed call
%% exercises the whole backend path (dispatch -> backend -> execute -> reply)
%% without needing a cluster.
%%
%% The concurrency case is the important one: it fails against a backend that
%% applies `M:F(A)' inline in its `handle_info/2', because a single slow call
%% then head-of-line blocks every other call behind it in the server mailbox.
%% @end
%% =============================================================================
-module(partisan_rpc_test).

-include_lib("eunit/include/eunit.hrl").

%% Applied by the tests via RPC.
-export([echo/1]).
-export([slow_echo/2]).
-export([boom/0]).
-export([notify/2]).

rpc_test_() ->
    {timeout, 60,
        {foreach, fun setup/0, fun cleanup/1, [
            fun local_call_returns_result/0,
            fun call_to_crashing_function_returns_badrpc/0,
            fun slow_call_does_not_block_other_calls/0,
            fun call_honours_finite_timeout/0,
            fun async_call_and_yield/0,
            fun nb_yield_is_non_blocking/0,
            fun cast_returns_true_and_runs/0,
            fun multicall_returns_replies_and_badnodes/0,
            fun block_call_returns_result/0,
            fun abcast_delivers_to_registered_name/0,
            fun sbcast_partitions_good_and_bad_nodes/0,
            fun per_call_opts_win_over_global_forward_options/0,
            fun global_forward_options_fill_unspecified_keys/0,
            fun async_call_accepts_per_call_opts/0,
            fun cast_accepts_per_call_opts/0,
            fun multicall_accepts_an_opts_map_in_place_of_a_timeout/0,
            fun legacy_call_framing_is_still_served/0,
            fun legacy_call_framing_honours_the_concurrency_cap/0,
            fun unreachable_node_is_reported_as_nodedown/0,
            fun an_undeliverable_reply_does_not_kill_the_worker/0
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

%% A self-directed call reaches the backend, runs, and returns its result.
local_call_returns_result() ->
    Node = partisan:node(),
    ?assertEqual(
        pong, partisan_rpc:call(Node, ?MODULE, echo, [pong], 5000)
    ).

%% An exception in the applied function is translated, not propagated.
call_to_crashing_function_returns_badrpc() ->
    Node = partisan:node(),
    ?assertMatch(
        {badrpc, _}, partisan_rpc:call(Node, ?MODULE, boom, [], 5000)
    ).

%% THE regression test for head-of-line blocking.
%%
%% One caller issues a call that occupies the backend for `SlowMs'. While it is
%% in flight, an independent caller issues a fast call with a timeout far
%% shorter than `SlowMs'. If the backend executes user code inline, the fast
%% call cannot be served until the slow one finishes and therefore times out.
slow_call_does_not_block_other_calls() ->
    Node = partisan:node(),
    SlowMs = 3000,
    FastTimeoutMs = 1000,

    Self = self(),

    %% Occupy the backend from a *separate* process, so the two calls do not
    %% share a mailbox.
    Slow = spawn(fun() ->
        R = partisan_rpc:call(Node, timer, sleep, [SlowMs], SlowMs * 2),
        Self ! {slow_done, R}
    end),

    %% Give the backend time to pick the slow request up.
    timer:sleep(300),

    T0 = erlang:monotonic_time(millisecond),
    Result = partisan_rpc:call(Node, ?MODULE, echo, [fast], FastTimeoutMs),
    Elapsed = erlang:monotonic_time(millisecond) - T0,

    ?assertEqual(fast, Result),
    ?assert(Elapsed < FastTimeoutMs),

    %% And the slow call still completes correctly.
    receive
        {slow_done, SlowResult} ->
            ?assertEqual(ok, SlowResult)
    after SlowMs * 3 ->
        exit(Slow, kill),
        ?assert(false)
    end.

%% A finite timeout takes the transport path (not the local short-circuit) and
%% must still return the value.
call_honours_finite_timeout() ->
    Node = partisan:node(),
    ?assertEqual(v, partisan_rpc:call(Node, ?MODULE, echo, [v], 5000)),
    ?assertEqual(
        {badrpc, timeout},
        partisan_rpc:call(Node, ?MODULE, slow_echo, [v, 3000], 300)
    ).

%% `async_call' + `yield' — previously missing entirely, so transformed code
%% calling them hit `undef'.
async_call_and_yield() ->
    Key = partisan_rpc:async_call(partisan:node(), ?MODULE, echo, [async]),
    ?assertEqual(async, partisan_rpc:yield(Key)).

nb_yield_is_non_blocking() ->
    Key = partisan_rpc:async_call(
        partisan:node(), ?MODULE, slow_echo, [later, 1500]
    ),
    ?assertEqual(timeout, partisan_rpc:nb_yield(Key)),
    ?assertEqual({value, later}, partisan_rpc:nb_yield(Key, 5000)).

cast_returns_true_and_runs() ->
    Self = partisan:self(),
    Ref = make_ref(),
    ?assertEqual(
        true, partisan_rpc:cast(partisan:node(), ?MODULE, notify, [Self, Ref])
    ),
    receive
        {done, Ref} -> ok
    after 5000 -> ?assert(false)
    end.

%% `{Replies, BadNodes}' — an unreachable node must land in BadNodes, not
%% corrupt the reply list.
multicall_returns_replies_and_badnodes() ->
    Node = partisan:node(),
    Bogus = 'nonexistent@nowhere',

    {Replies, Bad} = partisan_rpc:multicall(
        [Node, Bogus], ?MODULE, echo, [m], 2000
    ),

    ?assertEqual([m], Replies),
    ?assertEqual([Bogus], Bad).

%% `block_call' runs on the backend server itself. It must still return the
%% right value; its serialisation is the documented semantic, not a defect.
block_call_returns_result() ->
    Node = partisan:node(),
    ?assertEqual(b, partisan_rpc:block_call(Node, ?MODULE, echo, [b], 5000)),
    ?assertMatch(
        {badrpc, {'EXIT', _}},
        partisan_rpc:block_call(Node, ?MODULE, boom, [], 5000)
    ).

abcast_delivers_to_registered_name() ->
    Ref = make_ref(),
    Self = self(),
    Pid = spawn(fun() ->
        receive
            M -> Self ! {got, M}
        end
    end),
    true = register(partisan_rpc_test_target, Pid),

    ?assertEqual(
        abcast,
        partisan_rpc:abcast([partisan:node()], partisan_rpc_test_target, Ref)
    ),

    receive
        {got, Got} -> ?assertEqual(Ref, Got)
    after 5000 -> ?assert(false)
    end.

%% Registered name present => good node; absent => bad node.
sbcast_partitions_good_and_bad_nodes() ->
    Node = partisan:node(),

    {Good0, Bad0} = partisan_rpc:sbcast(
        [Node], partisan_rpc_test_absent_name, hello
    ),
    ?assertEqual([], Good0),
    ?assertEqual([Node], Bad0),

    Self = self(),
    Pid = spawn(fun() ->
        receive
            M -> Self ! {got, M}
        end
    end),
    true = register(partisan_rpc_test_present, Pid),

    {Good1, Bad1} = partisan_rpc:sbcast(
        [Node], partisan_rpc_test_present, hello
    ),
    ?assertEqual([Node], Good1),
    ?assertEqual([], Bad1),

    receive
        {got, Got} -> ?assertEqual(hello, Got)
    after 5000 -> ?assert(false)
    end.

%% A per-call `channel' must survive a globally configured `forward_options'.
%%
%% The old code read `partisan_config:get(forward_options, CallerOpts)', which
%% makes the caller's options a mere *fallback*: `get/2' returns the configured
%% value whenever one is set, so setting the global silently discarded every
%% per-call channel and partition key.
per_call_opts_win_over_global_forward_options() ->
    Old = partisan_config:get(forward_options, undefined),
    ok = partisan_config:set(forward_options, #{channel => global_channel}),

    try
        Opts = effective_opts(#{channel => per_call_channel}),
        ?assertEqual(per_call_channel, maps:get(channel, Opts)),

        %% And the call itself still works end to end with those options.
        ?assertEqual(
            ok_value,
            partisan_rpc:call(
                partisan:node(),
                ?MODULE,
                echo,
                [ok_value],
                #{timeout => 5000, channel => partisan:default_channel()}
            )
        )
    after
        restore_forward_options(Old)
    end.

%% The global value is not ignored — it fills in whatever the caller omitted.
global_forward_options_fill_unspecified_keys() ->
    Old = partisan_config:get(forward_options, undefined),
    ok = partisan_config:set(forward_options, #{partition_key => 7}),

    try
        Opts = effective_opts(#{channel => per_call_channel}),
        ?assertEqual(per_call_channel, maps:get(channel, Opts)),
        ?assertEqual(7, maps:get(partition_key, Opts))
    after
        restore_forward_options(Old)
    end.

%% `rpc' has no per-call transport options at all — OTP's `rpc:async_call/4' is
%% the whole surface — so these arities are Partisan extensions that mirror the
%% ones added to `partisan_erpc'. The key they return is used with `yield/1'
%% exactly as usual: the options affect forwarding, nothing else.
async_call_accepts_per_call_opts() ->
    Key = partisan_rpc:async_call(
        partisan:node(),
        ?MODULE,
        echo,
        [async_opts],
        #{channel => partisan:default_channel()}
    ),
    ?assertEqual(async_opts, partisan_rpc:yield(Key)).

cast_accepts_per_call_opts() ->
    Self = partisan:self(),
    Ref = make_ref(),

    ?assertEqual(
        true,
        partisan_rpc:cast(partisan:node(), ?MODULE, notify, [Self, Ref], #{
            channel => partisan:default_channel()
        })
    ),

    receive
        {done, Ref} -> ok
    after 5000 -> ?assert(false)
    end.

%% The `{Replies, BadNodes}' contract must survive the options overload, and the
%% timeout must be read out of the map — a too-short one puts the node in
%% `BadNodes' rather than blocking or crashing.
multicall_accepts_an_opts_map_in_place_of_a_timeout() ->
    Node = partisan:node(),
    Chan = partisan:default_channel(),

    ?assertEqual(
        {[m], []},
        partisan_rpc:multicall([Node], ?MODULE, echo, [m], #{
            timeout => 5000, channel => Chan
        })
    ),

    ?assertEqual(
        {[], [Node]},
        partisan_rpc:multicall([Node], ?MODULE, slow_echo, [s, 2000], #{
            timeout => 200, channel => Chan
        })
    ).

%% Failure detection carries a *reason*, never a value, and the reason has to
%% arrive as the one `rpc' callers expect.
%%
%% `partisan_erpc' raises `error({partisan_erpc, noconnection})' when the target's
%% backend is unreachable — that is the reason `erpc' specifies — and the vendored
%% `rpcify_exception/2' maps precisely that to `{badrpc, nodedown}'. The two ends
%% of that translation were written independently (one in the transport, one in a
%% verbatim copy of OTP's `rpc'), so the join between them is worth pinning.
unreachable_node_is_reported_as_nodedown() ->
    Bogus = 'nonexistent@nowhere',

    ?assertEqual(
        {badrpc, nodedown},
        partisan_rpc:call(Bogus, ?MODULE, echo, [never_runs], 5000)
    ),

    %% The same failure on the surface underneath, untranslated.
    ?assertError(
        {partisan_erpc, noconnection},
        partisan_erpc:call(Bogus, ?MODULE, echo, [never_runs], 5000)
    ).

%% A reply the worker cannot deliver is not an error *in the worker*.
%%
%% It means the caller's node went away between the request and the reply, which
%% the caller detects for itself — `partisan_erpc' holds a monitor precisely so
%% that it reports `noconnection'. The worker used to write
%% `ok = partisan:forward_message(...)', which looked total because the spec read
%% `-> ok'. It never was: forwarding returns `{error, disconnected}' when there
%% is no usable connection, so the worker died with a badmatch and logged a crash
%% report for an entirely routine condition.
an_undeliverable_reply_does_not_kill_the_worker() ->
    Unreachable = {some_registered_name, 'nonexistent@nowhere'},

    %% Sanity: this really is undeliverable, so the case below is exercising
    %% the failure branch rather than passing vacuously.
    ?assertMatch(
        {error, _},
        partisan:forward_message(Unreachable, a_message, #{
            channel => partisan:default_channel()
        })
    ),

    %% And sending a reply to it returns rather than raising.
    ?assertEqual(
        ok,
        partisan_rpc_backend:send_reply(Unreachable, {rpc_response, v}, #{
            channel => partisan:default_channel()
        })
    ).

%% =============================================================================
%% ROLLING UPGRADE — the pre-6.0.0 request framing
%% =============================================================================
%%
%% `{call, M, F, A, Timeout, {origin, Caller}}' is how a 5.x node asks for an
%% RPC. No 6.x node sends it any more — `partisan_rpc:call/5' is a shim over
%% `partisan_erpc' and emits the correlated protocol — but the backend must still
%% *serve* it, or upgrading one node at a time would break RPCs issued by the
%% peers not yet upgraded. These cases pin that receiver so it cannot be dropped
%% by accident before 7.0.0; see the `partisan_rpc_backend' module doc.
%%
%% They speak the wire protocol directly rather than through any API, because the
%% API deliberately no longer produces it.
legacy_call_framing_is_still_served() ->
    ok = send_legacy(echo, [legacy]),

    receive
        {rpc_response, Response} -> ?assertEqual(legacy, Response)
    after 5000 ->
        ?assert(false)
    end.

%% The concurrency bound must cover this framing too. An un-upgraded peer is
%% still a peer, and a cap that any 5.x caller can walk straight past is not a
%% cap — the legacy clause used to spawn without consulting it.
legacy_call_framing_honours_the_concurrency_cap() ->
    Old = partisan_config:get(rpc_max_concurrency, 10000),
    ok = partisan_config:set(rpc_max_concurrency, 1),

    try
        %% Occupy the only slot.
        ok = send_legacy(slow_echo, [held, 2000]),
        timer:sleep(300),

        %% This one has nowhere to run. The legacy protocol has no error channel
        %% of its own, so the rejection has to arrive as a `badrpc'.
        ok = send_legacy(echo, [rejected]),

        receive
            {rpc_response, First} ->
                ?assertEqual({badrpc, overloaded}, First)
        after 5000 ->
            ?assert(false)
        end,

        %% The request already in flight still completes normally, and its slot
        %% is returned — the rejection must not have consumed one.
        receive
            {rpc_response, Second} ->
                ?assertEqual(held, Second)
        after 10000 ->
            ?assert(false)
        end
    after
        partisan_config:set(rpc_max_concurrency, Old)
    end.

send_legacy(Function, Args) ->
    Msg = {call, ?MODULE, Function, Args, 5000, {origin, partisan:self()}},
    partisan:forward_message(
        partisan:node(),
        partisan_rpc_backend,
        Msg,
        #{channel => partisan:default_channel()}
    ).

%% Calls the *production* resolver that `partisan_rpc:call/5' uses, so this
%% asserts the real precedence rather than a copy of it restated in the test.
effective_opts(CallerOpts) ->
    partisan_rpc:forward_opts(CallerOpts).

restore_forward_options(undefined) ->
    partisan_config:set(forward_options, #{});
restore_forward_options(Old) ->
    partisan_config:set(forward_options, Old).

%% =============================================================================
%% FUNCTIONS APPLIED BY THE TESTS
%% =============================================================================

echo(X) ->
    X.

slow_echo(X, Ms) ->
    timer:sleep(Ms),
    X.

boom() ->
    error(deliberate).

notify(Origin, Ref) ->
    partisan:forward_message(Origin, {done, Ref}, #{}).
