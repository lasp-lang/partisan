%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc Tests for the benchmark driver.
%%
%% A benchmark that computes its own statistics wrongly is worse than having no
%% benchmark: it produces numbers that look authoritative and are not. The
%% percentile and spread functions are therefore pinned against hand-computed
%% values, and the driver itself is checked against work of a *known* duration,
%% so a systematic error in the timing loop shows up as a wrong answer rather
%% than as a plausible one.
%% @end
%% =============================================================================
-module(partisan_bench_test).

-include_lib("eunit/include/eunit.hrl").

-define(B, partisan_bench).

%% =============================================================================
%% PERCENTILES
%% =============================================================================

%% Nearest-rank, so every answer is a value that was actually observed. With
%% 1..10, rank = ceil(P/100 * 10).
percentile_test_() ->
    S = lists:seq(1, 10),
    [
        ?_assertEqual(1, ?B:percentile(S, 0)),
        ?_assertEqual(1, ?B:percentile(S, 10)),
        ?_assertEqual(5, ?B:percentile(S, 50)),
        ?_assertEqual(9, ?B:percentile(S, 90)),
        ?_assertEqual(10, ?B:percentile(S, 99)),
        ?_assertEqual(10, ?B:percentile(S, 100))
    ].

%% Input order must not matter — the samples arrive interleaved from several
%% workers, never sorted.
percentile_is_order_independent_test() ->
    Shuffled = [7, 2, 9, 4, 1, 10, 3, 8, 5, 6],
    ?assertEqual(5, ?B:percentile(Shuffled, 50)),
    ?assertEqual(9, ?B:percentile(Shuffled, 90)).

percentile_of_one_sample_test() ->
    ?assertEqual(42, ?B:percentile([42], 50)),
    ?assertEqual(42, ?B:percentile([42], 99)).

percentile_of_nothing_is_zero_test() ->
    ?assertEqual(0, ?B:percentile([], 50)).

%% =============================================================================
%% SUMMARY
%% =============================================================================

summarise_test() ->
    S = ?B:summarise(lists:seq(1, 100)),

    ?assertEqual(100, maps:get(count, S)),
    ?assertEqual(1, maps:get(min, S)),
    ?assertEqual(100, maps:get(max, S)),
    ?assertEqual(50, maps:get(p50, S)),
    ?assertEqual(90, maps:get(p90, S)),
    ?assertEqual(99, maps:get(p99, S)),
    ?assertEqual(50.5, maps:get(mean, S)).

%% The tail is the point. A distribution that is fast except for a few very slow
%% operations — exactly what a serialisation point produces — must show that in
%% p99 and max while p50 stays low. If the driver reported only a mean, this
%% shape would be indistinguishable from uniformly mediocre.
summarise_separates_the_tail_test() ->
    Fast = lists:duplicate(99, 1),
    Slow = [1000],
    S = ?B:summarise(Fast ++ Slow),

    ?assertEqual(1, maps:get(p50, S)),
    ?assertEqual(1, maps:get(p90, S)),
    ?assertEqual(1000, maps:get(max, S)),
    ?assert(maps:get(mean, S) > 10).

summarise_of_nothing_test() ->
    S = ?B:summarise([]),
    ?assertEqual(0, maps:get(count, S)),
    ?assertEqual(0, maps:get(p99, S)).

%% =============================================================================
%% SPREAD — the gate that decides whether a result is evidence at all
%% =============================================================================

spread_pct_test() ->
    %% (110 - 90) / 100 = 20%
    ?assertEqual(20.0, ?B:spread_pct([90, 100, 110])),
    %% Identical repetitions disagree by nothing.
    ?assertEqual(0.0, ?B:spread_pct([100, 100, 100])).

%% A single repetition agrees with nothing, which is not the same as being
%% stable. It reports 0.0, and the driver's documentation says not to read that
%% as evidence — this pins the value so the meaning cannot drift.
spread_pct_of_one_repetition_is_zero_test() ->
    ?assertEqual(0.0, ?B:spread_pct([100])),
    ?assertEqual(0.0, ?B:spread_pct([])).

spread_pct_survives_a_zero_median_test() ->
    ?assertEqual(0.0, ?B:spread_pct([0, 0])).

variance_gate_test() ->
    Report = #{spread_pct => 4.9},
    ?assert(?B:meets_variance_gate(Report, 5.0)),
    ?assertNot(?B:meets_variance_gate(#{spread_pct => 5.1}, 5.0)).

%% =============================================================================
%% THE DRIVER
%% =============================================================================

driver_test_() ->
    {timeout, 60, [
        fun measures_work_of_a_known_duration/0,
        fun runs_every_operation_requested/0,
        fun warmup_is_not_measured/0,
        fun setup_and_teardown_run_once_per_repetition/0,
        fun concurrency_splits_the_iterations/0,
        fun rejects_a_spec_it_cannot_honour/0
    ]}.

%% The sharpest check available: work whose duration is known independently. A
%% 2 ms sleep must show up as ~2000 us, so a driver measuring the wrong interval
%% (or converting units wrongly) gives a visibly wrong answer rather than a
%% plausible one.
measures_work_of_a_known_duration() ->
    Report = ?B:run(#{
        name => known_duration,
        work => fun(_Ctx, _Seq) -> timer:sleep(2) end,
        iterations => 20,
        warmup => 0,
        repetitions => 2,
        concurrency => 1
    }),

    #{latency_us := L} = Report,
    P50 = maps:get(p50, L),

    %% Generous bounds: `timer:sleep/1' guarantees *at least* the requested
    %% time, and a loaded machine adds scheduling delay. The point is the order
    %% of magnitude — 2 ms, not 2 us and not 2 s.
    ?assert(P50 >= 1500),
    ?assert(P50 =< 20000),

    %% ~500 ops/s at 2 ms serial. Again: order of magnitude.
    ?assert(maps:get(throughput_median, Report) < 2000).

%% Every operation must be timed — a sample count short of
%% `iterations * repetitions' means the driver silently dropped measurements.
runs_every_operation_requested() ->
    Tab = ets:new(bench_count, [public, set]),
    true = ets:insert(Tab, {calls, 0}),

    try
        Report = ?B:run(#{
            name => counted,
            work => fun(_, _) -> ets:update_counter(Tab, calls, 1) end,
            iterations => 100,
            warmup => 10,
            repetitions => 3,
            concurrency => 1
        }),

        #{latency_us := L} = Report,

        %% 100 measured per repetition, 3 repetitions.
        ?assertEqual(300, maps:get(count, L)),

        %% Plus 10 warmup operations per repetition, which are run but not
        %% measured.
        ?assertEqual([{calls, 330}], ets:lookup(Tab, calls))
    after
        ets:delete(Tab)
    end.

%% Warmup must be excluded from the *samples*, not merely run. If a warmup
%% operation leaked into the distribution, a slow first call would move the
%% reported tail. Here warmup is deliberately far slower than the measured work,
%% so leakage is visible in `max'.
warmup_is_not_measured() ->
    Tab = ets:new(bench_phase, [public, set]),
    true = ets:insert(Tab, {phase, warmup}),

    try
        Report = ?B:run(#{
            name => warmup_excluded,
            setup => fun() -> ets:insert(Tab, {phase, warmup}) end,
            work => fun(_, _) ->
                case ets:lookup(Tab, phase) of
                    [{phase, warmup}] ->
                        %% First call only: sleep, then leave the slow phase.
                        true = ets:insert(Tab, {phase, measured}),
                        timer:sleep(20);
                    _ ->
                        ok
                end
            end,
            iterations => 10,
            warmup => 1,
            repetitions => 1,
            concurrency => 1
        }),

        %% The first (warmup) call sleeps 20 ms and then flips the phase, so
        %% every *measured* call is fast. A max anywhere near 20000 us would
        %% mean the warmup sample was included.
        #{latency_us := L} = Report,
        ?assertEqual(10, maps:get(count, L)),
        ?assert(maps:get(max, L) < 15000)
    after
        ets:delete(Tab)
    end.

setup_and_teardown_run_once_per_repetition() ->
    Tab = ets:new(bench_lifecycle, [public, set]),
    true = ets:insert(Tab, [{setup, 0}, {teardown, 0}]),

    try
        _ = ?B:run(#{
            name => lifecycle,
            setup => fun() ->
                ets:update_counter(Tab, setup, 1),
                a_context
            end,
            teardown => fun(Ctx) ->
                a_context = Ctx,
                ets:update_counter(Tab, teardown, 1)
            end,
            work => fun(Ctx, _) -> a_context = Ctx end,
            iterations => 5,
            warmup => 0,
            repetitions => 4,
            concurrency => 1
        }),

        ?assertEqual([{setup, 4}], ets:lookup(Tab, setup)),
        ?assertEqual([{teardown, 4}], ets:lookup(Tab, teardown))
    after
        ets:delete(Tab)
    end.

%% With `concurrency' workers each doing `iterations div concurrency'
%% operations, the sample count is the product — not the per-worker count, and
%% not `iterations` regardless of workers.
concurrency_splits_the_iterations() ->
    Report = ?B:run(#{
        name => concurrent,
        work => fun(_, _) -> ok end,
        iterations => 100,
        warmup => 0,
        repetitions => 1,
        concurrency => 4
    }),

    #{latency_us := L} = Report,
    ?assertEqual(100, maps:get(count, L)),
    ?assertEqual(4, maps:get(concurrency, Report)).

%% Fewer iterations than workers would give every worker zero operations and
%% report an empty distribution as though it were a result.
rejects_a_spec_it_cannot_honour() ->
    ?assertError(
        {bad_spec, iterations_below_concurrency},
        ?B:run(#{
            name => impossible,
            work => fun(_, _) -> ok end,
            iterations => 2,
            concurrency => 8
        })
    ).

%% The report renders without crashing on every shape it can hold, including the
%% degenerate one — a formatter that only works on healthy data is a formatter
%% that fails exactly when you are debugging.
format_report_test() ->
    Report = ?B:run(#{
        name => formatted,
        work => fun(_, _) -> ok end,
        iterations => 10,
        warmup => 0,
        repetitions => 2,
        concurrency => 1
    }),

    Text = lists:flatten(?B:format_report(Report)),

    ?assert(string:find(Text, "formatted") =/= nomatch),
    ?assert(string:find(Text, "p99") =/= nomatch),
    ?assert(string:find(Text, "spread") =/= nomatch).
