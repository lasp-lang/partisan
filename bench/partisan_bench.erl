%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc A repeatable benchmark driver.
%%
%% This exists because Partisan had no way to state a performance claim. The
%% pre-existing `partisan_SUITE:performance_test' wraps a whole run in
%% `timer:tc/1' and appends one wall-clock number to a CSV. That measures
%% *something*, but it cannot answer the questions a change to the data path
%% raises — did the tail get worse, is the difference bigger than the noise, was
%% the first iteration paying for connection setup — so it cannot be used to
%% accept or reject an optimisation.
%%
%% This driver is deliberately ignorant of Partisan. It knows about warmup,
%% repetitions, per-operation timing, percentiles and run-to-run spread. The
%% scenarios live in `partisan_bench_SUITE', which owns the cluster.
%%
%% == The measurement ==
%%
%% A run is `repetitions' independent repetitions. Each repetition spawns
%% `concurrency' workers, each of which executes `iterations div concurrency'
%% operations, timing every one. A `warmup' count of operations runs first and is
%% discarded — the first messages to a peer pay for connection establishment,
%% code loading and the first ETS/atomics touches, and including them turns a
%% steady-state question into a startup question.
%%
%% Two numbers matter and both are reported:
%%
%% <ul>
%% <li><strong>Latency percentiles</strong>, from every individual operation.
%% p50 tells you the common case; p99 and max tell you what a serialisation point
%% does to the tail, which is the whole subject of the concurrency work.</li>
%% <li><strong>Throughput</strong>, operations per second, wall-clock per
%% repetition. This is what a mean latency cannot give you once there is
%% concurrency.</li>
%% </ul>
%%
%% == The spread gate ==
%%
%% `spread_pct' is `(max - min) / median' across repetition throughputs. A
%% benchmark whose own repetitions disagree by more than a few percent cannot
%% resolve a change smaller than that disagreement, so the report carries the
%% number and {@link meets_variance_gate/2} answers the question directly. **A
%% result whose spread exceeds the gate is not evidence**, whichever direction it
%% points.
%% @end
%% =============================================================================
-module(partisan_bench).

-export([run/1]).
-export([format_report/1]).
-export([meets_variance_gate/2]).

%% Exported for test: these are the parts that can be silently wrong, and a
%% benchmark that computes its statistics incorrectly is worse than none.
-export([percentile/2]).
-export([summarise/1]).
-export([spread_pct/1]).

-type spec() :: #{
    name := atom() | string(),
    %% One measured operation. Receives the context from `setup' and the
    %% operation's 0-based sequence number within its worker.
    work := fun((Ctx :: any(), Seq :: non_neg_integer()) -> any()),
    %% Run once per repetition, before warmup.
    setup => fun(() -> any()),
    %% Run once per repetition, after the measured operations.
    teardown => fun((Ctx :: any()) -> any()),
    iterations => pos_integer(),
    warmup => non_neg_integer(),
    repetitions => pos_integer(),
    concurrency => pos_integer(),
    %% Bounds how long one repetition may take before it is abandoned.
    timeout => timeout()
}.

-type stats() :: #{
    count := non_neg_integer(),
    min := number(),
    p50 := number(),
    p90 := number(),
    p99 := number(),
    max := number(),
    mean := number()
}.

-type report() :: #{
    name := atom() | string(),
    concurrency := pos_integer(),
    iterations := pos_integer(),
    repetitions := pos_integer(),
    %% Latency in microseconds, pooled across all repetitions.
    latency_us := stats(),
    %% Operations per second, one entry per repetition.
    throughput_ops := [float()],
    throughput_median := float(),
    spread_pct := float()
}.

-export_type([spec/0]).
-export_type([report/0]).
-export_type([stats/0]).

-define(DEFAULT_ITERATIONS, 10000).
-define(DEFAULT_WARMUP, 1000).
-define(DEFAULT_REPETITIONS, 3).
-define(DEFAULT_CONCURRENCY, 1).
-define(DEFAULT_TIMEOUT, 300000).

%% =============================================================================
%% API
%% =============================================================================

-spec run(spec()) -> report().

run(#{name := Name, work := Work} = Spec) when is_function(Work, 2) ->
    Iterations = maps:get(iterations, Spec, ?DEFAULT_ITERATIONS),
    Warmup = maps:get(warmup, Spec, ?DEFAULT_WARMUP),
    Repetitions = maps:get(repetitions, Spec, ?DEFAULT_REPETITIONS),
    Concurrency = maps:get(concurrency, Spec, ?DEFAULT_CONCURRENCY),

    Iterations >= Concurrency orelse
        error({bad_spec, iterations_below_concurrency}),

    Results = [
        repetition(Spec, Iterations, Warmup, Concurrency)
     || _ <- lists:seq(1, Repetitions)
    ],

    Samples = lists:append([S || {S, _Ops} <- Results]),
    Throughputs = [Ops || {_S, Ops} <- Results],

    #{
        name => Name,
        concurrency => Concurrency,
        iterations => Iterations,
        repetitions => Repetitions,
        latency_us => summarise(Samples),
        throughput_ops => Throughputs,
        throughput_median => median(Throughputs),
        spread_pct => spread_pct(Throughputs)
    };
run(_) ->
    error(badarg).

%% -----------------------------------------------------------------------------
%% @doc Answers the gate directly: are this run's repetitions consistent enough
%% for the result to mean anything? `MaxPct' is a percentage, e.g. `5.0'.
%% @end
%% -----------------------------------------------------------------------------
-spec meets_variance_gate(report(), number()) -> boolean().

meets_variance_gate(#{spread_pct := Spread}, MaxPct) ->
    Spread =< MaxPct.

-spec format_report(report()) -> iolist().

format_report(#{latency_us := L} = R) ->
    io_lib:format(
        "~n"
        "  ~s~n"
        "    concurrency ~p, ~p iterations x ~p repetitions~n"
        "    latency us   p50 ~.1f   p90 ~.1f   p99 ~.1f   max ~.1f   "
        "(n=~p)~n"
        "    throughput   ~.1f ops/s (median of ~p)   spread ~.2f%~n",
        [
            to_text(maps:get(name, R)),
            maps:get(concurrency, R),
            maps:get(iterations, R),
            maps:get(repetitions, R),
            float(maps:get(p50, L)),
            float(maps:get(p90, L)),
            float(maps:get(p99, L)),
            float(maps:get(max, L)),
            maps:get(count, L),
            maps:get(throughput_median, R),
            maps:get(repetitions, R),
            maps:get(spread_pct, R)
        ]
    ).

%% =============================================================================
%% STATISTICS — exported for test
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc The `P'th percentile of `Samples' (`P' in 0..100), by the
%% nearest-rank method: the smallest value at or below which at least `P'% of
%% the samples fall.
%%
%% Nearest-rank is chosen over interpolation deliberately — every reported value
%% is then an observation that actually happened, which matters for a tail
%% statistic used to argue about a serialisation point.
%% @end
%% -----------------------------------------------------------------------------
-spec percentile([number()], number()) -> number().

percentile([], _P) ->
    0;
percentile(Samples, P) when P >= 0, P =< 100 ->
    Sorted = lists:sort(Samples),
    N = length(Sorted),
    %% Rank is 1-based and always within 1..N.
    Rank = max(1, min(N, ceil(P / 100 * N))),
    lists:nth(Rank, Sorted).

-spec summarise([number()]) -> stats().

summarise([]) ->
    #{count => 0, min => 0, p50 => 0, p90 => 0, p99 => 0, max => 0, mean => 0};
summarise(Samples) ->
    Sorted = lists:sort(Samples),
    N = length(Sorted),
    #{
        count => N,
        min => hd(Sorted),
        p50 => percentile(Sorted, 50),
        p90 => percentile(Sorted, 90),
        p99 => percentile(Sorted, 99),
        max => lists:last(Sorted),
        mean => lists:sum(Sorted) / N
    }.

%% -----------------------------------------------------------------------------
%% @doc `(max - min) / median', as a percentage. Zero for fewer than two
%% samples — a single repetition disagrees with nothing, which is not the same
%% as being stable, so callers should not read 0.0 from one repetition as
%% evidence of anything.
%% @end
%% -----------------------------------------------------------------------------
-spec spread_pct([number()]) -> float().

spread_pct(Values) when length(Values) < 2 ->
    0.0;
spread_pct(Values) ->
    case median(Values) of
        0 ->
            0.0;
        0.0 ->
            0.0;
        Median ->
            (lists:max(Values) - lists:min(Values)) / Median * 100
    end.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% One repetition: setup, warmup, measure, teardown. Returns the operation
%% latencies (microseconds) and the repetition's throughput (ops/s).
repetition(Spec, Iterations, Warmup, Concurrency) ->
    Setup = maps:get(setup, Spec, fun() -> undefined end),
    Teardown = maps:get(teardown, Spec, fun(_) -> ok end),
    Timeout = maps:get(timeout, Spec, ?DEFAULT_TIMEOUT),
    Work = maps:get(work, Spec),

    Ctx = Setup(),

    try
        %% Warmup is discarded. The first operations to a peer pay for
        %% connection establishment and first-touch costs that have nothing to
        %% do with steady-state throughput.
        ok = warmup(Work, Ctx, Warmup),

        PerWorker = Iterations div Concurrency,
        Total = PerWorker * Concurrency,

        Parent = self(),
        Ref = make_ref(),

        T0 = erlang:monotonic_time(),

        Pids = [
            spawn_link(fun() ->
                Samples = measure(Work, Ctx, PerWorker, []),
                Parent ! {Ref, self(), Samples}
            end)
         || _ <- lists:seq(1, Concurrency)
        ],

        Samples = collect(Ref, Pids, Timeout, []),

        Elapsed = erlang:monotonic_time() - T0,
        ElapsedUs = erlang:convert_time_unit(Elapsed, native, microsecond),

        Ops =
            case ElapsedUs of
                0 -> 0.0;
                _ -> Total * 1000000 / ElapsedUs
            end,

        {Samples, Ops}
    after
        Teardown(Ctx)
    end.

%% @private
warmup(_Work, _Ctx, 0) ->
    ok;
warmup(Work, Ctx, N) ->
    _ = Work(Ctx, N),
    warmup(Work, Ctx, N - 1).

%% @private
%% Times each operation individually. The accumulator is a plain list of
%% integers; it is reversed nowhere because order carries no meaning here.
measure(_Work, _Ctx, 0, Acc) ->
    Acc;
measure(Work, Ctx, N, Acc) ->
    T0 = erlang:monotonic_time(),
    _ = Work(Ctx, N),
    T1 = erlang:monotonic_time(),
    Us = erlang:convert_time_unit(T1 - T0, native, microsecond),
    measure(Work, Ctx, N - 1, [Us | Acc]).

%% @private
collect(_Ref, [], _Timeout, Acc) ->
    Acc;
collect(Ref, Pids, Timeout, Acc) ->
    receive
        {Ref, Pid, Samples} ->
            collect(Ref, lists:delete(Pid, Pids), Timeout, Samples ++ Acc)
    after Timeout ->
        _ = [exit(P, kill) || P <- Pids],
        error({bench_timeout, length(Pids)})
    end.

%% @private
median([]) ->
    0.0;
median(Values) ->
    percentile(Values, 50).

%% @private
to_text(Name) when is_atom(Name) ->
    atom_to_list(Name);
to_text(Name) ->
    Name.
