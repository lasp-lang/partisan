# Benchmark baseline

Numbers for the current working tree, recorded so that later changes to the data
path have something to be measured against. **These are not targets and not a
pass/fail condition** — they are one machine's readings, and only their movement
between runs on *the same* machine means anything.

## How to reproduce

```bash
make bench                              # all scenarios
make bench BENCH_CASE=rpc_throughput    # one scenario
make bench BENCH_NODES=25               # fan-out at a different cluster size
```

Run on an otherwise idle machine. Each scenario runs `warmup` operations that
are discarded, then `iterations` timed operations per repetition, over
`repetitions` independent repetitions. Latency percentiles pool every individual
operation; throughput is per repetition.

`spread` is `(max - min) / median` across the repetition throughputs. The suite
**fails** a scenario whose spread exceeds 25%: a run whose own repetitions
disagree by more than that cannot resolve a change smaller than the
disagreement, so it is not evidence in either direction.

## Environment

| | |
|---|---|
| Tree | `f309812` plus the v6.0.0 working-tree changes |
| Machine | Apple M4 Pro, 14 cores |
| OS | macOS 26.5.1 |
| Erlang | OTP 28 |
| Cluster | Peer nodes on one host — so these measure loopback, not a network |
| Date | 2026-07-30 |

**Every node is on one machine.** Loopback latency is not network latency, and
peers compete for the same 14 cores. These numbers are therefore useful for
*comparison against themselves* and misleading as absolutes.

## Results

Latency in microseconds; throughput in operations per second.

| Scenario | p50 | p90 | p99 | max | throughput | spread |
|---|---|---|---|---|---|---|
| `p2p_roundtrip` (100 B) | 81 | 91 | 118 | 253 | 12,003 | 0.84% |
| `p2p_roundtrip_large_payload` (100 KB) | 108 | 127 | 161 | 1,779 | 8,945 | 1.92% |
| `p2p_roundtrip_bounded` (mark enabled) | 79 | 90 | 118 | 247 | 12,360 | 6.07% |
| `acked_roundtrip` (100 B, `ack => true`) | 102 | 116 | 136 | 379 | 9,727 | 5.23% |
| `rpc_throughput_c1` | 283 | 328 | 399 | 5,592 | 3,487 | 4.08% |
| `rpc_throughput_c8` | 554 | 638 | 712 | 815 | 14,200 | 1.80% |
| `rpc_under_a_slow_call` (c4) | 400 | 453 | 544 | 1,871 | 9,835 | 1.51% |
| `broadcast_fanout_n5` | 164 | 205 | 265 | 385 | 5,934 | 13.37% |

2000 iterations × 3 repetitions for every scenario except the fan-out, which
uses 500 × 3 because each operation costs every peer.

`broadcast_fanout_n5` was measured on its own. In a full sweep it lands after
six other scenarios and its spread exceeded the gate — which is the gate working:
the machine had not settled, so that reading was correctly refused rather than
recorded.

## What these say

**RPC concurrency scales.** Eight concurrent callers reach 14,200 ops/s against
one target where a single caller reaches 3,487 — roughly 4× on 8× the callers,
with p99 rising from 399 us to 712 us. Under the pre-6.0.0 backend, which applied
every inbound RPC inline in one `gen_server` callback, throughput could not
exceed what that one process could serve regardless of caller count.

**A slow RPC no longer blocks unrelated ones — this is the head-of-line number.**
`rpc_under_a_slow_call` holds a 60-second RPC open on the target for the entire
run. Its p99 is **544 us**, sitting between the unobstructed c1 (399 us) and c8
(712 us) figures for its concurrency of 4. The slow call is invisible in the
tail. Against an inline backend the tail would have been the slow call's own
duration — six orders of magnitude larger.

**Acknowledgement costs about 25% of round-trip latency** (102 us vs 81 us) and
does not serialise: it is a lock-free counter plus a direct ETS write, not a
`gen_server:call`.

**The connection high-water mark is not measurable here.** `p2p_roundtrip_bounded`
runs the identical work with `connection_high_watermark` set (high enough never
to be reached, so it measures the *check* rather than a refusal), which makes
`partisan_peer_connections:admit/2` call `process_info/2` on every send. It came
out at 12,360 ops/s against the unbounded 12,003 — nominally *faster*, which is
simply noise: the difference is inside the run-to-run spread of both. The honest
statement is that one `process_info/2` per send is below the noise floor at
~12k ops/s on this machine. It does not follow that it is free at 500k ops/s, and
nothing here tests that.

The default path does not even reach the BIF: `connection_high_watermark`
defaults to `infinity` and short-circuits first, so an installation that has not
opted in pays nothing by construction rather than by measurement.

**A 1000× payload increase costs ~33% latency** (81 us → 108 us for 100 B →
100 KB), which is what you would expect when encoding happens once, in the
calling process, rather than inside the connection process.

## A scenario that has no recorded result, and why

`broadcast_fanout_concurrent` (1/4/8 processes broadcasting into one group) is
**not in the table above**, because neither attempt met the stability gate: 59%
spread at 500 iterations, 60% at 4,000. It is kept in the suite anyway, because
what it refused to report turned out to matter more than a number would have.

The second run showed a **maximum of 542 ms against a p50 of 199 us**. Had the
gate averaged instead of refusing, that would have been recorded as a throughput
figure and the stall would still be undiscovered.

The scenario now samples the mailbox depth of the broadcast group server and of
every peer's handler process at the end of each repetition, so it can say *where*
a stall is rather than only that there is one. Every sample reads **zero** —
nothing queues at either funnel point — and throughput rises ~3.6x from
concurrency 1 to 4. That closed the single-group-bottleneck question
against the premise it was built to test.

Concurrency 1 is stable (1,175 ops/s, 11.65% spread); 4 and 8 are not on this
hardware, and are not treated as evidence. Still unexplained: the scenario is
~8x slower per operation at 4,000 iterations than at 500 with no queue anywhere,
so whatever degrades is not a backlog — candidates are the benchmark handler's
ETS table (never cleared between repetitions) and GC pressure.

## Caveats worth keeping attached to these numbers

- **No before/after comparison exists.** This harness was built *after* the
  changes it would have measured, so every figure above is a baseline for future
  work, not evidence about past work. The structural claims in
  `ADR-000006` remain justified by what the code does, not by a measured delta — with the single exception of the head-of-line
  result above, which is a property of this tree observable on its own.
- **The fan-out scenario measures completion, not send cost.** It times from
  "broadcast issued" to "every peer acknowledged", because
  `partisan_broadcast:broadcast/2` returns as soon as the group server has the
  message. An earlier version timed the call itself and reported 1.2M ops/s at a
  p50 of 0 us — true, and useless.
- **Five nodes is not a fan-out test.** Twenty-five or a hundred beam nodes on
  one host measure that host's scheduler, so only `n=5` is recorded here.
  `BENCH_NODES` raises it on hardware that can hold it, and the node count is in
  the scenario name, so no reading can be detached from the size it was taken
  at.
- **`rpc_throughput_c1` has a max of 5,592 us** against a p99 of 399. A single
  outlier of that size on an otherwise tight distribution is characteristic of a
  scheduler or GC pause, not of the RPC path.
