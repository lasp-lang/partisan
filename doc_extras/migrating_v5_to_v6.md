# Migrating from v5 to v6

Partisan v6.0.0's headline change is internal: the frozen OTP module forks under
`priv/otp/24/` are replaced by a compile-time system that generates Partisan's
OTP modules from the OTP source installed on the build host. Most of v6 is
therefore transparent to application code. This guide covers the parts that are
not — the breaking changes you must act on, the rolling-upgrade caveats, and the
behavioural and API changes worth reviewing before you deploy.

It assumes you are upgrading from **v5.0.3** (the last v5 release) to
**v6.0.0**. Read the [CHANGELOG](CHANGELOG.md) for the exhaustive list; this guide
is the task-ordered path through the changes that affect you.

## Before you upgrade

**Partisan v6 requires Erlang/OTP 27 or later.** The minimum was OTP 24 in v5.
The build now hard-fails on anything older (`rebar.config.script` exits during
compilation), so this is the first gate, not a runtime surprise. v6 is supported
and tested on **OTP 27, 28 and 29**.

Confirm your toolchain before changing your dependency:

```bash
erl -eval 'io:format("~s~n", [erlang:system_info(otp_release)]), halt().' -noshell
```

If that prints `24`, `25` or `26`, upgrade OTP first. Partisan generates its OTP
modules from the running release's own sources at build time, so there is nothing
version-specific to pin in your application beyond meeting the floor. See the
[installation guide](doc_extras/installation.md) for the build story.

Then update your dependency:

```erlang
%% rebar.config
{deps, [{partisan, "6.0.0"}]}.
```

```elixir
# mix.exs
{:partisan, "~> 6.0"}
```

## Breaking changes

### `partisan_gen_fsm` has been removed

`partisan_gen_fsm` was deprecated and incomplete in v5; it is gone in v6. If any
of your processes are built on it, port them to **`partisan_gen_statem`**, which
is the actively maintained state-machine behaviour and mirrors OTP's own
`gen_statem`. This is the same migration OTP itself asks of `gen_fsm` users, so
the state-function / handle-event restructuring is unchanged by Partisan.

### The inter-node monitor protocol has changed

`partisan:monitor/3` now binds each monitor to a **channel** for its lifetime and
delivers the eventual `DOWN` on that channel, in order with other traffic on it —
matching Erlang distribution's "any messages the dying process sent are delivered
before the `DOWN`" guarantee. Monitor garbage collection uses a new inter-node
cast, `{gc_proc_mon_out, _}`.

For a single-version cluster this is an improvement you get for free. It matters
during upgrades — see [Rolling upgrades](#rolling-upgrades) below — because a v5
node does not understand the new messages.

Two related observable changes:

- **Failure reason on a transport timeout is now `noconnection`** (nodedown-style),
  not `timeout`. If you pattern-match the `DOWN` reason, update the clause.
- **Ordering holds only on a `parallelism = 1` channel.** The "messages before
  `DOWN`" guarantee assumes a per-channel total order. A channel with
  `parallelism > 1` dispatches across several sockets by partition key, so neither
  user traffic nor the `DOWN` is totally ordered on it. Bind a latency-sensitive
  monitor to a `parallelism = 1` channel if you depend on that ordering.

### Membership eventing has moved off `gen_event`

`partisan_peer_service_events` — the `gen_event` bus with its `add_handler/2`,
`add_sup_handler/2` and `add_callback/1` functions — **has been removed**. Its
synchronous fan-out ran every subscriber's callback serially in one process and
blocked the peer-service manager on the membership path.

Every membership-bearing manager (pluggable, HyParView, static, client-server)
now publishes changes two ways:

- a lock-free ETS **snapshot** you can poll, and
- a **non-blocking** `{partisan_membership, Members}` message pushed to subscribers.

Observe membership through the new `partisan_membership` module:

```erlang
%% Push: receive {partisan_membership, Members} in your own process.
ok = partisan_membership:subscribe(),

%% Or poll the snapshot.
Members = partisan_membership:members(),
Version = partisan_membership:version().
```

`partisan_peer_service:add_sup_callback/1` still exists but is **deprecated**: it
is now a compatibility shim over the push feed, running your callback
asynchronously in its own caller-linked process. Prefer `partisan_membership`.

## Rolling upgrades

Two subsystems change the inter-node protocol, so a cluster running mixed v5 and
v6 nodes needs care. **Prefer a full cluster upgrade over long-lived mixed-version
operation.**

- **Monitors.** A v6 node may send a v5 monitor server a message (the new
  `DOWN`/GC forms) it does not understand, which can leave a stale bookkeeping
  entry on the v5 side. Monitors established within one version behave correctly;
  the risk is confined to the upgrade window.
- **Broadcast.** Partisan's own control-plane broadcast group keeps the legacy
  registered name `partisan_plumtree_broadcast`, so membership keeps converging in
  a mixed cluster. A compatibility shim forwards legacy-addressed messages for
  application handlers to their new per-group process. Application gossip
  old→new is delivered through the shim; new→old is best-effort during the window
  and heals via anti-entropy once the upgrade completes.

Neither the HyParView active-view maintenance nor the security frame cap
(below) introduces a new wire message, so those are safe across versions.

## Behavioural changes to review

These are not breaking, but they change observable behaviour under specific
conditions.

### Inbound peer frames are size-capped

A new `max_message_size` option (default **64 MB**) bounds the `{packet, 4}`
framing on both the connect and accept paths, so an oversized frame is rejected
before it is assembled or decoded. This closes a pre-authentication
memory-exhaustion / decompression-bomb vector.

**Action:** v5 accepted frames up to ~4 GB. If your application legitimately sends
peer messages larger than 64 MB — for example very large application broadcasts or anti-entropy deltas — raise `max_message_size`. A frame over the cap causes the receiving socket to report `emsgsize` and close, which drops the peer from the active view until it reconnects.

### disterl-hybrid routing (opt-in)

When `connect_disterl = true`, `partisan:monitor/3` and message forwarding now
use native `erlang:monitor` / `erlang:send` for peers reachable over Erlang
distribution — but only for atom/pid targets with no `ack` or `causal_label`
option, so interposition, acknowledgement and causal-delivery paths still use the Partisan transport. **The default is unchanged (`connect_disterl = false`)**, so this affects you only if you opt in. A new helper, 
`partisan:remote_ref_to_disterl/1`, supports these deployments.

## Broadcast: per-group epidemic broadcast

The epidemic-broadcast substrate is no longer a single shared process. Each
broadcast handler now runs in its own supervised **broadcast group** — its own
process, mailbox, spanning tree and outstanding-lazy table — so independent
gossip streams no longer share a tree or a mailbox with Partisan's membership
heartbeat or with each other.

For most applications this is transparent: `partisan:broadcast/2` still works, and
the default group keeps the legacy registered name. You only need to read the rest
of this section if you configure your own broadcast handlers.

### Declaring groups: `broadcast_mods` and `broadcast_groups`

A **broadcast group** is a set of handler modules that share one spanning tree and one process. Two configuration keys declare groups, because they answer two
different questions — you are not expected to use both.

**`broadcast_mods` is the simple, v5-compatible key.** It is a flat list of
handler modules, exactly as in v5:

```erlang
{broadcast_mods, [my_app_backend, my_other_backend]}
```

In v6, Partisan **derives** a group from this list automatically: each handler is placed in its own group, keyed by a deterministic name
(`partisan_broadcast:group_name/1`), and Partisan's own built-in handler keeps the legacy shared group. **A v5 `broadcast_mods` setting keeps working unchanged** — you simply get one isolated group per handler instead of one shared process. For most deployments this is all you need.

**`broadcast_groups` is a new v6 key for what a flat list cannot express.** It is a list of explicit group specs, and you reach for it only when you want to:

- **put several handlers in one group**, so they deliberately share a tree and
  process (the derived form always gives each handler its own group);
- **name a group** explicitly; or
- **tune a group's tick periods** (`lazy_tick_period`, `exchange_tick_period`).

```erlang
{broadcast_groups, [
    %% Two handlers that deliberately share one tree, under an explicit name.
    #{name => app_gossip, mods => [my_app_backend, my_other_backend]},

    %% A single-handler group with a slower lazy-push cadence.
    #{mods => [bulk_backend], lazy_tick_period => 10000}
]}
```

The two keys **compose**: the groups Partisan runs are the union of those derived from `broadcast_mods` and those declared in `broadcast_groups`. When a
`broadcast_groups` spec resolves to the same name as a derived group, the explicit spec **wins** — so `broadcast_groups` is also how you override the default for a handler that would otherwise get a plain per-handler group.

Groups can also be created and retired at runtime with
`partisan_broadcast:start_group/1` and `stop_group/1`; `groups/0` lists the running groups.

### Non-blocking handler apply (optional)

The `partisan_plumtree_broadcast_handler` behaviour gains two optional callbacks:
`claim/2`, a fast atomic novelty check run on the tree process, and
`handle_broadcast/2`, the heavy apply run off the tree in the handler's own
process. A handler that implements them no longer blocks the broadcast tree — or
any other handler — while it merges or persists a payload. Handlers that implement
only `merge/2` keep working unchanged on the synchronous path.

If you maintain a broadcast handler and its apply is expensive, adopting `claim/2`
+ `handle_broadcast/2` is the recommended change — but it is optional, not
breaking.

## Security posture

v6 makes an insecure peer-plane configuration loud instead of silent, and ships
operator guidance. None of this changes the default runtime behaviour — the peer
plane is still plaintext and unauthenticated unless you configure otherwise — but you should review it as part of the upgrade.

- **Startup diagnostics.** Partisan logs a `NOTICE` at boot when the peer plane 
is plaintext (`tls = false`) and a `WARNING` when TLS is on but peers are not verified (`verify_peer` missing → encrypted but MITM-able). The check is best-effort and never affects application start.
- **Bounded TLS handshake.** A new `tls_handshake_timeout` option (default
  **5000 ms**) stops a peer that completes TCP but stalls the TLS handshake from
  pinning an acceptor indefinitely.
- **Guidance.** Read [Securing the cluster peer plane](doc_extras/cluster_security.md)
  before exposing a cluster to anything but a fully trusted network. It covers
  isolating the peer port and configuring mutual TLS with a private cluster CA —
  the `verify_none` examples that v5's docs showed are gone, because they model an
  unauthenticated, MITM-able configuration.

## New and changed configuration

| Key | Default | Notes |
| --- | --- | --- |
| `max_message_size` | `67108864` (64 MB) | Caps inbound peer frame size. Raise for very large broadcasts. |
| `tls_handshake_timeout` | `5000` (ms) | Bounds the server-side TLS handshake. |
| `broadcast_mods` | `[partisan_plumtree_backend]` | Unchanged key, changed meaning: in v6 each listed handler is derived into its own isolated broadcast group. |
| `broadcast_groups` | `[]` | New. Explicit group specs for what a flat `broadcast_mods` list cannot express (shared groups, explicit names, per-group tick tuning). Composes with `broadcast_mods`; wins on name collision. |
| `active_view_maintenance_interval` | `random_promotion_interval` | HyParView active-view re-assertion cadence. Now honoured — in v5 the key was silently ignored. |

## New and changed API

- **Added:** `partisan:remote_ref_to_disterl/1` (disterl-hybrid deployments).
- **Added:** the `partisan_membership` module — `subscribe/0`, `members/0`,
  `version/0` — for observing membership.
- **Added:** the `partisan_broadcast` module — `broadcast/2`, `start_group/1`,
  `stop_group/1`, `groups/0`.
- **Removed:** `partisan_gen_fsm` (migrate to `partisan_gen_statem`).
- **Removed:** `partisan_peer_service_events` (migrate to `partisan_membership`).
- **Deprecated:** `partisan_peer_service:add_sup_callback/1` (a shim over the push
  feed; prefer `partisan_membership`).

## Building from source

If you build Partisan from source or vendor it as a checkout dependency, note the
toolchain changes:

- Partisan generates seven `partisan_gen_*` modules from the installed OTP source
  at compile time, via a `rebar3` pre-compile hook, with a runtime fallback at
  application start. A checkout that never ran the hook fails to start with
  `{partisan_otp_modules_missing, _}`. See the [installation guide](doc_extras/installation.md).
- eqWAlizer has been dropped from the build and CI; Dialyzer remains the
  static-analysis gate.
- CI is split by resource footprint: light suites on standard runners, the
  heavy multi-node cluster suites on a large ephemeral machine (`make ci-light` /
  `make ci-heavy`).
- Source is formatted with `erlfmt`.

## Upgrade checklist

1. Confirm every build and runtime host is on **OTP 27, 28 or 29**.
2. Replace any `partisan_gen_fsm` process with `partisan_gen_statem`.
3. Replace any `partisan_peer_service_events` subscription with
   `partisan_membership:subscribe/0` (or polling), and review any
   `add_sup_callback/1` use.
4. Update `DOWN`-reason clauses that expect `timeout` to accept `noconnection`;
   bind ordering-sensitive monitors to a `parallelism = 1` channel.
5. If you send peer messages larger than 64 MB, raise `max_message_size`.
6. Plan a **full** cluster upgrade rather than long-lived mixed v5/v6 operation.
7. Review your peer-plane security posture against
   [the security guide](doc_extras/cluster_security.md).
