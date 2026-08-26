# CHANGELOG
# v6.2.0

Three fixes: a single TCP connection could permanently disable a node's
listener, one unrecognised message could permanently kill a peer link, and OTP
28 message priority was discarded on the last hop of delivery.

No public function was added. It ships as a minor rather than a patch because
delivery changes observably for the **default** manager on OTP 28 and later,
and because the connection-wedge fix is a prerequisite for any future release
that introduces new envelope types — a peer on 6.1.0 or earlier drops the link
rather than ignoring an envelope it does not recognise.

## Fixes

### Connection acceptance
* **One connect-then-disconnect could permanently disable a node's listener.**
  `acceptor.erl` escalated a *per-connection* error onto the *listening* socket
  with `exit(LSock, Reason)`. Ports are linked to their owner, so this killed
  `partisan_acceptor_socket` too, and its supervisor restarted it into the same
  condition until `reached_max_restart_intensity` shut the subtree down.

  The trigger is ordinary: on darwin a peer that closes right after connecting
  makes the post-accept `inet:setopts/2` return `{error, einval}`, and one that
  sends RST makes `gen_tcp:accept/2` return it. Any TCP health check, load
  balancer probe or port scan was sufficient — no TLS and no cluster required.
  TLS only raised the rate enough to exhaust the restart intensity, which is
  why it surfaced under `with_tls` first.

  Per-connection errors now terminate only the acceptor that saw them, exiting
  `normal` and sending `'CANCEL'` when `'ACCEPT'` has not been sent — without
  the cancel, `acceptor_pool` charges the exit to the pool's restart intensity
  and the pool dies instead. Escalation for listener-fatal conditions is
  unchanged. Covered by `partisan_listener_resilience_test`.

  Upstream `acceptor_pool` fixed the same class of defect independently
  (commit `7346b985`, Dec 2025), but its version still escalates an error from
  `gen_tcp:accept/2` itself onto the listener — the exact case that failed
  here — and terminates the post-accept paths without cancelling, so those
  exits are still charged to the pool's restart intensity.

### Peer service managers
* **An unrecognised inbound envelope permanently killed the peer link.** The
  catch-all clause of `handle_message/4` was the only one that did not answer
  its caller. That caller is the connection process, calling
  `receive_message/3` synchronously with `infinity`, so it blocked forever and
  never re-armed its `{active, once}` socket. All four managers now log and
  answer. Covered by `partisan_inbound_envelope_test` and the manager
  conformance suite.

### Message delivery
* **OTP 28 message priority was discarded on the last hop.** A priority alias
  yields a priority message only if the sender passes **both** the alias and
  `[priority]` to `erlang:send/3`; `Ref ! Message` supplies only the alias.
  `do_deliver/2` now sends with `[priority]` on OTP 28 and later. The option
  has no effect on a pid, a plain reference or a non-priority alias, so only a
  receiver that deliberately opted in is affected. OTP 27 and earlier are
  unchanged.

## Testing
* `acceptor_pool_SUITE` now runs, covering the vendored acceptor pool. It never
  had: `init_per_suite` started an `acceptor_pool` *application*, which does
  not exist because Partisan vendors the modules, so all 20 cases auto-skipped.
  Wired into `make acceptor-test`, `ci-light` and the GitHub matrix.

Verified on OTP 27.3.4, 28.5 and 29.0.5.

# v6.1.0

Fixes a startup failure that made `partisan_static_peer_service_manager`
unusable, brings that manager up to the same feature level as the others, and
corrects defects in the shared peer service manager surface that were reachable
from every topology.

Every change here is a fix and no public function was added. It ships as a
minor rather than a patch because three of the fixes change observable
behaviour for users of the **default** manager, who have no reason to be
reading a static-manager release note: channel-scoped callbacks that were inert
now fire, `partisan:demonitor/2` returns different values in four cases, and
the membership snapshot is seeded at init. Upgrading is a decision, not a
formality.

## Breaking changes
* `partisan_static_peer_service_manager:members_for_orchestration/0` returns
  `{ok, [partisan:node_spec()]}`. It returned `{ok, sets:set(node_spec())}`,
  which no other manager did and which the behaviour never specified. Code that
  called `sets:to_list/1` on the result must drop that call. This ships in a
  minor rather than a major because the manager could not start in 6.0.0 or in
  5.0.3, so no working code can be calling it.

## Fixes

### Peer service managers
* **`partisan_static_peer_service_manager` could not start**
  ([#267](https://github.com/lasp-lang/partisan/issues/267)). Configuring
  `peer_service_manager` to it aborted application startup with
  `{undef, [{partisan_static_peer_service_manager, on_up, 3}]}`:
  `partisan_monitor:init/1` calls `partisan_peer_service:on_up/3` on every boot
  and the manager did not export it. Two independent defects, both fixed:
    * `partisan_peer_service:on_up/3` and `on_down/3` dispatched to the manager
      unconditionally. They are *optional* callbacks, so a manager that does
      not implement one raised `undef` instead of returning the
      `{error, not_implemented}` the API documents. Dispatch is now guarded
      (`partisan_util:apply/4`).
    * The static manager implements `on_up/3` and `on_down/3`, including the
      `channel` option, and reports `supports_capability(monitoring) -> true`.
      Process and node monitoring work under it.
* **`{global, Name}` and `{via, Mod, Name}` were unroutable in every manager.**
  In `forward_message/3` the `{Name, Node}` clause preceded them and matched
  first, so both forms were treated as a registered name on a node. Reordered
  in all four managers. `partisan_hyparview_peer_service_manager`'s deliberate
  "global not supported" clause was unreachable and now applies.
* **Channel-scoped subscriptions never fired under
  `partisan_pluggable_peer_service_manager`.** Subscriptions are keyed by
  `{Node, Channel}` but were looked up by node name alone, which cannot match a
  tuple; `channel_up_funs` was never read and no `channel_up`/`channel_down`
  event was emitted. Every channel-scoped callback was silently inert,
  including those `partisan_monitor` registers. Channel events are now emitted
  and delivered, edge-triggered per `{node, channel}` so that a channel with
  `parallelism > 1` announces once rather than once per socket.
* **Local-node forwarding.** `partisan_client_server_peer_service_manager` and
  `partisan_hyparview_peer_service_manager` routed a message addressed to the
  local node through the connection table, which holds no connection to
  ourselves. Local targets are now delivered directly.
* **`partisan_client_server_peer_service_manager:leave/1` crashed the manager**
  with a `function_clause` (`disconnect(Peer)` where `disconnect/1` takes a
  list).
* **Membership snapshot seeded at init in every manager.** Only the pluggable
  manager published its initial membership, so under the other three a reader
  going through `partisan_membership` — broadcast among them — saw an empty
  member set that did not even contain the local node until the first change.

### `partisan_static_peer_service_manager`
Membership remains what the name says: the peer set is declared by the
operator and no undeclared node can join. The additions below close contract
gaps; none of them introduce discovery.

* `forward_message/3` accepts the full documented `server_ref()` surface —
  pid, registered name, `{Name, Node}`, `{global, Name}`, `{via, Mod, Name}`,
  encoded remote references and reference aliases. `{Name, Node}` previously
  raised `badarg`.
* `leave/0` and `leave/1` remove the peer and drop its connections instead of
  replying with a bare `error` and changing nothing.
* `update_members/1` is implemented. `partisan_peer_discovery_agent` calls it
  as `ok = partisan_peer_service:update_members(Members)` to apply the
  configured peer list, and crash-looped on `{error, not_implemented}`. It
  rejects anything that is not a node spec rather than quietly shrinking the
  peer set.
* `sync_join/1` is implemented, completing the manager callback surface.
* Removed the disk persistence of membership. It was never read back, and the
  operator's configured peer set is the only source of truth — a restart must
  not resurrect a peer that was removed from the configuration.

### Process monitoring
* **`partisan:demonitor/2` with `info` could report that a `DOWN` was queued
  for a monitor that would never fire.** The answer came from the peer's own
  `erlang:demonitor/2`, but delivery is arbitrated locally: every handler that
  can deliver a `DOWN` must first claim the `proc_mon_out` entry, and they all
  run inline in the local `partisan_monitor`. Winning that claim already
  guarantees no signal will be delivered. A caller following the
  `erlang:demonitor/2` contract — `false` means "already in your mailbox",
  checked with a zero-timeout receive — could wait for a message that never
  came. `info` is now answered from the local claim.
    * The unreachable-peer branches (`noconnection`, `timeout`, `noproc`,
      `nodedown`) returned a literal `true`, turning an already-fired monitor
      into "still live". They return the local answer.
    * `demonitor(Ref, [])` returned the peer's boolean; `erlang:demonitor/2`
      returns `true` unless `info` was requested. Code written as
      `true = partisan:demonitor(Ref, [flush])` could badmatch.
    * `flush` now removes the signal by reference when the record carrying the
      monitor's tag has already been reclaimed. A monitor created with
      `{tag, T}` could otherwise leave its `DOWN` in the caller's mailbox.

### Type specifications
* `partisan_peer_service_manager` callbacks `members/0` and
  `members_for_orchestration/0` specified a bare list; every implementation
  returns `{ok, List}`. Corrected, along with the matching specs in
  `partisan_peer_service` and `partisan_hyparview_peer_service_manager`.

## Testing
* Added `partisan_manager_conformance_test`: one checklist run against all four
  managers — boot and supervision, membership reads, snapshot seeding, optional
  callbacks answering rather than raising, the full `server_ref()` surface,
  capability honesty, and channel-event liveness. Per-manager expectations are
  data, so a capability disappearing is a failing test rather than a silent
  regression. No test referenced a non-default manager before this.
* Added `partisan_peer_service_manager_boot_test` covering the static
  manager's membership operations and the pluggable manager's channel events.
* `partisan_monitor_SUITE` passes and is wired into `make ci-heavy`. It ran in
  no make target and 8 of its 28 cases failed on a clean checkout; all eight
  were defects in the suite's own adaptation to Partisan, except the
  `demonitor/2` fault above, which it found. One case remains skipped: it needs
  `erts_test_utils` from OTP's `erts/emulator/test`, which
  `test/fetch_otp_test_sources.sh` does not fetch.
* Fixed two `partisan_support` defects that silently affected every suite using
  it: `start/3` read `node_config` only from its `Options` argument while
  `partisan_support_otp:start_node/2` passes it in `Config`, so per-test peer
  settings were dropped; and peer `args` never reached `peer:start/1`, so
  emulator flags never applied.

# v6.0.0

## Breaking changes
* **Minimum supported OTP version is now 27** (previously 24). The build now hard-fails on OTP < 27 (`rebar.config.script`).
* **Removed `partisan_gen_fsm`** (previously deprecated and incomplete). Code based on `gen_fsm` is no longer supported — migrate to `partisan_gen_statem`.
* **Monitor inter-node protocol changed.** `DOWN` signals are now delivered directly to the monitoring process on the monitor's bound channel (FIFO-ordered with user traffic), and a new inter-node cast `{gc_proc_mon_out, _}` is used for monitor GC.
    * **Rolling upgrade:** in a mixed v5/v6 cluster a v6 node may send a v5 monitor server a message it does not understand (leaving a stale bookkeeping entry); prefer a full cluster upgrade over long-lived mixed operation. See "Process & Peer Monitoring".

## Changes
### OTP compatibility
* Replaced the static OTP module forks in `priv/otp/24/` with a compile-time AST transformation system that generates the partisan OTP modules from the installed OTP source.
    * New modules: `partisan_gen_transform`, `partisan_otp_rewrite`, `partisan_otp_patches` — a pipeline that extracts abstract code from the installed OTP, applies mechanical AST rewrites (module renames, BIF replacements) and version-adaptive structural patches, and compiles the result.
    * Generated at compile time via `priv/generate_otp_sources.escript` (a rebar3 **pre-compile** hook), with a runtime fallback in `partisan_app:start/2` (`ensure_otp_modules/0`) for checkout dependencies; startup fails with `{partisan_otp_modules_missing, _}` if generation did not run.
    * 7 modules generated: `partisan_gen`, `partisan_proc_lib`, `partisan_sys`, `partisan_gen_server`, `partisan_gen_event`, `partisan_gen_statem`, `partisan_gen_supervisor`.
    * Version-adaptive: patches adjust to the OTP version (e.g. OTP 28 supervisor replies include `hibernate_after_action/1`, OTP 27 does not), eliminating the "OTP N+1 broke our forks" bug class.
* **Supported and tested on OTP 27, 28 and 29** (CI matrix `27.3`, `28.3`, `29.0`).
    * OTP 29 introduces **supervisor hibernation**, in which a leading `handle_call/3` clause wakes a hibernating supervisor before the call is dispatched. `partisan_gen_supervisor` supplies `handle_call/3` in full and so emits that clause; without it a hibernating supervisor would not wake. The build stops on any OTP major whose supervisor internals have not been checked against these replacements (`partisan_otp_patches:assert_reviewed_otp_version/1`).
* Added `partisan_otp_test_gen` (generator for the OTP-compatibility test suites).
    * The OTP test suites it adapts are **fetched for the OTP version in use** (`test/fetch_otp_test_sources.sh`) and cached under `otp_src/`, which version control ignores. An OTP release ships module sources but not its own test suites, so these are obtained separately; fetching them per version means a new OTP release needs no new copy in the repository. A build without network access can pre-seed `otp_src/otp_<version>/test/`, after which the fetch does nothing.
* Added installation / build documentation (`doc_extras/installation.md`).

### Process & Peer Monitoring
* Monitors are now bound to a channel for their lifetime; the eventual `DOWN` is delivered on that channel, in order with other traffic on it (matches disterl's "messages before DOWN" guarantee). Added per-channel down detection: a single channel dropping while the node stays up fires `DOWN`/`noconnection` for monitors on that channel only.
    * **Ordering caveat:** the "messages sent by the dying process before its `DOWN`" ordering holds only when the bound channel has `parallelism = 1`. On a parallel channel Partisan dispatches across sockets by partition key, so neither user traffic nor the `DOWN` has a per-channel total order — bind latency-sensitive monitors to a `parallelism = 1` channel if you rely on this.
* Monitor failure reason on a transport-level timeout is now reported as `noconnection` (nodedown-style) instead of `timeout`.

### disterl-hybrid routing
* When `connect_disterl = true`, `partisan:monitor/3` and message forwarding now use native `erlang:monitor`/`erlang:send` for peers reachable over Erlang distribution (narrowed to atom/pid targets with no `ack`/`causal_label` option, so interposition, ack and causal paths still use the partisan transport). Default is unchanged (`connect_disterl = false`).

### Broadcast — per-group epidemic broadcast (ADR-000001)
* The epidemic-broadcast substrate is no longer a single shared process. Each broadcast handler now runs in its own supervised **broadcast group** — its own process, mailbox, spanning tree and outstanding-lazy table — so independent gossip streams (e.g. an application's `plum_db` broadcasts) no longer share a tree or a mailbox with Partisan's own membership heartbeat or with each other. Groups are declared by `broadcast_mods` / the new `broadcast_groups` config and can be created or retired at runtime.
    * New modules: `partisan_broadcast` (public API — `broadcast/2`, `start_group/1`, `stop_group/1`, `groups/0`), `partisan_broadcast_group_sup` (the group supervisor), and `partisan_membership` (the membership snapshot, below). `partisan_plumtree_broadcast` is now started once per group (`start_link/2`), one instance per handler module; group identity is the handler module.
* **Off-path handler apply (non-blocking).** The `partisan_plumtree_broadcast_handler` behaviour gains two optional callbacks — `claim/2` (a fast, atomic novelty check run on the tree process) and `handle_broadcast/2` (the heavy apply, run off the tree process in the handler's own process). A handler that implements them no longer blocks the broadcast tree — or any other handler — while it merges/persists a payload. Handlers implementing only `merge/2` keep working unchanged (the synchronous path). `partisan_plumtree_backend` is migrated to the new contract as the reference implementation.
* **Membership via a lock-free snapshot.** The peer service manager now publishes membership to a public, lock-free ETS snapshot (`partisan_membership`) that broadcast groups read directly, rather than each subscribing to `partisan_peer_service_events` (whose synchronous `gen_event` fan-out would block the oracle and not scale to many groups). `partisan_peer_service:broadcast_members/0` is now answered from this snapshot. `partisan_peer_service_events` is unchanged and still available for external subscribers.
    * **Rolling upgrade:** the default (Partisan heartbeat) group keeps the legacy registered name `partisan_plumtree_broadcast`, so the control plane keeps converging in a mixed old/new cluster; a compatibility shim forwards legacy-addressed messages for application handlers to their group. Application gossip old→new is delivered via the shim; new→old is best-effort during the upgrade window and heals via anti-entropy. Prefer a full cluster upgrade over long-lived mixed operation.
    * The tree engine is a per-group **pluggable behaviour** (`partisan_broadcast_engine`): a group delegates tree construction/repair to an engine and owns everything else. Plumtree is engine #1 (`partisan_plumtree_engine`, the logic extracted from the group process into an I/O-free, action-returning engine); the group shell is unchanged in behaviour. A second engine, **Thicket** (`partisan_thicket_engine`), ships **experimental and off by default**: it embeds multiple interior-node-disjoint trees to spread forwarding load across nodes, is opt-in per group via `engine => partisan_thicket_engine`, and stays gated on a measured interior-node-load imbalance — the default Plumtree path is byte-for-byte unchanged. Supporting a self-describing engine like Thicket added an optional **raw-dispatch** path to the behaviour (`handle_message/2` + `repair_tick/1`, selected by an engine's `dispatch_mode/0`); the typed Plumtree callbacks are unchanged (ADR-000002, ADR-000004).

* **A broadcast group may declare its own channel.** Group specs accept `channel`, which wins over the handler's `broadcast_channel/0` callback; omitting it keeps the callback's answer, so the change is additive. This is the only way to put a handler **you do not own** on a dedicated channel — the channel was previously a property of the module and of nothing else. Introspect with `partisan_plumtree_broadcast:group_channel/1`.
    * The channel is deliberately a property of a **group**, not of an individual `broadcast/2` call: repair traffic follows the tree, so a per-message channel would serve a grafted retransmission on the group's channel and put the full payload on the channel it was meant to stay off, exactly when the network is stressed enough to need repair. A handler that needs two channels should run in two groups. Documented under "Groups and channels" in the migration guide.

### Membership eventing (ADR-000003)
* Retired the `partisan_peer_service_events` `gen_event` bus. Every peer-service manager (pluggable, hyparview, static, client_server) now publishes membership changes to the lock-free `partisan_membership` snapshot and delivers a **non-blocking** asynchronous `{partisan_membership, Members}` message to subscribers — replacing a synchronous `gen_event:sync_notify` that blocked the manager on every subscriber's callback and ran all callbacks serially in one process.
    * **Fixes a gap introduced with the membership snapshot:** the snapshot was previously written only by the default (pluggable) manager, so broadcast groups under the hyparview/static/client_server managers observed empty membership. All managers now feed it (regression-guarded by an assertion in the hyparview `partisan_SUITE` cases).
    * **Membership API:** observe changes via `partisan_membership:subscribe/0` (handle `{partisan_membership, Members}` in your own process) or by polling `partisan_membership:members/0` / `version/0`.
    * **Deprecated:** `partisan_peer_service:add_sup_callback/1` is now a compatibility shim over the push feed — the callback runs asynchronously in its own caller-linked process and no longer blocks the membership path. Prefer the API above. `partisan_peer_service_events` (with its `add_handler`/`add_sup_handler`/`add_callback` functions) is removed.

### RPC — `partisan_erpc` becomes the primary surface (ADR-000006)
* **`call` no longer runs on the `partisan_rpc_backend` server.** Every inbound RPC used to be applied inline in that one `gen_server`'s `handle_info/2`, so a single slow, blocking or hung `M:F(A)` stalled every unrelated RPC behind it in the mailbox, a crashing RPC was a supervision event, and replies carried no request id — a late reply from a timed-out call could be consumed by an unrelated later call in the same process. RPC is now an explicit correlated request/response: each request carries its own reference and is executed by a worker process spawned per request, which replies directly to the caller.
    * The correlation reference is a process alias (`erlang:alias([explicit_unalias])`), so abandoning a request deactivates its reply address and the runtime drops a late reply instead of leaving it in the caller's mailbox.
    * The failure-detection monitor deliberately carries only a reason, never a return value: in Partisan every remote `DOWN` is relayed through one `partisan_monitor` process per node, so using it as a value channel would funnel every RPC result on a node through it.
* **`partisan_erpc` works over the Partisan transport.** It was a vendored OTP 23/24-era `erpc` snapshot that still reached peers with the auto-imported `spawn_request/5` BIF and received results as a distributed monitor's exit reason — both distribution mechanisms — so it did not function as a Partisan surface at all, and no test had ever loaded it. It is now the **primary** RPC API; prefer it for new code. `-compile({no_auto_import, [spawn_request/5, spawn_request_abandon/1]})` makes any remaining distribution call a compile error rather than a silent disterl fallback.
    * Added the OTP 25+ request-identifier collection API the snapshot predated — `send_request/6`, `receive_response/3`, `wait_response/3`, `check_response/3`, `reqids_new/0`, `reqids_size/1`, `reqids_add/3`, `reqids_to_list/1` — plus upstream's `send_request/4` fun+label+collection clause. Idiomatic modern fan-out code previously failed with `undef`. An export-parity test now asserts the module stays a superset of `erpc`.
* **`partisan_rpc` is re-based as a thin shim over `partisan_erpc`**, mirroring how OTP has implemented `rpc` over `erpc` since OTP 23, including a verbatim copy of `?RPCIFY`/`rpcify_exception/2`. It is documented as the legacy surface but is **not** deprecated (OTP has not deprecated `rpc`, and existing code depends on the `rpc => partisan_rpc` rewrite).
    * **Closes a live `undef` bug:** `cast/4`, `multicall/3,4,5`, `async_call/4` + `yield/1`, `nb_yield/1,2` and `block_call/4,5` are reachable through the `rpc` rewrite but several did not exist. They do now.
* **`partisan_rpc_backend` is kept permanently as the counterpart of OTP's `rex`**, scoped to the operations whose semantics require a server — `block_call/4,5`, `sbcast/2,3`, `abcast/2,3`, `eval_everywhere/3,4` — exactly the split OTP still uses. `block_call` continues to apply inline **by design**: executing on that server, serialised with other `block_call`s, is what distinguishes it from `call`.
* **Inbound RPC concurrency is bounded** by the new `rpc_max_concurrency` (default `10000`, `infinity` disables). Each request runs in its own process, so without a bound a peer could spawn without limit; OTP tolerates the unbounded form only because the distribution buffer backpressures, which Partisan has no equivalent of. Over the cap a request is **rejected** rather than queued (a queue with no credit scheme is an unbounded mailbox with extra steps), surfacing as `error({partisan_erpc, overloaded})` / `{badrpc, {'EXIT', overloaded}}`. New telemetry event `[partisan, rpc, overload]`.
* **Per-call transport options on every RPC surface**, so RPC can be put on a channel of its own. `call/5` and `multicall/5` accept `forward_opts()` in place of a bare timeout; `partisan_erpc:send_request/5,7`, `cast/5`, `multicast/5` and `partisan_rpc:async_call/5`, `cast/5` are new arities.
    * **Per-call options now win over the global `forward_options`.** The precedence was inverted (`partisan_config:get(forward_options, CallerOpts)` returns the *configured* value whenever one is set), so a per-call `channel` or `partition_key` was silently discarded as soon as anything set the global.
* **Rolling upgrade:** a v6 node still serves the v5 `{call, ...}` framing for the whole 6.x series, so **v5 caller → v6 node is unchanged**; the reverse is not — a v5 node has no clause for the correlated request and discards it, so **v6 caller → v5 node times out**. Upgrade every node before relying on RPC between them. The v5 receiver is removed in 7.0.0, and the concurrency bound now covers it too.

### Backpressure and the forwarding contract
* **`partisan:forward_message/2,3,4` and the three `partisan_peer_service_manager` `forward_message` callbacks are no longer specced `-> ok`.** They never were: forwarding returns `{error, disconnected | not_yet_connected | notalive}` — or `{error, partitioned}` under the hyparview manager — when it cannot hand a message to a connection. The contract is now `t:partisan_peer_service_manager:forward_result/0` and `partisan:send/3`'s spec is widened to match.
    * **Runtime behaviour of `forward_message` is unchanged, so nothing breaks on upgrade — but code written against the old spec drops messages silently.** Check the return value where delivery matters.
* **`partisan:send/2` no longer crashes when the destination is unreachable.** It was `ok = send(Dest, Msg, [])`, which badmatched on `{error, disconnected}` — a crash in the caller for a function whose Erlang counterpart never fails that way (`erlang:send/2` to a dead process or unreachable node simply returns). It now follows `erlang:send/2`: best-effort, returns `Msg` regardless. Use `send/3` when the outcome matters. The mismatch was invisible while `forward_message/3` was mis-specced `-> ok`.
    * The RPC workers used to die on an undeliverable reply for the same reason (`ok = partisan:forward_message(...)` looked total); a reply that cannot be delivered is now logged and dropped, since the caller detects the loss through its own monitor.
* **New `connection_high_watermark` (default `infinity`, opt-in).** Dispatch is a `gen_server:cast/2` into an unbounded mailbox, so a sender faster than its socket grew that mailbox without limit. Past the mark a send is refused with `{error, overloaded}` and **not** queued. `partisan_peer_connections:cast_encoded/3` is now the single admission point for outbound data — all previous dispatch sites route through it. New telemetry event `[partisan, connection, overload]`.
    * `monotonic` channels are **exempt**: their existing strategy is to drop a superseded message when the connection has backlog, which is correct for traffic where only the freshest value matters, and applying the mark would turn deliberate silent drops into errors those senders have never handled.
    * The default preserves v5 behaviour exactly; turning an unbounded queue into a refusing one changes what callers observe, so it is opt-in.

### Development & tooling
* Removed eqWAlizer from CI and the build: the `Eqwalize` GitHub workflow, the `make eqwalizer`/`eqwalize-all` targets, and the `eqwalizer_support`/`eqwalizer_rebar3` injection in `rebar.config.script`. Dialyzer remains the static-analysis gate (`make dialyzer` / `make check`). All in-source `-eqwalizer(...)` attributes and `%% eqwalizer:ignore` comments have been stripped (they were inert without the checker).
* Split CI by resource footprint: the light suites (`compile`, `eunit`, `otp-compat-test`, `otp-test`) run on GitHub runners (`build_and_test.yml`), while the heavy multi-node cluster suites (`partisan_SUITE`, `partisan_alt_SUITE`, PropEr) run on a large ephemeral Fly.io machine — they exceed a GitHub runner's memory. New `make ci-light` / `make ci-heavy` aggregate targets and a `test/fly/` runner.
* Fixed the dialyzer PLT configuration: the old `{dialyzer_base_plt_apps, ...}` key is not a valid rebar3 option and was silently ignored, so `compiler`, `ssl`, `public_key` and `inets` were absent from the PLT. Replaced with a proper `{dialyzer, [{base_plt_apps, [...]}]}`, clearing ~100 spurious "unknown function" warnings.
* Adopted `erlfmt` for source formatting (rebar3 plugin + config).
* **Restored static analysis over the connection processes.** `#state.ping_tref` in `partisan_peer_service_client`/`_server` was typed as an encoded *remote* reference, but both writers store a local timer `reference()` (`erlang:start_timer/3`, directly or via `partisan_retry:fire/1`) and it is passed to `erlang:cancel_timer/1`; `#state.ping_idle_timeout` was typed `non_neg_integer()` despite both modules having an explicit `#state{ping_idle_timeout = undefined}` clause for when pings are disabled. Runtime behaviour was correct throughout — the annotations were not — but the two wrong types produced 13 cascading warnings including "no local return" on `send_ping/1` and `acceptor_continue/3`, so dialyzer was effectively not analysing the ping or accept paths at all. Both modules are now warning-free (project total 42 → 27).
* Exported `exchange/0`, `exchanges/0` and `selector/0` from `partisan_plumtree_broadcast`: `partisan_peer_service:exchanges/0,1` and `cancel_exchanges/1` — public API — carried specs referring to types that were not visible outside the defining module.
* Removed two unreachable private clauses in `partisan_interval_sets` (the bare-integer forms of `unsafe_element_intersection/2` and `do_element_subtract/2`): both are only reached after their callers have normalised the arguments to intervals. The integer forms of the element operations reachable from the public API are unaffected.
* Removed three dead macros from the public `partisan.hrl` (`?PLUMTREE_OUTSTANDING`, `?GOSSIP_FANOUT`, `?GOSSIP_GC_MIN_SIZE`) and corrected the annotation on `?FANOUT`, which was marked "not used?" but is the default for the `fanout` configuration option.
* **Added a benchmark harness** (`bench/`, `make bench`): a driver with warmup, repetitions and per-operation latency percentiles, plus scenarios for the point-to-point, acknowledged, RPC and broadcast-fan-out paths, and recorded baselines in `bench/BASELINE.md`. It is **not** part of `make test` or CI — a machine-dependent number cannot be a pass/fail condition — but it does fail a scenario whose own repetitions disagree by more than 25%, since such a run cannot resolve a change smaller than the disagreement.
* Test suite: disabled OTP 25+ `global` `prevent_overlapping_partitions` on the disterl-based CT control plane (`partisan_support`). On OTP 27 it disconnected peer nodes mid-test as HyParView churned connections, making the HyParView cases flaky (`global … requested disconnect … to prevent overlapping partitions`). Partisan itself runs `connect_disterl = false`, so production is unaffected.

## Security
* Bounded inbound peer message frames: a new `max_message_size` config option (default **64 MB**) sets `{packet_size, _}` on the `{packet, 4}` framing of both the connect (`partisan_peer_service_client`) and accept (`partisan_acceptor_socket`) paths, so an oversized frame is rejected before it is assembled or decoded — closing a pre-authentication memory-exhaustion / decompression-bomb vector on the peer plane.
    * **Upgrade / behavioural change:** previously `{packet, 4}` accepted frames up to ~4 GB; frames larger than `max_message_size` are now rejected (the receiving socket reports `emsgsize` and closes, which drops the peer from the active view until it reconnects). If your application legitimately sends peer messages larger than 64 MB (e.g. very large `plum_db` broadcasts / AAE deltas), raise `max_message_size` accordingly.
* Bounded the server-side TLS handshake: `partisan_peer_socket:accept/1` now passes a timeout (new `tls_handshake_timeout` option, default **5000 ms**) to `ssl:handshake/3`, so a peer that completes the TCP connection but stalls the TLS handshake can no longer pin an acceptor indefinitely.
* Startup security-posture logging (`partisan_app:start/2`): a `?LOG_WARNING` when cluster TLS is enabled but peers are not verified (`verify_peer` missing → encrypted but MITM-able), and a `?LOG_NOTICE` when the peer plane is plaintext/unauthenticated (`tls = false`), so an insecure peer-plane configuration is surfaced at boot rather than silent. The diagnostic is best-effort and never affects application start.
* Documentation: replaced the `verify_none` TLS examples (which modelled an unauthenticated, MITM-able configuration) with `verify_peer` mTLS, documented the new `max_message_size` / `tls_handshake_timeout` options, and added a "Securing the cluster peer plane" deployment guide (`doc_extras/cluster_security.md`).

## Fixes
* **HyParView could leave a permanent one-sided active-view link.** Active-view links are symmetric by definition (Leitao et al., DSN'07, §4.1: *"if node q is in the active view of node p then node p is also in the active view of node q"*), but two handlers guarded the add with `partisan_peer_connections:is_connected/1` and, when it was false, returned unchanged **without answering the peer at all**:
    * the `neighbor` handler, which also receives the periodic symmetry re-assertions. A node absent from our active view has no connection kept open for it, so this was not a transient race but the steady state — every re-assertion hit the silent branch, and the asserting peer was never told to drop us. Observed as a stable asymmetry that survived 600 consecutive checks over 60 seconds.
    * the `neighbor_request` handler, which sent neither `neighbor_accepted` nor `neighbor_rejected`, leaving the initiator's promotion hanging. §4.3 requires the initiator be told so it can try another peer from its passive view.

    Both now answer: a node that cannot hold a peer sends a DISCONNECT (or a rejection) so the peer drops the one-sided link. This fixed `partisan_SUITE:hyparview_manager_high_client_test`, which had been failing on constrained hardware since well before this release.
* Hardened `get_next_id/3` in `partisan_hyparview_peer_service_manager` against an epoch mismatch, which was a `case_clause` that would have taken the manager down. Unreachable today — `init/1` starts `sent_message_map` empty and the epoch only advances across a restart — so this is defensive, not a fix.
* **`partisan_interval_sets:del_element/2` raised `{badarg, List}` instead of removing the element**, whenever the element partially overlapped a stored interval *and* the set held a further interval after it. `element_subtract/2` returns a *list* of the parts of the element the stored interval did not cover, and each still has to be removed from the rest of the set; the list was passed as a single element instead, and `validate_element/1` rejected it. Removing `{5,25}` from `[{0,10},{20,30}]` crashed where it should return `[{0,4},{26,30}]`. Now folded over the remainder. The existing test cases never combined a partial overlap with a later interval, so none of them reached it.
* Resolves a crash on OTP 28 caused by the supervisor returning new `{timeout, T, Msg}`/`hibernate_after` action tuples the frozen `partisan_gen_server` did not understand.
* **Interposition:** fixed a pterm key mismatch (`{partisan_peer_service_server, peer}` written but `peer_node` read) that caused the origin `Node` passed to interposition functions on inbound-forwarded messages to always be `undefined`.
* Fixed `send_request` in the generated `partisan_gen` code to use `{alias, demonitor}` so `[alias | Mref]` replies route correctly.
* Fixed `partisan_interval_sets:from_list/1`: it now validates every element (including single-element lists) and sorts with a correct total order (`compare_lex`) before compaction. The previous implementation validated only the elements its `usort` comparator happened to touch — so a single-element list was never validated — and could drop distinct intervals sharing a start bound.
* **disterl-hybrid routing correctness (`connect_disterl = true`).** Three fixes to the opt-in native-transport fast path (default `connect_disterl = false` was unaffected): (1) a remote pid ref is no longer converted to a native pid via `list_to_pid/1` — Partisan stores pids in node-localized `"<0.X.Y>"` form, so that produced a *local* pid and misdelivered cross-node forwards/casts/replies; `remote_ref_to_disterl/1` now only reconstructs genuinely-local pids and otherwise falls back to the partisan transport; (2) `forward_message/3`'s remote-ref path now applies the same guard as `/4` — it no longer short-circuits to `erlang:send` when `ack`/`causal_label` are set (those need the partisan path) and only disterl-sends to a peer that is actually disterl-reachable (`erlang:nodes()`); (3) `monitor/3` selects native `erlang:monitor` only when the peer is actually disterl-reachable, avoiding a spurious immediate `noconnection` DOWN for a process still reachable over the partisan overlay.
* **TLS handshake failures no longer crash the acceptor.** `partisan_peer_socket:accept/1` matched `{ok, _} = ssl:handshake(...)` strictly, so a failed or timed-out server-side handshake raised `badmatch` and emitted a crash report per connection — a log-flood / acceptor-pool-exhaustion vector (slowloris, or a misconfigured peer). It now closes the socket and terminates the acceptor normally with a debug log; the pool replaces it.
* **HyParView active-view symmetry repair.** HyParView requires a symmetric active view — if node A holds peer B, then B must hold A. A control message lost during churn (a `NEIGHBOR` racing a not-yet-established reverse connection, or an undelivered `DISCONNECT`) could strand a *stable* one-sided view that the still-open connection never repaired, occasionally failing the high-fanout convergence cases (`hyparview_manager_high_client_test` / `high_active_test` — a ~20% flake on OTP 28, worse under load). Added periodic active-view maintenance to `partisan_hyparview_peer_service_manager`: each node re-asserts its membership to its active peers using the ordinary `NEIGHBOR` message, so a peer that is missing us re-adds us and one that already has us ignores it. Uses **no new wire message** (safe for peers on older releases) and is a no-op once the view is symmetric. Cadence defaults to `random_promotion_interval`, overridable via the `active_view_maintenance_interval` application env. Measured: 8/10 → **10/10** passes for `high_client_test` locally.
    * Setting `active_view_maintenance_interval` now takes effect. The key was not registered with `partisan_config`, which reads only the application-environment keys it knows, so the cadence always followed `random_promotion_interval`. Leaving the key unset still selects that cadence.

## Additions
* New export `partisan:remote_ref_to_disterl/1` — helper for disterl-hybrid deployments (`connect_disterl = true`).

# v5.0.3
## Fixes
* Fixed implementation of  `partisan_peer_service_client` and 
`partisan_peer_service_server` ping implementation that would close a 
connection when receiving and invalid ping message. Also added latency 
calculation and publich two telemetry events 
`[partisan, connection, client, heartbeat]` and 
`[partisan, connection, server, hearbeat]`

# v5.0.0
## Changes
* Drop `rc` tag and graduate to v5.0.0!
* Added `connection_ping` configuration option to prevent staleness during TCP half-open connections and other netorking issues. The same configuration works both for the client and server sides of the connection.

# v5.0.0-rc.17
## Changes
* Drop support for OTP24
* Added missing export `to_reference/1` in `partisan_remote_ref`

## Fixes
* Fixes plumtree calling the local node
* Update to `partisan_interval_set` util module

# v5.0.0-rc.16
## Fixes
* Fixes a bug introduced in previous commit in the return of the `graft` callback.

# v5.0.0-rc.15
## Fixes
* Allow `ok` as result for `partisan_plumtree_broadcast:exchange/1` callback.

# v5.0.0-rc.14
## Changes
* Add `ok` as valid return for `exchange` callback in `partisan_plumtree_broadcast_handler`.
# v5.0.0-rc.14
## Fixes
* Fixes the case where `partisan_plumbtree_broadcast` behaviour implementors' callbacks throw an exception which would crash the broadcast server.
* Replace use of RPC in `partisan_plumbtree_broadcast` and use 
  `partisan_gen_server:call/3` instead
* Other minor fixes


# v5.0.0-rc.13
## Fixes
* set `distance_enabled` options to `false` by default.


# v5.0.0-rc.12
## Fixes
* Fix a bug causing fast forward to be disabled in full-mesh topologies
* Merged [PR #254](https://github.com/lasp-lang/partisan/pull/254) - Thanks Massimo Cesaro!

# v5.0.0-rc.11
## Fixes
* Fix a bug when dealing with deprecated configs

# v5.0.0-rc.10
## Changes
* `partisan_peer_discovery_dns` configuration changes. Added support for IPV6 via `aaaa` record_type and additional `options`.
```
{partisan, [
    {peer_discovery, [
         {type, partisan_peer_discovery_dns},
         {config, #{
             record_type => aaaa,
             query => "foo.local",
             node_basename => "foo",
             options => #{
                nameservers => ["fdaa::3"]
             }
         }}
    ]}
]}
```

# v5.0.0-rc.9
## Changes
* `partisan_peer_discovery_dns` configuration changes. The configuration parameters `name` was renamed to `query` and `nodename` was renamed to `node_basename`. `name` and `nodename` are still valid inputs but they are transformed during init.
```
{partisan, [
    {peer_discovery, [
         {type, partisan_peer_discovery_dns},
         {config, #{
             record_type => fqdns,
             query => "foo.local",
             node_basename => "foo"
         }}
    ]}
]}
```
* New implementation of plumtree heartbeats in `partisan_plumtree_backend` to bound the timestamps stored by each peer. This is done using the new module `partisan_invertal_sets`.
The module also offers new performance improvements by avoiding calling the server when possible (using ets directly instead).

# v5.0.0-rc.8
### Bug Fixes
* Fixes #250 `peer_host` not working. The `peer_host` was an experimental option that was never rally implemented and thus has been deprecated and the original feature has been now implemented using the `listen_addrs` feature and the new host resolution algorithm

### Changes
* `listen_addrs` is now the preferred way to configure the IP/Ports where Partisan will listen for connections. The new implementation allows for multiple different formats and coerces them to the `partisan:listen_addr()` type i.e. `#{ip => inet:ip_address(), port => 1..65535}`. The following example shows the different formats accepted by the option.
```erlang
    {listen_addrs, [
        "127.0.0.1:12345",
        <<"127.0.0.1:12345">>,
        {"127.0.0.1", "12345"},
        {{127, 0, 0, 1}, 12345},
        #{ip => "127.0.0.1", port => "12345"},
        #{ip => <<"127.0.0.1">>, port => <<"12345">>},
        #{ip => {127, 0, 0, 1}, port => 12345}
    ]},
```
* A new algorithm has been implemented to determine the listen address when `listen_addr` is not defined in the configuration. The algorithm uses `peer_ip` the Erlang nodename or `name` configuration option to extract the host from the name e.g. `HOST` in `mynode@HOST` and uses `inet:getaddr` to determine the IP Address.

# v5.0.0-rc.7
### Changes
* Performance improvements for `partisan:forward/2,3,4`.

# v5.0.0-rc.2
### Bug Fixes
* Fixes a bug in `partisan:spawn/2`

# v5.0.0-rc.1

### Bug Fixes
* Make sure a message forward to a local process never fails (restoring the original behaviour).
* Minor bug fixes
* Fixed type issues detected by Eqwalizer and Dialyzer
### Changes
* Readme Docs improvements

# v5.0.0-beta.24
* Removed eqwalizer from default profile

# v5.0.0-beta.23

### Bug Fixes
* Coerce `forward_options` configuration option to map format.
* Fix bug in merge of forward options on `partisan_pluggable_peer_service` module
* Test suite fixes
* Export missing `partisan:monitor_node/3` function.
* Fix a bug in `partisan_hyparview_peer_service_message` when Options are passed as list.

### Changes
* Remove unused module `partisan_promise_backend`

# v5.0.0-beta.22

### Bug Fixes

* Continued adding support for OTP.
    * The OTP modules `sys`, `proc_lib` where patched (`partisan_sys`, `partisan_proc_lib`) so that they support the `partisan_remote_ref:t()` type and use the `partisan` module functions for finding, monitoring and sending messages instead of the native Erlang counterparts.
    * OTP patched files are located in the priv directory and loaded dynamically by `rebar.config.script` based on the Erlang/OTP version being used.
    * Patched the CT suites (`gen_server_SUITE`, `gen_statem_SUITE`, `gen_event_SUITE`) to test the partisan OTP modules. All tests passing except for some test cases that require not-yet implemented features like global and some `rpc` functions.
    * Notice `global` is not yet supported by Partisan.
* Added support for Eqwalizer, and passed both Eqwalizer and Dialyzer checks

### Additions

* New improper list format for `partisan_remote_ref`. This deprecates the config option `remote_ref_as_uri` and adds `remote_ref_format` instead which accepts `improper_list` (the new default), `tuple` (the legacy format) and `uri` (also introduced in v5).
* Adds `partisan_erpc`. The patched version of the Erlang's `erpc` module.


# v5.0.0-beta.19

### Bug Fixes
* Fix implementation of `partisan_pluggable_peer_service_manager:sync_join/1`.



# v5.0.0-beta.18

### Bug Fixes
* Remove optimisation from `partisan:self/0` and add `partisan:self/1` which accepts the `cache` option making the use of th optimization to be explicit. Check the docs for the explanation.
* Fixed bug in `partisan:monitor/2` introduced in previous version.


# v5.0.0-beta.17

### Bug Fixes
* Fix bugs in `partisan_gen_statem` and `partisan_gen`

# v5.0.0-beta.16

### Bug Fixes
* Fix a bug in `partisan:send/2,3`

### Changes
* Ensure the membership channel (`partisan_membership`) exits and is properly configured.

# v5.0.0-beta.15

### Bug Fixes
* General bug fixes including:
    * #121 updated_members should only accept a list of maps (an never a list of nodes)
    * fix wrong calls to `self()` and `node()` as opposed to their partisan counterparts
* Fixed bugs in `partisan_monitor`
* Several bug fixes in the OTP implementation
* Several bug fixes in the CT suite

### Changes
* Changed signature of partisan_membership_strategy and the implementing modules; added API e.g.  `join(state(), partisan:node_spec(), state())`  is now ` join(partisan:node_spec(), state(), state())` which is more natural.
* Added partisan_membership_strategy API functions, so that pluggable manager can call these functions
* Some other naming changes to disambiguate e.g. membership -> members
* moved some opt types from partisan_monitor to partisan module
* Fixed missing of gen_ and partisan_gen function calls.
* Made `channel` options to be respected across the stack
    * Added channel configuration to `partisan_monitor` calls.
    * Added channel to OTP behaviours.
        * The messages and the monitor signals will be sent using the configured channel.
        * overloaded gen_server/statem functions to accept options including channel so that we do not add another function to the API
        * store the Partisan opts in the process dict (again to avoid modifying our changed versions of the behaviours)
        *
* Configuration parameters renaming. Several configuration parameters were renamed. Check `partisan_config` module description. The old parameters are still accepted but are renamed during startup.
* Deprecated the `partisan_peer_service_manager:myself` callback
* Fix `partisan_util` term encoding and renamed function; added compression option for encoding and for memberhip payload



### Additions
* Added the following modules:
    * `partisan_supervisor` behaviour
* Added the following functions:
    * `partisan:exit/2`
    * `partisan:send/2`
    * `partisan:send/3`
    * `partisan:send_after/3`
    * `partisan:send_after/4`
    * `partisan:cancel_timer/1`
    * `partisan:cancel_timer/2`
 * Peer Service manager now allows subscribing to events per channel
    * `partisan_peer_service_manager:on_up/3` accepting a channel
    * `partisan_peer_service_manager:on_down/3` accepting a channel


# v5.0.0-beta.14

## API

#### Changes

* Several functions previously found in `partisan_util` are now in `partisan_peer_service_manager`.
- Types previously found in `partisan.hrl` are now defined and exported by the `partisan` module.

## Peer Membership

#### Fixes

* Several bug fixes in the following backends:
    * `partisan_hyparview_peer_service_manager`
    * `partisan_xbot_hyparview_peer_service_manager`
    * `partisan_client_server_peer_service_manager`
* Fixes a bug in `partisan_plumbtree_broadcast` where not all the handlers were used.
    * The configuration option `broadcast_start_exchange_limit` is now considered to refer to each handler i.e. a limit of `1` means Partisan will only allow one instance of a broadcast AAE exchange per handler (and not a single one in total).

## Peer Connection Management

#### Changes

* **Channel parallelism** can now be defined **per channel**
    * `channels` configuration option is overloaded to allow the new configuration options while keeping backwards compatibility. Check the documentation for the new formats in [partisan_config](partisan_config.html).
    * The `partisan:node_spec()` representation was changed:
        * `parallelism` was removed
        * `channels` was changed from a list of atoms or tuples to a the return of `partisan_config:get(channels)` i.e. a map.
    * `parallelism` is now used as a default when the user doesn’t define a per channel parallelism.
    * The `partisan` module now exports the new function  `channel_opts/1` with returns the options for a given channel.

# v5.0.0-beta.13

## API
In general, the API was redesigned to concentrate all functions around two modules: `partisan` and `partisan_peer_service`.

#### Changes

* `partisan` module was repurposed as a replacement for the `erlang` module for use cases related to distribution e.g. `erlang:nodes/0` -> `partisan:nodes/0`.
    * Several functions previously found in `partisan_peer_service`, `partisan_monitor` and `partisan_util` are now in this module:
        * `partisan:broadcast/2`
        * `partisan:cast_message/2`
        * `partisan:cast_message/3`
        * `partisan:cast_message/4`
        * `partisan:default_channel/0`
        * `partisan:demonitor/1`
        * `partisan:demonitor/2`
        * `partisan:disconnect_node/1`.
        * `partisan:forward_message/2`
        * `partisan:forward_message/3`
        * `partisan:forward_message/4`
        * `partisan:is_alive/0`
        * `partisan:is_connected/1`
        * `partisan:is_connected/2`
        * `partisan:is_fully_connected/1`
        * `partisan:is_local/1`
        * `partisan:is_pid/1`
        * `partisan:is_process_alive/1`
        * `partisan:is_reference/1`
        * `partisan:make_ref/0`
        * `partisan:monitor/1`
        * `partisan:monitor/2`
        * `partisan:monitor/3`
        * `partisan:monitor_node/2`
        * `partisan:monitor_nodes/1`
        * `partisan:monitor_nodes/2`
        * `partisan:node/0`
        * `partisan:node/1`
        * `partisan:node_spec/0`
        * `partisan:node_spec/1`
        * `partisan:node_spec/2`
        * `partisan:nodes/0`
        * `partisan:nodes/1`
        * `partisan:nodestring/0`
        * `partisan:self/0`
* Added the following functions:
    * `partisan_peer_service:broadcast_members/0`
    * `partisan_peer_service:broadcast_members/1`
    * `partisan_peer_service:cancel_exchanges/1`
    * `partisan_peer_service:exchanges/0`
    * `partisan_peer_service:exchanges/1`
    * `partisan_peer_service:get_local_state/0`
    * `partisan_peer_service:inject_partition/2`
    * `partisan_peer_service:leave/1`
    * `partisan_peer_service:member/1`
    * `partisan_peer_service:members_for_orchestration/0`
    * `partisan_peer_service:on_down/2`
    * `partisan_peer_service:on_up/2`
    * `partisan_peer_service:partitions/0`
    * `partisan_peer_service:reserve/1`
    * `partisan_peer_service:resolve_partition/1`
    * `partisan_peer_service:update_members/1`
* Use of `partisan_peer_service:mynode/0` has been replaced by `partisan:node/0` to follow Erlang convention
* Use of `partisan_peer_service:myself/0` has been replaced by `partisan:node_spec/0` to disambiguate from `partisan:node/0`.
* Use of `Node` variable name for `node()` type (as opposed to `Name`) and `NodeSpec` for `node_spec()` (as opposed to `Node`) to disambiguate.
* Adde new module `partisan_rpc` that will provide and API that mirrors Erlangs `rpc` and `erpc` modules
* Added `partisan_remote_ref` to encapsulate the creation of reference and added an optional/alternative representation for encoded pids, references and registered names. The module offers all the functions to convert pids, references and names to/from Partisan encoded references.
    * Alternative representation: In cases where lots of references are stored in process state, ets and specially where those are uses as keys, a binary format is preferable to the tuple format in order to save memory usage and avoid copying the term every time a message is send between processes. `partisan_remote_ref` represents an encoded reference as binary URI. This is controlled by the config option `remote_ref_as_uri` and `remote_ref_binary_padding` in case the resulting URIs are smaller than 65 bytes.

        ```erlang
        1> partisan_remote_ref:from_term(self()).
        {partisan_remote_reference,nonode@nohost,{partisan_process_reference,"<0.1062.0>"}}
        2> partisan_config:set(remote_ref_as_uri, true).
        ok
        3> partisan_remote_ref:from_term(self()).
        <<"partisan:pid:nonode@nohost:0.1062.0">>
        4> partisan_config:set(remote_ref_binary_padding, true).
        ok
        5> partisan_remote_ref:from_term(self()).
        <<"partisan:pid:nonode@nohost:0.1062.0:"...>>
        ```

## Peer Membership

#### Fixes
* Extracted the use of `state_orset` from `partisan_full_membership_strategy` into its own module `partisan_membership_set` which will allow the possibility to explore alternative data structures to manage the membership set.
* Introduced a membership prune operation to remove duplicate node specifications in the underlying `state_orset` data structure. This isto avoid an issue where a node will crash and restart with a different IP address e.g. when deploying in cloud orchestration platforms. As the membership set contains `node_spec()` objects which contain IP addresses we ended up with duplicate entries for the node.  The prune operation tries to break ties between these duplicates at time of connection, trying to recognise when a node specification might be no longer valid forcing the removal of the spec from the set.
* Fixes several bugs related to the `leave` operation in `partisan_pluggable_peer_service_manager`:
    * Added a missing call to update the membership set during leave
    * Fixed a concurrency issue whereby on self leave the peer service server will restart before being able to sending the new state with the cluster peers and thus the node would remain as a member in all other nodes.
* Resolves an issue `partisan_plumtree_broadcast` where the `all_members` set was not updated when a member is removed.
* Resolves the issue where the `partisan_plumtree_broadcast` was not removing the local node from the broadcast member set.
* Gen Behaviours take new option `channel` if defined.
* Fixed implementation of `on_up` and `on_down` callback functions in `partisan_pluggable_peer_service_manager`


#### Changes
* Added function `partisan_peer_service_manager:member/1`
* Replaced the use of in-process sets in `plumtree_broadcast_backend` with an `ets` table for outstanding messages keeping the gen_server stack lean and avoiding garbage collection


## Peer Connection management

#### Fixes

* Fixes a bug where connections where not properly killed during a leave
* Split TLS options for client and server roles
    * Removed `tls_options`
    * Added `tls_client_options` and `tls_server_options`

#### Changes

* New module `peer_service_connections`:
    * Replaces the former `peer_service_connections` process state data structure and the `partisan_connection_cache` module.
    * As a result, the `partisan_connection_cache` module has been was removed.
    * Checking connection status is now very fast and cheap. The implementation uses `ets`  to handle concurreny. It leverages leverages `ets:update_counter/4`, `ets:lookup_element/3` and `ets:select_count/2` for fast access and to minimise copying data into the caller's process heap.


## Process and Peer Monitoring

#### Fixes
* A more complete/safe implementation of process monitoring in `partisan_monitor`.
* More robust implementation of monitors using the new subscription capabilities provided by `peer_service:on_up` and `peer_service:on_down` callback functions.
    - monitor a node or all nodes
    - use node monitors to signal a process monitor when the remote node is disconnected
    - local cache of process monitor to ensure the delivery of DOWN signal when the connection to the process node is down.
    - avoid leaking monitors
    - new supervisor to ensure that `partisan_monitor` is restarted every time the configured `partisan_peer_service_manager` is restarted.
    - re-implementation based on ets tables
    - If using OTP25 the monitor gen_server uses the parallel signal optimisation by placing the process inbox data off heap

> #### NOTICE {: .warning}
>
> At the moment this only works for `partisan_pluggable_peer_service_manager` backend.

#### Changes

* New api in `partisan` module following the same name, signature and semantics of their `erlang` and `net_kernel` modules counterparts:
    * `partisan:monitor/1`
    * `partisan:monitor/2`
    * `partisan:monitor/3`
    * `partisan:monitor_node/2`
    * `partisan:monitor_nodes/1`
    * `partisan:monitor_nodes/2`


## OTP compatibility

#### Fixes

#### Changes

* Partisan now requires **OTP24 or later**.
* Upgraded `partisan_gen` and `partisan_gen_server` to match their OTP24 counterparts implementation
* Added `partisan_gen_statem`
* `partisan_gen_fsm` deprecated as it was not complete and focus was given to the implementation of `partisan_gen_statem` instead
* Module `partisan_mochiglobal` has been removed and replaced by `persistent_term`

## Misc

#### Fixes

* Most existing `INFO` level logs have been reclassified as `DEBUG`
* Fixed types specifications in various modules

#### Changes

* `lager` dependency has been removed and all logging is done using the new Erlang `logger`
* Most uses of the `orddict` module have been replaced by maps for extra performance and better usability
* Most API options using `proplists` module have been replaced by maps for extra performance and better usability
* In several functions the computation of options (merging user provided with defaults, validation, etc.) has been postponed until (and only if) it is needed for extra performance e.g. `partisan_pluggable_peer_servie_manager:forward_message`
* More utils in `partisan_util`
* Added `ex_doc` (Elixir documentation) rebar plugin
* Upgraded the following dependencies:
    * `uuid`
    * `types`
    * rebar plugins
