# CHANGELOG
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

### Broadcast — per-group epidemic broadcast (PDDR-000001)
* The epidemic-broadcast substrate is no longer a single shared process. Each broadcast handler now runs in its own supervised **broadcast group** — its own process, mailbox, spanning tree and outstanding-lazy table — so independent gossip streams (e.g. an application's `plum_db` broadcasts) no longer share a tree or a mailbox with Partisan's own membership heartbeat or with each other. Groups are declared by `broadcast_mods` / the new `broadcast_groups` config and can be created or retired at runtime.
    * New modules: `partisan_broadcast` (public API — `broadcast/2`, `start_group/1`, `stop_group/1`, `groups/0`), `partisan_broadcast_group_sup` (the group supervisor), and `partisan_membership` (the membership snapshot, below). `partisan_plumtree_broadcast` is now started once per group (`start_link/2`), one instance per handler module; group identity is the handler module.
* **Off-path handler apply (non-blocking).** The `partisan_plumtree_broadcast_handler` behaviour gains two optional callbacks — `claim/2` (a fast, atomic novelty check run on the tree process) and `handle_broadcast/2` (the heavy apply, run off the tree process in the handler's own process). A handler that implements them no longer blocks the broadcast tree — or any other handler — while it merges/persists a payload. Handlers implementing only `merge/2` keep working unchanged (the synchronous path). `partisan_plumtree_backend` is migrated to the new contract as the reference implementation.
* **Membership via a lock-free snapshot.** The peer service manager now publishes membership to a public, lock-free ETS snapshot (`partisan_membership`) that broadcast groups read directly, rather than each subscribing to `partisan_peer_service_events` (whose synchronous `gen_event` fan-out would block the oracle and not scale to many groups). `partisan_peer_service:broadcast_members/0` is now answered from this snapshot. `partisan_peer_service_events` is unchanged and still available for external subscribers.
    * **Rolling upgrade:** the default (Partisan heartbeat) group keeps the legacy registered name `partisan_plumtree_broadcast`, so the control plane keeps converging in a mixed old/new cluster; a compatibility shim forwards legacy-addressed messages for application handlers to their group. Application gossip old→new is delivered via the shim; new→old is best-effort during the upgrade window and heals via anti-entropy. Prefer a full cluster upgrade over long-lived mixed operation.
    * The tree engine is a per-group **pluggable behaviour** (`partisan_broadcast_engine`): a group delegates tree construction/repair to an engine and owns everything else. Plumtree is engine #1 (`partisan_plumtree_engine`, the logic extracted from the group process into an I/O-free, action-returning engine); the group shell is unchanged in behaviour. A second engine, **Thicket** (`partisan_thicket_engine`), ships **experimental and off by default**: it embeds multiple interior-node-disjoint trees to spread forwarding load across nodes, is opt-in per group via `engine => partisan_thicket_engine`, and stays gated on a measured interior-node-load imbalance — the default Plumtree path is byte-for-byte unchanged. Supporting a self-describing engine like Thicket added an optional **raw-dispatch** path to the behaviour (`handle_message/2` + `repair_tick/1`, selected by an engine's `dispatch_mode/0`); the typed Plumtree callbacks are unchanged (PDDR-000002, PDDR-000004).

### Membership eventing (PDDR-000003)
* Retired the `partisan_peer_service_events` `gen_event` bus. Every peer-service manager (pluggable, hyparview, static, client_server) now publishes membership changes to the lock-free `partisan_membership` snapshot and delivers a **non-blocking** asynchronous `{partisan_membership, Members}` message to subscribers — replacing a synchronous `gen_event:sync_notify` that blocked the manager on every subscriber's callback and ran all callbacks serially in one process.
    * **Fixes a gap introduced with the membership snapshot:** the snapshot was previously written only by the default (pluggable) manager, so broadcast groups under the hyparview/static/client_server managers observed empty membership. All managers now feed it (regression-guarded by an assertion in the hyparview `partisan_SUITE` cases).
    * **Membership API:** observe changes via `partisan_membership:subscribe/0` (handle `{partisan_membership, Members}` in your own process) or by polling `partisan_membership:members/0` / `version/0`.
    * **Deprecated:** `partisan_peer_service:add_sup_callback/1` is now a compatibility shim over the push feed — the callback runs asynchronously in its own caller-linked process and no longer blocks the membership path. Prefer the API above. `partisan_peer_service_events` (with its `add_handler`/`add_sup_handler`/`add_callback` functions) is removed.

### Development & tooling
* Removed eqWAlizer from CI and the build: the `Eqwalize` GitHub workflow, the `make eqwalizer`/`eqwalize-all` targets, and the `eqwalizer_support`/`eqwalizer_rebar3` injection in `rebar.config.script`. Dialyzer remains the static-analysis gate (`make dialyzer` / `make check`). All in-source `-eqwalizer(...)` attributes and `%% eqwalizer:ignore` comments have been stripped (they were inert without the checker).
* Split CI by resource footprint: the light suites (`compile`, `eunit`, `otp-compat-test`, `otp-test`) run on GitHub runners (`build_and_test.yml`), while the heavy multi-node cluster suites (`partisan_SUITE`, `partisan_alt_SUITE`, PropEr) run on a large ephemeral Fly.io machine — they exceed a GitHub runner's memory. New `make ci-light` / `make ci-heavy` aggregate targets and a `test/fly/` runner.
* Fixed the dialyzer PLT configuration: the old `{dialyzer_base_plt_apps, ...}` key is not a valid rebar3 option and was silently ignored, so `compiler`, `ssl`, `public_key` and `inets` were absent from the PLT. Replaced with a proper `{dialyzer, [{base_plt_apps, [...]}]}`, clearing ~100 spurious "unknown function" warnings.
* Adopted `erlfmt` for source formatting (rebar3 plugin + config).
* Test suite: disabled OTP 25+ `global` `prevent_overlapping_partitions` on the disterl-based CT control plane (`partisan_support`). On OTP 27 it disconnected peer nodes mid-test as HyParView churned connections, making the HyParView cases flaky (`global … requested disconnect … to prevent overlapping partitions`). Partisan itself runs `connect_disterl = false`, so production is unaffected.

## Security
* Bounded inbound peer message frames: a new `max_message_size` config option (default **64 MB**) sets `{packet_size, _}` on the `{packet, 4}` framing of both the connect (`partisan_peer_service_client`) and accept (`partisan_acceptor_socket`) paths, so an oversized frame is rejected before it is assembled or decoded — closing a pre-authentication memory-exhaustion / decompression-bomb vector on the peer plane.
    * **Upgrade / behavioural change:** previously `{packet, 4}` accepted frames up to ~4 GB; frames larger than `max_message_size` are now rejected (the receiving socket reports `emsgsize` and closes, which drops the peer from the active view until it reconnects). If your application legitimately sends peer messages larger than 64 MB (e.g. very large `plum_db` broadcasts / AAE deltas), raise `max_message_size` accordingly.
* Bounded the server-side TLS handshake: `partisan_peer_socket:accept/1` now passes a timeout (new `tls_handshake_timeout` option, default **5000 ms**) to `ssl:handshake/3`, so a peer that completes the TCP connection but stalls the TLS handshake can no longer pin an acceptor indefinitely.
* Startup security-posture logging (`partisan_app:start/2`): a `?LOG_WARNING` when cluster TLS is enabled but peers are not verified (`verify_peer` missing → encrypted but MITM-able), and a `?LOG_NOTICE` when the peer plane is plaintext/unauthenticated (`tls = false`), so an insecure peer-plane configuration is surfaced at boot rather than silent. The diagnostic is best-effort and never affects application start.
* Documentation: replaced the `verify_none` TLS examples (which modelled an unauthenticated, MITM-able configuration) with `verify_peer` mTLS, documented the new `max_message_size` / `tls_handshake_timeout` options, and added a "Securing the cluster peer plane" deployment guide (`doc_extras/cluster_security.md`).

## Fixes
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
