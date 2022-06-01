# CHANGELOG

# v5.0.0

## Fixes

* Fixes bugs in `partisan_plumtree_broadcast`
    * `neighbors_down` where the all_members set was not updated when a member is removed.
    * use of `myself()` as opposed to `mynode()` on Exchange supporting functions
* Fixes in partisan_monitor
* Fixes in partisan_gen_server

## Changes

### Performance improvements

* Most uses of the `orddict` module have been replaced by maps.
* Most API options using `proplists` module have been replaced by maps.
* In several functions the computation of options (merging user provided with defaults, validation, etc) has been posponed until (and if) they are needed for extra performance e.g. `partisan_pluggable_peer_servie_manager:forward_message`
* Use `ets` table for plumtree broadcast outstanding messages keeping the gen_server stack lean and avoiding garbage collection

### OTP compatibility

* Partisan now require OTP24 or later.
* Upgraded `partisan_gen_server` to match OTP24 implementation

### Use of new Erlang features

* persistent_term
    * Replaced `partisan_mochiglobal` with Erlang's `persistent_term`
* Logging
    * `lager` was replaced by the new Erlang `logger`.
    * Most existing `INFO` level logs have been reclassified as `DEBUG`

### Dependencies

* Upgraded `uuid`, `types` dependencies and rebar plugins




## Added
### Monitoring
* `partisan_monitor:monitor_node/2`

### OTP compatibility
* `partisan_gen_statem`

### Misc
* More utils in `partisan_util`
* `partisan_peer_service_manager:member/1`