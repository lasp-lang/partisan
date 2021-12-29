# CHANGELOG

# v4.2.0

## Fixes

* Fixes bugs in `partisan_plumtree_broadcast`
    * `neighbors_down` where the all_members set was not updated when a member is removed.
    * use of `myself()` as opposed to `mynode()` on Exchange supporting functions
* Fixes in partisan_monitor
* Fixes in partisan_gen_server

## Changes
* Require OTP24+
* Replaced lager with logger
    * Re-assigned most previous INFO logs are to DEBUG
* Upgraded `uuid`, `types` dependencies and rebar plugins
* Replaced `partisan_mochiglobal` with Erlang's `persistent_term`
* Replaced API options data structure from proplists to maps for extra performance
* Use `ets` table for plumtree broadcast outstanding messages keep the gen_server stack lean
* Upgraded partisan_gen_ to their OTP24 counterparts

## Added
* `partisan_monitor:monitor_node/2`
* `partisan_gen_statem`
* More utils in `partisan_util`