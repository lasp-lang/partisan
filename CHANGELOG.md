# CHANGELOG

# v5.0.0 (beta)

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
