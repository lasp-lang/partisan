# Partisan

[![Build Status](https://travis-ci.org/lasp-lang/partisan.svg?branch=master)](https://travis-ci.org/lasp-lang/partisan)

Partisan is a scalable and flexible, TCP-based membership system and distribution layer for the BEAM. It bypasses the use of Distributed Erlang for manual connection management via TCP, and has several pluggable backends for different deployment scenarios.


## Why do we need Partisan?

Erlang/OTP, specifically distributed erlang (a.k.a. `disterl`), uses a full-mesh overlay network. This means that in the worst case scenario all nodes are connected-to and communicate-with all other nodes in the system.

**Failure detector**. These nodes send periodic heartbeat messages to their connected nodes and deem a node "failed" or "unreachable" when it misses a certain number of heartbeat messages i.e. `net_tick_time` setting in `disterl`.

Due to this hearbeating and other issues in the way Erlang handles certain internal data structures, Erlang systems present a limit to the number of connected nodes that depending on the application goes between 60 and 200 nodes.

Also, Erlang conflates control plane messages with application messages on the same TCP/IP connection and uses a single TCP/IP connection between two nodes, making it liable to the [Head-of-line blocking](https://en.wikipedia.org/wiki/Head-of-line_blocking) issue. This also leads to congestion and contention that further affects latency.

This model might scale well for datacenter deployments, where low latency can be assumed, but not for geo-distributed deployments, where latency is non-uniform and can show wide variance.

Partisan was also designed to handle failures:

* Message omission
* Message reordering, this can happen in Erlang during reconnections
* State loss
* Bit flips
* Message corruption
* Faulty failure detectors
* Etc.

## Partisan features

Partisan was designed to increase scalability, reduce latency and improve failure detection for Erlang/BEAM distributed applications.

* Scalability
    * Provides serveral overlays that are configurable at runtime
        * `partisan_pluggable_peer_service_manager`: full mesh with TCP-based failure detection. All nodes maintain active connections to all other nodes in the system using one or more TPC connections.
        * `partisan_hyparview_peer_service_manager.`: modified implementation of the HyParView protocol, peer-to-peer, designed for high scale, high churn environments. A hybrid partial view membership protocol, with TCP-based failure detection.
        * `partisan_client_server_peer_service_manager.`: star topology, where clients communicate with servers, and servers communicate with other servers.
        * `partisan_static_peer_service_manager`: static membership, where connections are explicitly made between nodes
    * Specialised to particular application communication patterns
    * Doesn't alter application behaviour
* Reduce latency
    * Increasing parallelism - by configuring the number of TCP/IP connections between nodes (named channels).
    * Pushing background and maintenance traffic to other communication channels - to avoid the Head-of-line blocking due to background activity
    * Leveraging monotonicity - so that you can do load shedding more possible.
        * Monotonic channels drop messages when state is increasing on the channel to reduce load and transmission of redundation information. Ideal for growing monotonic hash rings, objects designated with vector clock, CRDTs, etc.
* Failure detection
    * Performed using TCP/IP
    * Connections are verified at each gossip round
    * Reliable delivery and stronger ordering
    * Fault-injection
* Erlang-like API and OTP compliance
    * Partisan offers re-implementations of `gen_server` and `gen_statem`.
    * Monitoring: Partisan offers an API similar to the modules `erlang` and `net_kernel` for monitoring nodes and remote processes. Notice this currently only works for `partisan_pluggable_peer_service_manager` backend.
* Other
    * On join, gossip is performed immediately, instead of having to wait for the next gossip round.
    * Single node testing, facilitated by a disterl control channel for figuring out which ports the peer service is operating at.


## Requirements

* Erlang/OTP 24+


## Documentation
Find the documentation for Partisan releases at [hex.pm](https://hex.pm).

Alternatively you can build it yourself locally using `make docs`.
The resulting documentation will be found in the `docs` directory, just open the `index.html` file with your preferred Web Browser.


## Who is using Partisan

* [Erleans](https://github.com/erleans/erleans)
* [PlumDB](https://github.com/Leapsight/plum_db)
* [Bondy](https://github.com/bondy-io/bondy)
* [Leapsight](https://www.leapsight.com)

