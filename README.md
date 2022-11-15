# Partisan

[![Build Status](https://travis-ci.org/lasp-lang/partisan.svg?branch=master)](https://travis-ci.org/lasp-lang/partisan)

Partisan is a scalable and flexible, TCP-based membership system and distribution layer for the BEAM. It bypasses the use of Distributed Erlang for manual connection management via TCP, and has several pluggable backends for different deployment scenarios.


## Partisan features

* Erlang-like API
* OTP compliance: Partisan offers re-implementations of `gen_server` and `gen_statem`.
* Monitoring: Partisan offers an API similar to the modules `erlang` and `net_kernel` for monitoring nodes and remote processes.
* Messages are sent via TCP connections that are maintained to a subset or all cluster members (depending on the backend).
* Failure detection is performed using TCP.
* Connections are verified at each gossip round.
* Configurable number of connections between nodes (named channels and fanout).
* On join, gossip is performed immediately, instead of having to wait for the next gossip round.
* Single node testing, facilitated by a disterl control channel for figuring out which ports the peer service is operating at.

Partisan has many available backends a.k.a peer service managers:

* `partisan_pluggable_peer_service_manager`: full mesh with TCP-based failure detection. All nodes maintain active connections to all other nodes in the system using one or more TPC connections.
* `partisan_hyparview_peer_service_manager.`: modified implementation of the HyParView protocol, peer-to-peer, designed for high scale, high churn environments. A hybrid partial view membership protocol, with TCP-based failure detection.
* `partisan_client_server_peer_service_manager.`: star topology, where clients communicate with servers, and servers communicate with other servers.
* `partisan_static_peer_service_manager`: static membership, where connections are explicitly made between nodes

## Requirements

* Erlang/OTP 24+


## Documentation
Find the documentation at [hex.pm](https://hex.pm).

Alternatively you can build it yourself locally using `make docs`.

The resulting documentation will be found in the `docs` directory.


## Who is using Partisan

* [Erleans](https://github.com/erleans/erleans)
* [PlumDB](https://github.com/Leapsight/plum_db)
* [Bondy](https://github.com/bondy-io/bondy)
* [Leapsight](https://www.leapsight.com)

