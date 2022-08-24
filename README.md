# Partisan

[![Build Status](https://travis-ci.org/lasp-lang/partisan.svg?branch=master)](https://travis-ci.org/lasp-lang/partisan)

Partisan is a flexible, TCP-based membership system for Erlang/Elixir.

Partisan features:
* Erlang-like API
* OTP compliance: Partisan offers re-implementations of `gen_server` and `gen_statem`.
* Monitoring: Partisan offers an PI similar to the modules `erlang` and `net_kernel` for monitoring nodes and remote processes.
* Messages are sent via TCP connections that are maintained to all cluster members.
* Failure detection is performed using TCP.
* Configurable number of connections between nodes (named channels and fanout).
* Connections are verified at each gossip round.
* On join, gossip is performed immediately, instead of having to wait for the next gossip round.
* HyParView implementation.
* Single node testing, facilitated by a disterl control channel for figuring out which ports the peer service is operating at.

Partisan has many available peer service managers:

* Full membership with TCP-based failure detection: `partisan_pluggable_peer_service_manager.`
* Client/server topology: `partisan_client_server_peer_service_manager.`
* HyParView, hybrid partial view membership protocol, with TCP-based failure detection: `partisan_hyparview_peer_service_manager.`
* Static topology: `partisan_static_peer_service_manager`.

## Requirements

* Erlang/OTP 24+


## Documentation
Find the documentation at hex.pm or build yourself locally


