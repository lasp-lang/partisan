Partisan
=======================================================

[![Build Status](https://travis-ci.org/lasp-lang/partisan.svg?branch=master)](https://travis-ci.org/lasp-lang/partisan)

Partisan is a flexible, TCP-based membership system for Erlang/Elixir.

Partisan features:

* Single node testing, facilitated by a disterl control channel for figuring out which ports the peer service is operating at.
* Messages are sent via TCP connections that are maintained to all cluster members.
* Failure detection is performed TCP.
* Connections are verified at each gossip round.
* Configurable fanout.
* On join, gossip is performed immediately, instead of having to wait for the next gossip round.
* HyParView implementation.

Partisan has many available peer service managers:

* Full membership with TCP-based failure detection: `partisan_pluggable_peer_service_manager.`
* Client/server topology: `partisan_client_server_peer_service_manager.`
* HyParView, hybrid partial view membership protocol, with TCP-based failure detection: `partisan_hyparview_peer_service_manager.`
* Static topology: `partisan_static_peer_service_manager`.
