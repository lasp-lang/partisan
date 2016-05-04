Partisan
=======================================================

[![Build Status](https://travis-ci.org/lasp-lang/partisan.svg?branch=master)](https://travis-ci.org/lasp-lang/partisan)

Partisan is not complete, and is currently a work-in-progress replacement
for the Plumtree peer service manager that comes with it.

Partisan is still full membership and under active development (for now!).

* Single node testing, facilitated by a disterl control channel for figuring out which ports the peer service is operating at.
* Designed to be compatible with the Lasp fork of Basho's plumtree implementation.
* Messages are sent via TCP connections that are maintained to all cluster members.
* Failure detection is performed TCP.
* Connections are verified at each gossip round.
* Configurable fanout.
* On join, gossip is performed immediately, instead of having to wait for the next gossip round.

Next steps:

* Augment protocol to support partial views via HyParView
* Investigate optimizations from X-BOT paper for better clustering coefficients and degree distribution
