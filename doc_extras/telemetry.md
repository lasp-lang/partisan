# Telemetry Events

Partisan publishes runtime metrics through [`telemetry`](https://github.com/beam-telemetry/telemetry). A consumer attaches a handler to one or more event names and receives, on every occurrence, the event's **measurements** (a map of numeric values) and its **metadata** (a map of context you can use to tag or filter the metric):

```erlang
telemetry:attach(
    my_handler_id,
    [partisan, connection, client, heartbeat],
    fun(_EventName, Measurements, Metadata, _Config) ->
        #{latency := Latency} = Measurements,
        #{peer_node := Peer} = Metadata,
        logger:info("Round-trip to ~p: ~pms", [Peer, Latency])
    end,
    undefined
).
```

This page catalogues every event Partisan currently emits. Most of what a service mesh gives you for free at the OS/interface level (raw byte counters, container-level connection counts) is *not* duplicated here — these events exist because only Partisan knows the peer, channel, and protocol-level identity behind a given socket or message.

Two conventions hold across every event below: `node` is always *this* node (the one emitting the event) — a remote peer, where relevant, is always `peer_node`, never `node`. And `count` is always a delta meant to be summed into a rate (`#{count => 1}`); a gauge's live value always uses a different key (`size`, `total`, `value`, or a named field like `active`/`passive`) — never `count`.

`node` is `undefined` instead of an atom on the rare event that fires before this node has finished configuring its own name (see `[partisan, channel, configured]`) — an explicit "not known yet" rather than a value (e.g. the underlying distributed-Erlang node name) that could differ from this node's configured Partisan name and never reappear in any later event.

## Connection events

### `[partisan, connection, client, connect]`

Fired by `partisan_peer_service_client` on every outbound connection attempt (`init/1`), whether it succeeds or fails. For a TLS-enabled deployment this includes the TLS handshake — `partisan_peer_socket:connect/5` performs TCP connect and TLS handshake as one call, so there is no separate client-side handshake event.

##### Measurements
```erlang
#{
    latency => 8  % milliseconds spent in the connection attempt, success or failure
}
```

##### Metadata
```erlang
#{
    node => 'client@192.168.0.20',
    peer_node => 'server@192.168.0.21',
    channel => default,
    listen_addr => #{ip => {192,168,0,21}, port => 10200},
    result => ok  % or `error`
}
```

### `[partisan, socket, server, handshake]`

Fired by `partisan_peer_socket:accept/1` after a TLS handshake on an inbound connection, whether it succeeds or fails. Only fires when TLS is enabled (`tls = true`) — a plain TCP accept has no separate handshake phase to measure. This is the exact code path a failed/slowloris-style handshake used to crash the acceptor on before it was hardened to close and terminate normally; a spike in `result => error` here is the production signal for that class of problem.

##### Measurements
```erlang
#{
    latency => 15  % milliseconds spent in ssl:handshake/3
}
```

##### Metadata
```erlang
#{
    node => 'server@192.168.0.21',
    result => error,       % or `ok`
    reason => timeout      % only present when result is `error`
}
```

### `[partisan, connection, client, heartbeat]`

Fired by `partisan_peer_service_client` on every ping/pong round-trip it completes with the peer it connected to. Emission cadence follows the `connection_ping` configuration: a ping is sent after `idle_timeout` (20000ms by default) of connection inactivity, and this event fires when the matching pong arrives.

##### Measurements
```erlang
#{
    latency => 12,      % milliseconds, round-trip since the ping was sent

    %% The remaining keys come straight from `inet:getstat/2` (transparently via
    %% `ssl:getstat/2` for a TLS connection) on the socket already in state — no
    %% counting done by Partisan. `send_pend` is the backpressure signal: bytes
    %% queued to send but not yet flushed to the kernel.
    recv_cnt => 204,    % packets received on this socket since it opened
    recv_oct => 51200,  % bytes received
    send_cnt => 198,    % packets sent
    send_oct => 49800,  % bytes sent
    send_pend => 0      % bytes currently queued to send
}
```

##### Metadata
```erlang
#{
    node => 'client@192.168.0.20',      % this node
    peer_node => 'server@192.168.0.21', % the peer the connection is to
    channel => default,                 % partisan:channel()
    listen_addr => #{ip => {192,168,0,21}, port => 10200},
    socket => Socket                    % partisan_peer_socket:t()
}
```

### `[partisan, connection, server, heartbeat]`

The server-side counterpart: fired by `partisan_peer_service_server` on every ping/pong round-trip it completes with a connected client. Same cadence and the same `inet:getstat/2` measurements as the client event above.

##### Measurements
```erlang
#{
    latency => 12,
    recv_cnt => 204,
    recv_oct => 51200,
    send_cnt => 198,
    send_oct => 49800,
    send_pend => 0
}
```

##### Metadata
```erlang
#{
    node => 'server@192.168.0.21',      % this node
    peer_node => 'client@192.168.0.20', % the peer the connection is from
    channel => default,                 % partisan:channel()
    socket => Socket                    % partisan_peer_socket:t()
}
```

### `[partisan, connection, up]`

Fired by `partisan_peer_connections:store/4` whenever a connection is added to the connections table — the single choke point every peer service manager (pluggable, HyParView, static, client-server) uses to record a new connection. A pure counter: sum over a window for a connection-open rate.

##### Measurements
```erlang
#{count => 1}
```

##### Metadata
```erlang
#{
    node => 'client@192.168.0.20',       % this node
    peer_node => 'server@192.168.0.21',  % the peer this connection is to/from
    channel => default,
    listen_addr => #{ip => {192,168,0,21}, port => 10200}
}
```

### `[partisan, connection, down]`

Fired by `partisan_peer_connections:prune/2` for every connection removed from the table — the single choke point every manager uses to record a connection teardown. `reason` is whatever the connection process' exit reason was (e.g. `normal` for a voluntary disconnect, `noconnection`, `tls_alert`, or a raw socket error) — `undefined` if the caller used `prune/1`, which does not carry one.

##### Measurements
```erlang
#{count => 1}
```

##### Metadata
```erlang
#{
    node => 'server@192.168.0.21',       % this node
    peer_node => 'client@192.168.0.20',  % the peer this connection was to/from
    channel => default,
    reason => normal
}
```

## Channel events

### `[partisan, channel, configured]`

Fired once per channel every time the `channels` configuration parameter is set — at startup, and again on any runtime reconfiguration. `Measurements.max` is the channel's configured `parallelism` (its target connection count), not a live count of open connections.

This is the one event that can fire before a node has finished configuring its own name — `channels` can be set very early in boot. When that happens, `node` is `undefined` rather than an atom (see the note at the top of this page).

##### Measurements
```erlang
#{
    max => 4  % the channel's configured `parallelism`
}
```

##### Metadata
```erlang
#{
    node => 'server@192.168.0.21',
    channel => user_data,
    channel_opts => #{
        parallelism => 4,
        monotonic => false,
        compression => false
    }
}
```

`channel_opts` is a `t:partisan:channel_opts/0`.

### `[partisan, channel, connections]`

A gauge fired alongside every `[partisan, connection, up]`/`[partisan, connection, down]`: the current connection count for a given peer/channel against that channel's configured `parallelism`. `target` is `undefined` if `channel` is not currently configured. `size < target` is the direct signal that a channel is running under-provisioned for that peer (e.g. after churn, before the manager re-establishes the missing connections).

##### Measurements
```erlang
#{
    size => 3,    % current connection count for this node/channel
    target => 4   % the channel's configured `parallelism`, or `undefined`
}
```

##### Metadata
```erlang
#{
    node => 'server@192.168.0.21',       % this node
    peer_node => 'client@192.168.0.20',  % the peer this count is for
    channel => default
}
```

## Membership events

### `[partisan, membership, changed]`

Fired by `partisan_membership:set/1` — the single write path every peer service manager uses to publish a membership change — whenever the member set actually differs from what was previously published (diffed by node name; re-asserting the same membership emits nothing). This is the one event every manager produces regardless of overlay topology.

##### Measurements
```erlang
#{
    added => 1,    % nodes present now that were not present before
    removed => 0,  % nodes present before that are gone now
    total => 4     % total member count after this change (a gauge, not a delta — see the `count` note on `[partisan, channel, connections]`)
}
```

##### Metadata
```erlang
#{
    node => 'server@192.168.0.21',
    version => 42  % partisan_membership:version/0 after this write
}
```

## Broadcast events

### `[partisan, broadcast, interior_load]`

Fired after every repair tick of a broadcast group running a **raw-dispatch** engine (currently Thicket, `partisan_thicket_engine`) that exports `interior_load/1`. This is the exact measurement PDDR-000002/PDDR-000004 gate *enabling* Thicket for a group on: the number of trees this node is currently an interior (forwarding) node for, against the group's configured `max_load`. A Plumtree group (the default, typed-dispatch engine) never emits this — there is no interior-load concept in Plumtree.

##### Measurements
```erlang
#{value => 2}  % number of trees this node is interior for
```

##### Metadata
```erlang
#{
    node => 'server@192.168.0.21',
    group => partisan_plumtree_broadcast,  % the broadcast group's registered name
    engine => partisan_thicket_engine
}
```

## HyParView events

These events only fire when the configured peer service manager is `partisan_hyparview_peer_service_manager`.

### `[partisan, hyparview, view, size]`

A gauge of the active- and passive-view sizes, fired on the periodic `active_view_maintenance` tick (cadence: `active_view_maintenance_interval`, defaulting to `random_promotion_interval`). Active view is bounded by `active_max_size` (default 6); passive by `passive_max_size` (default 30).

##### Measurements
```erlang
#{
    active => 6,
    passive => 24
}
```

##### Metadata
```erlang
#{node => 'server@192.168.0.21'}
```

### `[partisan, hyparview, active_view, peer_added]`

Fired whenever an inbound `NEIGHBOR` message causes a peer to be added to the active view where it was not already present. This handler serves double duty — a peer's genuine first `NEIGHBOR` (a fresh join) and the periodic active-view symmetry re-assertion both land here — so a single occurrence does not by itself distinguish the two. On an otherwise-quiescent cluster (no recent `[partisan, membership, changed]`), a trickle of these is the asymmetry-repair signal: evidence that a `NEIGHBOR`/`DISCONNECT` was lost during churn and self-healed.

##### Measurements
```erlang
#{count => 1}
```

##### Metadata
```erlang
#{
    node => 'server@192.168.0.21',
    peer_node => 'client@192.168.0.20'
}
```

## Causal messaging events

These events only fire for a broadcast label configured in `causal_labels` (empty, i.e. disabled, by default) — one `partisan_causality_backend` instance runs per configured label.

### `[partisan, causal, backlog]`

Fired on every redelivery tick (cadence: `redelivery_interval`, default 1000ms) of the causal-delivery backend: the size of the redelivery queue, the size of the per-peer order buffer, and the highest delivery-attempt count among currently-buffered messages. Neither queue is bounded today, so both sizes are also unbounded-growth canaries; `max_attempts` climbing without bound is the signal for a message whose causal dependency will never arrive (e.g. a lost message, or a sender that crashed before its dependency was delivered) — it stays buffered forever with no other visibility.

##### Measurements
```erlang
#{
    buffered => 0,          % messages waiting on an unmet causal dependency
    order_buffer_size => 3, % distinct peers this node tracks a causal clock for
    max_attempts => 0       % highest redelivery-attempt count among buffered messages
}
```

##### Metadata
```erlang
#{
    node => 'server@192.168.0.21',
    label => default  % the configured causal_labels entry
}
```
