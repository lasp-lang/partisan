# Telemetry Events

Partisan publishes runtime metrics through [`telemetry`](https://github.com/beam-telemetry/telemetry). A consumer attaches a handler to one or more event names and receives, on every occurrence, the event's **measurements** (a map of numeric values) and its **metadata** (a map of context you can use to tag or filter the metric):

```erlang
telemetry:attach(
    my_handler_id,
    [partisan, connection, client, hearbeat],
    fun(_EventName, Measurements, Metadata, _Config) ->
        #{latency := Latency} = Measurements,
        #{peer_node := Peer} = Metadata,
        logger:info("Round-trip to ~p: ~pms", [Peer, Latency])
    end,
    undefined
).
```

This page catalogues every event Partisan currently emits.

## Connection events

### `[partisan, connection, client, hearbeat]`

Fired by `partisan_peer_service_client` on every ping/pong round-trip it completes with the peer it connected to. Emission cadence follows the `connection_ping` configuration: a ping is sent after `idle_timeout` (20000ms by default) of connection inactivity, and this event fires when the matching pong arrives.

> #### The event name misspells "heartbeat" {: .warning}
> The atom is `hearbeat`, not `heartbeat` — a naming defect carried forward from the v5.0.3 release that first introduced this event. Handler code that filters on the event name must match the misspelling as it exists today.

##### Measurements
```erlang
#{
    latency => 12  % milliseconds, round-trip since the ping was sent
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

### `[partisan, connection, server, hearbeat]`

The server-side counterpart: fired by `partisan_peer_service_server` on every ping/pong round-trip it completes with a connected client. Same cadence and the same event-name defect as the client event above.

##### Measurements
```erlang
#{
    latency => 12  % milliseconds, round-trip since the ping was sent
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

## Configuration events

### `[partisan, channel, configured]`

Fired once per channel every time the `channels` configuration parameter is set — at startup, and again on any runtime reconfiguration. `Measurements.max` is the channel's configured `parallelism` (its target connection count), not a live count of open connections.

##### Measurements
```erlang
#{
    max => 4  % the channel's configured `parallelism`
}
```

##### Metadata
```erlang
#{
    channel => user_data,
    channel_opts => #{
        parallelism => 4,
        monotonic => false,
        compression => false
    }
}
```

`channel_opts` is a `t:partisan:channel_opts/0`.
