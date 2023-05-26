# Telemetry Events

Partisan uses the `telemetry` library for instrumentation.

A Telemetry event is made up of the following:

* `name` - A list of atoms that uniquely identifies the event.

* `measurements` - A map of atom keys (e.g. duration) and numeric values.

* `metadata` - A map of key-value pairs that can be used for tagging metrics.

## Membership Events

### [partisan, membership, peer, join]
This event is triggered when a node joins the cluster.

##### Measurements
```erlang
#{
    count => 1
}
```

##### Metadata
```erlang
#{
    name => 'node@192.168.0.20',
    listen_addrs => [
        #{ip => {127,0,0,1}, port => 10200}
    ]
}
```

### [partisan, membership, peer, leave]
This event is triggered when a node leaves the cluster.

##### Measurements
```erlang
#{
    count => 1
}
```

##### Metadata
```erlang
#{
    name => 'node@192.168.0.20',
    listen_addrs => [
        #{ip => {127,0,0,1}, port => 10200}
    ]
}
```

### [partisan, membership, peer, up]
This event is triggered when a node successfully establishes all necessary channel connections with a peer. In other words, the node has established the number of connections for each channel based on the `parallelism` setting for the corresponding channel.

##### Measurements
```erlang
#{
    count => 1
}
```

##### Metadata
```erlang
#{
    name => 'node@192.168.0.20',
    listen_addrs => [
        #{ip => {127,0,0,1}, port => 10200}
    ]
}
```

### [partisan, membership, peer, down]
This event occurs when a peer disconnects, and there are no established channel connections with that peer.

##### Measurements
```erlang
#{
    count => 1
}
```

##### Metadata

```erlang
#{
    name => 'node@192.168.0.20',
    listen_addrs => [
        #{ip => {127,0,0,1}, port => 10200}
    ]
}
```


### [partisan, membership, peer, channel, up]
Emitted every time a channel reaches the desired number of connections i.e. `parallelism`.

##### Measurements
```erlang
#{
    count => 1,
    sockets => 3
}
```

##### Metadata
```erlang
#{

}
```

### [partisan, membership, peer, channel, down]
Emitted every time a channel reaches the desired number of connections i.e. `parallelism`.

##### Measurements
```erlang
#{

}
```

##### Metadata
```erlang
#{

}
```



## Channel Events
### [partisan, channel, configured]

Emitted once for every channel every time the `channels` configuration parameter is modified. It provides a measurement of the count of channels. The metadata includes the channel name and options i.e. `partisan:channel_opts()`.

##### Measurements
```erlang
#{
    count => 1
}
```

##### Metadata
```erlang
#{
    name => user_data,
    config => #{
        monotonic => false,
        compression => false,
        parallelism => 4
    }
}
```

## Socket Events

### [partisan, socket, open]
Emitted once for every socket that is open and waiting for connection with another socket. It provides a measurement of the count of connect `open` events.

##### Measurements
```erlang
#{

}
```

##### Metadata
```erlang
#{

}
```

### [partisan, socket, connect]

##### Measurements
```erlang
#{

}
```

##### Metadata
```erlang
#{

}
```
### [partisan, socket, recv]

##### Measurements
```erlang
#{

}
```

##### Metadata
```erlang
#{

}
```
### [partisan, socket, send]

##### Measurements
```erlang
#{

}
```

##### Metadata
```erlang
#{

}
```

### [partisan, socket, close]

##### Measurements
```erlang
#{

}
```

##### Metadata
```erlang
#{

}
```




* `[partisan, channel, configured]` -
* `[partisan, socket, open]` -
* `[partisan, socket, connect]` -
* `[partisan, socket, recv]` -
* `[partisan, socket, send]` -
* `[partisan, socket, close]` -
* `[partisan, socket, tag_msg]` -
* `[partisan, connection, connections]` -
* `[partisan, connection, monitors]` -
* `[partisan, connection, new]` -
* `[partisan, connection, outstanding]` -
* `[partisan, connection, owners]` -
* `[partisan, queue, unacknowledged, count]` -
* `[partisan, queue, unacknowledged, bytes]` -
* `[partisan, queue, retransmitted, count]` -
* `[partisan, queue, retransmitted, bytes]` -
* `[partisan, queue, undelivered, count]` -
* `[partisan, queue, undelivered, bytes]` -
* `[partisan, queue, delivered, count]` -
* `[partisan, queue, delivered, bytes]` -
plumbtree_broadcast oustanding, neweagers, lazy_sets, all_members (ordset)
