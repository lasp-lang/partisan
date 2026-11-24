# Securing the cluster peer plane

Partisan nodes talk to each other over a **peer (inter-node) plane** — a
TCP-based transport, separate from Erlang distribution (`connect_disterl` is
`false` by default). This guide explains its security posture and how to harden
it. Read it before exposing a Partisan cluster to anything but a fully trusted
network.

## Threat model — read this first

By default the peer plane is **plaintext and unauthenticated**:

- `tls` defaults to `false` — peer traffic (including any replicated
  application state) is sent in the clear and can be read or modified on-path.
- `peer_auth_methods` defaults to `[]` — **no peer authentication runs**. Unless
  you enable it (section 3) or deploy certificate-based mTLS (section 2), *any*
  host that can reach the peer port and speak the protocol can participate in
  the cluster's gossip/broadcast.
- A node decodes and delivers every frame from a peer that has not
  authenticated. One frame can exhaust the atom table and stop the node. One
  frame can also deliver a message to any registered process of the node,
  which includes the remote procedure call server: a host that reaches the
  peer port can run code on the node.
- A connected peer's membership gossip is merged into local membership; a
  malicious peer can therefore influence membership and cause connections to be
  established to addresses it supplies.

Consequences: with the defaults, treat **network reachability of the peer port
as equivalent to cluster membership**. The controls that matter are (1) keeping
the peer port off untrusted networks, (2) enabling mTLS so only nodes bearing a
CA-signed certificate are admitted, and (3) enabling peer authentication so
only nodes that hold the cluster's keys are admitted.

At startup Partisan logs its posture: a `WARNING` when the node does not
authenticate its peers, with or without TLS, and a `NOTICE` when it does but
the peer plane is plaintext. `partisan_peer_auth:authenticates_peers/0`
returns the same answer to an application.

## 1. Isolate the peer port

The peer listener binds `listen_addrs` / `listen_port` (`peer_port`, default
`12345`). **Never expose it to the internet or to tenant/untrusted networks.**

- Bind it to a private interface, not `0.0.0.0`.
- Restrict it with a firewall / security group / network policy to the cluster's
  own nodes only.
- On shared networks (shared VPC, container overlay), assume lateral movement is
  possible and enable mTLS (below) in addition to network controls.

## 2. Enable cluster mTLS (authenticated + encrypted)

Partisan passes `tls_client_options` / `tls_server_options` straight to `ssl`,
so full mutual TLS is achievable in configuration. Encryption alone
(`verify_none`) is **not** enough — it stops on-path reading but not a rogue peer
presenting any certificate. You want `verify_peer` with a **private cluster CA**
so only nodes holding a CA-signed cert are admitted.

1. **Create a cluster CA** and issue a key/cert per node signed by it. For local
   experimentation, `test/make_certs.erl` shows how to generate a CA and node
   certs; for production use your own PKI / secrets tooling.
2. **Configure every node** (both sides — a node is a server for inbound peers
   and a client for outbound):

   ```erlang
   {partisan, [
       {tls, true},
       {tls_server_options, [
           {certfile,   "/etc/partisan/tls/node-cert.pem"},
           {keyfile,    "/etc/partisan/tls/node-key.pem"},
           {cacertfile, "/etc/partisan/tls/cluster-ca.pem"},
           {verify, verify_peer},
           {fail_if_no_peer_cert, true}
       ]},
       {tls_client_options, [
           {certfile,   "/etc/partisan/tls/node-cert.pem"},
           {keyfile,    "/etc/partisan/tls/node-key.pem"},
           {cacertfile, "/etc/partisan/tls/cluster-ca.pem"},
           {verify, verify_peer}
       ]}
   ]}
   ```

3. **Optional hostname verification** on the client side: set
   `{hostname_verification, wildcard}` in `tls_client_options` (Partisan rewrites
   it into the appropriate `customize_hostname_check`).

With `verify_peer` + `fail_if_no_peer_cert`, a peer without a CA-signed
certificate cannot complete the connection, which gates the membership/gossip
path behind cryptographic peer authentication.

## 3. Enable peer authentication

Peer authentication is a challenge and response exchange that runs on every new
connection, before `hello`. It works with and without TLS. Until a peer
authenticates, a node accepts only authentication frames of at most 4096 bytes
and reads them without the term decoder, so a host without the cluster's keys
cannot reach the message decoder, the membership or any process of the node.

Mutual TLS (section 2) gives the same protection on its own: a host without a
certificate of the cluster CA does not complete the TLS handshake and sends no
frame. A cluster with mutual TLS does not need peer authentication. The two
combine: `cryptosign` over mutual TLS also binds each node name to a key, which
a certificate check alone does not. Peer authentication over TLS 1.3 without
certificate verification is also sound: the keys authenticate the nodes and
the exchange is bound to the TLS session. `m:partisan_peer_auth`
describes the exchange, and ADR-000005 records the design.

`peer_auth_methods` lists the methods a node accepts, strongest first:

| Method | Trust material | What a peer proves |
|---|---|---|
| `cryptosign` | `peer_auth_cryptosign`: this node's Ed25519 private key, and the public key of each trusted peer by name | That it is the named node. |
| `gossip_keyring` | `peer_auth_keyring`: an ordered list of shared keys | That it holds a key of the cluster. Keys rotate without downtime. |
| `hmac_secret` | `peer_auth_secret`: one shared key | That it holds the key of the cluster. |
| `anonymous` | none | Nothing. Use it only while you roll authentication out. |

A shared key is a binary of at least 16 bytes. Use 32 random bytes, for example
`crypto:strong_rand_bytes(32)`.

```erlang
{partisan, [
    {peer_auth_methods, [hmac_secret]},
    {peer_auth_secret, <<"a 32-byte value from your secret store">>}
]}
```

For `cryptosign`, make a key pair per node with
`partisan_peer_auth_cryptosign:generate_key/0`:

```erlang
{partisan, [
    {peer_auth_methods, [cryptosign]},
    {peer_auth_cryptosign, #{
        private_key => <<"64 hexadecimal characters">>,
        trusted_peers => #{
            'node2@host2' => <<"64 hexadecimal characters">>,
            'node3@host3' => <<"64 hexadecimal characters">>
        }
    }}
]}
```

A node does not start when a method in its list has no trust material.

A node accepts whichever listed method the connecting node names. The weakest
method in the list is therefore what the list guarantees: `[cryptosign,
hmac_secret]` admits every holder of the shared key, under any node name.

### Allow only named nodes

`peer_auth_allowed_nodes` is the gate of `net_kernel:allow/1`. The default,
`all`, allows every node. With a list, the node refuses a peer that presents
another name and does not dial one. `partisan_peer_auth:allow/1` adds nodes
while the node runs.

```erlang
{partisan, [
    {peer_auth_allowed_nodes, ['node1@host1', 'node2@host2', 'node3@host3']}
]}
```

Only `cryptosign` proves that a peer may use the name it presents. With a
shared-key method the list stops a mistake, not a holder of the key.

### Enable it on a running cluster

A node that requires authentication refuses a node that does not authenticate,
in both directions. To enable authentication without a partition:

1. Upgrade every node to a release that has peer authentication. A node on an
   earlier release cannot connect to a node with a non-empty
   `peer_auth_methods`.
2. On every node, set the trust material and a list that ends in `anonymous`,
   for example `[hmac_secret, anonymous]`. Restart the node. Nodes now
   authenticate where both ends can, and still accept the others.
3. On every node, remove `anonymous` from the list. Restart the node.

The node is protected only after step 3. While its list contains `anonymous`,
or is empty, a peer can skip the proof.

### Rotate a keyring

A node signs with the first key of `peer_auth_keyring` and accepts a proof made
with any key in the list.

1. Add the new key at the end of the list on every node.
2. Move the new key to the front of the list on every node.
3. Remove the old key from the list on every node.

### What peer authentication does not cover

- It does not encrypt or sign the traffic that follows it. Over TCP, a party
  that can alter traffic between two nodes can let the exchange pass and then
  inject frames. Use TLS against that party.
- Over TLS 1.3 the exchange is bound to the TLS session: a party that
  terminates TLS between two nodes cannot relay it, even when the nodes do not
  verify certificates. An earlier TLS version has no such binding. Two nodes
  that both allow TLS 1.3 refuse a connection that was forced down to an
  earlier version. A node whose `versions` option excludes `'tlsv1.3'` gets
  no binding with any peer.
- The shared-key methods do not identify a node: every holder of a key can use
  any node name. Only `cryptosign` binds a name to a key.
- A peer that has authenticated is trusted, as a node with the cookie is in
  Distributed Erlang. It can send a message to any process and call any
  function on any node, read the trust material of the nodes it reaches, send
  a frame of up to `max_message_size`, and have its membership gossip merged.
  One compromised node, or one leaked shared key, is the whole cluster.
- TLS options that list `'tlsv1.3'` in `versions` but exclude it in another
  way, for example with `ciphers` or `signature_algs`, make every connection
  fail with `channel_binding_downgrade`. Remove `'tlsv1.3'` from `versions`
  on such a node.

## 4. Bound inbound resources

These options limit what an inbound peer can force before it is trusted:

| Option | Default | Purpose |
|---|---|---|
| `max_message_size` | `67108864` (64 MB) | Rejects an oversized peer frame before it is assembled/decoded — guards against memory exhaustion and decompression bombs. |
| `tls_handshake_timeout` | `5000` (ms) | Bounds a stalled server-side TLS handshake so it cannot pin an acceptor. |
| `peer_auth_timeout` | `5000` (ms) | Closes a connection whose authentication exchange does not complete. |
| `peer_auth_max_pending` | `1024` | The most accepted connections that have not authenticated. The node closes one more at once. Applies when `peer_auth_methods` is not empty. |

Tune `max_message_size` down toward the largest legitimate membership/broadcast
payload your deployment produces if you want a tighter bound.

On OTP 27, `ssl` does not apply this limit: a TLS connection accepts a frame
of any size from a peer. The limit of the authentication phase does not
depend on it. On OTP 27 with TLS, `max_message_size` therefore bounds no peer
that has authenticated, and no peer at all on a node that does not require
authentication. OTP 28 and later apply it.

## 5. Partisan and Distributed Erlang

Partisan lets a node do what a node can do in Distributed Erlang, and gives
the same kind of gates. Neither restricts what a member does once it is
admitted.

| Distributed Erlang | Partisan |
|---|---|
| Cookie | `peer_auth_methods` with `hmac_secret` or `gossip_keyring` |
| A cookie per node (`erlang:set_cookie/2`) | `cryptosign`, a key per node |
| `net_kernel:allow/1` | `peer_auth_allowed_nodes`, `partisan_peer_auth:allow/1` |
| TLS distribution (`-proto_dist inet_tls`) | `tls`, `tls_server_options`, `tls_client_options` |
| `net_setuptime` | `peer_auth_timeout`, `tls_handshake_timeout` |
| Listen interface and port range | `listen_addrs` |
| No gate on remote calls and spawns | No gate on `partisan_rpc`, `partisan_erpc` or `forward_message` |

## 6. What Partisan does NOT protect (know the limits)

- **The orchestration store is trusted.** With an `orchestration_strategy`
  set, a node reads node specifications from an external store (Redis) and
  decodes them as Erlang terms. Whoever can write to that store can exhaust
  the atom table of every node and give the nodes addresses to dial. Restrict
  access to the store as you restrict the peer port. The default is no
  orchestration strategy.

- **No peer authentication by default.** With `peer_auth_methods = []` and
  without mTLS, network reach is enough to join gossip, and to send a frame
  that stops the node.
- **Membership entries from a connected peer are trusted.** Even with mTLS,
  authenticated nodes are trusted to gossip membership; mTLS restricts *who* is
  authenticated, network isolation restricts *who can reach the port*.
- **At-rest protection of any replicated data is the application's concern** —
  Partisan secures the transport, not storage.

## Secure-deployment checklist

- [ ] Peer port bound to a private interface, never `0.0.0.0`.
- [ ] Peer port firewalled to cluster nodes only.
- [ ] `tls = true` with `verify_peer` + a private cluster CA on **both**
      `tls_server_options` and `tls_client_options`.
- [ ] `fail_if_no_peer_cert = true` on the server side.
- [ ] `peer_auth_methods` set on every node, without `anonymous`.
- [ ] No startup `WARNING`/`NOTICE` about an insecure peer plane in your logs.
- [ ] `max_message_size` reviewed against your real payload sizes.
