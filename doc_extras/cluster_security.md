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
- There is **no shared-secret / cookie handshake** independent of TLS. If you do
  not deploy certificate-based mTLS, *any* host that can reach the peer port and
  speak the protocol can participate in the cluster's gossip/broadcast.
- A connected peer's membership gossip is merged into local membership; a
  malicious peer can therefore influence membership and cause connections to be
  established to addresses it supplies.

Consequences: treat **network reachability of the peer port as equivalent to
cluster membership**. The two controls that matter are (1) keeping the peer port
off untrusted networks and (2) enabling mTLS so only nodes bearing a
CA-signed certificate are admitted.

At startup Partisan logs its posture: a `NOTICE` when the peer plane is plaintext
(`tls = false`), and a `WARNING` when TLS is on but peers are not verified
(`verify_peer` missing → encrypted but MITM-able).

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

## 3. Bound inbound resources

Two options limit what an inbound peer can force before it is trusted:

| Option | Default | Purpose |
|---|---|---|
| `max_message_size` | `67108864` (64 MB) | Rejects an oversized peer frame before it is assembled/decoded — guards against memory exhaustion and decompression bombs. |
| `tls_handshake_timeout` | `5000` (ms) | Bounds a stalled server-side TLS handshake so it cannot pin an acceptor. |

Tune `max_message_size` down toward the largest legitimate membership/broadcast
payload your deployment produces if you want a tighter bound.

## 4. What Partisan does NOT protect (know the limits)

- **No app-layer peer authentication without TLS.** If `tls = false`, there is no
  cookie/secret check — network reach is enough to join gossip. Certificate mTLS
  is the supported authentication mechanism.
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
- [ ] No startup `WARNING`/`NOTICE` about an insecure peer plane in your logs.
- [ ] `max_message_size` reviewed against your real payload sizes.
