-define(APP, partisan).
-define(PEER_IP, {127, 0, 0, 1}).
-define(PEER_PORT, 9090).
-define(PEER_SERVICE_SERVER, partisan_peer_service_server).
-define(FANOUT, 5).
-define(CACHE, partisan_connection_cache).
-define(PARALLELISM, 1).
-define(RPC_CHANNEL, rpc).
-define(DEFAULT_CHANNEL, undefined).
-define(DEFAULT_PARTITION_KEY, undefined).
-define(CHANNELS, [?DEFAULT_CHANNEL]).
-define(CONNECTION_JITTER, 1000).

-define(TRACING, false).
-define(RELAY_TTL, 5).
-define(MEMBERSHIP_PROTOCOL_CHANNEL, membership).

%% Pluggable manager.
-define(PERIODIC_INTERVAL, 10000).

%% Scamp protocol.
-define(SCAMP_C_VALUE, 5).
-define(SCAMP_MESSAGE_WINDOW, 10).

-define(DEFAULT_PEER_SERVICE_MANAGER, partisan_pluggable_peer_service_manager).
-define(DEFAULT_MEMBERSHIP_STRATEGY, partisan_full_mesh_membership_strategy).

-define(UTIL, partisan_plumtree_util).
-define(DEFAULT_LAZY_TICK_PERIOD, 1000).
-define(DEFAULT_EXCHANGE_TICK_PERIOD, 10000).

-define(XBOT_MIN_INTERVAL, 5000).
-define(XBOT_RANGE_INTERVAL, 60000).

% parameter used for xbot optimization
% - latency (uses ping to check better nodes)
% - true (always returns true when checking better)
-define(XPARAM, latency).

-type options() :: [{atom(), term()}].

-type actor() :: binary().
-type listen_addr() :: #{ip => inet:ip_address(), port => non_neg_integer()}.
-type node_spec() :: #{name => node(),
                       listen_addrs => [listen_addr()],
                       channels => [channel()],
                       parallelism => non_neg_integer()}.
-type message() :: term().
-type name() :: node().
-type partitions() :: [{reference(), node_spec()}].
-type ttl() :: non_neg_integer().
-type channel() :: atom().
