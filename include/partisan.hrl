-define(APP, partisan).
-define(PEER_IP, {127, 0, 0, 1}).
-define(PEER_PORT, 9090).
-define(PEER_SERVICE_SERVER, partisan_peer_service_server).
-define(FANOUT, 5).
-define(CACHE, partisan_connection_cache).
-define(CONNECTION_JITTER, 1000).
-define(TRACING, false).
-define(RELAY_TTL, 5).
-define(MEMBERSHIP_PROTOCOL_CHANNEL, membership).

%% Optimizations.
-define(RPC_CHANNEL, rpc).
-define(DEFAULT_CHANNEL, undefined).
-define(DEFAULT_PARTITION_KEY, undefined).
-define(PARALLELISM, 1).                            %% How many connections should exist between nodes?
% -define(CHANNELS,                                 %% What channels should be established?
%         [undefined, broadcast, vnode, {monotonic, gossip}]).   
-define(CHANNELS, [?DEFAULT_CHANNEL]).
-define(CAUSAL_LABELS, []).                         %% What causal channels should be established?

%% Gossip.
-define(GOSSIP_CHANNEL, gossip).
-define(GOSSIP_FANOUT, 5). %% TODO: FIX ME.
-define(GOSSIP_GC_MIN_SIZE, 10).

%% Pluggable manager.
-define(PERIODIC_INTERVAL, 10000).

%% Scamp protocol.
-define(SCAMP_C_VALUE, 5). %% TODO: FIX ME.
-define(SCAMP_MESSAGE_WINDOW, 10).

%% Defaults.
-define(DEFAULT_PEER_SERVICE_MANAGER, partisan_pluggable_peer_service_manager).
-define(DEFAULT_MEMBERSHIP_STRATEGY, partisan_full_membership_strategy).
-define(DEFAULT_ORCHESTRATION_STRATEGY, undefined).

%% Test variables.
-define(TEST_NUM_NODES, 4).

-define(SUPPORT, partisan_support).

-define(OVERRIDE_PERIODIC_INTERVAL, 1000).

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

-record(orchestration_strategy_state, 
               {orchestration_strategy,
                is_connected,
                was_connected,
                attempted_nodes,
                peer_service,
                graph,
                tree,
                eredis,
                servers,
                nodes}).