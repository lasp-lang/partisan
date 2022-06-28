-define(APP, partisan).
-define(PEER_IP, {127, 0, 0, 1}).
-define(PEER_PORT, 9090).
-define(FANOUT, 5).
-define(PLUMTREE_OUTSTANDING, partisan_plumtree_broadcast).
-define(CONNECTION_JITTER, 1000).
-define(RELAY_TTL, 5).

-define(CHANNELS, [?DEFAULT_CHANNEL]).
-define(MEMBERSHIP_PROTOCOL_CHANNEL, membership).
-define(RPC_CHANNEL, rpc).
-define(DEFAULT_CHANNEL, undefined).
-define(GOSSIP_CHANNEL, gossip).

-define(PARALLELISM, 1).
-define(DEFAULT_PARTITION_KEY, undefined).

-define(CAUSAL_LABELS, []).

%% Gossip.

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

-define(PEER_SERVICE_MANAGER,
    partisan_config:get(
        partisan_peer_service_manager,
        ?DEFAULT_PEER_SERVICE_MANAGER
    )
).

%% Pluggable manager options.
-define(DISTANCE_ENABLED, true).
-define(PERIODIC_ENABLED, true).

%% Test variables.
-define(MEMBERSHIP_STRATEGY_TRACING, false).

-record(property_state,
        {joined_nodes :: [node()],
         nodes :: [node()],
         node_state :: {dict:dict(), dict:dict()},
         fault_model_state :: term(),
         counter :: non_neg_integer()}).

-define(SUPPORT, partisan_support).

-define(OVERRIDE_PERIODIC_INTERVAL, 10000).

-define(DEFAULT_LAZY_TICK_PERIOD, 1000).
-define(DEFAULT_EXCHANGE_TICK_PERIOD, 10000).

-define(XBOT_MIN_INTERVAL, 5000).
-define(XBOT_RANGE_INTERVAL, 60000).

-if(?OTP_RELEASE >= 25).
    -define(PARALLEL_SIGNAL_OPTIMISATION(L),
        lists:keystore(
            message_queue_data, 1, L, {message_queue_data, off_heap}
        )
    ).
-else.
    -define(PARALLEL_SIGNAL_OPTIMISATION(L), L).
-endif.

% parameter used for xbot optimization
% - latency (uses ping to check better nodes)
% - true (always returns true when checking better)
-define(XPARAM, latency).

-type options() :: [{atom(), term()}] | #{atom() => term()}.

-type actor() :: binary().
-type listen_addr() :: #{ip => inet:ip_address(), port => non_neg_integer()}
                        | #{host => list(), port => non_neg_integer()}.
-type node_spec() :: #{name => node(),
                       listen_addrs => [listen_addr()],
                       channels => [channel()],
                       parallelism => non_neg_integer()}.
-type message() :: term().
-type partitions() :: [{reference(), node_spec()}].
-type ttl() :: non_neg_integer().
-type channel() :: atom().
-type remote_ref() :: {
        partisan_remote_reference,
        node(),
        process_ref() | registered_name_ref() | encoded_ref()
}.
-type remote_ref(T) :: {
        partisan_remote_reference,
        node(),
        T
}.
-type remote_process_ref() :: remote_ref(process_ref()).
-type remote_registered_name_ref() :: remote_ref(registered_name_ref()).
-type remote_encoded_ref() :: remote_ref(encoded_ref()).
-type process_ref()  :: {partisan_process_reference, list()}.
-type registered_name_ref()  :: {partisan_registered_name_reference, list()}.
-type encoded_ref()  :: {partisan_encoded_reference, list()}.

%% TODO: add type annotations
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
