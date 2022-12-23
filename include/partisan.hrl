-define(APP, partisan).
-define(PEER_IP, {127, 0, 0, 1}).
-define(PEER_PORT, 9090).



%% PLUMTREE
-define(PLUMTREE_OUTSTANDING, partisan_plumtree_broadcast).
-define(BROADCAST_MODS, [partisan_plumtree_backend]).


%% CHANNELS
-define(DEFAULT_CHANNEL, undefined).
-define(MEMBERSHIP_CHANNEL, partisan_membership).
-define(RPC_CHANNEL, rpc).
-define(PARALLELISM, 1).

-define(DEFAULT_PARTITION_KEY, undefined).

-define(CAUSAL_LABELS, []).

%% Gossip.
-define(GOSSIP_FANOUT, 5). % TODO: FIX ME. % not used?
-define(GOSSIP_GC_MIN_SIZE, 10). % not used?
-define(FANOUT, 5). % not used?

%% PEER SERVICE
-define(DEFAULT_PEER_SERVICE_MANAGER, partisan_pluggable_peer_service_manager).
-define(DEFAULT_MEMBERSHIP_STRATEGY, partisan_full_membership_strategy).
-define(DEFAULT_ORCHESTRATION_STRATEGY, undefined).
-define(CONNECTION_JITTER, 1000).
-define(RELAY_TTL, 5).
-define(PERIODIC_INTERVAL, 10000).

-define(PEER_SERVICE_MANAGER,
    partisan_config:get(
        partisan_peer_service_manager,
        ?DEFAULT_PEER_SERVICE_MANAGER
    )
).

-define(MEMBERSHIP_STRATEGY,
    partisan_config:get(
        membership_strategy,
        ?DEFAULT_MEMBERSHIP_STRATEGY
    )
).

%% Pluggable manager options.
-define(DISTANCE_ENABLED, true).
-define(PERIODIC_ENABLED, true).


%% COMMON TYPES
-type options()         :: [{atom(), term()}] | #{atom() => term()}.


-type ttl()             ::  non_neg_integer().



%% TODO: add type annotations
-record(orchestration_strategy_state, {
    orchestration_strategy,
    is_connected,
    was_connected,
    attempted_nodes,
    peer_service,
    graph,
    tree,
    eredis,
    servers,
    nodes
}).



%% =============================================================================
%% PROTOCOLS
%% =============================================================================

%% Scamp protocol.
-define(SCAMP_C_VALUE, 5). %% TODO: FIX ME.
-define(SCAMP_MESSAGE_WINDOW, 10).



%% =============================================================================
%% USED IN TESTING
%% =============================================================================



-define(CHANNELS, ?CHANNELS(?PARALLELISM)).
-define(CHANNELS(Parallelism), #{
    undefined => #{
        name => ?DEFAULT_CHANNEL,
        parallelism => Parallelism,
        monotonic => false
    }
}).
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
