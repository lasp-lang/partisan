
%% =============================================================================
%% ERLANG VERSION MIGRATION SUPPORT
%% =============================================================================


-if(?OTP_RELEASE >= 27).
    -define(MODULEDOC(Str), -moduledoc(Str)).
    -define(DOC(Str), -doc(Str)).
-else.
    -define(MODULEDOC(Str), -compile([])).
    -define(DOC(Str), -compile([])).
-endif.


%% =============================================================================
%% COMMON TYPES
%% =============================================================================

-type optional(T)       ::  T | undefined.
-type options()         :: [{atom(), term()}] | #{atom() => term()}.
-type ttl()             ::  non_neg_integer().


%% =============================================================================
%% APP
%% =============================================================================
%%

-define(APP, partisan).
-define(LOCALHOST, {127, 0, 0, 1}).
-define(PEER_PORT, 9090).
-define(DEFAULT_TIMEOUT, 5000). % Same as OTP
%% Idea from erpc.erl
-define(MAX_INT_TIMEOUT, 4294967295).
-define(TIMEOUT_TYPE, 0..?MAX_INT_TIMEOUT | 'infinity').
-define(IS_VALID_TMO_INT(TI_), (is_integer(TI_)
                                andalso (0 =< TI_)
                                andalso (TI_ =< ?MAX_INT_TIMEOUT))).
-define(IS_VALID_TMO(T_), ((T_ == infinity) orelse ?IS_VALID_TMO_INT(T_))).
-define(IS_VALID_TIME(T), (is_integer(T) andalso T >= 0)).
-define(IS_VALID_MFA(M, F, A),
    (is_atom(M) andalso is_atom(F) andalso is_list(A))
).

-define(IS_INET_POSIX(X),
    (
        X == eaddrinuse orelse
        X == eaddrnotavail orelse
        X == eafnosupport orelse
        X == ealready orelse
        X == econnaborted orelse
        X == econnrefused orelse
        X == econnreset orelse
        X == edestaddrreq orelse
        X == ehostdown orelse
        X == ehostunreach orelse
        X == einprogress orelse
        X == eisconn orelse
        X == emsgsize orelse
        X == enetdown orelse
        X == enetunreach orelse
        X == enopkg orelse
        X == enoprotoopt orelse
        X == enotconn orelse
        X == enotty orelse
        X == enotsock orelse
        X == eproto orelse
        X == eprotonosupport orelse
        X == eprototype orelse
        X == esocktnosupport orelse
        X == etimedout orelse
        X == ewouldblock orelse
        X == exbadport orelse
        ?IS_FILE_POSIX(X)
    )
).

-define(IS_FILE_POSIX(X),
    (
        X == eacces orelse
        X == eagain orelse
        X == ebadf orelse
        X == ebadmsg orelse
        X == ebusy orelse
        X == edeadlk orelse
        X == edeadlock orelse
        X == edquot orelse
        X == eexist orelse
        X == efault orelse
        X == efbig orelse
        X == eftype orelse
        X == eintr orelse
        X == einval orelse
        X == eio orelse
        X == eisdir orelse
        X == eloop orelse
        X == emfile orelse
        X == emlink orelse
        X == emultihop orelse
        X == enametoolong orelse
        X == enfile orelse
        X == enobufs orelse
        X == enodev orelse
        X == enolck orelse
        X == enolink orelse
        X == enoent orelse
        X == enomem orelse
        X == enospc orelse
        X == enosr orelse
        X == enostr orelse
        X == enosys orelse
        X == enotblk orelse
        X == enotdir orelse
        X == enotsup orelse
        X == enxio orelse
        X == eopnotsupp orelse
        X == eoverflow orelse
        X == eperm orelse
        X == epipe orelse
        X == erange orelse
        X == erofs orelse
        X == espipe orelse
        X == esrch orelse
        X == estale orelse
        X == etxtbsy orelse
        X == exdev
    )
).

%% =============================================================================
%% PLUMTREE
%% =============================================================================



-define(PLUMTREE_OUTSTANDING, partisan_plumtree_broadcast).
-define(BROADCAST_MODS, [partisan_plumtree_backend]).



%% =============================================================================
%% CHANNELS
%% =============================================================================



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
    (partisan_config:get(peer_service_manager, ?DEFAULT_PEER_SERVICE_MANAGER))
).

-define(MEMBERSHIP_STRATEGY,
    partisan_config:get(
        membership_strategy,
        ?DEFAULT_MEMBERSHIP_STRATEGY
    )
).

%% Pluggable manager options.
-define(DISTANCE_ENABLED, false).
-define(PERIODIC_ENABLED, true).


%% =============================================================================
%% ORCHESTRATION
%% =============================================================================





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
%% PROTOCOLS: HYPARVIEW
%% =============================================================================

-define(XBOT_MIN_INTERVAL, 5000).
-define(XBOT_RANGE_INTERVAL, 60000).
% parameter used for xbot optimization
% - latency (uses ping to check better nodes)
% - true (always returns true when checking better)
-define(HYPARVIEW_XBOT_ORACLE, latency).
-define(HYPARVIEW_XBOT_INTERVAL,
    rand:uniform(?XBOT_RANGE_INTERVAL) + ?XBOT_MIN_INTERVAL
).

-define(HYPARVIEW_DEFAULTS, #{
    active_max_size => 6,
    active_min_size => 3,
    active_rwl => 6,
    passive_max_size => 30,
    passive_rwl => 6,
    random_promotion => true,
    random_promotion_interval => 5000,
    shuffle_interval => 10000,
    shuffle_k_active => 3,
    shuffle_k_passive => 4,
    xbot_enabled => false,
    xbot_interval => ?HYPARVIEW_XBOT_INTERVAL
}).


-type config()              ::  #{
                                active_max_size := non_neg_integer(),
                                active_min_size := non_neg_integer(),
                                active_rwl := non_neg_integer(),
                                passive_max_size := non_neg_integer(),
                                passive_rwl := non_neg_integer(),
                                random_promotion := boolean(),
                                random_promotion_interval := non_neg_integer(),
                                shuffle_interval := non_neg_integer(),
                                shuffle_k_active := non_neg_integer(),
                                shuffle_k_passive := non_neg_integer(),
                                xbot_enabled := boolean(),
                                xbot_interval := non_neg_integer()
                            }.

%% =============================================================================
%% PROTOCOLS: SCAMP
%% =============================================================================

%% Scamp protocol.
-define(SCAMP_C_VALUE, 5). %% TODO: FIX ME.
-define(SCAMP_MESSAGE_WINDOW, 10).



%% =============================================================================
%% USED IN TESTING
%% =============================================================================

-if(?OTP_RELEASE >= 25).
    %% already defined by OTP
    -define(CT_NODE, peer).
-else.
    -define(CT_NODE, ct_slave).
-endif.

-compile([nowarn_export_all, export_all]).


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


-if(?OTP_RELEASE >= 25).
    -define(PARALLEL_SIGNAL_OPTIMISATION(L),
        lists:keystore(
            message_queue_data, 1, L, {message_queue_data, off_heap}
        )
    ).
-else.
    -define(PARALLEL_SIGNAL_OPTIMISATION(L), L).
-endif.

