-define(APP, partisan).
-define(PEER_IP, {127,0,0,1}).
-define(PEER_PORT, 9000).
-define(PEER_SERVICE_SERVER, partisan_peer_service_server).
-define(FANOUT, 5).

-define(RECONNECT_INTERVAL, 10000). %% 10s

-type actor() :: binary().
-type connections() :: dict:dict(node(), port()).
-type node_spec() :: {node(), inet:ip_address(), non_neg_integer()}.
-type message() :: term().
-type name() :: node().
-type partitions() :: [{reference(), node_spec()}].
-type ttl() :: non_neg_integer().
-type error() :: {error, term()}.
