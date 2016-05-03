-define(SET, riak_dt_orswot).
-define(APP, partisan).
-define(PEER_IP, {127,0,0,1}).
-define(PEER_PORT, 9000).
-define(PEER_SERVICE_SERVER, partisan_peer_service_server).
-define(FANOUT, 5).

%% @todo Reduce me.
-define(GOSSIP_INTERVAL, 300).

-type node_spec() :: {node(), inet:ip_address(), non_neg_integer()}.

