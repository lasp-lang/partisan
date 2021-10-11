%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(partisan_config).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan_logger.hrl").
-include("partisan.hrl").


-export([init/0,
         channels/0,
         parallelism/0,
         trace/2,
         listen_addrs/0,
         set/2,
         seed/0,
         seed/1,
         get/1,
         get/2]).

init() ->
    DefaultPeerService = application:get_env(partisan,
                                             partisan_peer_service_manager,
                                             ?DEFAULT_PEER_SERVICE_MANAGER),

    PeerService = case os:getenv("PEER_SERVICE", "false") of
                      "false" ->
                          DefaultPeerService;
                      PeerServiceList ->
                          list_to_atom(PeerServiceList)
                  end,

    %% Configure the partisan node name.
    Name = case node() of
        nonode@nohost ->
            NodeName = gen_node_name(),
            ?LOG_INFO(#{
                description => "Partisan node name configured",
                name => NodeName,
                disterl_enabled => false
            }),
            NodeName;
        Other ->
            ?LOG_INFO(#{
                description => "Partisan node name configured",
                name => Other,
                disterl_enabled => true
            }),
            Other
    end,

    %% Must be done here, before the resolution call is made.
    partisan_config:set(name, Name),

    DefaultTag = case os:getenv("TAG", "false") of
                    "false" ->
                        undefined;
                    TagList ->
                        Tag = list_to_atom(TagList),
                        application:set_env(?APP, tag, Tag),
                        Tag
                end,

    %% Determine if we are replaying.
    case os:getenv("REPLAY", "false") of
        "false" ->
            false;
        _ ->
            application:set_env(?APP, replaying, true),
            true
    end,

    %% Determine if we are shrinking.
    case os:getenv("SHRINKING", "false") of
        "false" ->
            false;
        _ ->
            application:set_env(?APP, shrinking, true),
            true
    end,

    %% Configure system parameters.
    DefaultPeerIP = try_get_node_address(),
    DefaultPeerPort = random_port(),

    %% Configure X-BOT interval.
    XbotInterval = rand:uniform(?XBOT_RANGE_INTERVAL) + ?XBOT_MIN_INTERVAL,

    [env_or_default(Key, Default) ||
        {Key, Default} <- [{arwl, 5},
                           {prwl, 30},
                           {binary_padding, false},
                           {broadcast, false},
                           {broadcast_mods, [partisan_plumtree_backend]},
                           {causal_labels, []},
                           {channels, ?CHANNELS},
                           {connect_disterl, false},
                           {connection_jitter, ?CONNECTION_JITTER},
                           {disable_fast_forward, false},
                           {disable_fast_receive, false},
                           {distance_enabled, ?DISTANCE_ENABLED},
                           {egress_delay, 0},
                           {fanout, ?FANOUT},
                           {gossip, true},
                           {ingress_delay, 0},
                           {max_active_size, 6},
                           {max_passive_size, 30},
                           {min_active_size, 3},
                           {name, Name},
                           {passive_view_shuffle_period, 10000},
                           {parallelism, ?PARALLELISM},
                           {membership_strategy, ?DEFAULT_MEMBERSHIP_STRATEGY},
                           {partisan_peer_service_manager, PeerService},
                           {peer_ip, DefaultPeerIP},
                           {peer_port, DefaultPeerPort},
                           {periodic_enabled, ?PERIODIC_ENABLED},
                           {periodic_interval, 10000},
                           {pid_encoding, true},
                           {ref_encoding, true},
                           {membership_strategy_tracing, ?MEMBERSHIP_STRATEGY_TRACING},
                           {orchestration_strategy, ?DEFAULT_ORCHESTRATION_STRATEGY},
                           {random_seed, random_seed()},
                           {random_promotion, true},
                           {register_pid_for_encoding, false},
                           {replaying, false},
                           {reservations, []},
                           {shrinking, false},
                           {tracing, false},
                           {tls, false},
                           {tls_options, []},
                           {tag, DefaultTag},
                           {xbot_interval, XbotInterval}]],

    %% Setup default listen addr.
    DefaultListenAddrs = [#{ip => ?MODULE:get(peer_ip), port => ?MODULE:get(peer_port)}],
    env_or_default(listen_addrs, DefaultListenAddrs),

    ok.

%% Seed the process.
seed(Seed) ->
    rand:seed(exsplus, Seed).

%% Seed the process.
seed() ->
    RandomSeed = random_seed(),
    ?LOG_INFO(#{
        description => "Chossing random seed",
        node => node(),
        seed => RandomSeed
    }),
    rand:seed(exsplus, RandomSeed).

%% Return a random seed, either from the environment or one that's generated for the run.
random_seed() ->
    case partisan_config:get(random_seed, undefined) of
        undefined ->
            {erlang:phash2([partisan_peer_service_manager:mynode()]), erlang:monotonic_time(), erlang:unique_integer()};
        Other ->
            Other
    end.

trace(Message, Args) ->
    ?LOG_TRACE(#{
        description => "Trace",
        message => Message,
        args => Args
    }).

env_or_default(Key, Default) ->
    case application:get_env(partisan, Key) of
        {ok, Value} ->
            set(Key, Value);
        undefined ->
            set(Key, Default)
    end.

get(Key) ->
    persistent_term:get(Key).

get(Key, Default) ->
    persistent_term:get(Key, Default).

set(peer_ip, Value) when is_list(Value) ->
    {ok, ParsedIP} = inet_parse:address(Value),
    set(peer_ip, ParsedIP);
set(Key, Value) ->
    application:set_env(?APP, Key, Value),
    persistent_term:put(Key, Value).

listen_addrs() ->
    partisan_config:get(listen_addrs).

channels() ->
    partisan_config:get(channels).

parallelism() ->
    partisan_config:get(parallelism).

%% @private
random_port() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, {_, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    Port.

%% @private
try_get_node_address() ->
    case application:get_env(partisan, peer_ip) of
        {ok, Address} ->
            Address;
        undefined ->
            get_node_address()
    end.

%% @private
get_node_address() ->
    Name = atom_to_list(partisan_peer_service_manager:mynode()),
    [_Hostname, FQDN] = string:tokens(Name, "@"),

    %% Spawn a process to perform resolution.
    Me = self(),

    ResolverFun = fun() ->
        ?LOG_INFO(#{
            description => "Resolving FQDN",
            fqdn => FQDN
        }),
        case inet:getaddr(FQDN, inet) of
            {ok, Address} ->
                ?LOG_INFO(#{
                    description => "Resolved domain name",
                    name => Name,
                    address => Address
                }),
                Me ! {ok, Address};
            {error, Reason} ->
                ?LOG_INFO(#{
                    description => "Cannot resolve local name, resulting to 127.0.0.1",
                    fqdn => FQDN,
                    reason => Reason
                }),
                Me ! {ok, ?PEER_IP}
        end
    end,

    %% Spawn the resolver.
    ResolverPid = spawn(ResolverFun),

    %% Exit the resolver after a limited amount of time.
    timer:exit_after(1000, ResolverPid, normal),

    %% Wait for response, either answer or exit.
    receive
        {ok, Address} ->
            ?LOG_INFO(#{
                description => "Resolving FQDN",
                fqdn => FQDN,
                address => Address
            }),
            Address;
        Error ->
            ?LOG_INFO(#{
                description => "Error resolving FQDN",
                fqdn => FQDN,
                error => Error
            }),
            ?PEER_IP
    end.


%% @private
gen_node_name() ->
    UUIDState = uuid:new(self()),
    {UUID, _UUIDState1} = uuid:get_v1(UUIDState),
    StringUUID = uuid:uuid_to_string(UUID),
    list_to_atom(StringUUID ++ "@127.0.0.1").
