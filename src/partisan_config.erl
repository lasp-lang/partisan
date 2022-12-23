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

%% -----------------------------------------------------------------------------
%% @doc This module handles the validation, access and modification of Partisan
%% configuration options.
%% Some options will only take effect after a restart of the Partisan
%% application, while other will take effect while the application is still
%% running.
%%
%% As per Erlang convention the options are given using the `sys.config' file
%% under the `partisan' application section.
%%
%% == Options ==
%% The following is the list of all the options you can read using {@link get/
%% 1} and {@link get/2}, and modify using the `sys.config' file and {@link set/
%% 2}.
%% <dl>
%% <dt>`arwl'</dt><dd>TBD</dd>
%% <dt>`binary_padding'</dt><dd>TBD</dd>
%% <dt>`broadcast'</dt><dd>TBD</dd>
%% <dt>`broadcast_mods'</dt><dd>TBD</dd>
%% <dt>`causal_labels'</dt><dd>TBD</dd>
%% <dt>`channels'</dt><dd>Defines the channels to be used by Partisan. The
%% option takes either a channels map where keys are channel names
%% (`channel()') and values are channel options (`channel_opts()'), or a list
%% of values where each value can be any of the following types:
%% <ul>
%% <li>a channel name (`channel()') e.g. the atom `foo'</li>
%% <li>a channel with options: `{channel(), channel_opts()}'</li>
%% <li>a monotonic channel using the tuple `{monotonic, Name :: channel()}' e.g.
%% `{monotonic, bar}'. This is a legacy representation, the same can be
%% achieved with `{bar, #{monotonic => true}}'</li>
%% </ul>
%% The list can habe a mix of types and during startup they are all coerced to
%% channels map. Coercion works by defaulting `parallelism' to the
%% value of the global option `parallelism' which itself defaults to `1', and
%% `monotonic' to `false'.
%%
%% Finally the list is transformed to a map where keys are channel names and
%% values are channel map representation.
%%
%% <strong>Example:</strong>
%%
%% Given the following option value:
%% ```
%% [
%%     foo,
%%     {monotonic, bar},
%%     {bar, #{parallelism => 4}}
%% ]
%% '''
%% The coerced representation will be the following map (which is a valid input
%% and the final representation of this option after Partisan starts).
%% ```
%% #{
%%     foo => #{monotonic => false, parallelism => 1},
%%     bar => #{monotonic => true, parallelism => 1},
%%     baz => #{monotonic => false, parallelism => 4},
%% }
%% '''
%% </dd>
%% <dt>`connect_disterl'</dt><dd>Whether to use distributed erlang in addition
%% to Partisan channels. This is used for testing and only works for `partisan_full_membership_strategy' (See `membership_strategy')</dd>
%% <dt>`connection_jitter'</dt><dd>TBD</dd>
%% <dt>`default_channel'</dt><dd>The name of the default channel. This
%% should be the name of one of the channels defined in `channels'. If not
%% defined, an `undefined' channel will be created as the default channel. The
%% default channel is used when a specific channel hasn't been defined for
%% messaging operations.</dd>
%% <dt>`disable_fast_forward'</dt><dd>TBD</dd>
%% <dt>`disable_fast_receive'</dt><dd>TBD</dd>
%% <dt>`distance_enabled'</dt><dd>TBD</dd>
%% <dt>`egress_delay'</dt><dd>TBD</dd>
%% <dt>`exchange_selection'</dt><dd>TBD</dd>
%% <dt>`exchange_tick_period'</dt><dd>TBD</dd>
%% <dt>`fanout'</dt><dd>The number of nodes that are contacted at each gossip interval. DEPRECATED?</dd>
%% <dt>`gossip'</dt><dd>If `true' gossip is used to disseminate membership
%% state.</dd>
%% <dt>`ingress_delay'</dt><dd>TBD</dd>
%% <dt>`lazy_tick_period'</dt><dd>TBD</dd>
%% <dt>`max_active_size'</dt><dd>TBD</dd>
%% <dt>`max_passive_size'</dt><dd>TBD</dd>
%% <dt>`membership_strategy'</dt><dd>TBD</dd>
%% <dt>`membership_strategy_tracing'</dt><dd>TBD</dd>
%% <dt>`min_active_size'</dt><dd>TBD</dd>
%% <dt>`name'</dt><dd>TBD</dd>
%% <dt>`orchestration_strategy'</dt><dd>TBD</dd>
%% <dt>`parallelism'</dt><dd>TBD</dd>
%% <dt>`partisan_peer_service_manager'</dt><dd>TBD</dd>
%% <dt>`passive_view_shuffle_period'</dt><dd>TBD</dd>
%% <dt>`peer_host'</dt><dd>TBD</dd>
%% <dt>`peer_ip'</dt><dd>TBD</dd>
%% <dt>`peer_port'</dt><dd>TBD</dd>
%% <dt>`periodic_enabled'</dt><dd>TBD</dd>
%% <dt>`periodic_interval'</dt><dd>TBD</dd>
%% <dt>`pid_encoding'</dt><dd>TBD</dd>
%% <dt>`prwl'</dt><dd>TBD</dd>
%% <dt>`random_promotion'</dt><dd>TBD</dd>
%% <dt>`random_seed'</dt><dd>TBD</dd>
%% <dt>`ref_encoding'</dt><dd>TBD</dd>
%% <dt>`register_pid_for_encoding'</dt><dd>TBD</dd>
%% <dt>`remote_ref_as_uri'</dt><dd>If `true' partisan remote references (see
%% module {@link partisan_remote_ref}) will be encoded as a URI binary.
%% Otherwise it will be encoded as a tuple. The default is `false'.
%%
%% ```
%% 1> partisan_config:set(remote_ref_as_uri, true).
%% ok
%% 2> partisan_remote_ref:from_term(self()).
%% <<"partisan:pid:nonode@nohost:0.1062.0">>
%% 3> partisan_config:set(remote_ref_as_uri, false).
%% 4> partisan_remote_ref:from_term(self()).
%% {partisan_remote_reference,
%%    nonode@nohost,
%%    {partisan_process_reference,"<0.1062.0>"}}
%% '''
%% </dd>
%% <dt>`remote_ref_uri_padding'</dt><dd>If `true' and the URI encoding of a
%% remote reference results in a binary smaller than 65 bytes, the URI will be
%% padded. The default is `false'.
%% %% ```
%% 1> partisan_config:set(remote_ref_binary_padding, false).
%% 1> partisan_remote_ref:from_term(self()).
%% <<"partisan:pid:nonode@nohost:0.1062.0">>
%% 2> partisan_config:set(remote_ref_binary_padding, true).
%% ok
%% 3> partisan_remote_ref:from_term(self()).
%% <<"partisan:pid:nonode@nohost:0.1062.0:"...>>
%% '''
%% </dd>
%% <dt>`replaying'</dt><dd>TBD</dd>
%% <dt>`reservations'</dt><dd>TBD</dd>
%% <dt>`shrinking'</dt><dd>TBD</dd>
%% <dt>`tag'</dt><dd>TBD</dd>
%% <dt>`tls'</dt><dd>TBD</dd>
%% <dt>`tls_client_options'</dt><dd>TBD</dd>
%% <dt>`tls_server_options'</dt><dd>TBD</dd>
%% <dt>`tracing'</dt><dd>TBD</dd>
%% <dt>`xbot_interval'</dt><dd>TBD</dd>
%% </dl>
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_config).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan_logger.hrl").
-include("partisan.hrl").


-export([channel_opts/1]).
-export([channels/0]).
-export([default_channel/0]).
-export([default_channel_opts/0]).
-export([get/1]).
-export([get/2]).
-export([get_with_opts/2]).
-export([get_with_opts/3]).
-export([init/0]).
-export([listen_addrs/0]).
-export([parallelism/0]).
-export([seed/0]).
-export([seed/1]).
-export([set/2]).
-export([trace/2]).

-compile({no_auto_import, [get/1]}).
-compile({no_auto_import, [set/2]}).


%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Initialises the configuration from the application environment.
%%
%% <strong>You should never call this function</strong>. This is used by Partisan itself
%% during startup.
%% @end
%% -----------------------------------------------------------------------------
init() ->
    DefaultPeerService = application:get_env(
        partisan,
        partisan_peer_service_manager,
        ?DEFAULT_PEER_SERVICE_MANAGER
    ),

    PeerService =
        case os:getenv("PEER_SERVICE", "false") of
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
    partisan_config:set(nodestring, atom_to_binary(Name, utf8)),

    DefaultTag =
        case os:getenv("TAG", "false") of
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
        {Key, Default} <- [
            %% WARNING:
            %% This list show be exhaustive, anything key missing from this list
            %% will not be read from the application environment
            %% The following keys are missing as we need to process them after:
            %% [channels].
            {arwl, 5},
            {prwl, 30},
            {binary_padding, false},
            {broadcast, false},
            {broadcast_mods, [partisan_plumtree_backend]},
            {causal_labels, []},
            {connect_disterl, false},
            {connection_jitter, ?CONNECTION_JITTER},
            {disable_fast_forward, false},
            {disable_fast_receive, false},
            {distance_enabled, ?DISTANCE_ENABLED},
            {egress_delay, 0},
            {exchange_selection, optimized},
            {exchange_tick_period, ?DEFAULT_EXCHANGE_TICK_PERIOD},
            {fanout, ?FANOUT},
            {gossip, true},
            {ingress_delay, 0},
            {lazy_tick_period, ?DEFAULT_LAZY_TICK_PERIOD},
            {max_active_size, 6},
            {max_passive_size, 30},
            {min_active_size, 3},
            {name, Name},
            {passive_view_shuffle_period, 10000},
            {parallelism, ?PARALLELISM},
            {membership_strategy, ?DEFAULT_MEMBERSHIP_STRATEGY},
            {partisan_peer_service_manager, PeerService},
            {peer_host, undefined},
            {peer_ip, DefaultPeerIP},
            {peer_port, DefaultPeerPort},
            {periodic_enabled, ?PERIODIC_ENABLED},
            {periodic_interval, 10000},
            {remote_ref_uri_padding, false},
            {remote_ref_as_uri, false},
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
            {tls_server_options, []},
            {tls_client_options, []},
            {tag, DefaultTag},
            {xbot_interval, XbotInterval}
       ]
    ],

    %% Setup channels
    Channels = application:get_env(?APP, channels, #{}),
    set(channels, Channels),

    %% Setup default listen addr.
    %% This will be part of the partisan:node_spec() which is the map
    DefaultAddr0 = #{port => get(peer_port)},

    DefaultAddr =
        case get(peer_host) of
            undefined ->
                DefaultAddr0#{ip => get(peer_ip)};
            Host ->
                DefaultAddr0#{host => Host}
        end,

    %% We make sure they are sorted so that we can compare them (specially when
    %% part of the node_spec()).
    ok = env_or_default(listen_addrs, [DefaultAddr]),
    ListenAddrs = lists:usort(partisan_config:get(listen_addrs)),
    ok = partisan_config:set(listen_addrs, ListenAddrs).


%% Seed the process.
seed(Seed) ->
    rand:seed(exsplus, Seed).

%% Seed the process.
seed() ->
    RandomSeed = random_seed(),
    ?LOG_DEBUG(#{
        description => "Chossing random seed",
        node => node(),
        seed => RandomSeed
    }),
    rand:seed(exsplus, RandomSeed).

%% Return a random seed, either from the environment or one that's generated for the run.
random_seed() ->
    case partisan_config:get(random_seed, undefined) of
        undefined ->
            {erlang:phash2([partisan:node()]), erlang:monotonic_time(), erlang:unique_integer()};
        Other ->
            Other
    end.

trace(Message, Args) ->
    ?LOG_TRACE(#{
        description => "Trace",
        message => Message,
        args => Args
    }).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
get(broadcast_start_exchange_limit = Key) ->
    %% If there is no limit defined we assume a limit of 1 per module, as we
    %% This works because partisan_plumtree_broadcast will never run more than
    %% one exchange per module anyway.
    Default = length(get(broadcast_mods, [])),
    get(Key, Default);

get(Key) ->
    persistent_term:get(Key).

get(Key, Default) ->
    persistent_term:get(Key, Default).


%% -----------------------------------------------------------------------------
%% @doc Returns the value for `Key' in `Opts', if found. Otherwise, calls
%% {@link get/1}.
%% @end
%% -----------------------------------------------------------------------------
get_with_opts(Key, Opts) when is_map(Opts); is_list(Opts) ->
    case maps:find(Key, Opts) of
        {ok, Val} -> Val;
        error -> get(Key)
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the value for `Key' in `Opts', if found. Otherwise, calls
%% {@link get/2}.
%% @end
%% -----------------------------------------------------------------------------
get_with_opts(Key, Opts, Default) when is_map(Opts); is_list(Opts) ->
    case maps:find(Key, Opts) of
        {ok, Val} -> Val;
        error -> get(Key, Default)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
set(peer_ip, Value) when is_list(Value) ->
    {ok, ParsedIP} = inet_parse:address(Value),
    set(peer_ip, ParsedIP);

set(channels, Arg) when is_list(Arg) orelse is_map(Arg) ->
    %% We coerse any defined channel to channel spec map representations and
    %% build a
    %% mapping of {partisan:channel() -> partisan:channel_opts()}
    Channels0 = to_channels_map(Arg),

    %% We set default channel, overriding any user input
    Channels = maps:put(
        ?DEFAULT_CHANNEL, to_channel_spec(?DEFAULT_CHANNEL), Channels0
    ),

    do_set(channels, Channels);

set(broadcast_mods, Value) ->
    do_set(broadcast_mods, lists:usort(Value ++ ?BROADCAST_MODS));

set(Key, Value) ->
    do_set(Key, Value).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
listen_addrs() ->
    partisan_config:get(listen_addrs).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec channel_opts(Name :: partisan:channel()) -> partisan:channel_opts().

channel_opts(Name) when is_atom(Name) ->
    case maps:find(Name, channels()) of
        {ok, Channel} ->
            Channel;
        error ->
            error(badarg)
    end.


%% -----------------------------------------------------------------------------
%% @doc The spec of the default channel.
%% @end
%% -----------------------------------------------------------------------------
-spec default_channel_opts() -> partisan:channel_opts().

default_channel_opts() ->
    channel_opts(default_channel()).



%% -----------------------------------------------------------------------------
%% @doc The name of the default channel.
%% @end
%% -----------------------------------------------------------------------------
-spec default_channel() -> partisan:channel().

default_channel() ->
    ?DEFAULT_CHANNEL.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec channels() -> #{partisan:channel() => partisan:channel_opts()}.

channels() ->
    partisan_config:get(channels).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
parallelism() ->
    partisan_config:get(parallelism, ?PARALLELISM).



%% =============================================================================
%% PRIVATE
%% =============================================================================




%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
env_or_default(Key, Default) ->
    Value = application:get_env(partisan, Key, Default),
    set(Key, Value).


%% @private
do_set(Key, MergeFun) when is_function(MergeFun, 1) ->
    OldValue = persistent_term:get(Key, undefined),
    do_set(Key, MergeFun(OldValue));

do_set(Key, Value) ->
    application:set_env(?APP, Key, Value),
    persistent_term:put(Key, Value).


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
    Name = atom_to_list(partisan:node()),
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


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
to_channels_map(L) when is_list(L) ->
    maps:from_list(
        [
            begin
                Channel = to_channel_spec(E),
                {maps:get(name, Channel), maps:without([name], Channel)}
            end || E <- L
        ]
    );

to_channels_map(M) when is_map(M) ->
    %% This is the case where a user has passed
    %% #{channel() => channel_opts()}
    maps:map(
        fun(K, V) ->
            %% We return a channel_opts()
            maps:without([name], to_channel_spec({K, V}))
        end,
        M
    ).


%% @private
init_channel_opts() ->
    #{
        parallelism => get(parallelism, ?PARALLELISM),
        monotonic => false
    }.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Returns a channel specification map based on `Arg'.
%% This is a temp data structure used by this module to define the final value
%% for the `channels' option.
%% @end
%% -----------------------------------------------------------------------------
-spec to_channel_spec(
    Arg ::  map()
            | partisan:channel()
            | {partisan:channel(), partisan:channel_opts()}
            | {monotonic, partisan:channel()}) ->
    Spec :: map() | no_return().

to_channel_spec(#{name := Name, parallelism := N, monotonic := M} = Spec)
when is_atom(Name) andalso is_integer(N) andalso N >= 1 andalso is_boolean(M) ->
    Spec;

to_channel_spec(Name) when is_atom(Name) ->
    to_channel_spec(#{name => Name});

to_channel_spec({monotonic, Name}) when is_atom(Name) ->
    %% We support the legacy syntax
    to_channel_spec(#{name => Name, monotonic => true});

to_channel_spec({Name, Opts}) when is_atom(Name), is_map(Opts) ->
    to_channel_spec(Opts#{name => Name});

to_channel_spec(#{name := _} = Map) when is_map(Map) ->
    to_channel_spec(maps:merge(init_channel_opts(), Map));

to_channel_spec(_) ->
    error(badarg).

