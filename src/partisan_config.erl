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
%% === Example ===
%% ```
%% [
%%     ...
%%     {partisan, [
%%         {listen_addrs, [
%%             #{ip => {127, 0, 0, 1}, port => 12345}
%%         ]},
%%         {peer_port, 12345},
%%         {channels, #{
%%             data => #{parallelism => 4}
%%         }},
%%         {remote_ref_format, improper_list},
%%         {tls, true},
%%         {tls_server_options, [
%%             {certfile, "config/_ssl/server/keycert.pem"},
%%             {cacertfile, "config/_ssl/server/cacerts.pem"},
%%             {keyfile, "config/_ssl/server/key.pem"},
%%             {verify, verify_none}
%%         ]},
%%         {tls_client_options, [
%%             {certfile, "config/_ssl/client/keycert.pem"},
%%             {cacertfile, "config/_ssl/client/cacerts.pem"},
%%             {keyfile, "config/_ssl/client/key.pem"},
%%             {verify, verify_none}
%%         ]}
%%     ]}
%% ]
%% '''
%%
%% == Options ==
%% The following is the list of all the options you can read using
%% {@link get/1} and {@link get/2}, and modify using the `sys.config' file and
%% {@link set/2}.
%%
%% See {@section Deprecated Options} below.
%%
%% <h4 id="binary_padding" class="section-heading">binary_padding</h4>
%% A boolean value indicating whether to pad
%% encoded messages whose external binary representation consumes less than 65
%% bytes.
%%
%% <h4 id="broadcast" class="section-heading">broadcast</h4>
%% TBD
%%
%% <h4 id="broadcast_mods" class="section-heading">broadcast_mods</h4>
%% TBD
%%
%% <h4 id="broadcast_mods" class="section-heading">broadcast_mods</h4>
%% TBD
%%
%% <h4 id="channels" class="section-heading">channels</h4>
%% Defines the channels to be used by Partisan. The
%% option takes either a channels map where keys are channel names
%% ({@link partisan:channel()}) and values are channel options ({@link
%% partisan:channel_opts()}), or a list of values where each value can be any
%% of the following types:
%% <ul>
%% <li>a channel name ({@link partisan:channel()}) e.g. the atom `foo'</li>
%% <li>a channel with options: `{channel(), channel_opts()}'</li>
%% <li>a monotonic channel using the tuple `{monotonic, Name :: channel()}' e.g.
%% `{monotonic, bar}'. This is a legacy representation, the same can be
%% achieved with `{bar, #{monotonic => true}}'</li>
%% </ul>
%% The list can habe a mix of types and during startup they are all coerced to
%% channels map. Coercion works by defaulting the channel's `parallelism' to the
%% value of the global option `parallelism' (which itself defaults to `1'), and
%% the channel's `monotonic' to `false'.
%%
%% Finally the list is transformed to a map where keys are channel names and
%% values are channel map representation.
%%
%% ==== Example ====
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
%%
%% <h4 id="connect_disterl" class="section-heading">connect_disterl</h4>
%% A configuration that is intended solely for testing of the
%% {@link partisan_full_membership_strategy} (See `membership_strategy').
%% It defines whether to use Distributed Erlang (disterl) in addition to
%% Partisan channels. Defaults to `false'.
%%
%% Notice that this setting does not prevent you having both disterl and
%% Partisan enabled for your release. However, you need to have special care to
%% avoid mixing the two, for example by calling a {@link partisan_gen_server}
%% that uses Partisan for distribution with {@link gen_server} that uses
%% disterl.
%%
%% <h4 id="connection_interval" class="section-heading">connection_interval</h4>
%% Interval of time between peer connection attempts
%%
%% <h4 id="connection_jitter" class="section-heading">connection_jitter</h4>
%% TBD
%%
%% <h4 id="disable_fast_forward"
%% class="section-heading">disable_fast_forward</h4>
%% TBD
%%
%% <h4 id="disable_fast_receive"
%% class="section-heading">disable_fast_receive</h4>
%% TBD
%%
%% <h4 id="distance_enabled" class="section-heading">distance_enabled</h4>
%% TBD
%%
%% <h4 id="egress_delay" class="section-heading">egress_delay</h4>
%% TBD
%%
%% <h4 id="exchange_selection" class="section-heading">exchange_selection</h4>
%% TBD
%%
%% <h4 id="exchange_tick_period"
%% class="section-heading">exchange_tick_period</h4>
%% TBD
%%
%% <h4 id="gossip" class="section-heading">gossip</h4>
%% If `true' gossip is used to disseminate membership to peers.
%% Default is `true'.
%% At the moment used only by {@link partisan_full_membership_strategy}.
%%
%% <h4 id="hyparview" class="section-heading">hyparview</h4>
%%  The configuration for the {@link
%% partisan_hyparview_peer_service_manager}. A list with the following
%% properties:
%%
%% <ul>
%% <li>`active_max_size' - Defaults to `6'.</li>
%% <li>`active_min_size' - Defaults to `3'.</li>
%% <li>`active_rwl' - Active View Random Walk Length. Defaults to `6'.</li>
%% <li>`passive_max_size' - Defaults to `30'.</li>
%% <li>`passive_rwl' - Passive View Random Walk Length. Defaults to `6'.</li>
%% <li>`random_promotion' - A boolean indicating if random promotion is enabled. Defaults
%% `true'.</li>
%% <li>`random_promotion_interval' - Time after which the protocol attempts to promote a node in the passive
%% view to the active view. Defaults to `5000'.</li>
%% <li>`shuffle_interval' - Defaults to `10000'.</li>
%% <li>`shuffle_k_active' - Number of peers to include in the shuffle exchange. Defaults to `3'.</li>
%% <li>`shuffle_k_passive' - Number of peers to include in the shuffle exchange. Defaults to `4'.</li>
%% </ul>
%%
%% <h4 id="ingress_delay" class="section-heading">ingress_delay</h4>
%% TBD
%%
%% <h4 id="lazy_tick_period" class="section-heading">lazy_tick_period</h4>
%% TBD
%%
%% <h4 id="listen_addrs" class="section-heading">listen_addrs</h4>
%% A list of {@link partisan:listen_addr()} objects. This overrides
%% <a href="#peer_ip" class="no-underline"><code>peer_ip</code></a> and
%% <a href="#peer_port" class="no-underline"><code>peer_port</code></a>
%% (see below) and its the way to configure the peer
%% listener should you want to listen on multiple IP addresses. If
%% this option is missing, the `peer_ip' property will be used, unless is also
%% missing, in which case the nodename's host part will be used to determine the
%% IP address.
%%
%% The {@link partisan:listen_addr()} object can be represented using lists,
%% binaries, tuples or maps as shown in the following example:
%% ```
%% {listen_addrs, [
%%    "127.0.0.1:12345",
%%    <<"127.0.0.1:12345">>,
%%    {"127.0.0.1", "12345"},
%%    {{127, 0, 0, 1}, 12345},
%%    #{ip => "127.0.0.1", port => "12345"},
%%    #{ip => <<"127.0.0.1">>, port => <<"12345">>},
%%    #{ip => {127, 0, 0, 1}, port => 12345}
%% ]}
%% '''
%% Notice the above example will result in the following, as equivalent terms
%% are deduplicated.
%%
%% ```
%% {listen_addrs, [
%%    #{ip => {127, 0, 0, 1}, port => 12345}
%% ]}
%% '''
%%
%% This option also accepts IP addresses without a port e.g. "127.0.0.1".
%% In this case the port will be the value
%% <a href="#peer_port" class="no-underline"><code>peer_port</code></a> option.
%% Notice
%% <a href="#peer_port" class="no-underline"><code>peer_port</code></a>
%%  is also used by some peer discovery strategies that cannot
%% detect in which port the peer is listening e.g. DNS.
%%
%% See also
%% <a href="#peer_ip" class="no-underline"><code>peer_ip</code></a> for an
%% alternative when using a single IP address.
%%
%%
%% <h4 id="membership_binary_compression"
%% class="section-heading">membership_binary_compression</h4>
%% A boolean value or an integer in the range from `0..9' to be used with
%% {@link erlang:term_to_binary/2} when encoding the membership set for
%% broadcast.
%%
%% A value of `true' is equivalent to integer `6' (equivalent to
%% option `compressed' in {@link erlang:term_to_binary/2}).
%% A value of `false' is equivalent to `0' (no compression).
%%
%% Default is`true'.
%%
%% <h4 id="membership_strategy" class="section-heading">membership_strategy</h4>
%% The membership strategy to be used with {@link
%% partisan_pluggable_peer_service_manager}.
%% Default is {@link partisan_full_membership_strategy}
%%
%% <h4 id="membership_strategy_tracing" class="section-heading">membership_strategy_tracing</h4>
%% TBD
%%
%% <h4 id="name" class="section-heading">name</h4>
%% The nodename to be used when one was not provided via the Erlang
%% `vm.args' configuration file or via the shell flag `--name'. The value should
%% be a longname e.g. `{{Name}}@{{HostOrIPAddress}}'.
%%
%% When neither Erlang's nodename nor this value are defined,
%% Partisan will generate a random nodename. This is primarily used for testing
%% and you should always set the nodename when deploying to production, either
%% via Erlang or using this option.
%%
%% <h4 id="orchestration_strategy"
%% class="section-heading">orchestration_strategy</h4>
%% TBD
%%
%% <h4 id="parallelism" class="section-heading">parallelism</h4>
%% The default number of connections to use per channel, when a channel
%% hasn't been given a specific `parallelism' value via the `channels' option.
%% The default is `1'. For more information see option `channels'.
%%
%% <h4 id="peer_service_manager"
%% class="section-heading">peer_service_manager</h4>
%% The peer service manager to be used. An implementation of the {@link
%% partisan_peer_service_manager} behaviour which defines the overlay network
%% topology and the membership view maintenance strategy. Default is {@link
%% partisan_pluggable_peer_service_manager}.
%%
%% <h4 id="peer_ip" class="section-heading">peer_ip</h4>
%% The IP address to use for the peer connection listener when no
%% {@link partisan:listen_addr()} have been defined via option
%% <a href="#listen_addrs" class="no-underline"><code>listen_addrs</code></a>.
%% If a value is not defined (and
%% <a href="#listen_addrs" class="no-underline"><code>listen_addrs</code></a>
%%  was not used), Partisan
%% will attempt to resolve the IP address using the nodename's host i.e. the
%% part to the right of the `@' character in the nodename, and will default to
%% `{127,0,0,1}' if it can't.
%%
%% <h4 id="peer_port" class="section-heading">peer_port</h4>
%% The port number to use for the peer connection listener when no
%% {@link partisan:listen_addr()} have been defined via option
%% <a href="#listen_addrs" class="no-underline"><code>listen_addrs</code></a>.
%% If a value is not defined (and
%% <a href="#listen_addrs" class="no-underline"><code>listen_addrs</code>
%%  was not used), Partisan will use a randomly generated port.
%% However, the random port will only work for clusters deployed within the same
%% host i.e. used for testing. Moreover, the `peer_port' value is also used by
%% some peer discovery strategies that cannot detect in which port the peer is
%% listening e.g. DNS. So for production environments we recommend always
%% setting the same value on all peers, and having at least one
%% {@link partisan:listen_addr()} in each peer
%% <a href="#listen_addrs" class="no-underline"><code>listen_addrs</code>
%%  option (when used) having the same port value.
%%
%% <h4 id="periodic_enabled" class="section-heading">periodic_enabled</h4>
%% TBD
%%
%% <h4 id="periodic_interval" class="section-heading">periodic_interval</h4>
%% TBD
%%
%% <h4 id="pid_encoding" class="section-heading">pid_encoding</h4>
%% TBD
%%
%% <h4 id="random_seed" class="section-heading">random_seed</h4>
%% TBD
%%
%% <h4 id="ref_encoding" class="section-heading">ref_encoding</h4>
%% TBD
%%
%% <h4 id="register_pid_for_encoding"
%% class="section-heading">register_pid_for_encoding</h4>
%% TBD
%%
%% <h4 id="remote_ref_format" class="section-heading">remote_ref_format</h4>
%% Defines how partisan remote references pids, references and registered
%% names will be encoded. See {@link partisan_remote_ref}).
%%
%% Accepts the following atom values:
%% <ul>
%% <li> `uri' - remote references will be encoded as binary URIs.</li>
%% <li> `tuple' - remote references will be encoded as tuples (the
%% format used by Partisan v1 to v4).</li>
%% <li>`improper_list' - remote references will be encoded as  improper lists,
%% similar to how aliases are encoded by the OTP modules.</li>
%% </ul>
%%
%% This option exists to allow the user to tradeoff between memory and latency.
%% In terms of memory `uri' is the cheapest, followed by `improper_list'. In
%% terms of latency `tuple' is the fastest followed by `improper_list'. The
%% default is `improper_list' a if offers a good balance between memory and
%% latency.
%%
%% ```
%% 1> partisan_config:set(remote_ref_format, uri).
%% ok
%% 2> partisan:self().
%% <<"partisan:pid:nonode@nohost:0.1062.0">>
%% 3> partisan_config:set(remote_ref_format, tuple).
%% 4> partisan:self().
%% {partisan_remote_reference,
%%    nonode@nohost,
%%    {partisan_process_reference,"<0.1062.0>"}}
%% 5> partisan_config:set(remote_ref_format, improper_list).
%% 6> partisan:self().
%% [nonode@nohost|<<"Pid#<0.1062.0>">>]
%% '''
%%
%% <h4 id="remote_ref_uri_padding"
%% class="section-heading">remote_ref_uri_padding</h4>
%% If `true' and the URI encoding of a
%% remote reference results in a binary smaller than 65 bytes, the URI will be
%% padded. The default is `false'.
%% ```
%% 1> partisan_config:set(remote_ref_binary_padding, false).
%% 1> partisan:self().
%% <<"partisan:pid:nonode@nohost:0.1062.0">>
%% 2> partisan_config:set(remote_ref_binary_padding, true).
%% ok
%% 3> partisan:self().
%% <<"partisan:pid:nonode@nohost:0.1062.0:"...>>
%% '''
%%
%% <h4 id="replaying" class="section-heading">replaying</h4>
%% TBD
%%
%% <h4 id="reservations" class="section-heading">reservations</h4>
%% TBD
%%
%% <h4 id="retransmit_interval" class="section-heading">retransmit_interval</h4>
%%  When option `retransmission' is set to `true' in the
%% `partisan:forward_opts()' used in a call to
%% {@link partisan:forward_message/3} and message delivery fails, the Peer
%% Service will enqueue the message for retransmission. This option is used to
%% control the interval of time between retransmission attempts.
%%
%% <h4 id="shrinking" class="section-heading">shrinking</h4>
%% TBD
%%
%% <h4 id="tag" class="section-heading">tag</h4>
%% The role of this node when using the Client-Server topology implemented
%% by @{link partisan_client_server_peer_manager}.
%% ==== Options ====
%% <ul>
%% <li>`undefined' - The node acts as a normal peer in all other topologies. This the default value</li>
%% <li>`client' - The node acts as a client. To be used only in combination with
%% `{partisan_peer_manager, partisan_client_server_peer_manager}'</li>
%% <li>`server' - The node acts as a server. To be used only in combination with
%% `{partisan_peer_manager, partisan_client_server_peer_manager}'</li>
%% </ul>
%%
%% <h4 id="tls" class="section-heading">tls</h4>
%% A boolean value indicating whether channel connections should use TLS. If
%% enabled, you have to provide a value for `tls_client_options' and
%% `tls_server_options'. The default is `false'.
%%
%% <h4 id="tls_client_options" class="section-heading">tls_client_options</h4>
%% The TLS socket options used when establishing outgoing connections to
%% peers. The configuration applies to all Partisan channels.
%% The default is `[]'.
%% ==== Example ====
%% ```
%% {tls_client_options, [
%%     {certfile, "config/_ssl/client/keycert.pem"},
%%     {cacertfile, "config/_ssl/client/cacerts.pem"},
%%     {keyfile, "config/_ssl/client/key.pem"},
%%     {verify, verify_none}
%% ]}
%% '''
%%
%% <h4 id="tls_server_options" class="section-heading">tls_server_options</h4>
%% The TLS socket options used when establishing incoming connections from
%% peers. The configuration applies to all Partisan channels.
%% The default is `[]'.
%% ==== Example ====
%% ```
%% {tls_server_options, [
%%     {certfile, "config/_ssl/server/keycert.pem"},
%%     {cacertfile, "config/_ssl/server/cacerts.pem"},
%%     {keyfile, "config/_ssl/server/key.pem"},
%%     {verify, verify_none}
%% ]}
%% '''
%%
%% <h4 id="tracing" class="section-heading">tracing</h4>
%% a boolean value. The default is `false'.
%%
%% <h4 id="xbot_interval" class="section-heading">xbot_interval</h4>
%% TBD
%%
%% == Deprecated Options ==
%% The following is the list of options have been deprecated. Some of them have
%% been renamed and/or moved down a level in the configuration tree.
%%
%% <h4 id="arwl" class="section-heading">arwl</h4>
%% HyParView's Active View Random Walk Length. Defaults to
%% `6'. Use `active_rwl' in the `hyparview' option instead.
%%
%% <h4 id="fanout" class="section-heading">fanout</h4>
%% The number of nodes that are contacted at each gossip
%% interval.
%%
%% <h4 id="max_active_size" class="section-heading">max_active_size</h4>
%% HyParView's Active View Random Walk Length. Defaults to `6'.
%% Use `active_max_size' in the `hyparview' option instead.
%%
%% <h4 id="max_passive_size" class="section-heading">max_passive_size</h4>
%% HyParView's Active View Random Walk Length. Defaults to `30'.
%% Use `passive_max_size' in the `hyparview' option instead.
%%
%% <h4 id="mix_active_size" class="section-heading">mix_active_size</h4>
%% HyParView's Active View Random Walk Length. Defaults to `3'.
%% Use `active_min_size' in the `hyparview' option instead.
%%
%% <h4 id="passive_view_shuffle_period"
%% class="section-heading">passive_view_shuffle_period</h4>
%% Use `shuffle_interval' in the `hyparview' option instead.
%%
%% <h4 id="peer_port" class="section-heading">peer_host</h4>
%% Use
%% <a href="#listen_addrs" class="no-underline"><code>listen_addrs</code></a>
%%  instead.
%%
%% <h4 id="partisan_peer_service_manager"
%% class="section-heading">partisan_peer_service_manager</h4>
%% Use `peer_service_manager' instead.
%%
%% <h4 id="prwl" class="section-heading">prwl</h4>
%% HyParView's Passive View Random Walk Length. Defaults to
%% `6'. Use `passive_rwl' in the `hyparview' option instead.
%%
%% <h4 id="random_promotion" class="section-heading">random_promotion</h4>
%% Use `random_promotion' in the `hyparview' option instead.
%%
%% <h4 id="random_promotion_period"
%% class="section-heading">random_promotion_period</h4>
%% Use `random_promotion_interval' in the `hyparview' option instead.
%%
%% <h4 id="remote_ref_as_uri" class="section-heading">remote_ref_as_uri</h4>
%% Use `{remote_ref_format, uri}' instead
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_config).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan_logger.hrl").
-include("partisan_util.hrl").
-include("partisan.hrl").


-define(KEY(Arg), {?MODULE, Arg}).
-define(IS_KEY(Arg), is_tuple(Arg) andalso element(1, Arg) == ?MODULE).

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
-compile({inline,[{channel_opts,1}]}).
-compile({inline,[{channels,0}]}).
-compile({inline,[{default_channel,0}]}).
-compile({inline,[{get,1}]}).
-compile({inline,[{get,2}]}).
-compile({inline,[{get_with_opts,2}]}).
-compile({inline,[{get_with_opts,3}]}).


%% =============================================================================
%% API
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc Initialises the configuration from the application environment.
%%
%% <strong>You should never call this function</strong>. This is used by
%% Partisan itself during startup.
%% The function is (and should be) idempotent, which is required for testing.
%% @end
%% -----------------------------------------------------------------------------
init() ->
    ok = cleanup(),
    PeerService0 = application:get_env(
        partisan,
        peer_service_manager,
        ?DEFAULT_PEER_SERVICE_MANAGER
    ),

    PeerService =
        case os:getenv("PEER_SERVICE", "false") of
            "false" ->
                PeerService0;
            String ->
                list_to_atom(String)
        end,

    %% Configure the partisan node name.
    %% Must be done here, before the resolution call is made.
    ok = maybe_set_node_name(),

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
    DefaultPeerIP = try_get_peer_ip(),
    DefaultPeerPort = random_port(),

    [env_or_default(Key, Default) ||
        {Key, Default} <- [
            %% WARNING:
            %% This list should be exhaustive, anything key missing from this
            %% list will not be read from the application environment.
            %% The following keys are missing on purpose
            %% as we need to process them after: [channels].
            {binary_padding, false},
            {broadcast, false},
            {broadcast_mods, [partisan_plumtree_backend]},
            {causal_labels, []},
            {channel_fallback, true},
            {connect_disterl, false},
            {connection_jitter, ?CONNECTION_JITTER},
            {connection_interval, 1000},
            {disable_fast_forward, false},
            {disable_fast_receive, false},
            {distance_enabled, ?DISTANCE_ENABLED},
            {egress_delay, 0},
            {exchange_selection, optimized},
            {exchange_tick_period, ?DEFAULT_EXCHANGE_TICK_PERIOD},
            {fanout, ?FANOUT},
            {gossip, true},
            {hyparview, ?HYPARVIEW_DEFAULTS},
            {ingress_delay, 0},
            {lazy_tick_period, ?DEFAULT_LAZY_TICK_PERIOD},
            {listen_addrs, []},
            {membership_binary_compression, true},
            {membership_strategy, ?DEFAULT_MEMBERSHIP_STRATEGY},
            {membership_strategy_tracing, ?MEMBERSHIP_STRATEGY_TRACING},
            {orchestration_strategy, ?DEFAULT_ORCHESTRATION_STRATEGY},
            {parallelism, ?PARALLELISM},
            {peer_discovery, #{enabled => false}},
            {peer_service_manager, PeerService},
            {peer_ip, DefaultPeerIP},
            {peer_port, DefaultPeerPort},
            {periodic_enabled, ?PERIODIC_ENABLED},
            {periodic_interval, 10000},
            {pid_encoding, true},
            {random_seed, random_seed()},
            {ref_encoding, true},
            {register_pid_for_encoding, false},
            {remote_ref_format, improper_list},
            {remote_ref_uri_padding, false},
            {replaying, false},
            {retransmit_interval, 1000},
            {reservations, []},
            {shrinking, false},
            {tag, DefaultTag},
            {tls, false},
            {tls_client_options, []},
            {tls_server_options, []},
            {tracing, false},
            {transmission_logging_mfa, undefined}
       ]
    ],

    %% Setup channels
    Channels = application:get_env(?APP, channels, #{}),
    set(channels, Channels),

    %% Setup default listen addr.
    %% This will be part of the partisan:node_spec() which is the map
    DefaultAddr = #{
        ip => get(peer_ip),
        port => get(peer_port)
    },

    case get(listen_addrs) of
        [] ->
            set(listen_addrs, [DefaultAddr]);
        L when is_list(L) ->
            ok
    end.



%% -----------------------------------------------------------------------------
%% @doc Seed the process.
%% @end
%% -----------------------------------------------------------------------------
seed(Seed) ->
    rand:seed(exsplus, Seed).


%% -----------------------------------------------------------------------------
%% @doc Seed the process.
%% @end
%% -----------------------------------------------------------------------------
seed() ->
    RandomSeed = random_seed(),
    ?LOG_DEBUG(#{
        description => "Chossing random seed",
        node => partisan:node(),
        seed => RandomSeed
    }),
    rand:seed(exsplus, RandomSeed).


%% -----------------------------------------------------------------------------
%% @doc Return a random seed, either from the environment or one that's
%% generated for the run.
%% @end
%% -----------------------------------------------------------------------------
random_seed() ->
    case get(random_seed, undefined) of
        undefined ->
            {erlang:phash2([partisan:node()]), erlang:monotonic_time(), erlang:unique_integer()};
        Other ->
            Other
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
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
    persistent_term:get(?KEY(maybe_rename(Key))).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
get(Key, Default) ->
    persistent_term:get(?KEY(maybe_rename(Key)), Default).


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
set(listen_addrs, Value0) when is_list(Value0) ->
    %% We make sure they are sorted so that we can compare them (specially when
    %% part of the node_spec()).
    Value = lists:usort(validate_listen_addrs(Value0)),
    do_set(listen_addrs, Value);

set(peer_ip, Value) when is_list(Value) ->
    ParsedIP = partisan_util:parse_ip_address(Value),
    do_set(peer_ip, ParsedIP);

set(channels, Arg) when is_list(Arg) orelse is_map(Arg) ->
    %% We coerse any defined channel to channel spec map representations and
    %% build a
    %% mapping of {partisan:channel() -> partisan:channel_opts()}
    Channels0 = to_channels_map(Arg),

    %% We set default channel, overriding any user input
    DefaultChannel = maps:without([name], to_channel_spec(?DEFAULT_CHANNEL)),
    Channels1 = Channels0#{?DEFAULT_CHANNEL => DefaultChannel},

    Channels =
        case maps:find(?MEMBERSHIP_CHANNEL, Channels1) of
            {ok, MChannel0} when is_map(MChannel0) ->
                %% Make sure membership channel is not monotonic
                MChannel = maps:merge(MChannel0, #{monotonic => false}),
                Channels1#{?MEMBERSHIP_CHANNEL => MChannel};
            error ->
                Channels1#{
                    ?MEMBERSHIP_CHANNEL => #{
                        parallelism => 1,
                        monotonic => false,
                        compression => true
                    }
                }
        end,

    maps:foreach(
        fun(Channel, #{parallelism := N} = Opts) ->
            telemetry:execute(
                [partisan, channel, configured],
                #{
                    max => N
                },
                #{
                    channel => Channel,
                    channel_opts => Opts
                }
            )
        end,
        Channels
    ),

    do_set(channels, Channels);

set(broadcast_mods, L0) ->
    is_list(L0) orelse error({badarg, [broadcast_mods, L0]}),
    Map = fun
        ToAtom(Mod) when is_list(Mod) ->
            try
                {true, list_to_existing_atom(Mod)}
            catch
                error:Reason ->
                    ToAtom({error, Mod})
            end;

        ToAtom({error, Mod}) ->
            ?LOG_ERROR(#{
                description => "Configuration error. Broadcast module ignored",
                reason => "Invalid module",
                module => Mod
            }),
            false;

        ToAtom(Mod) when is_atom(Mod), Mod =/= undefined ->
            true;

        ToAtom(Mod) ->
            ToAtom({error, Mod})

    end,
    L = lists:filtermap(Map, L0),
    %% We always add the mods required by partisan itself.
    do_set(broadcast_mods, lists:usort(L ++ ?BROADCAST_MODS));

set(hyparview, Value) ->
    set_hyparview_config(Value);

set(membership_binary_compression, true) ->
    do_set(membership_binary_compression, true),
    do_set('$membership_encoding_opts', [compressed]);

set(membership_binary_compression, N) when is_integer(N), N >= 0, N =< 9 ->
    do_set(membership_binary_compression, N),
    do_set('$membership_encoding_opts', [{compressed, N}]);

set(membership_binary_compression, Val) ->
    do_set(membership_binary_compression, Val),
    do_set('$membership_encoding_opts', []);

set(forward_options, Opts) when is_list(Opts) ->
    set(forward_options, maps:from_list(Opts));

set(tls_client_options, Opts0) when is_list(Opts0) ->
    case lists:keytake(hostname_verification, 1, Opts0) of
        {value, {hostname_verification, wildcard}, Opts1} ->
            Match = public_key:pkix_verify_hostname_match_fun(https),
            Check = {customize_hostname_check, [{match_fun, Match}]},
            Opts = lists:keystore(customize_hostname_check, 1, Opts1, Check),
            do_set(tls_client_options, Opts);

        {value, {hostname_verification, _}, Opts1} ->
            do_set(tls_client_options, Opts1);

        false ->
            do_set(tls_client_options, Opts0)
    end;

set(Key, Value) ->
    do_set(maybe_rename(Key), Value).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
listen_addrs() ->
    get(listen_addrs).


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
    get(channels).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
parallelism() ->
    get(parallelism, ?PARALLELISM).



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
cleanup() ->
    _ = [
        persistent_term:erase(Key)
        || {Key, _} <- persistent_term:get(), ?IS_KEY(Key)
    ],
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc This call is idempotent
%% @end
%% -----------------------------------------------------------------------------
maybe_set_node_name() ->
    case get(name, undefined) of
        undefined ->
            %% We read directly from the env (not our cache)
            UserDefined = application:get_env(partisan, name, undefined),
            set_node_name(UserDefined);

        Nodename ->
            %% Name already set
            ?LOG_NOTICE(#{
                description =>
                    "Partisan node name generated and configured",
                name => Nodename,
                disterl_enabled => get(disterl_enabled)
            }),
            ok
    end.


%% @private
set_node_name(UserDefined) ->
    Name =
        case erlang:node() of
            nonode@nohost when UserDefined == undefined ->
                Generated = gen_node_name(),
                ?LOG_NOTICE(#{
                    description =>
                        "Partisan node name generated and configured",
                    name => Generated,
                    disterl_enabled => false
                }),
                Generated;

            nonode@nohost when UserDefined =/= undefined ->
                ?LOG_NOTICE(#{
                    description => "Partisan node name configured",
                    name => UserDefined,
                    disterl_enabled => false
                }),
                UserDefined;

            Other ->
                ?LOG_NOTICE(#{
                    description => "Partisan node name configured",
                    name => Other,
                    disterl_enabled =>
                        application:get_env(?APP, disterl_enabled, false)
                }),
                Other
        end,

    set(name, Name),
    set(nodestring, atom_to_binary(Name, utf8)).


%% @private
gen_node_name() ->
    {UUID, _UUIDState} = uuid:get_v1(uuid:new(self())),
    StringUUID = uuid:uuid_to_string(UUID),

    Host =
        case application:get_env(?APP, peer_ip, undefined) of
            undefined ->
                {ok, Val} = inet:gethostname(),
                Val;

            Val ->
                address_to_string(partisan_util:parse_ip_address(Val))
        end,

    list_to_atom(StringUUID ++ "@" ++ Host).



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
address_to_string(IPAddress) when ?IS_IP(IPAddress) ->
    inet:ntoa(IPAddress);

address_to_string(Address) when is_binary(Address) ->
    binary_to_list(Address);

address_to_string(Address) when is_list(Address) ->
    Address.





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
    OldValue = persistent_term:get(?KEY(Key), undefined),
    do_set(Key, MergeFun(OldValue));

do_set(Key, Value) ->
    application:set_env(?APP, Key, Value),
    persistent_term:put(?KEY(Key), Value).


%% @private
set_hyparview_config(Config) when is_map(Config) ->
    set_hyparview_config(maps:to_list(Config));

set_hyparview_config(Config) when is_list(Config) ->
    %% We rename keys
    M = lists:foldl(
        fun({Key, Val}, Acc) ->
            maps:put(maybe_rename(Key), Val, Acc)
        end,
        maps:new(),
        Config
    ),
    %% We merge with defaults
    do_set(hyparview, maps:merge(get(hyparview, ?HYPARVIEW_DEFAULTS), M)).


%% -----------------------------------------------------------------------------
%% @private
%% @doc Rename keys
%% @end
%% -----------------------------------------------------------------------------
maybe_rename(arwl) ->
    % hyparview
    active_rwl;

maybe_rename(prwl) ->
    % hyparview
    passive_rwl;

maybe_rename(max_active_size) ->
    % hyparview
    active_max_size;

maybe_rename(min_active_size) ->
    % hyparview
    active_min_size;

maybe_rename(max_passive_size) ->
    % hyparview
    passive_max_size;

maybe_rename(passive_view_shuffle_period) ->
    % hyparview
    shuffle_interval;

maybe_rename(random_promotion_period) ->
    % hyparview
    random_promotion_interval;

maybe_rename(partisan_peer_service_manager) ->
    peer_service_manager;

maybe_rename(Key) ->
    Key.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
validate_listen_addrs(Addrs) ->
    lists:foldl(
        fun(Addr, Acc) ->
            [partisan_util:parse_listen_address(Addr) | Acc]
        end,
        [],
        Addrs
    ).



%% @private
random_port() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, {_, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    Port.


%% @private
try_get_peer_ip() ->
    case application:get_env(partisan, peer_ip) of
        {ok, Value} when is_list(Value) orelse ?IS_IP(Value) ->
            partisan_util:parse_ip_address(Value);

        undefined ->
            get_peer_ip()
    end.


%% @private
get_peer_ip() ->
    LongName = atom_to_list(partisan:node()),
    [_ShortName, Host] = string:tokens(LongName, "@"),

    %% Spawn a process to perform resolution.
    Me = self(),
    ReqId = spawn_request(
        node(),
        fun() -> Me ! {ok, get_ip_addr(Host)} end,
        [{reply, error_only}]
    ),

    %% Wait for response, either answer or exit.
    receive
        {ok, Addr} ->
            Addr;

        {spawn_reply, ReqId, error, Reason} ->
            ?LOG_INFO(#{
                description =>
                    "Cannot resolve IP address for host, using 127.0.0.1",
                host => Host,
                reason => Reason
            }),
            ?LOCALHOST

    after
        5000 ->
            _ = spawn_request_abandon(ReqId),
            ?LOG_INFO(#{
                description =>
                    "Cannot resolve IP address for host, using 127.0.0.1",
                host => Host,
                reason => timeout
            }),
            ?LOCALHOST
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_ip_addr(Host :: inet:ip_address() | inet:hostname()) ->
    inet:ip_address().

get_ip_addr(Host) ->
    Families = case is_inet6_supported() of
        true -> [inet6, inet];
        false -> [inet]
    end,
    get_ip_addr(Host, Families, undefined).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
get_ip_addr(Host, [H|T], _) ->
    case inet:getaddr(Host, H) of
        {ok, Addr} ->
            ?LOG_NOTICE(#{
                description => "Resolved IP address for host",
                family => H,
                host => Host,
                addr => Addr
            }),
            Addr;

        {error, Reason} ->
            get_ip_addr(Host, T, Reason)
    end;

get_ip_addr(Host, [], Reason) ->
    %% Fallback, as we could't resolve Host
    ?LOG_NOTICE(#{
        description => "Cannot resolve IP address for host, using 127.0.0.1",
        host => Host,
        reason => partisan_util:format_posix_error(Reason)
    }),
    {127, 0, 0, 1}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
is_inet6_supported() ->
    case gen_tcp:listen(0, [inet6]) of
        {ok, Socket} ->
            ok = gen_tcp:close(Socket),
            true;

        _Error ->
            false
    end.


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
        monotonic => false,
        compression => false
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

