%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Christopher Meiklejohn.  All Rights Reserved.
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
%% @doc This module realises the {@link partisan_peer_service_manager}
%% behaviour implementing a peer sampling service with a pluggable overlay
%% topology by delegating the topology definition to a callback module
%% implementing the @{partisan_peer_service_strategy} behaviour.
%%
%% == Characteristics ==
%% <ul>
%% <li>Uses TCP/IP.</li>
%% <li>All nodes communicate and maintain connections with all other nodes.</li>
%% <li>Nodes periodically send heartbeat messages. The service considers a node
%% "failed" when it misses X heartbeats.</li>
%% <li>Point-to-point messaging with a single network hop.</li>
%% <li>Eventually consistent membership maintained in a CRDT and replicated
%% using gossip.</li>
%% <li>Scalability limited to hundres of nodes (60-200 nodes).</li>
%% </ul>
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_pluggable_peer_service_manager).

-behaviour(gen_server).
-behaviour(partisan_peer_service_manager).

-include("partisan_logger.hrl").
-include("partisan.hrl").

-define(SET_FROM_LIST(L), sets:from_list(L, [{version, 2}])).
%% Holds the `atomics' reference backing the message-clock counter. In
%% `persistent_term' so any process can read it without a message, and so the
%% counter survives a restart of this server (see `init_message_clock/0').
-define(MSG_CLOCK_KEY, {?MODULE, message_clock}).
%% `true' while any interposition function is registered. See
%% `publish_interposition_flag/1'.
-define(INTERPOSITION_KEY, {?MODULE, has_interposition_funs}).
-define(IS_ON_EVENT_FUN(X),
    (is_function(X, 0) orelse is_function(X, 1) orelse is_function(X, 2))
).

-ifdef(TEST).
-define(INTERPOSITION, true).
-endif.

-ifdef(INTERPOSITION).

%% returns ok
-define(FIRE_PRE_INTERPOSITIONS(Type, Node, Msg, Funs),
    maps:fold(
        fun(_Node, Fun, ok) ->
            ?LOG_DEBUG(
                "Firing pre-interposition fun for message: ~p",
                [Msg]
            ),
            Fun({forward_message, Node, Msg}),
            ok
        end,
        ok,
        Funs
    )
).

%% returns message
-define(FIRE_INTERPOSITIONS(Type, Node, Msg, Funs),
    maps:fold(
        fun(_Name, Fun, M) ->
            ?LOG_DEBUG(
                "Firing interposition fun for message: ~p",
                [M]
            ),
            Fun({Type, Node, M})
        end,
        Msg,
        Funs
    )
).

%% returns ok
-define(FIRE_POST_INTERPOSITIONS(Type, Node, Msg0, Msg1, Funs),
    maps:fold(
        fun(_Name, Fun, ok) ->
            ?LOG_DEBUG(
                "Firing post-interposition fun for messages: [~p, ~p]",
                [Msg0, Msg1]
            ),
            Fun(
                {Type, Node, Msg0},
                {Type, Node, Msg1}
            ),
            ok
        end,
        ok,
        Funs
    )
).

-else.

%% returns ok
-define(FIRE_PRE_INTERPOSITIONS(Type, Node, Msg, Funs), begin
    %% We do this so that we avoid the compiler warning us Funs is not
    %% used
    true = is_map(Funs),
    ok
end).

%% returns message
-define(FIRE_INTERPOSITIONS(Type, Node, Msg, Funs),
    Msg
).

%% returns ok
-define(FIRE_POST_INTERPOSITIONS(Type, Node, Msg0, Msg1, Funs),
    ok
).

-endif.

-record(state, {
    name :: node(),
    node_spec :: partisan:node_spec(),
    actor :: partisan:actor(),
    %% A materialised view of the membership_strategy_state as a list
    members :: [partisan:node_spec()],
    %% The nodes we still need to establish connections with
    pending :: [partisan:node_spec()],
    membership_strategy :: atom(),
    membership_strategy_state :: term(),
    leaving = false :: boolean(),
    distance_metrics :: map(),
    sync_joins :: #{partisan:node_spec() => sets:set(from())},
    out_links :: [term()],
    down_funs :: node_subs(),
    channel_down_funs :: channel_subs(),
    up_funs :: node_subs(),
    channel_up_funs :: channel_subs(),
    %% Channels we have already announced as up. A connection is stored
    %% before its handshake completes, so the connection table cannot tell us
    %% whether a `connected' signal is the first one for a channel — and with
    %% channel parallelism > 1 several connections carry the same channel.
    %% This makes the channel events edge-triggered.
    up_channels = sets:new() :: sets:set({node(), partisan:channel()}),
    pre_interposition_funs :: interposition_map(x_interpos_fun()),
    interposition_funs :: interposition_map(interpos_fun()),
    post_interposition_funs :: interposition_map(x_interpos_fun())
}).

-type t() :: #state{}.
-type from() :: {pid(), atom()}.
-type on_event_fun() :: partisan_peer_service_manager:on_event_fun().
-type node_subs() :: #{'_' | node() => [on_event_fun()]}.
-type channel_subs() :: #{
    {'_' | node(), '_' | partisan:channel()} =>
        [on_event_fun()]
}.
-type interposition_map(T) :: #{any() => T}.
-type interpos_arg() ::
    {receive_message, node(), any()}
    | {forward_message, node(), any()}.
-type interpos_fun() :: fun((interpos_arg()) -> interpos_arg()).
-type x_interpos_fun() :: fun((interpos_arg()) -> ok).
-type tag() :: atom().
-type info() ::
    connections
    | retransmit
    | periodic
    | instrumentation
    | distance
    | tree_refresh
    | {'EXIT', partisan:any_pid(), any()}
    | {
        connected,
        partisan:node_spec(),
        partisan:channel(),
        tag(),
        t()
    }.

%% %% API
-export([member/1]).

%% PARTISAN_PEER_SERVICE_MANAGER CALLBACKS
-export([add_interposition_fun/2]).
-export([add_post_interposition_fun/2]).
-export([add_pre_interposition_fun/2]).
-export([cast_message/2]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([decode/1]).
-export([forward_message/2]).
-export([forward_message/3]).
-export([forward_message/4]).

-ifdef(TEST).
%% Exported for test. `next_message_clock/1' is the message-clock source and
%% `fast_forward_acked/6' the acknowledged send path; neither routes through
%% this server, so neither is reachable from a single-node unit test
%% otherwise.
-export([next_message_clock/1]).
-export([fast_forward_acked/6]).
-endif.
-export([get_interposition_funs/0]).
-export([get_local_state/0]).
-export([get_pre_interposition_funs/0]).
-export([inject_partition/2]).
-export([join/1]).
-export([leave/0]).
-export([leave/1]).
-export([members/0]).
-export([members_for_orchestration/0]).
-export([on_down/2]).
-export([on_down/3]).
-export([on_up/2]).
-export([on_up/3]).
-export([partitions/0]).
-export([receive_message/3]).
-export([remove_interposition_fun/1]).
-export([remove_post_interposition_fun/1]).
-export([remove_pre_interposition_fun/1]).
-export([reserve/1]).
-export([resolve_partition/1]).
-export([send_message/2]).
-export([start_link/0]).
-export([supports_capability/1]).
-export([sync_join/1]).
-export([update_members/1]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% =============================================================================
%% API
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc Returns true if node `Node' is a member in the membership list.
%% Otherwise returns `false'.
%% @end
%% -----------------------------------------------------------------------------
member(#{name := Node}) ->
    member(Node);
member(Node) when is_atom(Node) ->
    gen_server:call(?MODULE, {member, Node}, infinity).

%% =============================================================================
%% PARTISAN_PEER_SERVICE_MANAGER CALLBACKS
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc Same as start_link([]).
%% @end
%% -----------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.

start_link() ->
    Opts = [
        {spawn_opt, ?PARALLEL_SIGNAL_OPTIMISATION([])}
    ],
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], Opts).

%% -----------------------------------------------------------------------------
%% @doc Return membership list.
%% @end
%% -----------------------------------------------------------------------------
members() ->
    gen_server:call(?MODULE, members, infinity).

%% -----------------------------------------------------------------------------
%% @doc Return membership list.
%% @end
%% -----------------------------------------------------------------------------
members_for_orchestration() ->
    gen_server:call(?MODULE, members_for_orchestration, infinity).

%% -----------------------------------------------------------------------------
%% @doc Update membership.
%% @end
%% -----------------------------------------------------------------------------
update_members(Members) ->
    gen_server:call(?MODULE, {update_members, Members}, infinity).

%% -----------------------------------------------------------------------------
%% @doc Return local node's view of cluster membership.
%% @end
%% -----------------------------------------------------------------------------
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).

%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection close for a given node.
%% `Fun' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_down(Arg, Fun) ->
    on_down(Arg, Fun, #{}).

%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection close for a given node.
%% `Fun' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_down(#{name := Node}, Fun, Opts) ->
    on_down(Node, Fun, Opts);
on_down(any, Fun, Opts) ->
    on_down('_', Fun, Opts);
on_down(Node, Fun, Opts) when
    is_atom(Node) andalso is_map(Opts) andalso ?IS_ON_EVENT_FUN(Fun)
->
    gen_server:call(?MODULE, {on_down, Node, Fun, Opts}, infinity).

%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection open for a given node.
%% `Fun' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_up(Arg, Fun) ->
    on_up(Arg, Fun, #{}).

%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection open for a given node.
%% `Fun' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_up(#{name := Node}, Fun, Opts) ->
    on_up(Node, Fun, Opts);
on_up(any, Fun, Opts) ->
    on_up('_', Fun, Opts);
on_up(Node, Fun, Opts) when
    is_atom(Node) andalso is_map(Opts) andalso ?IS_ON_EVENT_FUN(Fun)
->
    gen_server:call(?MODULE, {on_up, Node, Fun, Opts}, infinity).

%% -----------------------------------------------------------------------------
%% @doc Attempt to join a remote node.
%% @end
%% -----------------------------------------------------------------------------
join(#{name := _} = NodeSpec) ->
    gen_server:call(?MODULE, {join, NodeSpec}, infinity).

%% -----------------------------------------------------------------------------
%% @doc Attempt to join a remote node.
%% @end
%% -----------------------------------------------------------------------------
sync_join(#{name := _} = NodeSpec) ->
    gen_server:call(?MODULE, {sync_join, NodeSpec}, infinity).

%% -----------------------------------------------------------------------------
%% @doc Leave the cluster.
%% @end
%% -----------------------------------------------------------------------------
leave() ->
    gen_server:call(?MODULE, {leave, partisan:node_spec()}, infinity).

%% -----------------------------------------------------------------------------
%% @doc Remove another node from the cluster.
%% @end
%% -----------------------------------------------------------------------------
leave(#{name := _} = NodeSpec) ->
    gen_server:call(?MODULE, {leave, NodeSpec}, infinity).

%% -----------------------------------------------------------------------------
%% @doc Send message to a remote peer service manager.
%% @end
%% -----------------------------------------------------------------------------
send_message(Node, Message) ->
    %% TODO maybe deprecate, not used by Partisan and we can always do
    %% partisan_rpc:call/4
    Cmd = {send_message, Node, Message},
    gen_server:call(?MODULE, Cmd, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    Term :: partisan:any_pid() | partisan:any_name(),
    Message :: partisan:message()
) -> ok.

cast_message(Term, Message) ->
    _ = forward_message(Term, {'$gen_cast', Message}, #{}),
    ok.

%% -----------------------------------------------------------------------------
%% @doc Cast a message to a remote gen_server.
%% @end
%% -----------------------------------------------------------------------------
cast_message(Node, ServerRef, Message) ->
    cast_message(Node, ServerRef, Message, #{}).

%% -----------------------------------------------------------------------------
%% @doc Cast a message to a remote gen_server.
%% @end
%% -----------------------------------------------------------------------------
cast_message(Node, ServerRef, Message, Options) ->
    %% TODO maybe deprecate, since we have partisan_gen_server:cast
    %% and partisan_gen_statem:cast ?
    _ = forward_message(Node, ServerRef, {'$gen_cast', Message}, Options),
    ok.

%% -----------------------------------------------------------------------------
%% @doc Gensym support for forwarding.
%% @end
%% -----------------------------------------------------------------------------
forward_message(Term, Message) ->
    forward_message(Term, Message, #{}).

%% -----------------------------------------------------------------------------
%% @doc Gensym support for forwarding.
%% @end
%% -----------------------------------------------------------------------------
forward_message(PidOrName, Message, _Opts) when
    is_pid(PidOrName); is_atom(PidOrName)
->
    _ = erlang:send(PidOrName, Message),
    ok;
forward_message({global, _} = ServerRef, Message, _Opts) ->
    %% Will do nothing is disterl is not enabled as we currently do not have
    %% partisan_global
    partisan_peer_service_manager:deliver(ServerRef, Message);
forward_message({via, _, _} = ServerRef, Message, _Opts) ->
    partisan_peer_service_manager:deliver(ServerRef, Message);
forward_message({Name, Node}, Message, Opts) when
    is_atom(Name), is_atom(Node)
->
    case Node == partisan:node() of
        true ->
            _ = erlang:send(Name, Message),
            ok;
        false ->
            forward_message(Node, Name, Message, Opts)
    end;
forward_message(RemoteRef, Message, Opts0) ->
    %% A reference is admitted because a process alias (`erlang:alias/1') is
    %% one, and an encoded alias is used as a one-shot reply address by
    %% `partisan_erpc'. Delivery for it is handled by the `is_reference' clause
    %% of `partisan_peer_service_manager:do_deliver/2'.
    partisan_remote_ref:is_pid(RemoteRef) orelse
        partisan_remote_ref:is_name(RemoteRef) orelse
        partisan_remote_ref:is_reference(RemoteRef) orelse
        error(badarg),

    %% `forward_opts()' is `map() | proplist()'. Normalise before use — the
    %% short-circuit test below reads `Opts' with `maps:is_key/2', and the
    %% generated `partisan_gen' code calls this with a proplist
    %% (`partisan_gen:get_opts()' returns `[{channel, _}]'). `/4' also accepts
    %% either form.
    Opts =
        case is_list(Opts0) of
            true -> maps:from_list(Opts0);
            false -> Opts0
        end,

    Node = partisan_remote_ref:node(RemoteRef),
    Target = partisan_remote_ref:target(RemoteRef),
    %% Prefer disterl only when it is both safe and correct: the caller opted
    %% in (`connect_disterl'), the target is on a *different* node that is
    %% actually reachable over disterl, and the caller has not requested any
    %% partisan-specific delivery feature (ack / causal delivery) — those need
    %% the full partisan path. Only name refs are disterl-usable: an encoded
    %% pid cannot be reconstructed as a remote pid (see
    %% `remote_ref_to_disterl/1'), so it always falls through to the partisan
    %% transport. Otherwise route through the encoded target.
    ShortCircuit =
        partisan_config:get(connect_disterl, false) andalso
            Node =/= partisan:node() andalso
            lists:member(Node, erlang:nodes()) andalso
            not maps:is_key(ack, Opts) andalso
            not maps:is_key(causal_label, Opts),

    case ShortCircuit andalso partisan:remote_ref_to_disterl(RemoteRef) of
        {ok, {Name, _Node} = NN} when is_atom(Name) ->
            _ = (catch erlang:send(NN, Message, [noconnect])),
            ok;
        _ ->
            forward_message(Node, Target, Message, Opts)
    end.

%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
forward_message(Node, ServerRef, Message, Opts) when is_list(Opts) ->
    forward_message(Node, ServerRef, Message, maps:from_list(Opts));
forward_message(Node, ServerRef, Message, Opts) when is_map(Opts) ->
    %% TODO ServerRef heer can be atom(), pid(), partisan_ref(), {via, _, _},
    %% {Name, Node} or anything !!!!
    %% Local-node forwarding bypasses the forwarding machinery entirely.
    %% When `connect_disterl` is enabled and the target node lives in
    %% `erlang:nodes()' (i.e. there is a real disterl link to it) we use
    %% disterl directly so the message arrives even when the partisan
    %% transport hasn't established a peer connection. We only short-circuit
    %% when the caller has not asked for any partisan-specific forwarding
    %% feature — interposition, ack, causal delivery — those need the
    %% full forward path so `partisan_pluggable_peer_service_manager'
    %% callbacks can fire.
    case Node =:= partisan:node() of
        true ->
            partisan_peer_service_manager:deliver(ServerRef, Message);
        false ->
            CanShortCircuit =
                (is_atom(ServerRef) orelse erlang:is_pid(ServerRef)) andalso
                    partisan_config:get(connect_disterl, false) andalso
                    lists:member(Node, erlang:nodes()) andalso
                    not maps:is_key(ack, Opts) andalso
                    not maps:is_key(causal_label, Opts),
            case CanShortCircuit of
                true ->
                    Target =
                        case is_atom(ServerRef) of
                            true -> {ServerRef, Node};
                            false -> ServerRef
                        end,
                    _ =
                        (catch erlang:send(
                            Target, Message, [noconnect]
                        )),
                    ok;
                false ->
                    %% Get forwarding options and combine with message
                    %% specific options.
                    FwdOpts = maps:merge(
                        partisan_config:get(forward_options, #{}), Opts
                    ),

                    %% Attempt to get the partition key, if possible.
                    PartitionKey = maps:get(
                        partition_key, FwdOpts, ?DEFAULT_PARTITION_KEY
                    ),

                    %% Use a clock provided by the sender,
                    %% otherwise, use a generated one.
                    Clock = maps:get(clock, FwdOpts, undefined),

                    PaddedMessage = partisan_util:maybe_pad_term(Message),

                    Cmd = {
                        forward_message,
                        Node,
                        Clock,
                        PartitionKey,
                        ServerRef,
                        PaddedMessage,
                        FwdOpts
                    },

                    %% Is fast forward disabled?
                    DisableFastForward =
                        partisan_config:get(disable_fast_forward, false),

                    %% Needs ack?
                    NeedsAck = maps:get(ack, FwdOpts, false),

                    %% Use causal delivery?
                    CausalDelivery =
                        maps:get(causal_label, FwdOpts, undefined) =/=
                            undefined,

                    %% Should we use fast forwarding?
                    %%
                    %% Conditions:
                    %% - fastforward is not disabled
                    %% - not labeled for causal delivery
                    %%
                    %% Acknowledgement does not force the serialised path:
                    %% the message clock comes from a lock-free counter (see
                    %% `next_message_clock/1') and recording the outstanding
                    %% message is a direct ETS write, so neither needs this
                    %% process. Causal delivery does route through the
                    %% causality backend, by design.
                    FastForward =
                        not (DisableFastForward orelse CausalDelivery),

                    %% Any interposition function registered? Read lock-free;
                    %% see `publish_interposition_flag/1'.
                    IsInterposed = has_interposition_funs(),

                    %% Attempt to fast-path, dispatching it directly to the connection
                    %% process
                    Fast =
                        case {FastForward, NeedsAck} of
                            {false, _} ->
                                false;
                            {true, false} ->
                                partisan_peer_connections:dispatch(Cmd);
                            {true, true} when not IsInterposed ->
                                fast_forward_acked(
                                    Node,
                                    PartitionKey,
                                    ServerRef,
                                    PaddedMessage,
                                    Clock,
                                    FwdOpts
                                );
                            {true, true} ->
                                %% Acknowledged *and* interposition functions
                                %% are registered. Take the serialised path so
                                %% they fire: dropping or rewriting an
                                %% acknowledged message is precisely what
                                %% fault injection uses them for, and
                                %% retransmission is expected to deliver it
                                %% once the fun is removed
                                %% (`partisan_SUITE:ack_test').
                                false
                        end,

                    case Fast of
                        ok ->
                            ok;
                        _ ->
                            %% FastForward == false or {error, _} from dispatch
                            %% We do a serialized execution as Opts might require
                            %% retransmission
                            gen_server:call(?MODULE, Cmd, infinity)
                    end
            end
    end.

%% -----------------------------------------------------------------------------
%% @doc Receive message from a remote manager.
%% @end
%% -----------------------------------------------------------------------------
receive_message(
    Node,
    Channel,
    {forward_message, _SrcNode, _Clock, _ServerRef, _Msg} = Cmd
) ->
    %% Process the message and generate the acknowledgement.
    gen_server:call(?MODULE, {receive_message, Node, Channel, Cmd}, infinity);
receive_message(
    Node,
    Channel,
    {forward_message, ServerRef, {'$partisan_padded', _Padding, Msg}}
) ->
    receive_message(Node, Channel, {forward_message, ServerRef, Msg});
receive_message(
    _,
    _Channel,
    {forward_message, _ServerRef, {causal, Label, _, _, _, _, _} = Msg}
) ->
    partisan_causality_backend:receive_message(Label, Msg);
receive_message(Node, Channel, {forward_message, ServerRef, Msg} = Cmd) ->
    %% We received a message for a destination in this node.
    case partisan_config:get(disable_fast_receive, false) of
        true ->
            %% Serialize execution
            gen_server:call(
                ?MODULE, {receive_message, Node, Channel, Cmd}, infinity
            );
        false ->
            %% Concurrent execution
            partisan_peer_service_manager:deliver(ServerRef, Msg)
    end;
receive_message(Node, Channel, Msg) ->
    gen_server:call(?MODULE, {receive_message, Node, Channel, Msg}, infinity).

%% -----------------------------------------------------------------------------
%% @doc Decode state.
%% @end
%% -----------------------------------------------------------------------------
decode(Membership) ->
    Membership.

%% -----------------------------------------------------------------------------
%% @doc Reserve a slot for the particular tag.
%% @end
%% -----------------------------------------------------------------------------
reserve(Tag) ->
    gen_server:call(?MODULE, {reserve, Tag}, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec supports_capability(Arg :: atom()) -> boolean().

supports_capability(monitoring) ->
    true;
supports_capability(_) ->
    false.

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_pre_interposition_fun(any(), x_interpos_fun()) -> ok.

add_pre_interposition_fun(Name, Fun) ->
    gen_server:call(
        ?MODULE,
        {add_pre_interposition_fun, Name, Fun},
        infinity
    ).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_pre_interposition_funs() -> interposition_map(x_interpos_fun()).

get_pre_interposition_funs() ->
    gen_server:call(?MODULE, get_pre_interposition_funs, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec remove_pre_interposition_fun(any()) -> ok.

remove_pre_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_pre_interposition_fun, Name}, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_interposition_fun(any(), interpos_fun()) -> ok.

add_interposition_fun(Name, InterpositionFun) ->
    gen_server:call(
        ?MODULE, {add_interposition_fun, Name, InterpositionFun}, infinity
    ).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_interposition_funs() -> interposition_map(interpos_fun()).

get_interposition_funs() ->
    gen_server:call(?MODULE, get_interposition_funs, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec remove_interposition_fun(any()) -> ok.

remove_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_interposition_fun, Name}, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_post_interposition_fun(any(), x_interpos_fun()) -> ok.

add_post_interposition_fun(Name, PostInterpositionFun) ->
    gen_server:call(
        ?MODULE,
        {add_post_interposition_fun, Name, PostInterpositionFun},
        infinity
    ).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec remove_post_interposition_fun(any()) -> ok.

remove_post_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_post_interposition_fun, Name}, infinity).

%% -----------------------------------------------------------------------------
%% @doc Inject a partition.
%% @end
%% -----------------------------------------------------------------------------
inject_partition(_Origin, _TTL) ->
    {error, not_implemented}.

%% -----------------------------------------------------------------------------
%% @doc Resolve a partition.
%% @end
%% -----------------------------------------------------------------------------
resolve_partition(_Reference) ->
    {error, not_implemented}.

%% -----------------------------------------------------------------------------
%% @doc Return partitions.
%% @end
%% -----------------------------------------------------------------------------
partitions() ->
    {error, not_implemented}.

%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================

-spec init([]) -> {ok, t()}.

init([]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    case partisan_config:get(binary_padding, false) of
        true ->
            %% Use 64-byte binary to force shared heap usage to cut down on copying.
            BinaryPaddingTerm = rand_bits(512),
            partisan_config:set(binary_padding_term, BinaryPaddingTerm);
        _ ->
            undefined
    end,

    %% Process connection exits.
    process_flag(trap_exit, true),

    %% Schedule periodic.
    schedule_periodic(),

    %% Schedule instrumentation.
    schedule_instrumentation(),

    %% Schedule distance metric.
    schedule_distance(),

    %% Schedule periodic connections.
    schedule_connections(),

    %% Schedule periodic retransmissionj.
    schedule_retransmit(),

    %% Schedule tree peers refresh.
    schedule_tree_refresh(),

    Name = partisan:node(),
    Actor = gen_actor(Name),

    %% Message clocks come from a lock-free counter rather than a vector clock
    %% held in this server's state — see `next_message_clock/1'.
    ok = init_message_clock(),

    %% We init the connections table and we become owners, if we crash the
    %% table will be destroyed.
    ok = partisan_peer_connections:init(),

    MStrategy = partisan_config:get(membership_strategy),

    {ok, Members, MState} =
        partisan_membership_strategy:init(MStrategy, Actor),

    %% Seed the lock-free membership snapshot before any reader starts.
    ok = partisan_membership:set(Members),

    {ok, #state{
        name = Name,
        node_spec = partisan:node_spec(),
        actor = Actor,
        pending = [],
        pre_interposition_funs = #{},
        interposition_funs = #{},
        post_interposition_funs = #{},
        distance_metrics = #{},
        sync_joins = #{},
        up_funs = #{},
        channel_up_funs = #{},
        down_funs = #{},
        channel_down_funs = #{},
        out_links = [],
        members = Members,
        membership_strategy = MStrategy,
        membership_strategy_state = MState
    }}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};
handle_call({on_up, Name, Fun, #{channel := Channel}}, _From, State) when
    is_atom(Channel)
->
    Funs0 = State#state.channel_up_funs,
    Funs = partisan_util:maps_append({Name, Channel}, Fun, Funs0),
    {reply, ok, State#state{channel_up_funs = Funs}};
handle_call({on_up, Name, Fun, _}, _From, State) ->
    Funs0 = State#state.up_funs,
    Funs = partisan_util:maps_append(Name, Fun, Funs0),
    {reply, ok, State#state{up_funs = Funs}};
handle_call({on_down, Name, Fun, #{channel := Channel}}, _From, State) when
    is_atom(Channel)
->
    Funs0 = State#state.channel_down_funs,
    Funs = partisan_util:maps_append({Name, Channel}, Fun, Funs0),
    {reply, ok, State#state{channel_down_funs = Funs}};
handle_call({on_down, Name, Fun, _}, _From, State) ->
    Funs0 = State#state.down_funs,
    Funs = partisan_util:maps_append(Name, Fun, Funs0),
    {reply, ok, State#state{down_funs = Funs}};
handle_call({add_pre_interposition_fun, Name, Fun}, _From, #state{} = State) ->
    Funs = maps:put(Name, Fun, State#state.pre_interposition_funs),
    S = State#state{pre_interposition_funs = Funs},
    ok = publish_interposition_flag(S),
    {reply, ok, S};
handle_call({remove_pre_interposition_fun, Name}, _From, #state{} = State) ->
    Funs = maps:remove(Name, State#state.pre_interposition_funs),
    S = State#state{pre_interposition_funs = Funs},
    ok = publish_interposition_flag(S),
    {reply, ok, S};
handle_call({add_interposition_fun, Name, Fun}, _From, #state{} = State) ->
    Funs = maps:put(Name, Fun, State#state.interposition_funs),
    S = State#state{interposition_funs = Funs},
    ok = publish_interposition_flag(S),
    {reply, ok, S};
handle_call({remove_interposition_fun, Name}, _From, #state{} = State) ->
    Funs = maps:remove(Name, State#state.interposition_funs),
    S = State#state{interposition_funs = Funs},
    ok = publish_interposition_flag(S),
    {reply, ok, S};
handle_call(get_interposition_funs, _From, #state{} = State) ->
    {reply, {ok, State#state.interposition_funs}, State};
handle_call(get_pre_interposition_funs, _From, #state{} = State) ->
    {reply, {ok, State#state.pre_interposition_funs}, State};
handle_call({add_post_interposition_fun, Name, Fun}, _From, #state{} = State) ->
    Funs = maps:put(Name, Fun, State#state.post_interposition_funs),
    S = State#state{post_interposition_funs = Funs},
    ok = publish_interposition_flag(S),
    {reply, ok, S};
handle_call({remove_post_interposition_fun, Name}, _From, #state{} = State) ->
    Funs = maps:remove(Name, State#state.post_interposition_funs),
    S = State#state{post_interposition_funs = Funs},
    ok = publish_interposition_flag(S),
    {reply, ok, S};
handle_call({update_members, _}, _, #state{leaving = true} = State) ->
    %% We are leaving so do nothing
    {reply, ok, State};
handle_call({update_members, Members}, _From, #state{} = State0) ->
    %% For compatibility with external membership services.
    %% Also called by partisan_peer_service_agent.
    Mod = State0#state.membership_strategy,
    MState = State0#state.membership_strategy_state,

    {Joiners, Leavers} = Mod:compare(Members, MState),

    %% Issue leaves.
    State1 = lists:foldl(
        fun(NodeSpec, S) -> internal_leave(NodeSpec, S) end,
        State0,
        Leavers
    ),
    %% Issue joins.
    State = lists:foldl(
        fun(NodeSpec, S) -> internal_join(NodeSpec, undefined, S) end,
        State1,
        Joiners
    ),

    %% Finally schedule the removal of connections
    %% We do this async because internal_leave will schedule the sending of
    %% membership update messages
    LeavingNodes = [Node || #{name := Node} <- Leavers],
    gen_server:cast(?MODULE, {kill_connections, LeavingNodes}),

    {reply, ok, State};
handle_call({leave, #{name := Name} = NodeSpec}, From, State0) ->
    %% Perform leave.
    State = internal_leave(NodeSpec, State0),

    case Name == State0#state.name of
        true ->
            %% Self leave
            gen_server:reply(From, ok),

            %% We need to stop (to cleanup all connections and state) or do it
            %% manually. However, we cannot do it straight away as
            %% internal_leave/2 has send some async messages we need to process
            %% (casts to ourself) so we cast ourselves a shutdown message.
            gen_server:cast(?MODULE, stop),
            {noreply, State#state{leaving = true}};
        false ->
            gen_server:cast(?MODULE, {kill_connections, [NodeSpec]}),
            {reply, ok, State}
    end;
handle_call({join, #{name := N}}, _From, #state{name = N} = State0) ->
    %% Ignoring self join.
    {reply, ok, State0};
handle_call({join, NodeSpec}, _From, State0) ->
    State = internal_join(NodeSpec, undefined, State0),
    {reply, ok, State};
handle_call({sync_join, #{name := N}}, _From, #state{name = N} = State0) ->
    %% Ignoring self join.
    {reply, ok, State0};
handle_call({sync_join, NodeSpec}, From, State0) ->
    ?LOG_DEBUG(#{
        description => "Starting synchronous join with peer",
        node => State0#state.name,
        peer => NodeSpec
    }),
    State = internal_join(NodeSpec, From, State0),
    {noreply, State};
handle_call({send_message, Node, Message}, _From, State) ->
    schedule_self_message_delivery(
        Node,
        Message,
        ?DEFAULT_PARTITION_KEY,
        State
    ),
    {reply, ok, State};
handle_call(
    {forward_message, Node, Clock, PartitionKey, ServerRef, Msg, Opts},
    From,
    State
) ->
    %% Run all interposition functions.
    DeliveryFun =
        fun() ->
            %% Fire pre-interposition functions.
            ok = ?FIRE_PRE_INTERPOSITIONS(
                forward_message, Node, Msg, State#state.pre_interposition_funs
            ),

            %% Once pre-interposition returns, then schedule for delivery.
            Cmd = {
                forward_message,
                From,
                Node,
                Clock,
                PartitionKey,
                ServerRef,
                Msg,
                Opts
            },
            gen_server:cast(?MODULE, Cmd)
        end,

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver,
            %% preserving serial order of messages.
            DeliveryFun();
        true ->
            %% Allow the system to proceed, and the message will be delivered
            %% once pre-interposition is done.
            spawn_link(DeliveryFun)
    end,

    {noreply, State};
handle_call({receive_message, Node, Channel, Msg}, From, State) ->
    DeliveryFun = fun() ->
        ok = ?FIRE_PRE_INTERPOSITIONS(
            receive_message, Node, Msg, State#state.pre_interposition_funs
        ),

        %% Once pre-interposition returns, then schedule for delivery.
        gen_server:cast(?MODULE, {receive_message, Node, Channel, From, Msg})
    end,

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver,
            %% preserving serial order of messages.
            DeliveryFun();
        true ->
            %% Allow the system to proceed, and the message will be delivered
            %% once pre-interposition is done.
            spawn_link(DeliveryFun)
    end,

    {noreply, State};
handle_call(members_for_orchestration, _From, State) ->
    {reply, {ok, State#state.members}, State};
handle_call(members, _From, State) ->
    Members = [Node || #{name := Node} <- State#state.members],
    {reply, {ok, Members}, State};
handle_call({member, Node}, _From, State) ->
    IsMember = lists:any(
        fun(#{name := X}) -> X =:= Node end,
        State#state.members
    ),
    {reply, IsMember, State};
handle_call(get_local_state, _From, State) ->
    {reply, {ok, State#state.membership_strategy_state}, State};
handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.

-spec handle_cast(term(), t()) ->
    {noreply, t()}
    | {stop, normal, t()}.

handle_cast(stop, State) ->
    %% We send ourselves this message when we left the cluster
    %% We stop to cleanup, supervisor will start us again,
    %% terminate/1 will kill all connections.
    {stop, normal, State};
handle_cast({kill_connections, Nodes}, State) ->
    ok = kill_connections(Nodes, State),
    {noreply, State};
handle_cast({receive_message, Node, Channel, From, Msg0}, State) ->
    %% Filter messages using interposition functions.
    Msg1 = ?FIRE_INTERPOSITIONS(
        receive_message, Node, Msg0, State#state.interposition_funs
    ),
    ok = ?FIRE_POST_INTERPOSITIONS(
        receive_message, Node, Msg0, Msg1, State#state.post_interposition_funs
    ),

    case Msg1 of
        undefined ->
            gen_server:reply(From, ok),
            {noreply, State};
        {'$delay', Msg} ->
            ?LOG_DEBUG(
                "Delaying receive_message due to interposition result: ~p",
                [Msg]
            ),
            gen_server:cast(
                ?MODULE, {receive_message, Node, Channel, From, Msg}
            ),
            {noreply, State};
        _ ->
            handle_message(Msg1, From, Channel, State)
    end;
handle_cast(
    {forward_message, From, Node, Clock, PartitionKey, ServerRef, Msg0, Opts},
    State
) ->
    Msg = ?FIRE_INTERPOSITIONS(
        forward_message, Node, Msg0, State#state.interposition_funs
    ),

    %% Are we using causality?
    CausalLabel = maps:get(causal_label, Opts, undefined),

    %% Use local information for message unless it's a causal message.
    {MsgClock, FullMessage} =
        case CausalLabel of
            undefined ->
                %% Generate a message clock or use the provided clock.
                LocalClock =
                    case Clock of
                        undefined ->
                            next_message_clock(State#state.name);
                        Clock ->
                            Clock
                    end,

                {LocalClock, Msg};
            CausalLabel ->
                case Clock of
                    undefined ->
                        %% First time through.
                        %% We don't have a clock yet,
                        %% get one using the causality backend.
                        {ok, LocalClock0, CausalMessage} =
                            partisan_causality_backend:emit(
                                CausalLabel, Node, ServerRef, Msg
                            ),

                        %% Wrap the clock with a scope.
                        %% TODO: Maybe do this wrapping inside of the causality
                        %% backend.
                        LocalClock = {CausalLabel, LocalClock0},

                        %% Return clock and wrapped message.
                        {LocalClock, CausalMessage};
                    _ ->
                        %% Retransmission.
                        %% Get the clock and message we used last time.
                        {ok, LocalClock, CausalMessage} =
                            partisan_causality_backend:reemit(
                                CausalLabel, Clock
                            ),

                        %% Return clock and wrapped message.
                        {LocalClock, CausalMessage}
                end
        end,

    case Msg of
        undefined ->
            %% Store for reliability, if necessary.
            case maps:get(ack, Opts, false) of
                true ->
                    %% Acknowledgements.
                    case maps:get(retransmission, Opts, false) of
                        true ->
                            RescheduleableMessage = {
                                forward_message,
                                From,
                                Node,
                                MsgClock,
                                PartitionKey,
                                ServerRef,
                                Msg0,
                                Opts
                            },
                            partisan_acknowledgement_backend:store(
                                MsgClock, RescheduleableMessage
                            );
                        false ->
                            ok
                    end;
                false ->
                    ok
            end,

            ok = ?FIRE_POST_INTERPOSITIONS(
                forward_message,
                Node,
                Msg0,
                FullMessage,
                State#state.post_interposition_funs
            ),

            ?LOG_DEBUG(
                "~p: Message ~p after send interposition is: ~p",
                [State#state.name, Msg0, FullMessage]
            ),

            case From of
                undefined ->
                    ok;
                _ ->
                    gen_server:reply(From, ok)
            end,

            {noreply, State};
        {'$delay', NewMessage} ->
            ?LOG_DEBUG(
                "Delaying receive_message due to interposition result: ~p",
                [NewMessage]
            ),
            gen_server:cast(
                ?MODULE,
                {
                    forward_message,
                    From,
                    Node,
                    Clock,
                    PartitionKey,
                    ServerRef,
                    NewMessage,
                    Opts
                }
            ),
            {noreply, State};
        _ ->
            %% Store for reliability, if necessary.
            Result =
                case maps:get(ack, Opts, false) of
                    false ->
                        %% Tracing.
                        WrappedMessage =
                            {forward_message, ServerRef, FullMessage},

                        ok = ?FIRE_POST_INTERPOSITIONS(
                            forward_message,
                            Node,
                            {forward_message, ServerRef, Msg0},
                            WrappedMessage,
                            State#state.post_interposition_funs
                        ),

                        %% Send message along.
                        do_send_message(
                            Node,
                            PartitionKey,
                            WrappedMessage,
                            Opts,
                            State
                        );
                    true ->
                        %% Tracing.
                        WrappedMessage = {
                            forward_message,
                            State#state.name,
                            MsgClock,
                            ServerRef,
                            FullMessage
                        },

                        ?LOG_DEBUG(
                            "should acknowledge message: ~p", [WrappedMessage]
                        ),

                        ok = ?FIRE_POST_INTERPOSITIONS(
                            forward_message,
                            Node,
                            {
                                forward_message,
                                State#state.name,
                                MsgClock,
                                ServerRef,
                                Msg0
                            },
                            WrappedMessage,
                            State#state.post_interposition_funs
                        ),

                        ?LOG_DEBUG(
                            "~p: Sending message ~p with clock: ~p",
                            [State#state.name, Msg, MsgClock]
                        ),
                        ?LOG_DEBUG(
                            "~p: Message after send interposition is: ~p",
                            [State#state.name, Msg]
                        ),

                        %% Acknowledgements.
                        case maps:get(retransmission, Opts, false) of
                            false ->
                                RescheduleableMessage = {
                                    forward_message,
                                    From,
                                    Node,
                                    MsgClock,
                                    PartitionKey,
                                    ServerRef,
                                    Msg0,
                                    Opts
                                },
                                partisan_acknowledgement_backend:store(
                                    MsgClock, RescheduleableMessage
                                );
                            true ->
                                ok
                        end,

                        %% Send message along.
                        do_send_message(
                            Node,
                            PartitionKey,
                            WrappedMessage,
                            Opts,
                            State
                        )
                end,

            case From of
                undefined ->
                    ok;
                _ ->
                    gen_server:reply(From, Result)
            end,

            {noreply, State}
    end;
handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),
    {noreply, State}.

-spec handle_info(info(), t()) -> {noreply, t()}.

handle_info(tree_refresh, State) ->
    %% Get lazily computed outlinks.
    OutLinks = retrieve_outlinks(5000),

    %% Reschedule.
    schedule_tree_refresh(),

    {noreply, State#state{out_links = OutLinks}};
handle_info(distance, State0) ->
    %% Establish any new connections.
    State = establish_connections(State0),

    %% Record time.
    Time = erlang:timestamp(),

    %% Send distance requests.
    ok = lists:foreach(
        fun(Peer) ->
            schedule_self_message_delivery(
                Peer,
                {ping, State0#state.node_spec, Peer, Time},
                ?DEFAULT_PARTITION_KEY,
                State,
                #{channel => ?MEMBERSHIP_CHANNEL}
            )
        end,
        State#state.members
    ),

    schedule_distance(),

    {noreply, State};
handle_info(instrumentation, State) ->
    MessageQueueLen = process_info(self(), message_queue_len),
    ?LOG_DEBUG("message_queue_len: ~p", [MessageQueueLen]),
    schedule_instrumentation(),
    {noreply, State};
handle_info(periodic, #state{} = State0) ->
    #state{
        membership_strategy = MStrategy,
        membership_strategy_state = MState0
    } = State0,

    {ok, Members, OutgoingMessages, MState} =
        partisan_membership_strategy:periodic(MStrategy, MState0),

    %% Send outgoing messages.
    ok = lists:foreach(
        fun({Node, Message}) ->
            schedule_self_message_delivery(
                Node,
                Message,
                ?DEFAULT_PARTITION_KEY,
                State0,
                #{channel => ?MEMBERSHIP_CHANNEL}
            )
        end,
        OutgoingMessages
    ),

    State1 = State0#state{
        members = Members,
        membership_strategy_state = MState
    },

    %% Establish any new connections.
    State = establish_connections(State1),

    schedule_periodic(),

    {noreply, State};
handle_info(retransmit, State) ->
    RetransmitFun = fun(
        {_,
            {forward_message, From, Node, Clock, PartitionKey, ServerRef,
                Message, Options}}
    ) ->
        ?LOG_DEBUG(
            "~p no acknowledgement yet, "
            "restranmitting message ~p with clock ~p to ~p",
            [State#state.name, Message, Clock, Node]
        ),

        ok = ?FIRE_PRE_INTERPOSITIONS(
            forward_message, Node, Message, State#state.pre_interposition_funs
        ),

        %% Schedule message for redelivery.
        RetryOptions = Options#{retransmission => true},

        gen_server:cast(
            ?MODULE,
            {
                forward_message,
                From,
                Node,
                Clock,
                PartitionKey,
                ServerRef,
                Message,
                RetryOptions
            }
        )
    end,

    {ok, Outstanding} = partisan_acknowledgement_backend:outstanding(),

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver,
            %% preserving serial order of messages.
            lists:foreach(RetransmitFun, Outstanding);
        true ->
            %% Allow the system to proceed, and the message will be delivered
            %% once pre-interposition is done.
            lists:foreach(
                fun(OutstandingMessage) ->
                    spawn_link(fun() ->
                        RetransmitFun(OutstandingMessage)
                    end)
                end,
                Outstanding
            )
    end,

    %% Reschedule retransmission.
    schedule_retransmit(),

    {noreply, State};
handle_info(connections, State0) ->
    %% TODO #244 move connection establishing to a helper process as these tasks
    %% interleave with message forwarding. Also consider having a process per
    %% channel or channel connection.
    State1 = establish_connections(State0),

    %% Advance sync_join's if we have enough open connections to remote host.
    State = maybe_reply_sync_joins(State1),

    schedule_connections(),

    {noreply, State};
handle_info({'EXIT', Pid, Reason}, State0) ->
    ?LOG_DEBUG(#{
        description => "Connection closed",
        reason => Reason
    }),

    %% A connection has closed, prune it from the connections table
    try partisan_peer_connections:prune(Pid, Reason) of
        {Info, [Connection]} ->
            NodeSpec = partisan_peer_connections:node_spec(Info),
            #{name := Node} = NodeSpec,
            Channel = partisan_peer_connections:channel(Connection),

            State1 =
                case partisan_peer_connections:count(Node, Channel) of
                    0 ->
                        channel_down(Node, Channel, State0);
                    _ ->
                        State0
                end,

            State =
                case partisan_peer_connections:count(Info) of
                    0 ->
                        %% This was the last connection so the node is down.
                        %% We notify all subscribers.
                        ok = down(NodeSpec, State1),
                        %% If still a member we need to add it to pending,
                        %% so that we can reconnect and then compute the
                        %% on_up signal.
                        maybe_append_pending(NodeSpec, State1);
                    _ ->
                        State1
                end,

            {noreply, State}
    catch
        error:badarg ->
            %% Weird, connection pid did not exist
            {noreply, State0}
    end;
handle_info(
    {connected, NodeSpec, Channel, _Tag, RemoteState}, State0
) ->
    #state{
        pending = Pending0,
        members = Members0,
        membership_strategy = MStrategy,
        membership_strategy_state = MState0
    } = State0,

    ?LOG_DEBUG(#{
        description => "Node connected!",
        node => NodeSpec,
        pending => Pending0,
        membership => State0#state.members
    }),

    State1 =
        case lists:member(NodeSpec, Pending0) of
            true ->
                %% Move out of pending.
                Pending = Pending0 -- [NodeSpec],

                %% Update membership by joining with remote membership.
                {ok, Members, OutgoingMessages, MState} =
                    partisan_membership_strategy:join(
                        MStrategy, NodeSpec, RemoteState, MState0
                    ),

                %% Gossip the new membership.
                lists:foreach(
                    fun({Node, Message}) ->
                        schedule_self_message_delivery(
                            Node,
                            Message,
                            ?DEFAULT_PARTITION_KEY,
                            State0,
                            #{channel => ?MEMBERSHIP_CHANNEL}
                        )
                    end,
                    OutgoingMessages
                ),

                %% Notify event handlers
                ok =
                    case Members == Members0 of
                        true ->
                            ok;
                        false ->
                            ok = partisan_membership:set(Members),
                            ok = partisan_membership:notify(Members)
                    end,

                %% notify subscribers
                up(NodeSpec, State0),

                State0#state{
                    pending = Pending,
                    members = Members,
                    membership_strategy_state = MState
                };
            false ->
                State0
        end,

    %% Notify the channel subscribers. Unlike the node event above this does
    %% not depend on the peer being pending: an extra channel to a peer we are
    %% already connected to is still a channel coming up.
    #{name := Node} = NodeSpec,
    State2 = channel_up(Node, Channel, State1),

    %% Notify for sync join.
    State = maybe_reply_sync_joins(State2),

    {noreply, State};
handle_info(Msg, State) ->
    handle_message(Msg, undefined, ?DEFAULT_CHANNEL, State).

-spec terminate(term(), t()) -> term().

terminate(_Reason, #state{}) ->
    ok = partisan_peer_connections:kill_all().

-spec code_change(term() | {down, term()}, t(), term()) ->
    {ok, t()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
gen_actor(Name) ->
    Unique = erlang:unique_integer([positive]),
    TS = integer_to_list(Unique),
    Term = atom_to_list(Name) ++ TS,
    crypto:hash(sha, Term).

%% -----------------------------------------------------------------------------
%% @private
%% @doc Establish any new connections and prunes no longer valid nodes.
%% @end
%% -----------------------------------------------------------------------------
-spec establish_connections(t()) -> t().

establish_connections(State) ->
    Pending = State#state.pending,
    Members = State#state.members,

    %% @REVIEW Shouldn't this be a set union? Also what about IP Changes?
    %% Compute list of nodes that should be connected.
    NodesSpecs = Members ++ Pending,

    %% Reconnect disconnected members and members waiting to join.
    LoL = lists:foldl(
        fun
            (#{name := Name}, Acc) when Name == State#state.name ->
                %% We exclude ourselves
                Acc;
            (#{name := _} = Node, Acc) ->
                %% This function call returns the a list of stale
                %% NodeSpecs (nodes that have an invalid IP address because we
                %% already have a connection to NodeSpec.node on another IP
                %% address).
                %% We then remove those NodeSpecs from the membership set and
                %% update ourselves without the need for sending any leave/join
                %% gossip.
                {ok, StaleSpecs} = partisan_peer_service_manager:connect(
                    Node, #{prune => true}
                ),
                %% We will call lists:append at the end
                [StaleSpecs | Acc]
        end,
        [],
        NodesSpecs
    ),

    prune(lists:append(LoL), State).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
prune([], State) ->
    State;
prune(L, State) ->
    Mod = State#state.membership_strategy,
    MState0 = State#state.membership_strategy_state,
    {ok, Members, MState} = partisan_membership_strategy:prune(Mod, L, MState0),
    State#state{members = Members, membership_strategy_state = MState}.

%% @private
kill_connections(Nodes, State) ->
    Fun = fun(Node) ->
        ok = down(Node, State)
    end,
    partisan_peer_service_manager:disconnect(Nodes, Fun).

%% @private
handle_message(
    {ping, SrcNode, DestNode, SrcTime}, From, _Channel, #state{} = State0
) ->
    %% Establish any new connections.
    State = establish_connections(State0),

    %% Send ping response.
    schedule_self_message_delivery(
        SrcNode,
        {pong, SrcNode, DestNode, SrcTime},
        ?DEFAULT_PARTITION_KEY,
        State,
        %% we coerce channel, regardless of _Channel
        #{channel => ?MEMBERSHIP_CHANNEL}
    ),

    maybe_reply(From, ok),

    {noreply, State};
handle_message(
    {pong, SrcNode, DestNode, SrcTime}, From, _Channel, #state{} = State
) ->
    %% Compute difference.
    DistanceMetrics0 = State#state.distance_metrics,
    ArrivalTime = erlang:timestamp(),
    Difference = timer:now_diff(ArrivalTime, SrcTime),

    ?LOG_TRACE(
        "Updating distance metric for node ~p => ~p communication: ~p",
        [SrcNode, DestNode, Difference]
    ),

    %% Update differences.
    DistanceMetrics = maps:put(DestNode, Difference, DistanceMetrics0),

    %% Store in pdict.
    put(distance_metrics, DistanceMetrics),

    maybe_reply(From, ok),

    {noreply, State#state{distance_metrics = DistanceMetrics}};
handle_message(
    {membership_strategy, ProtocolMsg}, From, _Channel, #state{} = State0
) ->
    #state{
        members = Members0,
        membership_strategy = MStrategy,
        membership_strategy_state = MState0
    } = State0,

    %% Process the protocol message.
    {ok, Members, OutgoingMessages, MState} =
        partisan_membership_strategy:handle_message(
            MStrategy, ProtocolMsg, MState0
        ),

    %% Update users of the peer service.
    case Members == Members0 of
        true ->
            ok;
        false ->
            ok = partisan_membership:set(Members),
            ok = partisan_membership:notify(Members)
    end,

    %% Send outgoing messages.
    lists:foreach(
        fun({Node, Message}) ->
            schedule_self_message_delivery(
                Node,
                Message,
                ?DEFAULT_PARTITION_KEY,
                State0,
                %% we coerce channel, regardless of _Channel
                #{channel => ?MEMBERSHIP_CHANNEL}
            )
        end,
        OutgoingMessages
    ),

    State1 = State0#state{
        members = Members,
        membership_strategy_state = MState
    },

    {Pending, LeavingNodes} = pending_leavers(State1),
    State2 = State1#state{pending = Pending},

    %% Establish any new connections.
    State = establish_connections(State2),

    gen_server:cast(?MODULE, {kill_connections, LeavingNodes}),

    case lists:member(State#state.node_spec, Members) of
        false ->
            ?LOG_INFO(#{
                description => "Shutting down: membership doesn't contain us",
                reason => "We've been removed from the cluster."
            }),

            ?LOG_DEBUG(#{
                membership => Members
            }),

            %% Shutdown if we've been removed from the cluster.
            {stop, normal, State};
        true ->
            maybe_reply(From, ok),
            {noreply, State}
    end;
%% Causal and acknowledged messages.
handle_message(
    {forward_message, SrcNode, MsgClock, ServerRef, Msg},
    From,
    Channel,
    #state{} = State
) when is_tuple(Msg) andalso element(1, Msg) == causal ->
    {causal, Label, _, _, _, _, _} = Msg,

    %% Send message acknowledgement.
    send_acknowledgement(SrcNode, Channel, MsgClock, State),

    case partisan_causality_backend:is_causal_message(Msg) of
        true ->
            partisan_causality_backend:receive_message(Label, Msg);
        false ->
            %% Attempt message delivery.
            partisan_peer_service_manager:deliver(ServerRef, Msg)
    end,

    maybe_reply(From, ok),

    {noreply, State};
%% Acknowledged messages.
handle_message(
    {forward_message, SrcNode, MsgClock, ServerRef, Msg},
    From,
    Channel,
    #state{} = State
) ->
    %% Send message acknowledgement.
    send_acknowledgement(SrcNode, Channel, MsgClock, State),

    partisan_peer_service_manager:deliver(ServerRef, Msg),

    maybe_reply(From, ok),

    {noreply, State};
%% Causal messages.
handle_message(
    {forward_message, ServerRef, {causal, Label, _, _, _, _, _} = Msg},
    From,
    _Channel,
    State
) ->
    case partisan_causality_backend:is_causal_message(Msg) of
        true ->
            partisan_causality_backend:receive_message(Label, Msg);
        false ->
            %% Attempt message delivery.
            partisan_peer_service_manager:deliver(ServerRef, Msg)
    end,

    maybe_reply(From, ok),

    {noreply, State};
%% Best-effort messages.
%% TODO: Maybe remove me.
handle_message(
    {forward_message, ServerRef, Msg},
    From,
    _Channel,
    State
) ->
    partisan_peer_service_manager:deliver(ServerRef, Msg),
    maybe_reply(From, ok),
    {noreply, State};
handle_message(
    {ack, MsgClock},
    From,
    _Channel,
    State
) ->
    partisan_acknowledgement_backend:ack(MsgClock),
    maybe_reply(From, ok),
    {noreply, State};
handle_message(
    Msg,
    From,
    _Channel,
    State
) ->
    %% Every other clause answers `From'; this one did not. The caller is the
    %% connection process, which invokes `receive_message/3' synchronously with
    %% `infinity' (`partisan_peer_service_server:216'), so an unanswered call
    %% left it blocked forever, never re-arming its `{active, once}' socket --
    %% one unrecognised envelope permanently killed that peer link, silently.
    %%
    %% This is also what constrains protocol evolution: any envelope a peer on
    %% an older release does not recognise takes this path. Covered by
    %% `partisan_inbound_envelope_test'.
    ?LOG_WARNING(#{description => "Unhandled message", message => Msg}),
    maybe_reply(From, ok),
    {noreply, State}.

%% @private
schedule_distance() ->
    case partisan_config:get(distance_enabled, false) of
        true ->
            DistanceInterval = partisan_config:get(distance_interval, 10000),
            erlang:send_after(DistanceInterval, ?MODULE, distance);
        false ->
            ok
    end.

%% @private
schedule_instrumentation() ->
    case partisan_config:get(instrumentation, false) of
        true ->
            erlang:send_after(1000, ?MODULE, instrumentation);
        _ ->
            ok
    end.

%% @private
schedule_periodic() ->
    case partisan_config:get(periodic_enabled, false) of
        true ->
            Time = partisan_config:get(periodic_interval, ?PERIODIC_INTERVAL),
            erlang:send_after(Time, ?MODULE, periodic);
        false ->
            ok
    end.

%% @private
schedule_retransmit() ->
    Time = partisan_config:get(retransmit_interval, 1000),
    erlang:send_after(Time, ?MODULE, retransmit).

%% @private
schedule_connections() ->
    Time = partisan_config:get(connection_interval, 1000),
    erlang:send_after(Time, ?MODULE, connections).

%% @private
%% Publishes whether any interposition function is registered.
%%
%% The functions themselves live in this server's state, so testing for them
%% would need a call — exactly the round trip the acknowledged fast path exists
%% to avoid. A boolean in `persistent_term' is free to read from any process.
%% Registration only happens from test harnesses and the trace orchestrator, so
%% the write is rare and `persistent_term''s global cost is irrelevant.
publish_interposition_flag(#state{} = State) ->
    Any =
        map_size(State#state.pre_interposition_funs) > 0 orelse
            map_size(State#state.interposition_funs) > 0 orelse
            map_size(State#state.post_interposition_funs) > 0,
    persistent_term:put(?INTERPOSITION_KEY, Any).

%% @private
has_interposition_funs() ->
    persistent_term:get(?INTERPOSITION_KEY, false).

%% @private
%% Sends an acknowledged message without going through this server.
%%
%% Returns `ok', or `{error, Reason}' to make the caller fall back to the
%% serialised path.
%%
%% The connection is resolved **first**, before a clock is drawn or anything is
%% recorded as outstanding. That ordering is the safety property: if there is no
%% usable connection this function has had no side effects at all, so the
%% serialised path can handle the message exactly as it does today — including
%% its relay/broadcast fallbacks — with no risk of the message being both
%% recorded here and sent again there under a second clock.
%%
%% Once a connection is in hand the order matches the serialised path: record
%% the outstanding message, then put it on the wire. A cast that fails after
%% the record is left for the retransmission timer, which is what happens on
%% the serialised path too.
%%
%% Note this path does not fire interposition functions — consistent with every
%% other fast-path send (see the `FastForward' selection in
%% `forward_message/4'); interposition applies to the serialised path.
fast_forward_acked(Node, PartitionKey, ServerRef, Message, Clock, Opts) ->
    Channel = maps:get(channel, Opts, ?DEFAULT_CHANNEL),

    case partisan_peer_connections:dispatch_pid(Node, Channel, PartitionKey) of
        {ok, Pid} ->
            Myself = partisan:node(),

            %% A retransmission carries the clock it was first sent with, so
            %% the peer's acknowledgement still matches the recorded entry.
            MsgClock =
                case Clock of
                    undefined -> next_message_clock(Myself);
                    _ -> Clock
                end,

            WrappedMessage =
                {forward_message, Myself, MsgClock, ServerRef, Message},

            case maps:get(retransmission, Opts, false) of
                false ->
                    Rescheduleable = {
                        forward_message,
                        undefined,
                        Node,
                        MsgClock,
                        PartitionKey,
                        ServerRef,
                        Message,
                        Opts
                    },
                    ok = partisan_acknowledgement_backend:store(
                        MsgClock, Rescheduleable
                    );
                true ->
                    ok
            end,

            Data = partisan_util:encode(
                WrappedMessage,
                partisan_util:channel_encode_opts(
                    partisan_config:channel_opts(Channel)
                )
            ),
            partisan_peer_connections:cast_encoded(Pid, Data, Channel);
        {error, _} = Error ->
            Error
    end.

%% @private
%% Creates the message-clock counter, once per node.
%%
%% The clock is never compared or merged — it is an opaque identity token, used
%% as the key of the outstanding-message table and echoed back in `{ack, _}' —
%% so a single monotonic counter is sufficient. Keeping it lock-free rather than
%% in this server's state is what lets an acknowledged send skip this process
%% entirely: the clock is obtainable from any caller.
%%
%% The counter deliberately survives a restart of this server (it lives in
%% `persistent_term', not in state). Resetting it would let a fresh clock
%% collide with an entry still outstanding in the acknowledgement table, which
%% is owned by a different process and also survives.
init_message_clock() ->
    case persistent_term:get(?MSG_CLOCK_KEY, undefined) of
        undefined ->
            Ref = atomics:new(1, [{signed, false}]),
            ok = persistent_term:put(?MSG_CLOCK_KEY, Ref);
        _ ->
            ok
    end.

%% @private
%% Returns the next message clock. The shape is unchanged from the vector-clock
%% implementation — `{undefined, [{Node, Counter}]}' — because peers echo this
%% term back verbatim in their acknowledgements, so changing it would break
%% acknowledgement matching against an un-upgraded node.
next_message_clock(Name) ->
    Ref = persistent_term:get(?MSG_CLOCK_KEY),
    Counter = atomics:add_get(Ref, 1, 1),
    {undefined, [{Name, Counter}]}.

%% @private
do_send_message(Node, PartitionKey, Message, Options, State) ->
    %% Find a connection for the remote node, if we have one.
    Channel = maps:get(channel, Options, ?DEFAULT_CHANNEL),
    Res = partisan_peer_connections:dispatch_pid(Node, Channel, PartitionKey),

    case Res of
        {ok, Pid} ->
            %% Encode in this process rather than in the connection process —
            %% see the `send_encoded' clause in `partisan_peer_service_client'.
            ChannelOpts = partisan_config:channel_opts(Channel),
            EncodeOpts = partisan_util:channel_encode_opts(ChannelOpts),
            Data = partisan_util:encode(Message, EncodeOpts),
            partisan_peer_connections:cast_encoded(Pid, Data, Channel);
        {error, Reason} ->
            %% We were connected, but we're not anymore, or never connected
            case partisan_config:get(broadcast, false) of
                true ->
                    case maps:get(transitive, Options, false) of
                        true ->
                            ?LOG_DEBUG(
                                "Performing tree forward from node ~p "
                                "to node ~p and message: ~p",
                                [State#state.name, Node, Message]
                            ),
                            TTL = partisan_config:get(relay_ttl, ?RELAY_TTL),
                            do_tree_forward(
                                Node,
                                PartitionKey,
                                Message,
                                Options,
                                TTL,
                                State
                            );
                        false ->
                            ok
                    end;
                false ->
                    case Reason of
                        disconnected ->
                            ?LOG_TRACE(
                                "Node ~p was connected, "
                                "but is now disconnected!",
                                [Node]
                            ),
                            {error, disconnected};
                        not_yet_connected ->
                            ?LOG_TRACE(
                                "Node ~p not yet connected!",
                                [Node]
                            ),
                            {error, not_yet_connected}
                    end
            end
    end.

%% @private
up(NodeOrSpec, State) ->
    apply_node_funs(NodeOrSpec, State#state.up_funs).

%% @private
down(NodeOrSpec, State) ->
    apply_node_funs(NodeOrSpec, State#state.down_funs).

%% @private
%% @doc Announces `Channel' to `Node' as up, unless we already have. Returns
%% the updated state.
channel_up(Node, Channel, #state{up_channels = Up} = State) ->
    case sets:is_element({Node, Channel}, Up) of
        true ->
            State;
        false ->
            ok = apply_channel_funs(
                Node, Channel, State#state.channel_up_funs
            ),
            State#state{up_channels = sets:add_element({Node, Channel}, Up)}
    end.

%% @private
%% @doc The counterpart of `channel_up/3', called once `Channel' has no
%% connections left to `Node'.
channel_down(Node, Channel, #state{up_channels = Up} = State) ->
    case sets:is_element({Node, Channel}, Up) of
        false ->
            State;
        true ->
            ok = apply_channel_funs(
                Node, Channel, State#state.channel_down_funs
            ),
            State#state{up_channels = sets:del_element({Node, Channel}, Up)}
    end.

%% @private
%% @doc Callbacks registered for `Node' plus those registered for every node.
apply_node_funs(#{name := Node}, Subs) ->
    apply_node_funs(Node, Subs);
apply_node_funs(Node, Subs) when is_atom(Node) ->
    ?LOG_DEBUG(#{
        description => "Node status change notification",
        node => Node,
        funs => Subs
    }),

    Funs = lists:append(
        %% Notify functions matching the wildcard '_'
        maps:get('_', Subs, []),
        %% Notify functions matching Node
        maps:get(Node, Subs, [])
    ),

    apply_event_funs(Funs, Node, undefined).

%% @private
%% @doc Channel subscriptions are keyed by `{Node, Channel}', either half of
%% which may be the `'_'' wildcard, so all four combinations match. Looking
%% these up by node name alone finds nothing.
apply_channel_funs(Node, Channel, Subs) when is_atom(Node) ->
    ?LOG_DEBUG(#{
        description => "Channel status change notification",
        node => Node,
        channel => Channel,
        funs => Subs
    }),

    Funs = lists:append([
        maps:get({'_', '_'}, Subs, []),
        maps:get({'_', Channel}, Subs, []),
        maps:get({Node, '_'}, Subs, []),
        maps:get({Node, Channel}, Subs, [])
    ]),

    apply_event_funs(Funs, Node, Channel).

%% @private
%% @doc A subscriber takes no argument, the node, or the node and channel —
%% `partisan_monitor' registers the arity-2 form for channel events. A
%% callback is not allowed to take the manager down with it.
apply_event_funs(Funs, Node, Channel) ->
    _ = [
        case erlang:fun_info(F, arity) of
            {arity, 0} -> catch F();
            {arity, 1} -> catch F(Node);
            {arity, 2} -> catch F(Node, Channel)
        end
     || F <- Funs
    ],
    ok.

%% @private
pending_leavers(#state{} = State) ->
    %% @REVIEW Shouldn't this be a set union? Also what about IP Changes?

    Members = ?SET_FROM_LIST(State#state.members),
    Pending0 = ?SET_FROM_LIST(State#state.pending),
    Connected = ?SET_FROM_LIST(partisan_peer_connections:node_specs()),

    %% Connected nodes that are no longer members
    Leavers = sets:to_list(sets:subtract(Connected, Members)),

    %% Disconnected nodes that are members
    Pending = sets:to_list(
        sets:subtract(sets:union(Members, Pending0), Connected)
    ),

    {Pending, Leavers}.

%% @private
internal_leave(#{name := Name} = Node, State0) ->
    #state{
        membership_strategy = MStrategy,
        membership_strategy_state = MState0
    } = State0,

    ?LOG_DEBUG(#{
        description => "Processing leave",
        leaving_node => Name
    }),

    {ok, Members, OutgoingMessages, MState} =
        partisan_membership_strategy:leave(MStrategy, Node, MState0),

    ?LOG_DEBUG(#{
        description => "Processing leave",
        leaving_node => Name,
        outgoing_messages => OutgoingMessages,
        new_membership => Members
    }),

    State1 = State0#state{
        members = Members,
        membership_strategy_state = MState
    },

    %% Establish any new connections.
    %% This will also prune no longer valid node_specs, setting the new
    %% membership in State
    State = establish_connections(State1),

    %% Transmit outgoing messages.
    lists:foreach(
        fun({#{name := Peername}, Message}) ->
            schedule_self_message_delivery(
                Peername,
                Message,
                ?DEFAULT_PARTITION_KEY,
                State,
                #{channel => ?MEMBERSHIP_CHANNEL}
            )
        end,
        OutgoingMessages
    ),

    ok = partisan_membership:set(State#state.members),
    ok = partisan_membership:notify(State#state.members),

    State.

%% @private
internal_join(#{name := Node} = NodeSpec, From, #state{} = State0) ->
    ok = partisan_util:maybe_connect_disterl(Node),

    %% Sleep before connecting, to avoid a rush on connections.
    avoid_rush(),

    %% Add to list of pending connections.
    Pending0 = State0#state.pending,
    Pending = Pending0 ++ [NodeSpec],

    State = maybe_add_sync_join(
        NodeSpec, From, State0#state{pending = Pending}
    ),

    %% Establish any new connections.
    establish_connections(State).

%% @private
maybe_add_sync_join(_, undefined, State) ->
    State;
maybe_add_sync_join(NodeSpec, From, State) ->
    SyncJoins0 = State#state.sync_joins,
    SyncJoins =
        try
            Fun = fun(Value) -> sets:add_element(From, Value) end,
            maps:update_with(NodeSpec, Fun, SyncJoins0)
        catch
            error:{badkey, NodeSpec} ->
                maps:put(
                    NodeSpec,
                    sets:from_list([From], [{version, 2}]),
                    SyncJoins0
                )
        end,

    State#state{sync_joins = SyncJoins}.

maybe_reply_sync_joins(State) ->
    Fun = fun(#{name := Node} = NodeSpec, Set, Acc) ->
        case partisan:is_fully_connected(NodeSpec) of
            true ->
                ?LOG_DEBUG("Node ~p is now fully connected.", [Node]),
                [
                    gen_server:reply(FromPid, ok)
                 || FromPid <- sets:to_list(Set)
                ],
                %% We remove the entry from map
                Acc;
            false ->
                %% We keep the Node in the new map
                maps:put(NodeSpec, Set, Acc)
        end
    end,

    SyncJoins = maps:fold(Fun, maps:new(), State#state.sync_joins),

    State#state{sync_joins = SyncJoins}.

%% @private
rand_bits(BitLen) ->
    Bytes = (BitLen + 7) div 8,
    <<Result:BitLen/bits, _/bits>> = crypto:strong_rand_bytes(Bytes),
    Result.

%% @private
avoid_rush() ->
    %% Sleep before connecting, to avoid a rush on connections.
    Jitter = partisan_config:get(connection_jitter, ?CONNECTION_JITTER),
    case partisan_config:get(jitter, false) of
        true ->
            timer:sleep(rand:uniform(Jitter));
        false ->
            timer:sleep(Jitter)
    end.

%% @private
do_tree_forward(Node, PartitionKey, Message, Opts, TTL, State) ->
    ?LOG_TRACE(
        "Attempting to forward message ~p from ~p to ~p.",
        [Message, State#state.name, Node]
    ),

    %% Preempt with user-supplied outlinks.
    UserOutLinks = maps:get(out_links, Opts, undefined),

    OutLinks =
        case UserOutLinks of
            undefined ->
                try retrieve_outlinks() of
                    Value ->
                        Value
                catch
                    _:Reason ->
                        ?LOG_ERROR(#{
                            description => "Outlinks retrieval failed",
                            reason => Reason
                        }),
                        []
                end;
            OL ->
                OL -- [State#state.name]
        end,

    %% Send messages, but don't attempt to forward again, if we aren't
    %% connected.
    _ = lists:foreach(
        fun(Peer) ->
            ?LOG_TRACE(
                "Forwarding relay message ~p to node ~p "
                "for node ~p from node ~p",
                [Message, Peer, Node, State#state.name]
            ),

            RelayMessage = {relay_message, Node, Message, TTL - 1},

            schedule_self_message_delivery(
                Peer,
                RelayMessage,
                PartitionKey,
                State,
                maps:without([transitive], Opts)
            )
        end,
        OutLinks
    ),
    ok.

%% @private
retrieve_outlinks() ->
    retrieve_outlinks(1000).

%% @private
retrieve_outlinks(Timeout) when is_integer(Timeout) ->
    ?LOG_TRACE(#{description => "About to retrieve outlinks..."}),

    Root = partisan:node(),

    OutLinks =
        try
            {EagerPeers, _LazyPeers} =
                partisan_plumtree_broadcast:debug_get_peers(
                    Root, Root, Timeout
                ),
            ordsets:to_list(EagerPeers) -- [Root]
        catch
            _:Reason ->
                ?LOG_INFO(#{
                    description => "Request to get outlinks failed",
                    reason => Reason
                }),
                []
        end,

    ?LOG_TRACE("Finished getting outlinks: ~p", [OutLinks]),

    OutLinks.

%% @private
schedule_tree_refresh() ->
    case partisan_config:get(broadcast, false) of
        true ->
            Period = partisan_config:get(tree_refresh, 1000),
            erlang:send_after(Period, ?MODULE, tree_refresh);
        false ->
            ok
    end.

%% @private
schedule_self_message_delivery(Node, Message, PartitionKey, State) ->
    schedule_self_message_delivery(Node, Message, PartitionKey, State, #{}).

%% @private
schedule_self_message_delivery(Node, Message, PartitionKey, State, Options) ->
    Funs = State#state.pre_interposition_funs,

    DeliveryFun = fun() ->
        ok = ?FIRE_PRE_INTERPOSITIONS(forward_message, Node, Message, Funs),

        %% Once pre-interposition returns, then schedule for delivery.
        gen_server:cast(?MODULE, {
            forward_message,
            % from
            undefined,
            Node,
            % clock
            undefined,
            PartitionKey,
            % ServerRef
            ?MODULE,
            Message,
            Options
        })
    end,

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver,
            %% preserving serial order of messages.
            DeliveryFun();
        true ->
            %% Allow the system to proceed, and the message will be delivered
            %% once pre-interposition is done.
            spawn_link(DeliveryFun)
    end,

    ok.

%% @private
send_acknowledgement(Node, Channel, MsgClock, State) ->
    %% Generate message.
    Message = {ack, MsgClock},

    %% Send on the default channel.
    schedule_self_message_delivery(
        Node,
        Message,
        ?DEFAULT_PARTITION_KEY,
        State,
        #{channel => Channel}
    ).

%% @private
maybe_reply(From, Response) ->
    case From of
        undefined ->
            ok;
        _ ->
            gen_server:reply(From, Response)
    end.

%% @private
maybe_append_pending(NodeSpec, #state{} = State) ->
    Pending0 = State#state.pending,
    Members = State#state.members,

    Pending =
        case lists:member(NodeSpec, Members) of
            true ->
                Pending0 ++ [NodeSpec];
            false ->
                Pending0
        end,
    State#state{pending = Pending}.
