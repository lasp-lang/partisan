%% -----------------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
%% Copyright (c) 2022 Alejandro M. Ramallo. All Rights Reserved.
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
%% -----------------------------------------------------------------------------

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_peer_service_manager).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan_logger.hrl").
-include("partisan.hrl").

-type server_ref() ::
    partisan:any_pid()
    | partisan:any_name()
    | partisan_remote_ref:encoded_pid()
    | partisan_remote_ref:encoded_name()
    | {RegName :: atom(), node()}
    | {global, RegName :: atom()}
    | {via, module(), ViaName :: atom()}.

-type forward_opts() ::
    #{
        ack => boolean(),
        retransmission => boolean(),
        causal_label => atom(),
        channel => partisan:channel(),
        clock => any(),
        partition_key => non_neg_integer(),
        transitive => boolean(),
        %% To allow erlang opts
        atom() => any()
    }
    | [
        {ack, boolean()}
        | {causal_label, atom()}
        | {channel, partisan:channel()}
        | {clock, any()}
        | {partition_key, non_neg_integer()}
        | {transitive, boolean()}
        %% To allow erlang opts
        | {atom(), any()}
    ].

-type connect_opts() :: #{prune => boolean(), async => boolean()}.
-type attempt() :: {pid(), partisan:channel(), partisan:listen_addr()}.
-type partitions() :: [{reference(), partisan:node_spec()}].
-type on_event_fun() ::
    fun(() -> ok)
    | fun((node()) -> ok)
    | fun((node(), partisan:channel()) -> ok).

%% The result of a forward. **`ok' is not the only outcome**: forwarding is
%% best-effort, and a manager reports failure rather than raising when it cannot
%% reach the target. The reasons Partisan's own managers produce are
%% `disconnected' and `not_yet_connected' (no usable connection to the peer on
%% the requested channel — see `partisan_peer_connections:dispatch_pid/3'),
%% `notalive' (the connection process is gone), and `partitioned'
%% (`partisan_hyparview_peer_service_manager' only, when the target is inside an
%% injected partition). `Reason' is deliberately left open, because a
%% third-party manager implementing this behaviour may have its own.
%%
%% The error half of this type is not decorative: implementations do return
%% these, and a caller that treats forwarding as infallible silently drops
%% messages.
-type forward_result() :: ok | {error, Reason :: any()}.

-export_type([attempt/0]).
-export_type([connect_opts/0]).
-export_type([forward_opts/0]).
-export_type([forward_result/0]).
-export_type([on_event_fun/0]).
-export_type([partitions/0]).
-export_type([server_ref/0]).
-export_type([ttl/0]).

%% API
-export([connect/1]).
-export([connect/2]).
-export([disconnect/1]).
-export([is_stale/1]).
-export([disconnect/2]).
-export([mynode/0]).
-export([myself/0]).
-export([deliver/2]).
-export([send_message/2]).
-export([supports_capability/2]).

%% =============================================================================
%% BEHAVIOUR CALLBACKS
%% =============================================================================

-callback start_link() -> {ok, pid()} | ignore | {error, term()}.

%% TODO: Deprecate me.
-callback members() -> {ok, [node()]}.

-callback members_for_orchestration() -> {ok, [partisan:node_spec()]}.

%% `update_members/1' treats `Members' as the membership: a member absent
%% from the list leaves the cluster. It exists for an external membership
%% authority. `add_members/1' treats the list as peers to join and never
%% removes a member; it is the callback a discovery backend must use, since a
%% discovery answer (DNS, an orchestrator's endpoint list) omits nodes that
%% are booting or briefly unhealthy and is no authority on membership.
-callback update_members([partisan:node_spec()]) ->
    ok | {error, not_implemented}.

-callback add_members([partisan:node_spec()]) ->
    ok | {error, not_implemented}.

-callback get_local_state() -> term().

-callback on_down(node(), on_event_fun()) -> ok | {error, not_implemented}.

-callback on_down(node(), on_event_fun(), #{channel => partisan:channel()}) ->
    ok | {error, not_implemented}.

-callback on_up(node(), on_event_fun()) -> ok | {error, not_implemented}.

-callback on_up(node(), on_event_fun(), #{channel => partisan:channel()}) ->
    ok | {error, not_implemented}.

-callback join(partisan:node_spec()) -> ok.

-callback sync_join(partisan:node_spec()) -> ok | {error, not_implemented}.

-callback leave() -> ok.

-callback leave(partisan:node_spec()) -> ok | {error, not_implemented}.

-callback send_message(node(), partisan:message()) -> ok.

-callback cast_message(
    ServerRef :: server_ref(),
    Msg :: partisan:message()
) -> ok.

-callback cast_message(
    ServerRef :: server_ref(),
    Msg :: partisan:message(),
    Opts :: forward_opts()
) -> ok.

-callback cast_message(
    Node :: node(),
    ServerRef :: server_ref(),
    Msg :: partisan:message(),
    Opts :: forward_opts()
) -> ok.

-callback forward_message(
    ServerRef :: server_ref(),
    Msg :: partisan:message()
) -> forward_result().

-callback forward_message(
    ServerRef :: server_ref(),
    Msg :: partisan:message(),
    Opts :: forward_opts()
) -> forward_result().

-callback forward_message(
    Node :: node(),
    ServerRef :: server_ref(),
    Msg :: partisan:message(),
    Opts :: forward_opts()
) -> forward_result().

-callback receive_message(node(), partisan:channel(), any()) -> ok.

-callback decode(term()) -> term().

-callback reserve(atom()) -> ok | {error, no_available_slots}.

-callback supports_capability(Arg :: atom()) -> boolean().

-callback partitions() -> {ok, partitions()} | {error, not_implemented}.

-callback inject_partition(partisan:node_spec(), ttl()) ->
    {ok, reference()} | {error, not_implemented}.

-callback resolve_partition(reference()) -> ok | {error, not_implemented}.

-optional_callbacks([supports_capability/1]).
-optional_callbacks([on_up/3]).
-optional_callbacks([on_down/3]).

%% =============================================================================
%% API
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc If `Mod' implements callback `supports_capability/1' returns the result
%% of calling the callback passing argument `Arg'. Otherwise, returns `false'.
%% @end
%% -----------------------------------------------------------------------------
-spec supports_capability(Mode :: module(), Arg :: atom()) -> boolean().

supports_capability(Mod, Arg) ->
    case erlang:function_exported(Mod, supports_capability, 1) of
        true ->
            Mod:supports_capability(Arg);
        false ->
            false
    end.

%% -----------------------------------------------------------------------------
%% @doc Tries to create a new connection to a node, but only if required.
%% If successful it stores the new connection record in the
%% {@link partisan_peer_connections} table.
%%
%% This function calls {@link connect/2} with options `#{prune => false}'.
%% @end
%% -----------------------------------------------------------------------------
-spec connect(NodeSpec :: partisan:node_spec()) -> ok.

connect(NodeSpec) ->
    connect(NodeSpec, #{prune => false}).

%% -----------------------------------------------------------------------------
%% @doc Create a new connection to a node specified by `NodeSpec' and
%% options `Opts'.
%% If a new connection is created it will be stored in the
%% {@link partisan_peer_connections} table.
%%
%% If option `prune' is `true' returns the tuple `{ok, L :: [
%% partisan:node_spec()]}' where list L is the list of nodes specifications for
%% all stale nodes. Otherwise returns `ok'.
%%
%% If option `async' is `true' the connection attempts do not block the
%% caller, and the call returns `{ok, Attempts}', one `{Pid, Channel,
%% ListenAddr}' per client started. The caller must be linked to and trap
%% exits from those clients, and owns the outcome of each attempt:
%% `{client_connected, Pid}' means the caller should store the connection with
%% {@link partisan_peer_connections:store/4}; `{'EXIT', Pid, _}' before that
%% means the attempt failed, and {@link is_stale/1} tells whether the
%% specification should be pruned. See
%% {@link partisan_peer_service_client:start_link/6}. `async' takes precedence
%% over `prune'.
%%
%% A specification is stale if we have one or more active connections to its
%% node and none of them uses any of its `listen_addrs'. A stale
%% specification will exist when a node has crashed (without leaving the
%% cluster) and later on returned with a different IP address i.e. a normal
%% situation on cloud orchestration platforms. In this case the membership set
%% ({@link partisan_membership_set}) will have two node specifications for the
%% same node (with differing values for the `listen_addrs' property).
%% A specification that shares a listen address with the active connections,
%% for example one that differs only in `channels' or `parallelism', is not
%% reported.
%%
%% See the section **Stale Specifications** in {@link partisan_membership_set}.
%% @end
%% -----------------------------------------------------------------------------
-spec connect
    (NodeSpec :: partisan:node_spec(), #{async := true}) ->
        {ok, Attempts :: [attempt()]};
    (NodeSpec :: partisan:node_spec(), #{prune := true}) ->
        {ok, StaleSpecs :: [partisan:node_spec()]};
    (NodeSpec :: partisan:node_spec(), #{prune := false}) ->
        ok.

connect(#{listen_addrs := ListenAddrs} = NodeSpec, Opts) ->
    {Mode, Acc0} =
        case Opts of
            #{async := true} -> {async, []};
            #{prune := true} -> {sync, []};
            #{} -> {sync, ok}
        end,
    Acc = lists:foldl(
        fun(ListenAddr, Acc1) ->
            maybe_connect(NodeSpec, ListenAddr, Mode, Acc1)
        end,
        Acc0,
        ListenAddrs
    ),
    case Acc of
        ok -> ok;
        L -> {ok, L}
    end.

%% -----------------------------------------------------------------------------
%% @doc Returns `true' if `NodeSpec' is stale: its node has connections and
%% none of them uses any of `NodeSpec''s listen addresses. See {@link
%% connect/2}.
%% @end
%% -----------------------------------------------------------------------------
-spec is_stale(partisan:node_spec()) -> boolean().

is_stale(#{name := Node} = NodeSpec) ->
    %% The info record's node_spec is not consulted: the connection records
    %% carry the address actually in use.
    partisan_peer_connections:count(Node) > 0 andalso
        not partisan_peer_connections:is_connected(NodeSpec).

%% -----------------------------------------------------------------------------
%% @doc Kill all connections with node in `Nodes' and for each call function
%% `Fun' passing the node as argument
%% @end
%% -----------------------------------------------------------------------------
disconnect(Nodes) when is_list(Nodes) ->
    disconnect(Nodes, fun(_) -> ok end).

%% -----------------------------------------------------------------------------
%% @doc Kill all connections with node in `Nodes' and for each call function
%% `Fun' passing the node as argument
%% @end
%% -----------------------------------------------------------------------------
disconnect(Nodes, Fun) when is_list(Nodes), is_function(Fun, 1) ->
    Node = partisan:node(),
    _ = [
        begin
            _ =
                case partisan_config:get(connect_disterl, false) of
                    true ->
                        net_kernel:disconnect(N);
                    false ->
                        ok
                end,
            ok = partisan_peer_connections:kill(N),
            catch Fun(N)
        end
     || N <- Nodes, N =/= Node
    ],
    ok.

%% -----------------------------------------------------------------------------
%% @doc Send a message to a remote peer_service_manager.
%% @end
%% -----------------------------------------------------------------------------
-spec send_message(node(), partisan:message()) -> ok.

send_message(Node, Message) ->
    ?PEER_SERVICE_MANAGER:send_message(Node, Message).

%% -----------------------------------------------------------------------------
%% @doc Internal function used by peer_service manager implementations to
%% forward a message to a local process identified by `ServerRef' (or a global
%% process when connected using disterl).
%% Trying to send a message to a remote server reference when the process is
%% located at a node connected with Partisan will return `ok' but will not
%% succeed.
%% @end
%% -----------------------------------------------------------------------------
-spec deliver(ServerRef :: server_ref(), Msg :: any()) -> ok.

deliver(ServerRef, Msg) ->
    try
        do_deliver(ServerRef, Msg)
    catch
        Class:Reason:Stacktrace ->
            ?LOG_DEBUG(#{
                description => "Error forwarding message",
                message => Msg,
                destination => ServerRef,
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ok
    end.

%% @deprecated use {@link partisan:node_spec/0} instead
-spec myself() -> partisan:node_spec().

myself() ->
    partisan:node_spec().

%% @deprecated use {@link partisan:node/0} instead
-spec mynode() -> atom().

mynode() ->
    partisan:node().

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------

maybe_connect(#{name := Node} = NodeSpec, ListenAddr, Mode, Acc) ->
    Channels =
        case maps:find(channels, NodeSpec) of
            {ok, Value} -> Value;
            error -> partisan_config:channels()
        end,

    %% We check count using Node and not NodeSpec cause it is much faster and
    %% we are only interested in knowing if we have at least one connection
    %% even if this was a stale NodeSpec.
    %% See the section Stale Specifications in partisan_membership_set.
    case partisan_peer_connections:count(Node) of
        0 ->
            %% Found disconnected or not found. We try a first connection
            %% using the default channel.
            ?LOG_DEBUG(#{
                description =>
                    "We have no connections with peer. Trying to connect",
                peer => NodeSpec
            }),
            Channel = ?DEFAULT_CHANNEL,
            ChannelOpts = maps:get(Channel, Channels),
            start_client(
                NodeSpec, ListenAddr, Channel, ChannelOpts, 0, Mode, Acc
            );
        _ ->
            %% Found node with some channels already connected, we will try to
            %% complete the parallelism per channel across all channels
            ChannelsList = maps:to_list(Channels),
            maybe_connect(ChannelsList, NodeSpec, ListenAddr, Mode, Acc)
    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc This function is called by maybe_connect/4 only when the Node in
%% NodeSpec has at least one active connection.
%% @end
%% -----------------------------------------------------------------------------

maybe_connect([{Channel, ChannelOpts} | T], NodeSpec, ListenAddr, Mode, Acc) ->
    %% There is at least one connection for Node.
    Parallelism = get_opt(parallelism, ChannelOpts),

    NewAcc =
        case partisan_peer_connections:count(NodeSpec, Channel) of
            Count when Count < Parallelism ->
                ?LOG_DEBUG(
                    "~p of ~p connected for channel ~p) Connecting node ~p.",
                    [Count, Parallelism, Channel, NodeSpec]
                ),
                start_client(
                    NodeSpec, ListenAddr, Channel, ChannelOpts, Count, Mode, Acc
                );
            Count when Count == Parallelism ->
                Acc
        end,

    %% We continue with next channel, even though we might have not finished
    %% connecting all the connections required by the parallelism of the
    %% current channel. We will try on the next tick.
    %% It is fairer this way, so that we can get connections one channel at a
    %% time.
    maybe_connect(T, NodeSpec, ListenAddr, Mode, NewAcc);
maybe_connect([], _, _, _, Acc) ->
    Acc.

%% -----------------------------------------------------------------------------
%% @private
%% @doc Starts a client for one connection. `Count' is the number of
%% connections `NodeSpec' already has on `Channel'.
%%
%% `async': returns at once and adds the attempt to `Acc'; the caller owns the
%% outcome (see connect/2). `sync': blocks for up to the client's connect
%% timeout, stores the connection on success and, when pruning, adds a stale
%% `NodeSpec' to `Acc' on failure.
%% @end
%% -----------------------------------------------------------------------------
start_client(NodeSpec, ListenAddr, Channel, ChannelOpts, _, async, Acc) ->
    %% An async client's init/1 cannot fail: it only builds its state.
    {ok, Pid} = partisan_peer_service_client:start_link(
        NodeSpec, ListenAddr, Channel, ChannelOpts, self(), #{async => true}
    ),
    [{Pid, Channel, ListenAddr} | Acc];
start_client(NodeSpec, ListenAddr, Channel, ChannelOpts, Count, sync, Acc) ->
    %% We start a client which will connect with
    %% partisan_peer_service_server on the peer node
    Result = partisan_peer_service_client:start_link(
        NodeSpec, ListenAddr, Channel, ChannelOpts, self()
    ),

    case Result of
        {ok, Pid} ->
            ?LOG_DEBUG("Node ~p connected, pid: ~p", [NodeSpec, Pid]),
            ok = partisan_peer_connections:store(
                NodeSpec, Pid, Channel, ListenAddr
            ),
            Acc;
        {error, Reason} when Count == 0 ->
            %% Not connected on this channel through NodeSpec. NodeSpec might
            %% be a previous instance of Node with (i) different listen
            %% addresses, or (ii) the same listen addresses and different
            %% channels or parallelism. maybe_stale/6 flags case (i) only.
            maybe_stale(NodeSpec, Channel, ListenAddr, Acc, Count, Reason);
        Error ->
            %% We have some connections to this ListenAddr already
            ?LOG_ERROR(#{
                description => "Node failed to connect",
                error => Error,
                node_spec => NodeSpec,
                listen_address => ListenAddr,
                channel => Channel,
                channel_opts => ChannelOpts
            }),
            Acc
    end.

%% @private
maybe_stale(_, _, _, ok = Acc, 0, _) ->
    %% Options.prune == false
    Acc;
maybe_stale(NodeSpec, Channel, ListenAddr, Acc, 0, Reason) ->
    case is_stale(NodeSpec) of
        true ->
            ?LOG_DEBUG(#{
                description => "Flagging node specification to be pruned",
                reason => duplicate,
                node_spec => NodeSpec,
                listen_address => ListenAddr,
                channel => Channel
            }),
            [NodeSpec | Acc];
        false ->
            ?LOG_DEBUG(#{
                description => "Node failed to connect",
                reason => Reason,
                node_spec => NodeSpec,
                listen_address => ListenAddr,
                channel => Channel
            }),
            Acc
    end.

%% @private
get_opt(parallelism, #{parallelism := Value}) ->
    Value;
get_opt(parallelism, #{}) ->
    partisan_config:parallelism().

%% @private
do_deliver({global, Name}, Message) ->
    Pid = global:whereis_name(Name),
    Pid ! Message,
    ok;
do_deliver({via, Module, Name}, Message) ->
    Pid = Module:whereis_name(Name),
    Pid ! Message,
    ok;
do_deliver(Pid, Message) when is_pid(Pid) ->
    Pid ! Message,

    ?LOG_TRACE_IF(
        not is_process_alive(Pid),
        "Process ~p is NOT ALIVE.",
        [Pid]
    ),

    ok;
do_deliver(Name, Message) when is_atom(Name) ->
    Name ! Message,

    Pid = whereis(Name),

    ?LOG_TRACE_IF(
        Pid == undefined orelse not is_process_alive(Pid),
        "Process ~p is NOT ALIVE.",
        [Name]
    ),

    ok;
do_deliver(Ref, Message) when is_reference(Ref) ->
    %% A process alias (`erlang:alias/1'), used as a one-shot reply address.
    %% Sending to an alias that has been deactivated with `erlang:unalias/1' is
    %% dropped by the runtime, which is what lets a caller abandon a request
    %% without its late reply landing in the mailbox.
    send_ref(Ref, Message);
do_deliver(ServerRef, Message) ->
    ?LOG_DEBUG(
        "node ~p received message ~p for ~p",
        [partisan:node(), Message, ServerRef]
    ),

    try partisan_remote_ref:to_term(ServerRef) of
        Pid when is_pid(Pid) ->
            ?LOG_TRACE_IF(
                not is_process_alive(Pid),
                "Process ~p is NOT ALIVE for message: ~p",
                [ServerRef, Message]
            ),
            Pid ! Message,
            ok;
        Name when is_atom(Name) ->
            Name ! Message,
            ok;
        Ref when is_reference(Ref) ->
            %% Encoded alias — see the `is_reference' clause above. Without
            %% this the decoded reference matched no clause, the resulting
            %% `case_clause' was swallowed by `deliver/2', and the message was
            %% silently dropped.
            send_ref(Ref, Message)
    catch
        error:badarg ->
            ?LOG_INFO(#{
                description => "unknown destination, dropping message",
                to => ServerRef,
                message => Message
            }),
            ok
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Delivers to a reference destination, preserving the priority of an OTP
%% 28 priority alias.
%%
%% A priority alias (`erlang:alias/1' with the `priority' option) is the
%% receiver's opt-in to having a message inserted ahead of the ordinary part of
%% its message queue. The runtime honours that opt-in only when the sender
%% supplies both the alias and the `priority' option to `erlang:send/3'.
%% `Ref ! Message' supplies only the alias, so the opt-in was discarded here --
%% silently, since nothing reports the difference.
%%
%% The option is passed unconditionally because nothing exposes whether a given
%% reference is a priority alias. That is safe: on OTP 28.5 (erts 16.4)
%% `erlang:send/3' with `[priority]' returns `ok' for a pid, a plain
%% `make_ref/0', an ordinary alias and a deactivated alias, and the ordinary
%% alias is delivered in ordinary queue position. Held by
%% `partisan_priority_alias_test:ordinary_alias_is_not_promoted/0' and
%% `deactivated_alias_is_dropped_without_crashing/0'.
%%
%% The consequence is worth knowing: the sender no longer chooses per message,
%% so every message Partisan delivers to a priority alias is a priority
%% message. Only a receiver that deliberately created a priority alias and
%% handed it out can be affected.
%%
%% That has one edge worth naming. Because the option is unconditional, a peer
%% sending volume at a long-lived priority alias accumulates a large priority
%% queue -- the pattern the OTP docs single out: "Receiving processes have not
%% been optimized for handling large amounts of priority messages. If a process
%% accumulates a large amount of priority messages, the design of that message
%% protocol should be redesigned." It does not arise for the one-shot reply
%% aliases Partisan creates today (see `partisan_erpc'), and no test covers it.
%% A caller wanting a priority alias as a high-volume sink needs a per-message
%% option carried on the wire, which this deliberately is not.
%%
%% Cost of passing the option: measured at roughly a nanosecond per call on OTP
%% 28.5 (the option list is the whole of it -- `erlang:send/3' with `[]' is
%% within noise of `!'). The pid, registered name, `{global, _}' and
%% `{via, _, _}' clauses are untouched, so no other delivery path pays it.
%% @end
%% -----------------------------------------------------------------------------
-if(?OTP_RELEASE >= 28).

send_ref(Ref, Message) ->
    _ = erlang:send(Ref, Message, [priority]),
    ok.

-else.

%% OTP 27 rejects the `priority' option with `badarg' (`cause => badopt'),
%% verified on 27.3.4 (erts 15.2.7). Priority is therefore an optimisation on
%% 28 and later, never something the protocol may depend on.
send_ref(Ref, Message) ->
    _ = Ref ! Message,
    ok.

-endif.
