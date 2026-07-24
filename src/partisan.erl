%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Helium Systems, Inc. All Rights Reserved.
%% Copyright (c) 2016 Christopher Meiklejohn. All Rights Reserved.
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
%% -------------------------------------------------------------------

-module(partisan).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-moduledoc """
The primary Partisan API: process operations, messaging, monitoring and cluster
membership over Partisan's overlay network.

Partisan replaces Erlang's built-in distribution (disterl) with a configurable
overlay of TCP connections organised into **channels**. Many functions here are
counterparts of `erlang` and `net_kernel` — `send/2`, `monitor/2`, `node/1`,
`monitor_nodes/1` and so on — but they operate over that overlay rather than
disterl, and they speak in **remote references** rather than raw pids.

Two ideas underpin the API:

- **Remote references.** A raw pid, reference or registered name is only
  meaningful on the node that created it. Partisan addresses a process, reference
  or name on a peer by a *remote reference* — a node-localised, serialisable
  handle (`t:remote_pid/0`, `t:remote_reference/0`, `t:remote_name/0`). `self/0`,
  `make_ref/0` and `whereis/1` return these forms; see `partisan_remote_ref`.
- **Channels.** Traffic is carried on named channels, each with its own
  connections and parallelism, so unrelated streams do not queue behind one
  another. Messaging and monitoring take an optional channel; see `t:channel/0`
  and `t:channel_opts/0`.

## Where to start

- **Messaging** — `send/2`, `cast_message/2`, `forward_message/2`.
- **Monitoring** — `monitor/2`, `demonitor/1`, `monitor_node/2`, `monitor_nodes/1`.
- **Cluster** — `join/1`, `leave/0`, `nodes/0`, `node/0`; richer membership
  operations live in `partisan_peer_service`.
- **Identity and references** — `self/0`, `node/1`, `make_ref/0`, `whereis/1`, and
  the `is_local_pid/1` / `is_self/1` predicates.

Configuration is read through `partisan_config`.
""".

-doc "A process identifier that is either a local pid or a remote pid on a peer.".
-type any_pid() :: remote_pid() | pid().

-doc "A reference that is either a local reference or a remote reference on a peer.".
-type any_reference() :: remote_reference() | reference().

-doc "A registered name that is either a local atom or a remote name on a peer.".
-type any_name() :: remote_name() | atom().

-doc "A node-localised, serialisable handle for a pid on a peer. See `partisan_remote_ref`.".
-type remote_pid() :: partisan_remote_ref:p().

-doc "A node-localised, serialisable handle for a reference on a peer. See `partisan_remote_ref`.".
-type remote_reference() :: partisan_remote_ref:r().

-doc "A node-localised, serialisable handle for a registered name on a peer. See `partisan_remote_ref`.".
-type remote_name() :: partisan_remote_ref:n().

-doc """
An option for `monitor/2` and `monitor/3`: any standard `erlang` monitor option,
plus `{channel, Channel}` to bind the monitor — and the eventual `DOWN` — to a
channel.
""".
-type monitor_opt() ::
    erlang:monitor_option()
    | {channel, channel()}.

-doc "An option for `demonitor/2`: `flush` discards a pending `DOWN`; `info` reports whether the monitor was still live.".
-type demonitor_opt() :: flush | info.

-type net_kernel_opt() ::
    nodedown_reason
    % OTP 25
    | connection_id
    | {node_type, visible | hidden | all}.
-type channel_opt() ::
    net_kernel_opt()
    | {channel, channel()}
    | {channel_fallback, boolean()}.

-doc "An option for `monitor_nodes/2`: the standard `net_kernel` node-monitoring options together with a channel selector.".
-type monitor_nodes_opt() ::
    net_kernel_opt()
    | channel_opt().
-type send_dst() ::
    erlang:send_destination()
    | server_ref().

-doc """
A message or call destination: a pid or registered name (local or remote), an
encoded remote reference, or a `{Name, Node}`, `{global, Name}` or
`{via, Module, Name}` tuple. Defined by `partisan_peer_service_manager`.
""".
-type server_ref() :: partisan_peer_service_manager:server_ref().

-doc """
Options controlling how a message is routed — the channel to use, whether to
request acknowledgement, a causal label, and so on. Defined by
`partisan_peer_service_manager`.
""".
-type forward_opts() :: partisan_peer_service_manager:forward_opts().

-doc "The node-set selector accepted by `nodes/1`, mirroring the standard `erlang` node types.".
-type node_type() :: this | known | visible | connected | hidden.

-doc "The name of a channel: an independently connected, independently parallel class of traffic over the overlay.".
-type channel() :: atom().

-doc """
A channel's configuration: its `parallelism` (the number of connections it opens
to each peer), and whether it preserves per-sender `monotonic` ordering and
applies `compression`.
""".
-type channel_opts() :: #{
    parallelism := non_neg_integer(),
    monotonic => boolean(),
    compression => boolean() | 0..9
}.

-doc "The identity (a binary) under which this node contributes entries to Partisan's membership CRDT. See `partisan_membership_set`.".
-type actor() :: binary().

-doc "A peer-plane listen address — an `ip` and a `port`.".
-type listen_addr() :: #{
    ip := inet:ip_address(),
    port := 1..65535
}.

-doc """
The full specification of a peer: its node `name`, the `listen_addrs` it accepts
connections on, and the `channels` it offers. This is the unit of membership
Partisan gossips.
""".
-type node_spec() :: #{
    name := node(),
    listen_addrs := [listen_addr()],
    channels := #{channel() => channel_opts()}
}.
-type node_info() :: #{info_opt() => term()}.

-doc "A key selecting one field of a peer's node info, as returned by `node_spec/2`.".
-type info_opt() ::
    metadata
    | name
    | channels
    | listen_addrs
    | listen_ip
    | listen_port
    | connection_count.

-doc "An arbitrary Erlang term carried as a Partisan message.".
-type message() :: term().
-type time() :: non_neg_integer().
-type send_after_dst() ::
    pid()
    | (RegName :: atom())
    | (Pid :: remote_pid())
    | (RegName :: remote_name()).
-type send_after_opts() :: forward_opts() | [{abs, boolean()}].

-export_type([actor/0]).
-export_type([channel/0]).
-export_type([channel_opts/0]).
-export_type([demonitor_opt/0]).
-export_type([forward_opts/0]).
-export_type([info_opt/0]).
-export_type([listen_addr/0]).
-export_type([message/0]).
-export_type([monitor_nodes_opt/0]).
-export_type([monitor_opt/0]).
-export_type([node_spec/0]).
-export_type([node_type/0]).
-export_type([any_pid/0]).
-export_type([any_reference/0]).
-export_type([any_name/0]).
-export_type([remote_pid/0]).
-export_type([remote_reference/0]).
-export_type([remote_name/0]).
-export_type([server_ref/0]).

%% API
-export([start/0]).
-export([stop/0]).

%% Erlang API (erlang.erl counterparts)
-export([cancel_timer/1]).
-export([cancel_timer/2]).
-export([demonitor/1]).
-export([demonitor/2]).
-export([disconnect_node/1]).
-export([exit/2]).
-export([is_alive/0]).
-export([is_pid/1]).
-export([is_process_alive/1]).
-export([is_reference/1]).
-export([make_ref/0]).
-export([monitor/1]).
-export([monitor/2]).
-export([monitor/3]).
-export([node/0]).
-export([node/1]).
-export([process_info/1]).
-export([process_info/2]).
-export([self/0]).
-export([send/2]).
-export([send/3]).
-export([send_after/3]).
-export([send_after/4]).
-export([spawn/2]).
-export([spawn/4]).
-export([spawn_monitor/2]).
-export([spawn_monitor/4]).
-export([whereis/1]).

%% Erlang API (net_kernel.erl counterparts)
-export([monitor_node/2]).
-export([monitor_node/3]).
-export([monitor_nodes/1]).
-export([monitor_nodes/2]).

%% Partisan API
-export([broadcast/2]).
-export([cast_message/2]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([channel_opts/1]).
-export([default_channel/0]).
-export([forward_message/2]).
-export([forward_message/3]).
-export([forward_message/4]).
-export([is_connected/1]).
-export([is_connected/2]).
-export([is_fully_connected/1]).
-export([is_local/1]).
-export([is_local_name/1]).
-export([is_local_name/2]).
-export([is_local_pid/1]).
-export([is_local_pid/2]).
-export([is_local_reference/1]).
-export([is_local_reference/2]).
-export([is_self/1]).
-export([join/1]).
-export([kill_connections/1]).
-export([leave/0]).
-export([node_spec/0]).
-export([node_spec/1]).
-export([node_spec/2]).
-export([nodes/0]).
-export([nodes/1]).
-export([nodestring/0]).
-export([remote_ref_to_disterl/1]).
-export([self/1]).

-compile({no_auto_import, [demonitor/2]}).
-compile({no_auto_import, [is_pid/1]}).
-compile({no_auto_import, [is_process_alive/1]}).
-compile({no_auto_import, [is_reference/0]}).
-compile({no_auto_import, [make_ref/0]}).
-compile({no_auto_import, [monitor/2]}).
-compile({no_auto_import, [monitor/3]}).
-compile({no_auto_import, [monitor_node/2]}).
-compile({no_auto_import, [node/0]}).
-compile({no_auto_import, [node/1]}).
-compile({no_auto_import, [nodes/1]}).
-compile({no_auto_import, [process_info/2]}).
-compile({no_auto_import, [self/0]}).
-compile({no_auto_import, [whereis/1]}).
-compile({no_auto_import, [spawn/2]}).
-compile({no_auto_import, [spawn/4]}).
-compile({no_auto_import, [spawn_monitor/2]}).
-compile({no_auto_import, [spawn_monitor/4]}).

%% =============================================================================
%% API
%% =============================================================================

-doc "Starts the `partisan` application and all of its dependencies.".
start() ->
    application:ensure_all_started(partisan).

-doc "Stops the `partisan` application.".
stop() ->
    application:stop(partisan).

-doc """
Returns a fresh remote reference — the Partisan counterpart of `erlang:make_ref/0`.

Equivalent to `partisan_remote_ref:from_term(erlang:make_ref())`.
""".
-spec make_ref() -> remote_reference() | no_return().

make_ref() ->
    partisan_remote_ref:from_term(erlang:make_ref()).

-doc """
Returns the remote-reference form of the calling process's pid.

Equivalent to `partisan_remote_ref:from_term(self())`. This is more expensive than
`erlang:self/0`, so you may want to cache the result in your process state — see
`self/1`, which can cache it in the process dictionary (and note the shell caveat
described there).
""".
-spec self() -> remote_pid().

self() ->
    partisan_remote_ref:from_term(erlang:self()).

-doc """
Returns the remote-reference form of the calling process's pid, optionally caching
it.

With `Opts = []` this is equivalent to `self/0`. With `Opts = [cache]` the result
is computed once and stored in the process dictionary, then returned from there on
subsequent calls.

> #### Warning {: .warning}
>
> Avoid `[cache]` in the Erlang shell. When a shell process crashes it copies its
> dictionary to the replacement shell, so you would carry over a remote reference
> that no longer matches the running process.
""".
-spec self(Opts :: [cache]) -> remote_pid().

self([]) ->
    partisan_remote_ref:from_term(erlang:self());
self([cache]) ->
    Key = {?MODULE, ?FUNCTION_NAME},

    case get(Key) of
        undefined ->
            Ref = partisan_remote_ref:from_term(erlang:self()),
            _ = put(Key, Ref),
            Ref;
        Ref ->
            Ref
    end.

-doc """
Monitors `Term` as a process; equivalent to `monitor(process, Term)`.

_Deprecated: use `monitor/2` instead._
""".
monitor(Term) ->
    monitor(process, Term).

-doc """
Sends a monitor request of type `Type` for the entity identified by `Item`;
equivalent to `monitor(Type, Item, [])`.

If the monitored entity does not exist, or later changes monitored state, the
caller receives a `{Tag, MonitorRef, Type, Object, Info}` message. This is
Partisan's counterpart of `erlang:monitor/2`.

Fails with `notalive` if the `partisan_monitor` server is not running.
""".
-spec monitor
    (process, pid() | atom() | {atom(), node()}) ->
        reference();
    (process, remote_pid() | remote_name()) ->
        remote_reference() | no_return();
    (port, port() | atom()) ->
        reference() | no_return();
    (time_offset, clock_service) ->
        reference() | no_return().

%% Dialyzer does not support overloaded contracts
-dialyzer([{nowarn_function, monitor/2}]).

monitor(Type, Item) ->
    monitor(Type, Item, []).

-doc """
Sends a monitor request of type `Type` for the entity identified by `Item`, with
options.

If the monitored entity does not exist, or later changes monitored state, the
caller receives a `{Tag, MonitorRef, Type, Object, Info}` message. This differs
from the message `erlang:monitor/3` sends only when the monitored item is a remote
process, in which case `MonitorRef` is a `t:remote_reference/0` and `Object` is a
`t:remote_pid/0` or `t:remote_name/0`.

This is Partisan's counterpart of `erlang:monitor/3` and differs from it only when
monitoring a `process`; for a `port` or `time_offset` it calls `erlang:monitor/3`
directly. Unlike `erlang:monitor/3`, it does not support aliases.

## Monitoring a process

Creates a monitor between the calling process and the process identified by `Item`
— a local or remote pid, a registered-name atom, or a `{RegisteredName, Node}`
tuple for a process registered on another node. A monitor by name resolves the
name to a pid once, at creation; later changes to the registration do not affect
the existing monitor.

Pass `{channel, Channel}` in `Opts` to bind the monitor to a channel: the eventual
`DOWN` is then delivered on that channel, in order with other traffic on it (see
the channel-ordering note in the module documentation).

Fails soft: if the `partisan_monitor` server is not running, the returned reference
receives an immediate `DOWN` with reason `notalive`.
""".
-spec monitor
    (process, pid() | atom(), [monitor_opt()]) ->
        reference();
    (port, port() | atom(), [erlang:monitor_option()]) ->
        reference();
    (time_offset, clock_service, [erlang:monitor_option()]) ->
        reference();
    (process, {atom(), node()}, [monitor_opt()]) ->
        reference() | remote_reference();
    (process, remote_pid(), [monitor_opt()]) ->
        remote_reference();
    (process, remote_name(), [monitor_opt()]) ->
        remote_reference().

%% Dialyzer does not support overloaded contracts
-dialyzer([{nowarn_function, monitor/3}]).

monitor(process, RegisteredName, Opts) when is_atom(RegisteredName) ->
    erlang:monitor(process, RegisteredName, to_erl_monitor_opts(Opts));
monitor(process, Pid, Opts) when erlang:is_pid(Pid) ->
    erlang:monitor(process, Pid, to_erl_monitor_opts(Opts));
monitor(process, {RegisteredName, Node}, Opts) when
    is_atom(RegisteredName)
->
    case partisan:node() == Node of
        true ->
            erlang:monitor(process, RegisteredName, to_erl_monitor_opts(Opts));
        false ->
            %% Only use native disterl monitoring when the peer is actually
            %% reachable over disterl; otherwise `erlang:monitor' fabricates an
            %% immediate `noconnection' DOWN for a process still reachable over
            %% the partisan overlay.
            case
                partisan_config:get(connect_disterl, false) andalso
                    lists:member(Node, erlang:nodes())
            of
                true ->
                    erlang:monitor(
                        process,
                        {RegisteredName, Node},
                        to_erl_monitor_opts(Opts)
                    );
                false ->
                    Ref = partisan_remote_ref:from_term(
                        RegisteredName, Node
                    ),
                    partisan_monitor:monitor(Ref, Opts)
            end
    end;
monitor(process, Term, Opts) when erlang:is_pid(Term) orelse is_atom(Term) ->
    erlang:monitor(process, Term, to_erl_monitor_opts(Opts));
monitor(process, RemoteRef, Opts) ->
    %% When `connect_disterl' is true AND the peer is actually reachable over
    %% disterl, decode the remote-ref and use `erlang:monitor' so DOWN fires
    %% promptly. If the peer is not disterl-reachable, native `erlang:monitor'
    %% would fabricate an immediate `noconnection' DOWN for a process still
    %% reachable over the partisan overlay, so fall back to the partisan
    %% transport's own monitor (also the default when `connect_disterl' is off).
    %% Note: only name refs are disterl-usable; encoded pids cannot be
    %% reconstructed as remote pids (see `remote_ref_to_disterl/1').
    case
        partisan_config:get(connect_disterl, false) andalso
            lists:member(
                partisan_remote_ref:node(RemoteRef), erlang:nodes()
            ) andalso
            remote_ref_to_disterl(RemoteRef)
    of
        {ok, {Name, _Node} = NN} when is_atom(Name) ->
            erlang:monitor(process, NN, to_erl_monitor_opts(Opts));
        _ ->
            partisan_monitor:monitor(RemoteRef, Opts)
    end;
monitor(Type, Term, Opts) when Type == port orelse Type == time_offset ->
    erlang:monitor(Type, Term, to_erl_monitor_opts(Opts)).

-doc """
Removes the monitor identified by `MonitorRef`; equivalent to `demonitor(Ref, [])`.

Unlike `erlang:demonitor/1`, it does not fail if `MonitorRef` refers to a monitor
started by another process.
""".
-spec demonitor(MonitorRef :: reference() | remote_reference()) -> true.

demonitor(Ref) ->
    _ = demonitor(Ref, []),
    true.

-doc """
Removes the monitor identified by `MonitorRef`, with options — Partisan's
counterpart of `erlang:demonitor/2`.

`flush` removes a pending `DOWN` for this monitor from the caller's mailbox; `info`
makes the call return whether the monitor was still live when removed.
""".
-spec demonitor(
    MonitorRef :: reference() | remote_reference(),
    OptionList :: [demonitor_opt()]
) -> boolean().

demonitor(Ref, Opts) when erlang:is_reference(Ref) ->
    erlang:demonitor(Ref, Opts);
demonitor(Ref, Opts) ->
    %% partisan_monitor:demonitor will raise a badarg if Ref is not valid
    partisan_monitor:demonitor(Ref, Opts).

-doc """
Turns node-status monitoring of `Node` on (`Flag = true`) or off (`false`);
equivalent to `monitor_node(Node, Flag, [])`. `Node` may be a node name or a
`t:node_spec/0`.

Calling `monitor_node(Node, true)` repeatedly is not an error: each calling process
gets one independent monitor, so a process that called it twice still receives a
single `{nodedown, Node}` if `Node` goes down — this differs from
`erlang:monitor_node/2`.

`{nodedown, Node}` is delivered if `Node` fails, does not exist, or is not
connected. Under a membership strategy with a partial view you therefore cannot
monitor nodes outside your view. Monitoring the caller's own node returns `false`.
""".
-spec monitor_node(node() | node_spec(), boolean()) -> boolean().

monitor_node(#{name := Node}, Flag) ->
    monitor_node(Node, Flag, []);
monitor_node(Node, Flag) ->
    monitor_node(Node, Flag, []).

-doc """
Turns node-status monitoring of `Node` on or off, with options.

Behaves as `monitor_node/2` — see there for the repeated-call, delivery and
partial-view semantics. `Options` accepts `allow_passive_connect`, which applies
only on the Erlang-distribution path (`connect_disterl = true`); with the default
overlay transport the options are ignored.
""".
-spec monitor_node(
    Node :: node(),
    Flag :: boolean(),
    Options :: [allow_passive_connect]
) -> true.

monitor_node(Node, Flag, Opts) ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            erlang:monitor_node(Node, Flag, Opts);
        false ->
            %% No opts for partisan_monitor
            partisan_monitor:monitor_node(Node, Flag)
    end.

-doc """
Subscribes (`Flag = true`) or unsubscribes (`false`) the calling process to
node-status change messages; equivalent to `monitor_nodes(Flag, [])`.
""".
-spec monitor_nodes(Flag :: boolean()) -> ok | error | {error, term()}.

monitor_nodes(Flag) ->
    monitor_nodes(Flag, []).

-doc """
Subscribes (`Flag = true`) or unsubscribes (`false`) the calling process to
node-status change messages, with options.

While subscribed, the process receives a `{nodeup, Node}` message when a peer
connects and a `{nodedown, Node}` message when one disconnects. `Opts` mirrors the
`net_kernel` node-monitoring options and may also carry a channel selector.
""".
-spec monitor_nodes(Flag :: boolean(), [monitor_nodes_opt()]) ->
    ok | error | {error, term()}.

monitor_nodes(Flag, Opts) ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            NKOpts = to_net_kernel_opts(Opts),
            net_kernel:monitor_nodes(Flag, NKOpts);
        false ->
            partisan_monitor:monitor_nodes(Flag, Opts)
    end.

-doc """
Returns `true` if `Arg` belongs to the local node.

`Arg` may be a pid, port or reference, or one of their remote forms
(`t:remote_pid/0`, `t:remote_reference/0`).
""".
-spec is_local(Arg) -> Result when
    Arg ::
        pid()
        | port()
        | reference()
        | remote_pid()
        | remote_reference(),
    Result :: boolean().

is_local(Arg) ->
    Node = node(Arg),
    Node == 'nonode@nohost' orelse Node =:= node().

-doc """
Returns `true` if `Arg` refers to a name on the local node.

A plain atom is always a local name; a `t:remote_name/0` is checked against the
local node.
""".
-spec is_local_name(Arg :: atom() | remote_name()) ->
    boolean() | no_return().

is_local_name(Arg) when is_atom(Arg) ->
    true;
is_local_name(Arg) ->
    partisan_remote_ref:is_local_name(Arg).

-doc "Returns `true` if `Arg` refers to the locally registered name `Name`.".
-spec is_local_name(
    Arg :: atom() | remote_name(), Name :: atom()
) ->
    boolean() | no_return().

is_local_name(Name, Name) when is_atom(Name) ->
    true;
is_local_name(Arg, Name) when is_atom(Name) ->
    partisan_remote_ref:is_local_name(Arg, Name);
is_local_name(Arg, _) ->
    is_local_name(Arg) orelse error(badarg),
    false.

-doc "Returns `true` if `Arg` (a pid or `t:remote_pid/0`) is a pid on the local node.".
-spec is_local_pid(Arg :: pid() | remote_pid()) ->
    boolean() | no_return().

is_local_pid(Pid) when erlang:is_pid(Pid) ->
    is_local(Pid);
is_local_pid(Arg) ->
    partisan_remote_ref:is_local_pid(Arg).

-doc "Returns `true` if `Arg` refers to the local pid `Pid`.".
-spec is_local_pid(
    Arg :: pid() | remote_pid(), Pid :: pid()
) ->
    boolean() | no_return().

is_local_pid(Pid, Pid) when erlang:is_pid(Pid) ->
    is_local(Pid);
is_local_pid(Arg, Pid) when erlang:is_pid(Pid) ->
    partisan_remote_ref:is_local_pid(Arg, Pid);
is_local_pid(Arg, _) ->
    is_local_pid(Arg) orelse error(badarg),
    false.

-doc "Returns `true` if `Arg` (a reference or `t:remote_reference/0`) was created on the local node.".
-spec is_local_reference(Arg :: reference() | remote_reference()) ->
    boolean() | no_return().

is_local_reference(Ref) when erlang:is_reference(Ref) ->
    is_local(Ref);
is_local_reference(Arg) ->
    partisan_remote_ref:is_local_reference(Arg).

-doc "Returns `true` if `Arg` refers to the local reference `LocalRef`.".
-spec is_local_reference(
    Arg :: reference() | remote_reference(), LocalRef :: reference()
) ->
    boolean() | no_return().

is_local_reference(Ref, Ref) when erlang:is_reference(Ref) ->
    is_local(Ref);
is_local_reference(Arg, Ref) when erlang:is_reference(Ref) ->
    partisan_remote_ref:is_local_reference(Arg, Ref);
is_local_reference(Arg, _) ->
    is_local_reference(Arg) orelse error(badarg),
    false.

-doc "Returns `true` if `Arg` (a pid or `t:remote_pid/0`) is the calling process.".
-spec is_self(Arg) -> Result when
    Arg :: pid() | remote_pid(),
    Result :: boolean().

is_self(Arg) when erlang:is_pid(Arg) ->
    Arg =:= erlang:self();
is_self(Arg) ->
    partisan_remote_ref:is_local_pid(Arg, erlang:self()).

-doc "Adds a peer to the cluster; a shortcut for `partisan_peer_service:join/1`.".
-spec join(node_spec()) -> ok.

join(NodeSpec) ->
    partisan_peer_service:join(NodeSpec).

-doc "Removes the local node from the cluster; a shortcut for `partisan_peer_service:leave/0`.".
-spec leave() -> ok.

leave() ->
    partisan_peer_service:leave().

-doc """
Drops the peer connections to `Nodes` (a node name or a list of them).

The membership protocol re-establishes them if the nodes are still members, so this
forces a reconnect rather than removing a node from the cluster.
""".
kill_connections(Node) when is_atom(Node) ->
    kill_connections([Node]);
kill_connections(Nodes) when is_list(Nodes) ->
    partisan_peer_service_manager:disconnect(Nodes).

-doc "Returns the name of the local node.".
-spec node() -> node().

node() ->
    partisan_config:get(name).

-doc "Returns the name of the local node as a binary string.".
-spec nodestring() -> binary().

nodestring() ->
    partisan_config:get(nodestring).

-doc """
Returns the node on which `Arg` originates.

`Arg` may be a pid, port or reference, or a Partisan remote reference. For a remote
reference the node is read from the reference; for a local pid, port or reference it
is `erlang:node(Arg)`, falling back to `node/0` when Erlang distribution is
disabled.
""".
-spec node
    (pid() | port() | reference()) -> node();
    (partisan_remote_ref:t()) -> node() | no_return().

node(Arg) when
    erlang:is_pid(Arg) orelse erlang:is_reference(Arg) orelse is_port(Arg)
->
    Node = erlang:node(Arg),

    case partisan_config:get(connect_disterl) of
        true ->
            %% If node is down we will get 'nonode@nohost' and we should return
            %% this value, even if partisan:node() has been set
            Node;
        false ->
            case Node of
                'nonode@nohost' ->
                    %% Return the partisan node
                    node();
                Other ->
                    %% This is the case when the use has assigned a nodename
                    %% via vm.args but disabled erlang distribution
                    %% In this case erlang:node() == partisan:node()
                    Other
            end
    end;
node(Arg) ->
    partisan_remote_ref:node(Arg).

-doc """
Returns the peers connected to this node over Partisan; equivalent to
`nodes(visible)` and the counterpart of `erlang:nodes/0`.

If `connect_disterl` is `true` (as in some test setups) this does **not** include
Erlang-distribution nodes — use `erlang:nodes/0` for those.
""".
-spec nodes() -> [node()].

nodes() ->
    nodes(visible).

-doc """
Returns nodes of the given type — the counterpart of `erlang:nodes/1`. When `Arg`
is a list, returns the nodes satisfying any of its elements.

The node types differ from Erlang as follows:

- `hidden` — always `[]`; Partisan has no hidden nodes.
- `this` — the list containing `node/0`.
- `known` — the nodes known to `partisan_peer_service` (its
  `partisan_peer_service:members/0`), not the nodes of pids, ports and references
  located on this node.
- `visible` — as in Erlang.
""".
-spec nodes(Arg :: node_type() | [node_type()]) -> [node()].

nodes(Arg) ->
    case partisan_config:get(connect_disterl) of
        true ->
            erlang:nodes(Arg);
        false when Arg == hidden ->
            [];
        false when Arg == this ->
            [node()];
        false when Arg == known ->
            {ok, Nodes} = partisan_peer_service:members(),
            Nodes;
        false when Arg == visible ->
            partisan_peer_connections:nodes();
        false when is_list(Arg) ->
            L = lists:foldl(fun(X, Acc) -> [nodes(X) | Acc] end, [], Arg),
            lists:flatten(L)
    end.

-doc "Returns `true` if this node currently has a connection to `NodeOrSpec`.".
-spec is_connected(NodeOrSpec :: node_spec() | node()) -> boolean().

is_connected(NodeOrSpec) ->
    partisan_peer_connections:is_connected(NodeOrSpec).

-doc "Returns `true` if this node currently has a connection to `NodeOrSpec` on `Channel`.".
-spec is_connected(NodeOrSpec :: node_spec() | node(), Channel :: channel()) ->
    boolean().

is_connected(NodeOrSpec, Channel) ->
    partisan_peer_connections:is_connected(NodeOrSpec, Channel).

-doc """
Returns `true` if every expected connection to `NodeOrSpec` is established — one per
configured channel at its configured parallelism.
""".
-spec is_fully_connected(NodeOrSpec :: node_spec() | node()) -> boolean().

is_fully_connected(NodeOrSpec) ->
    partisan_peer_connections:is_fully_connected(NodeOrSpec).

-doc """
Disconnects the local node from `Node` — the counterpart of
`erlang:disconnect_node/1`.

If `Node` is the local node, the node leaves the cluster; otherwise it leaves the
peer identified by `Node`. Returns `true` on success and `false` if `Node` is not a
known peer.
""".
-spec disconnect_node(Node :: node()) -> boolean() | ignored.

disconnect_node(Node) ->
    case Node == node() of
        true ->
            ok = partisan_peer_service:leave(),
            true;
        false ->
            try node_spec(Node) of
                {ok, NodeSpec} ->
                    ok = partisan_peer_service:leave(NodeSpec),
                    true;
                {error, _} ->
                    false
            catch
                _:_ ->
                    false
            end
    end.

-doc """
Returns `true` if the local node is alive — that is, the peer service manager is
running and the node can take part in a cluster.
""".
-spec is_alive() -> boolean().

is_alive() ->
    undefined =/= erlang:whereis(?PEER_SERVICE_MANAGER).

-doc """
Returns the pid or port registered under `Arg`, or `undefined` — the counterpart of
`erlang:whereis/1`.

`Arg` is a local name (an atom) or a `t:remote_name/0` for the local node. Fails
with `badarg` if `Arg` is a remote name for another node.
""".
-spec whereis(Arg :: atom() | remote_name()) ->
    pid() | port() | undefined.

whereis(Arg) when is_atom(Arg) ->
    erlang:whereis(Arg);
whereis(Arg) ->
    case partisan_remote_ref:to_term(Arg) of
        Ref when is_atom(Ref) ->
            erlang:whereis(Ref);
        _ ->
            error(badarg)
    end.

-doc """
Returns information about the process `Arg` (a pid or `t:remote_pid/0`), or
`undefined` — the counterpart of `erlang:process_info/1`.
""".
-spec process_info(Arg :: pid() | remote_pid()) ->
    [tuple()] | undefined.

process_info(Arg) when erlang:is_pid(Arg) ->
    erlang:process_info(Arg);
process_info(Arg) ->
    try partisan_remote_ref:to_term(Arg) of
        Term when erlang:is_pid(Term) ->
            erlang:process_info(Term);
        _ ->
            throw(badarg)
    catch
        _:_ ->
            error(badarg)
    end.

-doc """
Returns the requested information `Item` (an item or a list of items) about the
process `Arg` (a pid or `t:remote_pid/0`), or `undefined` — the counterpart of
`erlang:process_info/2`.
""".
-spec process_info(
    Arg :: pid() | remote_pid(),
    Item :: atom() | [atom()]
) -> [tuple()] | undefined.

process_info(Arg, ItemOrItems) when erlang:is_pid(Arg) ->
    erlang:process_info(Arg, ItemOrItems);
process_info(Arg, ItemOrItems) ->
    try partisan_remote_ref:to_term(Arg) of
        Term when erlang:is_pid(Term) ->
            erlang:process_info(Term, ItemOrItems);
        _ ->
            throw(badarg)
    catch
        _:_ ->
            error(badarg)
    end.

-doc """
Returns the node specification (`t:node_spec/0`) of the local node.

This is the information another node needs in order to join this one (see
`partisan_peer_service:join/1`). The values of the map's keys must be sorted so
the peer service can compare specifications and keep the membership view free of
duplicates — relevant when you build a specification by hand for a custom
orchestration strategy. Erlang maps are already sorted, so the only field you
must keep sorted yourself is `listen_addrs`, which is a list.
""".
-spec node_spec() -> node_spec().

node_spec() ->
    %% Channels and ListenAddrs are sorted already
    #{
        name => node(),
        listen_addrs => partisan_config:get(listen_addrs),
        channels => partisan_config:get(channels)
    }.

-doc """
Returns `{ok, NodeSpec}` for the node named `Node`, or `{error, Reason}`.
Equivalent to `node_spec(Node, #{})`; see `node_spec/2` for how the specification
is resolved.
""".
-spec node_spec(node()) -> {ok, node_spec()} | {error, Reason :: any()}.

node_spec(Node) when is_atom(Node) ->
    node_spec(Node, #{}).

-doc """
Returns `{ok, NodeSpec}` for the node named `Node`, or `{error, Reason}`.

If a Partisan connection to `Node` already exists, the cached specification used
to establish that connection is returned. Otherwise — the case under a
peer-to-peer topology — the specification is fetched from the remote node with
`partisan_rpc`, which requires the `forward_opts` configuration to enable
`broadcast` and `transitive`. `Opts` may set `rpc_timeout` (default 5000 ms).

> #### Peer-to-peer topologies {: .warning}
> `partisan_rpc` may not resolve a specification reliably under a peer-to-peer
> topology.
""".
-spec node_spec(
    Node :: binary() | list() | node(),
    Opts :: #{rpc_timeout => timeout()}
) ->
    {ok, node_spec()} | {error, Reason :: any()}.

node_spec(Node, Opts) when is_binary(Node) ->
    node_spec(binary_to_atom(Node), Opts);
node_spec(Node, Opts) when is_list(Node) ->
    node_spec(list_to_atom(lists:flatten(Node)), Opts);
node_spec(Node, Opts) when is_atom(Node), is_map(Opts) ->
    Timeout = maps:get(rpc_timeout, Opts, 5000),

    case partisan:node() of
        Node ->
            {ok, partisan:node_spec()};
        _ ->
            case is_connected(Node) of
                true ->
                    {ok, Info} = partisan_peer_connections:info(Node),
                    {ok, partisan_peer_connections:node_spec(Info)};
                false ->
                    M = ?MODULE,
                    F = node_spec,
                    A = [],
                    case partisan_rpc:call(Node, M, F, A, Timeout) of
                        #{name := Node} = Spec ->
                            {ok, Spec};
                        {badrpc, Reason} ->
                            {error, Reason}
                    end
            end
    end.

-doc false.
-spec node_info() -> node_info().

node_info() ->
    node_info([]).

-doc false.
-spec node_info([info_opt()]) -> map().

node_info([]) ->
    #{};
node_info(L) when is_list(L) ->
    node_info(L, #{}).

-doc "Returns the name of the default channel.".
-spec default_channel() -> channel().

default_channel() ->
    ?DEFAULT_CHANNEL.

-doc """
Returns the options (`t:channel_opts/0`) of the channel named `Channel`.
Fails with `badarg` if no such channel exists.
""".
-spec channel_opts(Channel :: channel()) -> channel_opts() | no_return().

channel_opts(Channel) when is_atom(Channel) ->
    partisan_config:channel_opts(Channel).

-doc """
Returns `true` if `Arg` is a local process identifier or a `t:remote_pid/0`,
`false` otherwise.
""".
-spec is_pid(any()) ->
    boolean() | no_return().

is_pid(Arg) when erlang:is_pid(Arg) ->
    true;
is_pid(Arg) ->
    partisan_remote_ref:is_pid(Arg).

-doc """
Returns `true` if `Arg` is a local reference or a `t:remote_reference/0`,
`false` otherwise.
""".
-spec is_reference(reference() | remote_reference()) ->
    boolean() | no_return().

is_reference(Arg) when erlang:is_reference(Arg) ->
    true;
is_reference(Arg) ->
    partisan_remote_ref:is_reference(Arg).

-doc """
Returns `true` if the process identified by `Arg` (a local pid or a
`t:remote_pid/0`) is alive, `false` otherwise. For a remote reference the check
runs on the owning node via `partisan_rpc`.
""".
-spec is_process_alive(pid() | remote_pid()) ->
    boolean() | no_return().

is_process_alive(Pid) when erlang:is_pid(Pid) ->
    erlang:is_process_alive(Pid);
is_process_alive(RemoteRef) ->
    case partisan_remote_ref:is_local_pid(RemoteRef) of
        true ->
            erlang:is_process_alive(partisan_remote_ref:to_term(RemoteRef));
        false ->
            Node = node(RemoteRef),
            Res = partisan_rpc:call(
                Node, ?MODULE, is_process_alive, [RemoteRef], 5000
            ),
            case Res of
                {badrpc, Reason} ->
                    error(Reason);
                Bool when is_boolean(Bool) ->
                    Bool
            end
    end.

-doc """
Sends an exit signal with reason `Reason` to the process identified by `Pid` (a
local pid or a `t:remote_pid/0`) — the Partisan counterpart of `erlang:exit/2`.
For a remote reference the signal is delivered on the owning node via
`partisan_rpc`.
""".
-spec exit(Pid :: pid() | remote_pid(), Reason :: term()) -> true.

exit(Pid, Reason) when erlang:is_pid(Pid) ->
    erlang:exit(Pid, Reason);
exit(RemoteRef, Reason) ->
    try partisan_remote_ref:to_term(RemoteRef) of
        Pid when erlang:is_pid(Pid) ->
            erlang:exit(Pid, Reason);
        Name when is_atom(Name) ->
            erlang:exit(whereis(Name), Reason)
    catch
        error:badarg ->
            %% Not local
            Node = node(RemoteRef),
            Res =
                partisan_rpc:call(
                    Node, ?MODULE, exit, [RemoteRef, Reason], 5000
                ),
            case Res of
                {badrpc, _} ->
                    ?LOG_WARNING(#{
                        description => "The call might have failed",
                        reason => Res,
                        remote_ref => RemoteRef
                    }),
                    %% The call should not fail
                    true;
                true ->
                    true
            end
    end.

-doc """
Sends message `Msg` to the destination `Dest` and returns `Msg`.
Equivalent to `send(Dest, Msg, [])`.
""".
-spec send(Dest :: send_dst(), Msg :: message()) -> message().

send(Dest, Msg) ->
    ok = send(Dest, Msg, []),
    Msg.

-doc """
Sends message `Msg` to the destination `Dest`.

When distributed Erlang is enabled (`connect_disterl` is `true`) the message is
delivered with `erlang:send/3`. Otherwise it is forwarded over Partisan with
`forward_message/3`, honouring `Opts` (`t:forward_opts/0`).
""".
-spec send(Dest :: send_dst(), Msg :: message(), Opts :: forward_opts()) ->
    ok | nosuspend | noconnect.

send(Dest, Msg, Opts) ->
    case partisan_config:get(connect_disterl) of
        true ->
            erlang:send(Dest, Msg, to_erl_send_opts(Opts));
        false ->
            forward_message(Dest, Msg, Opts)
    end.

-doc "Equivalent to `send_after(Time, Dest, Msg, [])`.".
-spec send_after(
    Time :: time(),
    Destination :: send_after_dst(),
    Msg :: message()
) -> TRef :: reference().

send_after(Time, Dest, Msg) ->
    send_after(Time, Dest, Msg, []).

-doc """
The Partisan counterpart of `erlang:send_after/4`.

For a local destination it calls the native implementation. For a remote
destination it spawns a process that holds the timer and accepts cancellation
(via `cancel_timer/1,2`); this is less efficient than the native implementation.
""".
-spec send_after(
    Time :: time(),
    Destination :: send_after_dst(),
    Message :: message(),
    Opts :: send_after_opts()
) -> TRef :: reference().

send_after(Time, Dest, Msg, Opts0) when
    ?IS_VALID_TIME(Time) andalso is_list(Opts0) andalso
        ((erlang:is_pid(Dest) andalso erlang:node(Dest) == erlang:node()) orelse
            is_atom(Dest))
->
    %% Local send
    Opts = to_erl_send_after_opts(Opts0),
    erlang:send_after(Time, Dest, Msg, Opts);
send_after(Time, {RegName, Node}, Msg, Opts0) when
    ?IS_VALID_TIME(Time) andalso
        is_list(Opts0) andalso
        is_atom(RegName) andalso
        is_atom(Node) andalso
        Node == erlang:node()
->
    %% Local send
    Opts = to_erl_send_after_opts(Opts0),
    erlang:send_after(Time, RegName, Msg, Opts);
send_after(Time, Dest, Msg, Opts) when
    ?IS_VALID_TIME(Time) andalso is_list(Opts)
->
    case partisan_remote_ref:is_type(Dest) of
        true ->
            Caller = erlang:self(),
            Pid = spawn(
                fun() ->
                    %% Setup an alias which the receiver can use to send us an
                    %% cancel message
                    Alias = alias(),
                    Me = erlang:self(),

                    Caller ! {send_after_init, Me, Alias},

                    %% Set the timer and wait for the timeout or cancel message
                    TimerRef = erlang:start_timer(Time, Me, send),

                    try
                        receive
                            {timeout, TimerRef, send} ->
                                catch partisan:send(Dest, Msg, Opts),
                                ok;
                            {cancel, Alias, Pid, Info} ->
                                CancelOpts = [{info, Info}],
                                Result = erlang:cancel_timer(
                                    TimerRef, CancelOpts
                                ),
                                case partisan_util:get(info, Opts, true) of
                                    true ->
                                        Canceled = {
                                            send_after_canceled, Alias, Result
                                        },
                                        Pid ! Canceled;
                                    false ->
                                        ok
                                end
                        end
                    catch
                        _:_ ->
                            ok
                    after
                        unalias(Alias)
                    end
                end
            ),

            receive
                {send_after_init, Pid, Ref} ->
                    Ref
            after 3000 ->
                error(timeout)
            end;
        false ->
            Info = #{
                cause => #{
                    2 =>
                        "should be a local pid, local registered name, "
                        "a tuple containing registered name and node, "
                        "or a partisan_remote_ref for a remote pid or "
                        "registered name."
                }
            },
            erlang:error(
                badarg, [Time, Dest, Msg, Opts], [{error_info, Info}]
            )
    end;
send_after(Time, Dest, Msg, Opts) when not ?IS_VALID_TIME(Time) ->
    Info = #{cause => #{1 => "should be a pos_integer"}},
    erlang:error(
        badarg, [Time, Dest, Msg, Opts], [{error_info, Info}]
    );
send_after(Time, Dest, Msg, Opts) when not is_list(Opts) ->
    Info = #{cause => #{4 => "should be a list"}},
    erlang:error(
        badarg, [Time, Dest, Msg, Opts], [{error_info, Info}]
    ).

-doc "Equivalent to `cancel_timer(Ref, [])`.".
-spec cancel_timer(Ref :: reference()) ->
    ok | time() | false.

cancel_timer(Ref) ->
    cancel_timer(Ref, []).

-doc """
Cancels the timer `Ref` — the Partisan counterpart of `erlang:cancel_timer/2`.

`Opts` accepts `{async, boolean()}` and `{info, boolean()}` with the same meaning
as in `erlang:cancel_timer/2`. Works for both native timers and the emulated
timers `send_after/4` creates for remote destinations.
""".
-spec cancel_timer(Ref :: reference(), Opts :: list()) ->
    ok | time() | false.

cancel_timer(Ref, Opts) when erlang:is_reference(Ref), is_list(Opts) ->
    Me = erlang:self(),

    case partisan_util:get(async, Opts, false) of
        true ->
            _ = spawn(fun() -> do_cancel_timer(Ref, Opts, Me) end),
            ok;
        false ->
            do_cancel_timer(Ref, Opts, Me)
    end.

-doc """
Spawns `Fun` on `Node` and returns a `t:remote_pid/0` for the new process — the
Partisan counterpart of `erlang:spawn/2`. A remote spawn is carried out with
`partisan_rpc`.
""".
-spec spawn(Node :: node(), Fun :: fun(() -> any())) -> remote_pid().

spawn(Node, Fun) ->
    case Node == node() of
        true ->
            Pid = erlang:spawn(Fun),
            partisan_remote_ref:from_term(Pid);
        false ->
            case partisan_rpc:call(Node, erlang, spawn, [Fun], 5000) of
                {badrpc, Reason} ->
                    ?LOG_WARNING(#{
                        description =>
                            "Can not start erlang:apply/1 on remote node",
                        node => Node
                    }),
                    Pid = erlang:spawn(
                        fun() ->
                            erlang:exit(process_exit_reason(Reason))
                        end
                    ),
                    partisan_remote_ref:from_term(Pid);
                Pid when erlang:is_pid(Pid) ->
                    partisan_remote_ref:from_term(Pid, Node);
                Encoded ->
                    Encoded
            end
    end.

-doc """
Spawns `Module:Function(Args)` on `Node` and returns a `t:remote_pid/0` for the
new process — the Partisan counterpart of `erlang:spawn/4`. A remote spawn is
carried out with `partisan_rpc`.
""".
-spec spawn(
    Node :: node(), Mod :: module(), Function :: atom(), Args :: [term()]
) -> remote_pid().

spawn(Node, Module, Function, Args) ->
    case Node == node() of
        true ->
            Pid = erlang:spawn(Module, Function, Args),
            partisan_remote_ref:from_term(Pid);
        false ->
            SpawnArgs = [Module, Function, Args],
            %% We call ourselves in Node and not erlang module, as we need
            %% a remote_pid() even when pid_encoding is disabled
            case partisan_rpc:call(Node, erlang, spawn, SpawnArgs, 5000) of
                {badrpc, Reason} ->
                    ?LOG_WARNING(#{
                        description =>
                            "Can not start erlang:apply/1 on remote node",
                        node => Node
                    }),
                    Pid = erlang:spawn(
                        fun() ->
                            erlang:exit(process_exit_reason(Reason))
                        end
                    ),
                    partisan_remote_ref:from_term(Pid);
                EncodedPid ->
                    EncodedPid
            end
    end.

-doc """
Spawns `Fun` on `Node` and monitors it, returning `{Pid, MonitorRef}` — the
Partisan counterpart of `erlang:spawn_monitor/2`. See `monitor/2` for the shape
of the reference and the `DOWN` message.
""".
-spec spawn_monitor(Node :: node(), Fun :: fun(() -> any())) ->
    {remote_pid(), remote_reference()}
    | {pid(), reference()}.

spawn_monitor(Node, Fun) ->
    Pid = spawn(Node, Fun),
    Ref = monitor(process, Pid),
    {Pid, Ref}.

-doc """
Spawns `Module:Function(Args)` on `Node` and monitors it, returning
`{Pid, MonitorRef}` — the Partisan counterpart of `erlang:spawn_monitor/4`. See
`monitor/2` for the shape of the reference and the `DOWN` message.
""".
-spec spawn_monitor(
    Node :: node(), Mod :: module(), Function :: atom(), Args :: [term()]
) ->
    {remote_pid(), remote_reference()}
    | {pid(), reference()}.

spawn_monitor(Node, Module, Function, Args) ->
    Pid = spawn(Node, Module, Function, Args),
    Ref = monitor(process, Pid),
    {Pid, Ref}.

-doc """
Casts message `Msg` to the process identified by `ServerRef` (a
`t:server_ref/0`), returning `ok`. Delivery is asynchronous and best-effort.
""".
-spec cast_message(
    ServerRef :: server_ref(),
    Msg :: message()
) -> ok.

cast_message(Term, Message) ->
    ?PEER_SERVICE_MANAGER:cast_message(Term, Message).

-doc """
Casts message `Msg` to the process identified by `ServerRef`, honouring `Opts`
(`t:forward_opts/0`). Delivery is asynchronous and best-effort.
""".
-spec cast_message(
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()
) -> ok.

cast_message(ServerRef, Msg, Opts) ->
    ?PEER_SERVICE_MANAGER:cast_message(ServerRef, Msg, Opts).

-doc """
Casts message `Msg` to the process `ServerRef` on `Node`, honouring `Opts`
(`t:forward_opts/0`). Delivery is asynchronous and best-effort.
""".
-spec cast_message(
    Node :: node(),
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()
) -> ok.

cast_message(Node, ServerRef, Message, Options) ->
    ?PEER_SERVICE_MANAGER:cast_message(Node, ServerRef, Message, Options).

-doc """
Forwards message `Msg` to the process identified by `ServerRef` (a
`t:server_ref/0`), returning `ok`.
""".
-spec forward_message(
    ServerRef :: server_ref(),
    Msg :: message()
) -> ok.

forward_message(ServerRef, Message) ->
    ?PEER_SERVICE_MANAGER:forward_message(ServerRef, Message).

-doc """
Forwards message `Msg` to the process identified by `ServerRef`, honouring `Opts`
(`t:forward_opts/0`).
""".
-spec forward_message(
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()
) -> ok.

forward_message(ServerRef, Message, Opts) ->
    ?PEER_SERVICE_MANAGER:forward_message(ServerRef, Message, Opts).

-doc """
Forwards message `Msg` to the process `ServerRef` on `Node`, honouring `Opts`
(`t:forward_opts/0`).
""".
-spec forward_message(
    Node :: node(),
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()
) -> ok.

forward_message(Node, ServerRef, Message, Opts) ->
    ?PEER_SERVICE_MANAGER:forward_message(Node, ServerRef, Message, Opts).

-doc """
Broadcasts a message originating from this node.

The message is delivered to every node at least once. `Mod` is responsible for
handling the message on remote nodes and for supplying the related information
the broadcast substrate needs, both locally and on other nodes. `Mod` must be
loaded on every member of the cluster and implement the
`m:partisan_plumtree_broadcast_handler` behaviour.
""".
-spec broadcast(any(), module()) -> ok.

broadcast(Broadcast, Mod) ->
    partisan_plumtree_broadcast:broadcast(Broadcast, Mod).

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
node_info(L, Acc) ->
    node_info(L, Acc, node_spec()).

%% @private
node_info([name | T], Acc0, #{name := Val} = Spec) ->
    Acc1 = Acc0#{name => Val},
    node_info(T, Acc1, Spec);
node_info([listen_addrs | T], Acc0, #{listen_addrs := Val} = Spec) ->
    Acc1 = Acc0#{listen_addrs => Val},
    node_info(T, Acc1, Spec);
node_info([channels | T], Acc0, #{channels := Val} = Spec) ->
    Acc1 = Acc0#{channels => Val},
    node_info(T, Acc1, Spec);
node_info([listen_ip | T], Acc0, #{listen_addrs := [#{ip := Val} | _]} = Spec) ->
    Acc1 = Acc0#{listen_ip => Val},
    node_info(T, Acc1, Spec);
node_info(
    [listen_port | T], Acc0, #{listen_addrs := [#{port := Val} | _]} = Spec
) ->
    Acc1 = Acc0#{listen_port => Val},
    node_info(T, Acc1, Spec);
node_info([metadata | T], Acc0, Spec) ->
    Default = #{},
    Acc1 = Acc0#{metadata => partisan_config:get(metadata, Default)},
    node_info(T, Acc1, Spec);
node_info([connection_count | T], Acc0, Spec) ->
    Acc1 = Acc0#{connection_count => partisan_peer_connections:count()},
    node_info(T, Acc1, Spec);
node_info([_ | T], Acc, Spec) ->
    node_info(T, Acc, Spec);
node_info([], Acc, _) ->
    Acc.

%% @private
-spec to_erl_send_opts([tuple()]) -> [nosuspend | noconnect | tuple()].

to_erl_send_opts(Opts) ->
    to_erl_opts(Opts).

%% @private
%%  Returns [{abs, boolean()}].
-spec to_erl_send_after_opts([tuple()]) -> [tuple()].

to_erl_send_after_opts(Opts) ->
    to_erl_opts(Opts).

%% @private
-spec to_erl_monitor_opts(list()) -> [erlang:monitor_option()].

to_erl_monitor_opts(Opts) ->
    to_erl_opts(Opts).

%% @private
to_erl_opts(Opts0) when is_list(Opts0) ->
    case lists:keytake(channel, 1, Opts0) of
        {value, _, Opts} ->
            Opts;
        false ->
            Opts0
    end.

%% @private
-spec to_net_kernel_opts([monitor_nodes_opt()]) -> [net_kernel_opt()].

to_net_kernel_opts(Opts0) when is_list(Opts0) ->
    case lists:keytake(channel, 1, Opts0) of
        {value, _, Opts1} ->
            case lists:keytake(channel_fallback, 1, Opts1) of
                {value, _, Opts} ->
                    Opts;
                false ->
                    Opts1
            end;
        false ->
            Opts0
    end.

%% @private
maybe_wait_for_cancel_timer_result(Ref, Info, undefined) ->
    maybe_wait_for_cancel_timer_result(Ref, Info, erlang:self());
maybe_wait_for_cancel_timer_result(Ref, Info, Pid) when erlang:is_pid(Pid) ->
    maybe_wait_for_cancel_timer_result(Ref, Info, Pid, Pid == erlang:self()).

%% @private
maybe_wait_for_cancel_timer_result(_, false, _, _) ->
    ok;
maybe_wait_for_cancel_timer_result(Ref, true, _, true) ->
    receive
        {send_after_canceled, Ref, Result} ->
            Result
    after 500 ->
        false
    end;
maybe_wait_for_cancel_timer_result(Ref, true, Pid, _) ->
    receive
        {send_after_canceled, Ref, Result} ->
            Pid ! {cancel_timer, Ref, Result}
    after 500 ->
        Pid ! {cancel_timer, Ref, false}
    end.

%% @private
do_cancel_timer(Ref, Opts, Pid) ->
    Me = erlang:self(),
    Info = partisan_util:get(info, Opts, true),

    case erlang:cancel_timer(Ref) of
        false ->
            %% Timer not found, maybe is our custom solution for send_after
            Info = partisan_util:get(info, Opts, true),
            Ref ! {cancel, Ref, Me, Info},
            maybe_wait_for_cancel_timer_result(Ref, Info, Pid);
        Result when Info == true, erlang:is_pid(Pid) ->
            Pid ! {cancel_timer, Ref, Result};
        Result when Info == true ->
            Result;
        _ ->
            ok
    end.

%% @private
process_exit_reason(disconnected) ->
    noconnection;
process_exit_reason(not_yet_connected) ->
    noconnection;
process_exit_reason(_) ->
    noproc.

%% @private
%% When `connect_disterl' is set, convert a partisan_remote_ref to a
%% disterl-usable form: a foreign pid (via `list_to_pid/1') for encoded pids,
%% or `{Name, Node}' for encoded names. Returns `error' for refs we don't
%% know how to convert (the caller should fall back to partisan_monitor).
remote_ref_to_disterl(Ref) ->
    try
        Node = partisan_remote_ref:node(Ref),
        case partisan_remote_ref:target(Ref) of
            {encoded_pid, Str} ->
                %% A partisan-encoded pid is stored in node-localised
                %% "<0.X.Y>" form; `list_to_pid/1' rebuilds it as a pid on the
                %% LOCAL node, silently discarding the origin node. Only a
                %% genuinely-local pid can be reconstructed this way; a remote
                %% pid has no disterl-usable form, so the caller must fall back
                %% to the partisan transport.
                case Node =:= partisan:node() of
                    true -> {ok, list_to_pid(Str)};
                    false -> error
                end;
            {encoded_name, Str} ->
                {ok, {list_to_existing_atom(Str), Node}};
            _ ->
                error
        end
    catch
        _:_ -> error
    end.
