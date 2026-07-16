%% -------------------------------------------------------------------
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
%% -------------------------------------------------------------------

%% -----------------------------------------------------------------------------
%% @doc This module is responsible for monitoring processes on remote nodes and
%% implementing the monitoring API provided by the `partisan' module which
%% follows the API provided by the Erlang modules `erlang' and `net_kernel'.
%%
%% <strong>YOU SHOULD NEVER USE the functions in this module directly.</strong>
%% Use the related functions in {@link partisan} instead.
%%
%% <blockquote class="warning">
%% <h4 class="warning">NOTICE</h4>
%% <p>At the moment this only works for
%% <code class="inline">partisan_pluggable_peer_service_manager</code> backend.
%% </p>
%% <p>Also, certain partisan_peer_service_manager implementations might not
%% support the
%% <code class="inline">partisan_peer_service_manager:on_up/2</code> and
%% <code class="inline">partisan_peer_service_manager:on_down/2</code>
%%  callbacks which we need for node monitoring, so in those cases this module
%%  will not work.
%% </p>
%% </blockquote>
%%
%% @TODO on a monitor request this server should monitor the caller so that we
%% can GC its references locally. This not only for monitor/2 but for
%% monitor_node/2 and monitor_nodes/2 (which means all or part of their logic
%% should run on the server.
%% @TODO to improve on concurrency/latency we will need to have a shard of
%% servers
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_monitor).

-behaviour(partisan_gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-define(DUMMY_MREF_KEY, {?MODULE, monitor_ref}).
-define(IS_ENABLED, persistent_term:get({?MODULE, enabled})).

%% Table to record remote processes monitoring a local process
%% stores proc_mon_in() records
-define(PROC_MON_IN, partisan_proc_mon_in).

%% An index over ?PROC_MON_IN
%% refs by node, used to notify/cleanup on a nodedown signal
%% contains objects of type proc_mon_in_idx()
-define(PROC_MON_IN_IDX, partisan_proc_mon_in_idx).

%% Table to record local processes monitoring a remote processes.
%% For every record in ?PROC_MON_IN we have companion record in this table,
%% This is to be able to send a signal to the local monitoring process
%% when the remote node crashes or connection is lost.
%% contains objects of type proc_mon_out()
-define(PROC_MON_OUT, partisan_proc_mon_out).

%% An index over ?PROC_MON_OUT
%% refs grouped by node, used to notify/cleanup on a nodedown signal
%% contains objects of typeproc_mon_in_idx()
-define(PROC_MON_OUT_IDX, partisan_proc_mon_out_idx).

%% Table to record local processes monitoring nodes
%% contains objects of type node_mon()
-define(NODE_MON, partisan_node_mon).

%% Local pids that are monitoring all nodes of a certain type
%% contains objects of type node_type_mon()
-define(NODE_TYPE_MON, partisan_node_type_mon).

-record(state, {
    %% whether monitoring is enabled,
    %% depends on partisan_peer_service_manager offering support for on_up/down
    enabled :: boolean(),
    %% A map to store async requests
    requests :: #{reference() => pid()},
    %% We cache a snapshot of the nodes, so that if we are terminated we can
    %% notify the subscriptions. This is the set of nodes we are currently
    %% connected to. Also this might be a partial view of the whole cluster,
    %% dependending on the peer_service_manager backend topology.
    nodes :: sets:set(node())
}).

-record(partisan_proc_mon_in, {
    %% The local monitor reference obtaind by erlang:monitor/2
    ref :: reference(),
    %% The local process that is being monitored
    monitored :: pid() | atom(),
    %% A remote process monitoring a local process (monitored)
    monitor ::
        partisan:remote_pid()
        | partisan:remote_name(),
    %% The channel signals should be forwarded on
    channel :: partisan:channel()
}).

-record(partisan_proc_mon_out, {
    %% The remote monitor reference
    ref :: partisan:remote_reference(),
    %% The remote process being monitored
    monitored ::
        partisan:remote_pid()
        | partisan:remote_name(),
    %% A local process monitoring the remote process
    monitor :: pid() | atom(),
    %% The channel the monitor is bound to. We need this to (a) fabricate a
    %% DOWN with reason `noconnection' when this specific channel goes down
    %% (independently of the rest of the node), and (b) preserve FIFO with
    %% the user's traffic on the same channel.
    channel :: partisan:channel()
}).

-record(partisan_node_type_mon, {
    key :: {Monitor :: pid(), Hash :: integer()},
    node_type :: all | visible | hidden,
    nodedown_reason :: boolean()
}).

-type proc_mon_in() :: #partisan_proc_mon_in{}.
-type proc_mon_in_idx() :: {{node(), partisan:channel()}, reference()}.
-type proc_mon_out() :: #partisan_proc_mon_out{}.
-type proc_mon_out_idx() :: {
    {node(), partisan:channel()}, partisan:remote_reference()
}.
-type node_mon() :: {node(), pid()}.
-type node_type_mon() :: #partisan_node_type_mon{}.
-type node_type_mon_opts() :: {
    Type :: all | visible | hidden,
    InclReason :: boolean()
}.

% API
-export([demonitor/2]).
-export([monitor/2]).
-export([monitor_node/2]).
-export([monitor_nodes/2]).
-export([start_link/0]).

%% gen_server callbacks
-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([terminate/2]).

-compile({no_auto_import, [monitor_node/2]}).
-compile({no_auto_import, [monitor/3]}).
-compile({no_auto_import, [demonitor/2]}).

%% =============================================================================
%% API
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc Starts the `partisan_monitor' server.
%%
%% There is one `partisan_monitor' server instance per node.
%% @end
%% -----------------------------------------------------------------------------
start_link() ->
    Opts = [
        {spawn_opt, ?PARALLEL_SIGNAL_OPTIMISATION([])}
    ],
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, [], Opts).

%% -----------------------------------------------------------------------------
%% @doc Monitor a remote process. Returns a partisan monitor reference that
%% can be passed to {@link demonitor/2}.
%%
%% Unlike `erlang:monitor/2', delivery of the `DOWN' signal is best-effort:
%% the signal can be lost on transport disconnection, message loss, or
%% peer-service tree reconfiguration. In those cases the monitor server
%% will fabricate a `DOWN' with reason `noconnection' as soon as the
%% disconnection is detected.
%%
%% == Channel binding ==
%%
%% A monitor is bound to a single channel for its whole lifetime — by
%% default the partisan default channel, or the channel passed in
%% `{channel, Channel}'. The channel binding has two effects:
%%
%% <ol>
%% <li>The eventual `DOWN' signal travels over the same partisan
%% connection as user traffic on `Channel', so it is FIFO-ordered with
%% any messages the monitored process sent on that channel before
%% terminating — matching the disterl guarantee that messages from a
%% dying process are delivered before its `DOWN'.</li>
%% <li>If `Channel' goes down (even while other channels to the same
%% node remain up), the monitor fires a `DOWN' with reason
%% `noconnection'. This is partisan-specific behaviour — disterl has a
%% single connection per pair of nodes and therefore no per-channel
%% concept.</li>
%% </ol>
%%
%% == Options ==
%%
%% <dl>
%% <dt>`{channel, Channel}'</dt><dd>The channel to bind the monitor to.
%% Defaults to the partisan default channel.</dd>
%% <dt>`{channel_fallback, boolean()}'</dt><dd>If the requested channel
%% is not connected at monitor establishment, whether to fall back to
%% the default channel for the establishment RPC. Defaults to `true'
%% when `Channel' is the default channel and `false' otherwise — the
%% explicit-channel default avoids silently splitting the user's
%% traffic and the `DOWN' signal across two connections, which would
%% break the FIFO guarantee above.</dd>
%% </dl>
%%
%% == Failure ==
%%
%% <dl>
%% <dt>`notalive'</dt><dd>The partisan_monitor server is not running.</dd>
%% <dt>`not_implemented'</dt><dd>The active partisan peer service manager
%% does not support the capabilities required for monitoring.</dd>
%% <dt>`badarg'</dt><dd>`Process' is not a valid partisan remote
%% pid or registered-name reference, or `Opts' contains a malformed
%% option.</dd>
%% </dl>
%% @end
%% -----------------------------------------------------------------------------
-spec monitor(
    Process :: partisan:remote_pid() | partisan:remote_name(),
    Opts :: [partisan:monitor_opt()]
) -> partisan:remote_reference() | no_return().

monitor(Process, Opts) when is_list(Opts) ->
    partisan_remote_ref:is_pid(Process) orelse
        partisan_remote_ref:is_name(Process) orelse
        error(badarg),

    %% This might be a circular call. This occurs because this server
    %% implements the partisan_gen_server behaviour and both
    %% partisan_gen_server and partisan_gen use this server for monitoring.
    %% We also need to skip the request when somebody else is trying to monitor
    %% this server or a remote partisan_monitor server (saving the roundtrip).
    %% To solve the issue, we skip monitoring and return a node-wide static
    %% dummy reference.
    case is_monitor_server(Process) of
        true ->
            %% Return a static dummy reference and ignore the request
            persistent_term:get(?DUMMY_MREF_KEY);
        false ->
            case partisan_remote_ref:is_local(Process) of
                true ->
                    PidOrName = partisan_remote_ref:to_pid_or_name(Process),
                    %% partisan:monitor will coerce Opts to erlang monitor opts
                    partisan:monitor(process, PidOrName, Opts);
                false ->
                    Node = partisan_remote_ref:node(Process),
                    Channel = get_option(channel, Opts, ?DEFAULT_CHANNEL),
                    %% Channel-fallback policy: if the user explicitly named
                    %% a non-default channel for the monitor, default to no
                    %% fallback. Falling back silently routes the eventual
                    %% DOWN signal over a different connection than the
                    %% user's regular traffic, which breaks the FIFO
                    %% ordering between in-flight messages on the chosen
                    %% channel and the DOWN. Callers can still opt in
                    %% explicitly with `{channel_fallback, true}'.
                    DefaultFallback =
                        case Channel of
                            ?DEFAULT_CHANNEL -> true;
                            _ -> false
                        end,
                    Fallback = get_option(
                        channel_fallback, Opts, DefaultFallback
                    ),

                    IsConnected =
                        case Channel of
                            ?DEFAULT_CHANNEL ->
                                partisan_peer_connections:is_connected(Node);
                            _ ->
                                partisan_peer_connections:is_connected(
                                    Node, Channel
                                ) orelse
                                    (Fallback == true andalso
                                        partisan_peer_connections:is_connected(
                                            Node
                                        ))
                        end,

                    %% Finally monitor
                    monitor(Process, Opts, {connected, IsConnected})
            end
    end.

%% -----------------------------------------------------------------------------
%% @doc Remove a monitor previously installed by {@link monitor/2}. Returns
%% `true' if the monitor was active and `false' if it had already fired or
%% was already removed (matching `erlang:demonitor/2').
%%
%% Cleanup happens in two steps: the local bookkeeping (`proc_mon_out') is
%% removed synchronously, then a `demonitor' RPC is sent to the remote
%% partisan_monitor server on the monitored node so it can drop its native
%% monitor and forget the request. If that remote call fails because the
%% peer is unreachable (`noconnection', `timeout', `noproc', `nodedown')
%% the function returns `true' — we assume the remote side has already
%% cleaned up and any in-flight `DOWN' will be either discarded by the
%% transport or matched against a no-longer-active reference.
%%
%% == Options ==
%%
%% <dl>
%% <dt>`flush'</dt><dd>If a `DOWN' for this reference is currently in
%% the calling process's mailbox, remove it. The flush is a best-effort
%% local `receive ... after 0' — partisan does not provide disterl's
%% exact same-connection barrier, so a `DOWN' that has not yet been
%% appended to the mailbox cannot be flushed. Pair with
%% `{channel, _}' on the original `monitor/2' call to keep the `DOWN'
%% on the same connection as user traffic and minimise the window
%% where flush can miss.</dd>
%% </dl>
%%
%% == Failure ==
%%
%% <dl>
%% <dt>`notalive'</dt><dd>The partisan_monitor server is not running.</dd>
%% <dt>`not_implemented'</dt><dd>The active partisan peer service manager
%% does not support the capabilities required for monitoring.</dd>
%% <dt>`badarg'</dt><dd>`MonitoredRef' is not a partisan remote reference,
%% or `Opts' contains a malformed option.</dd>
%% </dl>
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(
    MonitoredRef :: partisan:remote_reference(),
    Opts :: [partisan:demonitor_opt()]
) -> boolean() | no_return().

demonitor(MPRef, Opts) ->
    partisan_remote_ref:is_reference(MPRef) orelse
        erlang:error(badarg, [MPRef, Opts], [
            {error_info, #{
                cause => #{1 => "not a partisan remote reference"}
            }}
        ]),

    Skip =
        %% Is this a dummy reference or a circular call?
        MPRef == persistent_term:get(?DUMMY_MREF_KEY) orelse
            is_monitor_server(),

    case Skip of
        true ->
            true;
        false ->
            Node = partisan_remote_ref:node(MPRef),

            %% We remove the local references. The proc_mon_out record holds
            %% the channel we need to clean its index entry.
            case take_proc_mon_out(MPRef) of
                #partisan_proc_mon_out{channel = Channel} ->
                    ok = del_proc_mon_out_idx(Node, Channel, MPRef);
                error ->
                    ok
            end,

            %% We call the remote node to demonitor.
            %% If the remote server is unreachable we assume we lost connection
            %% and thus it must have cleaned up our references.
            case call({?MODULE, Node}, {demonitor, MPRef, Opts}, 3000) of
                {ok, Bool} ->
                    case lists:member(flush, Opts) of
                        true ->
                            receive
                                {_, MPRef, _, _, _} ->
                                    Bool
                            after 0 ->
                                Bool
                            end;
                        false ->
                            Bool
                    end;
                {error, noconnection} ->
                    true;
                {error, timeout} ->
                    true;
                {error, noproc} ->
                    true;
                {error, {nodedown, _}} ->
                    true;
                {error, Reason} ->
                    ErrOpts = [{error_info, #{cause => Reason}}],
                    erlang:error(Reason, [MPRef, Opts], ErrOpts)
            end
    end.

%% -----------------------------------------------------------------------------
%% @doc Monitor the status of the node `Node'. If Flag is true, monitoring is
%% turned on. If `Flag' is `false', monitoring is turned off.
%%
%% Making several calls to `monitor_node(Node, true)' for the same `Node'
%% is not an error; it results in as many independent monitoring instances as
%% the number of different calling processes i.e. If a process has made two
%% calls to `monitor_node(Node, true)' and `Node' terminates, only one
%% `nodedown' message is delivered to the process (this differs from {@link
%% erlang:monitor_node/2}).
%%
%% If `Node' fails or does not exist, the message `{nodedown, Node}' is
%% delivered to the calling process. If there is no connection to Node, a
%% `nodedown' message is delivered. As a result when using a membership
%% strategy that uses a partial view, you cannot monitor nodes that are not
%% members of the view.
%%
%% Failure:
%% <ul>
%% <li>`notalive' if the partisan_monitor process is not alive.</li>
%% <li>`not_implemented' if the partisan peer service manager does not support
%% the required capabilities required for monitoring.</li>
%% <li>`badarg' if any of the arguments is invalid.</li>
%% </ul>
%%
%% This function is executed in the calling process.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_node(node() | partisan:node_spec(), boolean()) -> true.

monitor_node(#{name := Node}, Flag) ->
    monitor_node(Node, Flag);
monitor_node(Node, Flag) when is_atom(Node) ->
    %% TODO WE need the server to monitor the caller, so that we can cleanup if
    %% caller crashes!! Or store them in process dictionary
    case partisan_peer_connections:is_connected(Node) of
        true when Flag == true ->
            add_node_monitor(Node, self());
        false when Flag == true ->
            %% The node is down.
            %% We don't record the request and immediately send a
            %% nodedown signal
            self() ! {nodedown, Node},
            true;
        _ when Flag == false ->
            del_node_monitor(Node, self())
    end.

%% -----------------------------------------------------------------------------
%% @doc Subscribe (or unsubscribe) the calling process to node status change
%% messages. While subscribed, a `{nodeup, Node}' message is delivered when
%% a new node is connected and a `{nodedown, Node}' message is delivered
%% when a node is disconnected. If `nodedown_reason' is in `Opts' the
%% extended forms `{nodeup, Node, InfoList}' / `{nodedown, Node, InfoList}'
%% are delivered instead.
%%
%% If `Flag' is `true' a new subscription is started. If `Flag' is `false'
%% all subscriptions previously started with the same `Opts' are stopped.
%% Two option lists are considered equivalent if they contain the same set
%% of options.
%%
%% == Ordering with respect to disterl ==
%%
%% Partisan delivers these signals in line with the disterl guarantees,
%% with one important caveat:
%%
%% <ul>
%% <li>`nodeup' is fired before any message can flow over the
%% newly-established connection (the receiver-process for the new
%% connection is started after the up callbacks run).</li>
%% <li>`nodedown' is fired only after every message already received on
%% the dying connection has been placed in its destination process's
%% mailbox. The receiver process delivers messages synchronously
%% (`erlang:send') and only then exits, and the down callbacks run
%% inline in the manager's exit handler, so the runtime serialises
%% these naturally.</li>
%% <li><strong>Caveat:</strong> a partisan node-pair can have multiple
%% channels. `{nodeup, Node}' / `{nodedown, Node}' fire when the
%% <em>first/last</em> channel is up/down, but a process monitoring on
%% a specific non-default channel may see its monitor's `DOWN' before
%% the node-level `nodedown' (because that channel went down while the
%% node remained reachable on others). For per-channel correctness use
%% {@link monitor/2} with `{channel, _}'.</li>
%% </ul>
%%
%% == Options ==
%%
%% <dl>
%% <dt>`{node_type, all | visible | hidden}'</dt><dd>Filter the
%% subscription. `hidden' is a no-op since partisan does not have hidden
%% nodes. Defaults to `all'.</dd>
%% <dt>`nodedown_reason'</dt><dd>Deliver the extended 3-tuple form
%% with a reason in the `InfoList'.</dd>
%% </dl>
%%
%% This function is executed in the calling process.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_nodes(Flag :: boolean(), [partisan:monitor_nodes_opt()]) ->
    ok | error | {error, notalive | not_implemented | badarg}.

monitor_nodes(Flag, Opts0) when is_boolean(Flag), is_list(Opts0) ->
    case ?IS_ENABLED of
        true ->
            case parse_nodemon_opts(Opts0) of
                {hidden, _} ->
                    %% Do nothing as we do not have hidden nodes in Partisan
                    ok;
                Opts when Flag == true ->
                    add_node_type_mon(self(), Opts);
                Opts when Flag == false ->
                    del_note_type_mon(self(), Opts)
            end;
        false ->
            error
    end.

%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================

init([]) ->
    %% We trap exits so we get a `terminate/2' callback with reason
    %% `shutdown' when the supervisor terminates us. This happens when
    %% `partisan_peer_service:manager()' is terminated.
    erlang:process_flag(trap_exit, true),

    %% We subscribe to node status to implement node monitoring.
    %% Certain `partisan_peer_service_manager' implementations might not
    %% support the `on_up'/`on_down' events; in those cases this module
    %% will not work.
    Enabled = subscribe_to_node_status(),
    _ = persistent_term:put({?MODULE, enabled}, Enabled),

    _ = subscribe_to_channel_status(),

    %% `partisan_gen' behaviours call `monitor/2' and `demonitor/1'.
    %% Since this server is one, calling itself would deadlock — we use
    %% a static dummy ref for that case to avoid the round trip.
    _ = persistent_term:put(?DUMMY_MREF_KEY, partisan:make_ref()),

    TabOpts = [
        named_table,
        public,
        {write_concurrency, true},
        {read_concurrency, true},
        {decentralized_counters, true}
    ],

    %% Tables for process monitoring
    _ = ets:new(?PROC_MON_IN, [set, {keypos, 2} | TabOpts]),
    _ = ets:new(?PROC_MON_IN_IDX, [bag, {keypos, 1} | TabOpts]),
    _ = ets:new(?PROC_MON_OUT, [set, {keypos, 2} | TabOpts]),
    _ = ets:new(?PROC_MON_OUT_IDX, [bag, {keypos, 1} | TabOpts]),

    %% Tables for node status monitoring
    _ = ets:new(?NODE_MON, [duplicate_bag, {keypos, 1} | TabOpts]),
    _ = ets:new(?NODE_TYPE_MON, [set, {keypos, 2} | TabOpts]),

    State = #state{
        enabled = Enabled,
        requests = #{},
        nodes = sets:new([{version, 2}])
    },

    {ok, State}.

handle_call(_, _, #state{enabled = false} = State) ->
    %% The peer service manager does not implement support for remote monitoring
    %% Instead of failing we return the dummy ref
    Reply = {ok, persistent_term:get(?DUMMY_MREF_KEY)},
    {reply, Reply, State};
handle_call({monitor, Monitor, _}, {Monitor, _}, State) ->
    %% A circular call (partisan_gen)
    Reply = {ok, persistent_term:get(?DUMMY_MREF_KEY)},
    {reply, Reply, State};
handle_call({monitor, Process, Opts}, {Monitor, _}, State) ->
    %% A remote process (Monitor) wants to monitor a process (Process) on
    %% this node.

    %% This must be sequential, because in case the process we want to monitor
    %% is dead, we will get the reference and immediately the DOWN signal, so
    %% we need to return the reference to the user before the signal reaches it.

    %% We did this check in monitor/2, but we double check again in case
    %% someone is calling partisan_gen_server:call directly.
    Reply =
        case is_monitor_server(Process) of
            true ->
                %% The case for a process monitoring this server, this server
                %% monitoring itself or another node's monitor server monitoring
                %% this one.
                {ok, persistent_term:get(?DUMMY_MREF_KEY)};
            false ->
                %% TODO Implement options
                %%  {tag, UserDefinedTag} option
                try
                    Node = partisan_remote_ref:node(Monitor),

                    %% Process can be a pid or registered name for a local
                    %% process
                    PidOrName = partisan_remote_ref:to_pid_or_name(Process),

                    %% We monitor the process on behalf of the remote caller.
                    %% We will handle the EXIT signal and forward it to
                    %% Monitor when it occurs.
                    Mref = erlang:monitor(process, PidOrName),

                    Channel = get_option(channel, Opts, ?DEFAULT_CHANNEL),

                    %% We track the Mref to match the 'DOWN' signal
                    ok = add_proc_mon_in(
                        Node, Channel, Mref, PidOrName, Monitor
                    ),

                    %% We reply with the encoded monitor reference
                    {ok, partisan_remote_ref:from_term(Mref)}
                catch
                    error:badarg ->
                        {error, badarg}
                end
        end,

    {reply, Reply, State};
handle_call({demonitor, RemoteRef, Opts}, {_Monitor, _}, State) ->
    %% A remote process is requesting a demonitor
    case RemoteRef == persistent_term:get(?DUMMY_MREF_KEY) of
        true ->
            %% We skip.
            %% The case for a process monitoring this server or another node's
            %% monitor server monitoring this one.
            {reply, {ok, true}, State};
        false ->
            Reply = do_demonitor(RemoteRef, Opts),
            {reply, Reply, State}
    end;
handle_call(_Msg, _From, State) ->
    {reply, {error, unsupported_call}, State}.

handle_cast({gc_proc_mon_out, Mref}, State) ->
    %% Companion to the direct-DOWN delivery: the remote partisan_monitor has
    %% already delivered the DOWN signal directly to the local monitor on the
    %% user's channel. We just GC our `proc_mon_out' bookkeeping here. This
    %% cast does not need to be ordered with the DOWN — it only releases an
    %% ETS entry — so any stragglers are harmless.
    case take_proc_mon_out(Mref) of
        #partisan_proc_mon_out{channel = Channel, monitored = Monitored} ->
            Node = partisan_remote_ref:node(Monitored),
            ok = del_proc_mon_out_idx(Node, Channel, Mref);
        error ->
            ok
    end,
    {noreply, State};
handle_cast({'DOWN', Mref, process, _Process, Reason}, State) ->
    %% Backwards-compat path: a remote partisan_monitor running pre-(direct
    %% delivery) code sends the DOWN signal as a cast to us, expecting us to
    %% relay it to the local monitor. We do the work inline (no `spawn') to
    %% keep DOWN signals in the order we received them on this channel —
    %% that matches what disterl gives us for same-sender signals.
    case take_proc_mon_out(Mref) of
        #partisan_proc_mon_out{channel = Channel} = M ->
            Monitor = M#partisan_proc_mon_out.monitor,
            Monitored = M#partisan_proc_mon_out.monitored,
            Node = partisan_remote_ref:node(Monitored),

            ok = del_proc_mon_out_idx(Node, Channel, Mref),

            Down = {
                'DOWN',
                Mref,
                process,
                Monitored,
                Reason
            },
            Monitor ! Down,
            ok;
        error ->
            ok
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_, #state{enabled = false} = State) ->
    %% Functionality disabled
    {noreply, State};
handle_info({'DOWN', Mref, process, _Process, Reason}, State) ->
    %% A process we monitor on behalf of a remote node has terminated.
    %%
    %% We deliver the DOWN signal *directly* to the remote monitor on the
    %% same channel the user picked at monitor establishment, instead of
    %% routing through the remote partisan_monitor server. This preserves
    %% FIFO ordering between this DOWN and any user traffic on that channel
    %% — i.e. the disterl guarantee that messages from the dying process
    %% are delivered before its DOWN signal.
    %%
    %% The work is done inline (no `spawn') to keep consecutive DOWN
    %% signals ordered on the wire as they were received from the local
    %% runtime.
    case take_proc_mon_in(Mref) of
        #partisan_proc_mon_in{} = M ->
            Monitored0 = M#partisan_proc_mon_in.monitored,
            Monitor = M#partisan_proc_mon_in.monitor,
            Channel = M#partisan_proc_mon_in.channel,

            Node = partisan:node(Monitor),
            del_proc_mon_in_idx(Node, Channel, Mref),

            %% Encode local refs as remote refs the way the user would see
            %% them.
            EncMref = partisan_remote_ref:from_term(Mref),
            EncMonitored = partisan_remote_ref:from_term(Monitored0),
            Down = {
                'DOWN',
                EncMref,
                process,
                EncMonitored,
                Reason
            },

            %% (1) Direct delivery on the user's channel.
            _ = partisan:forward_message(
                Monitor, Down, #{channel => Channel}
            ),

            %% (2) Companion cast asking the remote partisan_monitor to GC
            %% its proc_mon_out entry. This need not be ordered with the
            %% DOWN — it only releases an ETS entry.
            try
                partisan_gen_server:cast(
                    {?MODULE, Node},
                    {gc_proc_mon_out, EncMref},
                    [{channel, Channel}]
                )
            catch
                exit:noproc -> ok
            end;
        error ->
            ok
    end,
    {noreply, State};
handle_info({nodeup, Node}, State0) ->
    %% Either a net_kernel or Partisan signal

    ?LOG_DEBUG(#{
        description => "Got nodeup signal",
        node => Node
    }),

    %% Inline so notifications fire in the order events arrived in this
    %% gen_server's mailbox.
    on_nodeup(Node),

    %% We update the node list cache
    State = State0#state{
        nodes = sets:add_element(Node, State0#state.nodes)
    },

    {noreply, State};
handle_info({nodedown, Node}, State0) ->
    %% Either a net_kernel or Partisan signal

    ?LOG_DEBUG(#{
        description => "Got nodedown signal",
        node => Node
    }),

    %% Inline (no `spawn') so the DOWN/nodedown notifications happen in the
    %% order events were received. Combined with the synchronous
    %% deliver-then-EXIT path of the receiver process, this preserves the
    %% disterl guarantee that all in-flight messages from `Node' are placed
    %% in destination mailboxes before any nodedown / DOWN signal.
    on_nodedown(Node, noconnection),

    State = State0#state{
        nodes = sets:del_element(Node, State0#state.nodes)
    },

    {noreply, State};
handle_info({channeldown, Node, Channel}, State) ->
    %% A single channel to `Node' has gone down while other channels (if
    %% any) may still be up. Notify only the monitors bound to that
    %% channel; do not touch monitors on other channels — they remain
    %% valid.
    ?LOG_DEBUG(#{
        description => "Got channeldown signal",
        node => Node,
        channel => Channel
    }),

    on_channeldown(Node, Channel, noconnection),

    {noreply, State};
handle_info({channelup, _Node, _Channel}, State) ->
    %% No per-channel state to update on the receive side. Channel-up
    %% delivery for `monitor_nodes' subscribers is handled via {nodeup, _}
    %% which fires when the first channel comes up.
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(shutdown, _State) ->
    ok;
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
get_option(Key, L, Default) ->
    case lists:keyfind(Key, 1, L) of
        {Key, Value} -> Value;
        false -> Default
    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc  We subscribe to either net_kernel's or Partisan's node status
%% signals to update an ets table that tracks each node status.
%% @end
%% -----------------------------------------------------------------------------
subscribe_to_node_status() ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            case net_kernel:monitor_nodes(true) of
                ok ->
                    true;
                error ->
                    error({monitor_nodes_failed, unknown});
                {error, Reason} ->
                    error({monitor_nodes_failed, Reason})
            end;
        false ->
            Me = self(),

            %% We subscribe to the nodeup event for all nodes
            Res1 = partisan_peer_service:on_up(
                '_',
                fun(Node) -> Me ! {nodeup, Node} end
            ),

            %% We subscribe to the nodedown event for all nodes
            Res2 = partisan_peer_service:on_down(
                '_',
                fun(Node) -> Me ! {nodedown, Node} end
            ),

            %% Not all service managers implement this capability so the result
            %% can be an error.
            Res1 =:= ok andalso Res2 =:= ok
    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
subscribe_to_channel_status() ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            true;
        false ->
            Me = self(),

            %% We subscribe to the channelup event for all nodes
            Res1 = partisan_peer_service:on_up(
                '_',
                fun(Node, Channel) -> Me ! {channelup, Node, Channel} end,
                #{channel => '_'}
            ),

            %% We subscribe to the channeldown event for all nodes
            Res2 = partisan_peer_service:on_down(
                '_',
                fun(Node, Channel) -> Me ! {channeldown, Node, Channel} end,
                #{channel => '_'}
            ),

            %% Not all service managers implement this capability so the result
            %% can be an error.
            Res1 =:= ok andalso Res2 =:= ok
    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc This call is executed by the caller's process
%% @end
%% -----------------------------------------------------------------------------
-spec monitor(
    RemoteRef :: partisan:remote_pid() | partisan:remote_name(),
    Opts :: [partisan:monitor_opt()],
    Status :: {connected, boolean()} | noconnection | timeout | noproc
) ->
    partisan:remote_reference() | no_return().

monitor(Process, Opts, {connected, true}) ->
    %% We call the remote partisan_monitor process to
    %% request a monitor
    Node = partisan_remote_ref:node(Process),
    Channel = get_option(channel, Opts, ?DEFAULT_CHANNEL),
    Cmd = {monitor, Process, Opts},

    case call({?MODULE, Node}, Cmd) of
        {ok, Mref} ->
            %% We add a local reference to the remote Mref so we can
            %% fabricate a process DOWN signal locally when this channel
            %% (or the whole node) goes down. Indexing by `{Node, Channel}'
            %% lets a single-channel disconnection notify only the monitors
            %% bound to that channel.
            ok = add_proc_mon_out(Mref, Process, self(), Channel),
            ok = add_proc_mon_out_idx(Node, Channel, Mref),
            Mref;
        {error, timeout} ->
            %% partisan transport hasn't yet noticed the peer dropped, but
            %% disterl may already know. If the target node is no longer in
            %% `erlang:nodes()' (and never was — we never had a connection
            %% — or has been removed), fire `noconnection' immediately
            %% rather than reporting `timeout' (which callers compare to
            %% `nodedown'-style reasons).
            Status =
                case is_node_reachable(Node) of
                    false -> noconnection;
                    true -> timeout
                end,
            monitor(Process, Opts, Status);
        {error, noproc} ->
            monitor(Process, Opts, noproc);
        {error, {nodedown, _}} ->
            monitor(Process, Opts, noconnection);
        {error, Reason} ->
            ErrOpts = [{error_info, #{cause => Reason}}],
            erlang:error(Reason, [Process, Opts], ErrOpts)
    end;
monitor(Process, Opts, {connected, false}) ->
    monitor(Process, Opts, noconnection);
monitor(Process, _Opts, Reason) ->
    %% We reply a transient ref and we immediately send a DOWN signal
    Mref = partisan:make_ref(),

    %% Because this is performed in the caller's process the signal can only be
    %% received after we return.
    Down = {'DOWN', Mref, process, Process, Reason},
    self() ! Down,

    Mref.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
do_demonitor(Term) ->
    do_demonitor(Term, []).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
do_demonitor(Term, Opts) ->
    try
        Mref = decode_ref(Term),
        Bool = erlang:demonitor(Mref, Opts),

        case take_proc_mon_in(Mref) of
            #partisan_proc_mon_in{channel = Channel, monitor = Monitor} ->
                del_proc_mon_in_idx(partisan:node(Monitor), Channel, Mref);
            error ->
                ok
        end,

        {ok, Bool}
    catch
        _:_ ->
            {error, badarg}
    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
call(ServerRef, Message) ->
    call(ServerRef, Message, 5000).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
call(ServerRef, Message, Timeout) ->
    try
        partisan_gen_server:call(ServerRef, Message, Timeout)
    catch
        exit:{timeout, _} ->
            {error, timeout};
        exit:{noproc, _} ->
            {error, notalive}
    end.

%% @private
%% True if Node is currently reachable. When the runner is disterl-alive we
%% trust disterl as the authority: it detects TCP closure within ~1s, while
%% partisan transport's heartbeat-based detection can lag for many seconds.
%% In pure-partisan setups (no disterl) we fall back to partisan's view.
is_node_reachable(Node) ->
    case Node =:= partisan:node() of
        true ->
            true;
        false ->
            case erlang:is_alive() of
                true ->
                    lists:member(Node, erlang:nodes());
                false ->
                    partisan_peer_connections:is_connected(Node)
            end
    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc This functions assumes we have a singleton partisan_monitor server per
%% node.
%% @end
%% -----------------------------------------------------------------------------
is_monitor_server() ->
    {registered_name, ?MODULE} == erlang:process_info(self(), registered_name).

%% -----------------------------------------------------------------------------
%% @private
%% @doc This functions assumes we have a singleton partisan_monitor server per
%% node.
%% @end
%% -----------------------------------------------------------------------------
is_monitor_server(Process) when is_pid(Process), Process == self() ->
    true;
is_monitor_server(Process) when is_pid(Process) ->
    %% Validate. In principle this could not happen now that partisan:monitor
    %% only send us remote pids
    {registered_name, ?MODULE} == erlang:process_info(self(), registered_name);
is_monitor_server(Process) ->
    %% This does not catch the case where Process is the pid of a remote server,
    %% we just forward the message as checking for regname will also incur
    %% sending a message to the remote server.
    partisan_remote_ref:is_local_pid(Process, self()) orelse
        %% Or is this a remote partisan_monitor server?
        partisan_remote_ref:is_name(Process, ?MODULE).

%% @private
decode_ref(Ref) when is_reference(Ref) ->
    Ref;
decode_ref(RemoteRef) ->
    partisan_remote_ref:to_term(RemoteRef).

%% =============================================================================
%% PRIVATE: SIGNALING
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
on_nodeup(Node) when is_atom(Node) ->
    Msg = {nodeup, Node},
    ExtMsg = {nodeup, Node, []},

    notify_node_type_monitors(Msg, ExtMsg).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
on_nodedown(Node, Reason) when is_atom(Node) ->
    %% We send proc DOWN signal to all local monitors i.e. local processes
    %% monitoring remote processes on Node
    ok = notify_proc_mon_out(Node, Reason),

    %% We demonitor all proc_mon_in for remote monitors moniroting proceseses
    %% on this node. This is because Node will be sending them the process DOWN
    %% locally by executing this same function i.e. the previous call to
    %% notify_proc_mon_out/3
    lists:foreach(
        fun({_, Ref}) -> do_demonitor(Ref) end,
        proc_mon_in_indices(Node)
    ),

    %% Finally we need to notify individual and wildcard nodedown monitors
    Msg = {nodedown, Node},
    ExtMsg = {nodedown, Node, [{nodedown_reason, Reason}]},

    ok = notify_node_monitors(Node, Msg),

    ok = notify_node_type_monitors(Msg, ExtMsg).

%% -----------------------------------------------------------------------------
%% @private
%% @doc A single channel to `Node' has gone down.
%%
%% This handles the partisan-specific case of one channel disconnecting
%% while the rest of the node remains reachable. We notify only the
%% monitors bound to that channel — they get a DOWN with `noconnection'
%% — and demonitor the corresponding proc_mon_in entries on the remote
%% side equivalents (the remote side will run this same code for its
%% half).
%%
%% Monitors bound to other channels of `Node' are left intact. Node-level
%% subscribers (`monitor_nodes', `monitor_node') are also left untouched
%% — the node is still reachable on at least one channel.
%% @end
%% -----------------------------------------------------------------------------
on_channeldown(Node, Channel, Reason) when is_atom(Node), is_atom(Channel) ->
    %% Local processes monitoring remote processes on (Node, Channel)
    ok = notify_proc_mon_out(Node, Channel, Reason),

    %% Drop our local monitor on processes that remote nodes asked us to
    %% watch via this channel. The remote side runs the same logic and
    %% will fabricate the DOWN locally for its monitor.
    lists:foreach(
        fun({_, Ref}) -> do_demonitor(Ref) end,
        proc_mon_in_indices(Node, Channel)
    ),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc Fabricate a process DOWN signal with `Reason' to every local process
%% monitoring a remote process on `Node' (any channel) and clean up the cache.
%% @end
%% -----------------------------------------------------------------------------
notify_proc_mon_out(Node, Reason) ->
    notify_proc_mon_out_indices(proc_mon_out_indices(Node), Reason).

%% -----------------------------------------------------------------------------
%% @private
%% @doc Same as `notify_proc_mon_out/2', but limited to a single channel.
%% @end
%% -----------------------------------------------------------------------------
notify_proc_mon_out(Node, Channel, Reason) ->
    notify_proc_mon_out_indices(
        proc_mon_out_indices(Node, Channel), Reason
    ).

%% @private
notify_proc_mon_out_indices(Indices, Reason) ->
    lists:foreach(
        fun({{Node, Channel}, Mref}) ->
            case proc_mon_out(Mref) of
                [] ->
                    ok;
                [#partisan_proc_mon_out{} = M] ->
                    Monitored = M#partisan_proc_mon_out.monitored,
                    Monitor = M#partisan_proc_mon_out.monitor,

                    ok = del_proc_mon_out(M),
                    ok = del_proc_mon_out_idx(Node, Channel, Mref),

                    Down = {
                        'DOWN',
                        Mref,
                        process,
                        Monitored,
                        Reason
                    },

                    Monitor ! Down
            end
        end,
        Indices
    ),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
notify_node_monitors(Node, Msg) ->
    _ = [Pid ! Msg || {_, Pid} <- node_monitors(Node)],
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc We send the nodedown signal to all local processes monitoring
%% ALL nodes
%% @end
%% -----------------------------------------------------------------------------
notify_node_type_monitors(Msg, ExtMsg) ->
    ets:foldl(
        fun(#partisan_node_type_mon{key = {Pid, _}} = M, ok) ->
            case M#partisan_node_type_mon.nodedown_reason of
                true ->
                    partisan:forward_message(Pid, ExtMsg),
                    ok;
                false ->
                    partisan:forward_message(Pid, Msg),
                    ok
            end
        end,
        ok,
        ?NODE_TYPE_MON
    ),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec parse_nodemon_opts([partisan:monitor_nodes_opt()]) ->
    node_type_mon_opts().

parse_nodemon_opts(Opts0) when is_list(Opts0) ->
    Type =
        case lists:keyfind(node_type, 1, Opts0) of
            {node_type, Val} ->
                Val;
            false ->
                all
        end,
    InclReason = lists:member(nodedown_reason, Opts0),
    {Type, InclReason}.

%% =============================================================================
%% PRIVATE: STORAGE OPS
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_process_mon
    (reference(), pid(), partisan:remote_pid(), partisan:channel()) ->
        proc_mon_in();
    (reference(), partisan:remote_pid(), pid(), partisan:channel()) ->
        proc_mon_in().

new_process_mon(Mref, Monitored, Monitor, Channel) when
    is_reference(Mref) andalso
        (is_pid(Monitored) orelse is_atom(Monitored)) andalso
        is_atom(Channel)
->
    #partisan_proc_mon_in{
        ref = Mref,
        monitor = Monitor,
        channel = Channel,
        monitored = Monitored
    }.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
add_proc_mon_in(Node, Channel, Mref, Monitored, Monitor) ->
    Obj = new_process_mon(Mref, Monitored, Monitor, Channel),
    _ = ets:insert(?PROC_MON_IN, Obj),

    %% We create an index so that we can locate all proc_mon_in() objects
    %% associated with a (node, channel) pair. We index by `{Node, Channel}'
    %% (rather than by `Node' alone) so that a per-channel disconnection can
    %% notify only the affected monitors.
    add_proc_mon_in_idx(Node, Channel, Mref).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
take_proc_mon_in(Mref) ->
    case ets:take(?PROC_MON_IN, Mref) of
        [#partisan_proc_mon_in{ref = Mref} = M] ->
            M;
        [] ->
            error
    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_proc_mon_in_idx(node(), partisan:channel(), reference()) ->
    proc_mon_in_idx().

new_proc_mon_in_idx(Node, Channel, Mref) when
    is_atom(Node), is_atom(Channel), is_reference(Mref)
->
    {{Node, Channel}, Mref}.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
add_proc_mon_in_idx(Node, Channel, Mref) when is_reference(Mref) ->
    _ = ets:insert(?PROC_MON_IN_IDX, new_proc_mon_in_idx(Node, Channel, Mref)),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_proc_mon_in_idx(node(), partisan:channel(), reference()) -> ok.

del_proc_mon_in_idx(Node, Channel, Mref) when is_reference(Mref) ->
    _ = ets:delete_object(?PROC_MON_IN_IDX, {{Node, Channel}, Mref}),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc Return all proc_mon_in indices for `Node' across every channel.
%% @end
%% -----------------------------------------------------------------------------
-spec proc_mon_in_indices(node()) -> [proc_mon_in_idx()].

proc_mon_in_indices(Node) ->
    MS = [{{{Node, '_'}, '_'}, [], ['$_']}],
    ets:select(?PROC_MON_IN_IDX, MS).

%% -----------------------------------------------------------------------------
%% @private
%% @doc Return proc_mon_in indices for a specific (Node, Channel).
%% @end
%% -----------------------------------------------------------------------------
-spec proc_mon_in_indices(node(), partisan:channel()) -> [proc_mon_in_idx()].

proc_mon_in_indices(Node, Channel) ->
    ets:lookup(?PROC_MON_IN_IDX, {Node, Channel}).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_proc_mon_out(
    Mref :: partisan:remote_reference(),
    Monitored :: partisan:remote_pid() | partisan:remote_name(),
    Monitor :: pid(),
    Channel :: partisan:channel()
) -> ok.

add_proc_mon_out(Mref, Monitored, Monitor, Channel) when
    (is_pid(Monitor) orelse is_atom(Monitor)), is_atom(Channel)
->
    Obj = #partisan_proc_mon_out{
        ref = Mref,
        monitored = Monitored,
        monitor = Monitor,
        channel = Channel
    },
    _ = ets:insert(?PROC_MON_OUT, Obj),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
take_proc_mon_out(Mref) ->
    case ets:take(?PROC_MON_OUT, Mref) of
        [#partisan_proc_mon_out{ref = Mref} = M] ->
            M;
        [] ->
            error
    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_proc_mon_out(proc_mon_out() | partisan:remote_reference()) -> ok.

del_proc_mon_out(#partisan_proc_mon_out{} = Obj) ->
    true = ets:delete_object(?PROC_MON_OUT, Obj),
    ok;
del_proc_mon_out(Mref) ->
    true = ets:delete(?PROC_MON_OUT, Mref),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec proc_mon_out(partisan:remote_reference()) -> [proc_mon_out()].

proc_mon_out(Ref) ->
    ets:lookup(?PROC_MON_OUT, Ref).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_proc_mon_out_idx(
    node(), partisan:channel(), partisan:remote_reference()
) -> ok.

add_proc_mon_out_idx(Node, Channel, Mref) when
    is_atom(Node), is_atom(Channel), not is_reference(Mref)
->
    _ = ets:insert(?PROC_MON_OUT_IDX, {{Node, Channel}, Mref}),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_proc_mon_out_idx(
    node(), partisan:channel(), partisan:remote_reference()
) -> ok.

del_proc_mon_out_idx(Node, Channel, Mref) when
    is_atom(Node), is_atom(Channel), not is_reference(Mref)
->
    _ = ets:delete_object(?PROC_MON_OUT_IDX, {{Node, Channel}, Mref}),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc Return all proc_mon_out indices for `Node' across every channel.
%% @end
%% -----------------------------------------------------------------------------
-spec proc_mon_out_indices(node()) -> [proc_mon_out_idx()].

proc_mon_out_indices(Node) when is_atom(Node) ->
    MS = [{{{Node, '_'}, '_'}, [], ['$_']}],
    ets:select(?PROC_MON_OUT_IDX, MS).

%% -----------------------------------------------------------------------------
%% @private
%% @doc Return proc_mon_out indices for a specific (Node, Channel).
%% @end
%% -----------------------------------------------------------------------------
-spec proc_mon_out_indices(node(), partisan:channel()) -> [proc_mon_out_idx()].

proc_mon_out_indices(Node, Channel) when
    is_atom(Node), is_atom(Channel)
->
    ets:lookup(?PROC_MON_OUT_IDX, {Node, Channel}).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_node_monitor(node(), pid()) -> node_mon().

new_node_monitor(Node, Pid) ->
    {Node, Pid}.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_node_monitor(node(), pid()) -> true.

add_node_monitor(Node, Pid) ->
    _ = ets:insert(?NODE_MON, new_node_monitor(Node, Pid)),
    true.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_node_monitor(node(), pid()) -> true.

del_node_monitor(Node, Pid) ->
    _ = ets:delete_object(?NODE_MON, new_node_monitor(Node, Pid)),
    true.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node_monitors(node()) -> [node_mon()].

node_monitors(Node) ->
    ets:lookup(?NODE_MON, Node).

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_node_type_mon(pid(), node_type_mon_opts()) -> node_type_mon().

new_node_type_mon(Pid, {Type, Reason} = Opts) ->
    #partisan_node_type_mon{
        key = {Pid, erlang:phash2(Opts)},
        node_type = Type,
        nodedown_reason = Reason
    }.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_node_type_mon(pid(), node_type_mon_opts()) -> ok.

add_node_type_mon(Pid, Opts) ->
    Obj = new_node_type_mon(Pid, Opts),
    _ = ets:insert(?NODE_TYPE_MON, Obj),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_note_type_mon(pid(), node_type_mon_opts()) -> ok.

del_note_type_mon(Pid, Opts) ->
    Obj = new_node_type_mon(Pid, Opts),
    _ = ets:delete_object(?NODE_TYPE_MON, Obj),
    ok.
