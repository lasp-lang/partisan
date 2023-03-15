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
    enabled                         ::  boolean(),
    %% A map to store async requests
    requests                        ::  #{reference() => pid()},
    %% We cache a snapshot of the nodes, so that if we are terminated we can
    %% notify the subscriptions. This is the set of nodes we are currently
    %% connected to. Also this might be a partial view of the whole cluster,
    %% dependending on the peer_service_manager backend topology.
    nodes                           ::  sets:set(node())
}).


-record(partisan_proc_mon_in, {
    %% The local monitor reference obtaind by erlang:monitor/2
    ref                             ::  reference(),
    %% The local process that is being monitored
    monitored                       ::  pid() | atom(),
    %% A remote process monitoring a local process (monitored)
    monitor                         ::  partisan:remote_pid()
                                        | partisan:remote_name(),
    %% The channel signals should be forwarded on
    channel                         ::  partisan:channel()
}).

-record(partisan_proc_mon_out, {
    %% The remote monitor reference
    ref                             ::  partisan:remote_reference(),
    %% The remote process being monitored
    monitored                       ::  partisan:remote_pid()
                                        | partisan:remote_name(),
    %% A local process monitoring the remote process
    monitor                         ::  pid() | atom()
}).


-record(partisan_node_type_mon, {
    key                             ::  {Monitor :: pid(), Hash :: integer()},
    node_type                       ::  all | visible | hidden,
    nodedown_reason                 ::  boolean()
}).


%% TODO add channel
-type proc_mon_in()                 ::  #partisan_proc_mon_in{}.
-type proc_mon_in_idx()             ::  {node(), reference()}.
-type proc_mon_out()                ::  #partisan_proc_mon_out{}.
-type proc_mon_out_idx()            ::  {node(), partisan:remote_reference()}.
-type node_mon()                    ::  {node(), pid()}.
-type node_type_mon()               ::  #partisan_node_type_mon{}.
-type node_type_mon_opts()          ::  {
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
%% @doc When you attempt to monitor a remote process, it is not
%% guaranteed that you will receive the DOWN message. A few reasons for not
%% receiving the message are message loss and tree reconfiguration.
%% The monitor options `Opts' are currently ignored.
%%
%% Failure:
%% <dl>
%% <dt>`notalive'</dt><dd>If the partisan_monitor server process is not
%% alive.</dd>
%% <dt>`not_implemented'</dt><dd>If the partisan peer service manager in use
%% doesn't support the capabilities required for monitoring.</dd>
%% <dt>`badarg'</dt><dd>If any of the arguments are invalid.</dd>
%% </dl>
%% @end
%% -----------------------------------------------------------------------------
-spec monitor(
    Process :: partisan:remote_pid() | partisan:remote_name(),
    Opts :: [partisan:monitor_opt()]) -> partisan:remote_reference() | no_return().

monitor(Process, Opts) when is_list(Opts) ->
    partisan_remote_ref:is_pid(Process)
        orelse partisan_remote_ref:is_name(Process)
        orelse error(badarg),

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
                    %% Whether we fallback to the default channel if the
                    %% requested channel is not connected
                    Fallback = get_option(channel_fallback, Opts, true),

                    IsConnected =
                        case Channel of
                            ?DEFAULT_CHANNEL ->
                                partisan_peer_connections:is_connected(Node);
                            _ ->
                                %% channel is connected orelse
                                %% falling back to the default channel is
                                %% enabled and default channel is connected
                                partisan_peer_connections:is_connected(
                                    Node, Channel
                                ) orelse (
                                    Fallback == true andalso
                                    partisan_peer_connections:is_connected(
                                        Node
                                    )
                                )
                        end,

                    %% Finally monitor
                    monitor(Process, Opts, {connected, IsConnected})
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% Failure:
%% <ul>
%% <li>`notalive' if the partisan_monitor process is not alive.</li>
%% <li>`not_implemented' if the partisan peer service manager does not support
%% the required capabilities required for monitoring.</li>
%% <li>`badarg' if any of the arguments is invalid.</li>
%% </ul>
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(
    MonitoredRef :: partisan:remote_reference(),
    Opts :: [partisan:demonitor_opt()]
    ) -> boolean() | no_return().

demonitor(MPRef, Opts) ->
    partisan_remote_ref:is_reference(MPRef)
        orelse erlang:error(badarg, [MPRef, Opts], [
            {error_info, #{
                cause => #{1 => "not a partisan remote reference"}
            }}
        ]),

    Skip =
        %% Is this a dummy reference or a circular call?
        MPRef == persistent_term:get(?DUMMY_MREF_KEY)
        orelse is_monitor_server(),

    case Skip of
        true ->
            true;

        false ->
            Node = partisan_remote_ref:node(MPRef),

            %% We remove the local references.
            ok = del_proc_mon_out(MPRef),
            ok = del_proc_mon_out_idx(Node, MPRef),

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
                            after
                                0 ->
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
%% @doc The calling process subscribes or unsubscribes to node status change
%% messages. A nodeup message is delivered to all subscribing processes when a
%% new node is connected, and a nodedown message is delivered when a node is
%% disconnected.
%% If Flag is true, a new subscription is started. If Flag is false, all
%% previous subscriptions started with the same Options are stopped. Two option
%% lists are considered the same if they contain the same set of options.
%%
%% Notice that the following two disterl guarantees are NOT yet provided by
%% Partisan:
%% <ul>
%% <li>`nodeup' messages are delivered before delivery of any message from the
%% remote node passed through the newly established connection.</li>
%% <li>`nodedown' messages are not delivered until all messages from the remote
%% node that have been passed through the connection have been delivered.</li>
%% </ul>
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
    %% We trap exists so that we get a call to terminate w/reason shutdown when
    %% the supervisor terminates us. This happens when
    %% partisan_peer_service:manager() is terminated.
    erlang:process_flag(trap_exit, true),

    %% We subscribe to node status to implement node monitoring.
    %% Certain partisan_peer_service_manager implementations might not support
    %% the on_up and on_down events which we need for node monitoring, so in
    %% those cases this module will not work.
    Enabled = subscribe_to_node_status(),
    _ = persistent_term:put({?MODULE, enabled}, Enabled),


    _ = subscribe_to_channel_status(),

    %% partisan_gen behaviours call monitor/2 and demonitor/1 so being
    %% this server one, that would create a deadlock due to a circular call.
    %% We use a static dummy ref for those calls to avoid calling ourselves.
    _ = persistent_term:put(?DUMMY_MREF_KEY, partisan:make_ref()),

    TabOpts = [
        named_table,
        public,
        {write_concurrency, true},
        {read_concurrency, true},
        {decentralized_counters, true}
    ],

    %% Tables for process monitoring
    _ = ets:new(?PROC_MON_IN,       [set, {keypos, 2} | TabOpts]),
    _ = ets:new(?PROC_MON_IN_IDX,   [bag, {keypos, 1} | TabOpts]),
    _ = ets:new(?PROC_MON_OUT,      [set, {keypos, 2} | TabOpts]),
    _ = ets:new(?PROC_MON_OUT_IDX,  [bag, {keypos, 1} | TabOpts]),

    %% Tables for node status monitoring
    _ = ets:new(?NODE_MON,          [duplicate_bag, {keypos, 1} | TabOpts]),
    _ = ets:new(?NODE_TYPE_MON,     [set, {keypos, 2} | TabOpts]),

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


handle_cast({'DOWN', Mref, process, _Process, Reason}, State) ->
    %% A down signal for a remote process forwarded to us by a remote
    %% partisan_monitor server.
    %% We do this as ooposed to sending the signal to each process directly as
    %% we need to cleanup the local state i.e. proc_mon_out() and
    %% proc_mon_out_idx().
    Fun = fun() ->
        case take_proc_mon_out(Mref) of
            #partisan_proc_mon_out{} = M ->
                Monitor = M#partisan_proc_mon_out.monitor,
                Monitored = M#partisan_proc_mon_out.monitored,
                Node = partisan_remote_ref:node(Monitored),

                ok = del_proc_mon_out_idx(Node, Mref),

                Tag = 'DOWN', %% can be user-defined tag when we enabled it

                Down = {
                    Tag,
                    Mref,
                    process,
                    Monitored,
                    Reason
                },
                partisan:send(Monitor, Down);

            error ->
                ok
        end,
        ok
    end,

    _ = spawn(Fun),

    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_, #state{enabled = false} = State) ->
    %% Functionality disabled
    {noreply, State};

handle_info({'DOWN', Mref, process, _Process, Reason}, State) ->
    %% A process we are monitoring has terminated
    %% We asynchronously send the process down signal to the monitors

    Fun = fun() ->
        case take_proc_mon_in(Mref) of
            #partisan_proc_mon_in{} = M ->
                Monitored0 = M#partisan_proc_mon_in.monitored,
                Monitor = M#partisan_proc_mon_in.monitor,
                Channel = M#partisan_proc_mon_in.channel,

                %% Cleanup index
                Node = partisan:node(Monitor),
                del_proc_mon_in_idx(Node, Mref),

                %% Send down signal
                Monitored = partisan_remote_ref:from_term(Monitored0),

                ok = cast_signal(
                    Monitor,
                    'DOWN',
                    Mref,
                    %% eqwalizer:ignore Monitored
                    Monitored,
                    Reason,
                    %% eqwalizer:ignore Opts
                    [{channel, Channel}]
                );
            error ->
                ok
        end
    end,

    _ = spawn(Fun),

    {noreply, State};

handle_info({nodeup, Node}, State0) ->
    %% Either a net_kernel or Partisan signal

    ?LOG_DEBUG(#{
        description => "Got nodeup signal",
        node => Node
    }),

    %% We process the signal asynchronously
    _ = spawn(fun() -> on_nodeup(Node) end),

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

    %% We process the signal asynchronously
    _ = spawn(fun() -> on_nodedown(Node, noconnection) end),

    State = State0#state{
        nodes = sets:del_element(Node, State0#state.nodes)
    },

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
    Status :: {connected, boolean()} | noconnection | timeout | noproc) ->
    partisan:remote_reference() | no_return().

monitor(Process, Opts, {connected, true}) ->
    %% We call the remote partisan_monitor process to
    %% request a monitor
    Node = partisan_remote_ref:node(Process),
    Cmd = {monitor, Process, Opts},

    case call({?MODULE, Node}, Cmd) of
        {ok, Mref} ->
            %% We add a local reference to the remote Mref so that we can
            %% simulate a process DOWN signal when node is down
            ok = add_proc_mon_out(Mref, Process, self()),
            ok = add_proc_mon_out_idx(Node, Mref),
            Mref;
        {error, timeout} ->
            monitor(Process, Opts, timeout);
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
            #partisan_proc_mon_in{} = M ->
                Mref = M#partisan_proc_mon_in.ref,
                Monitor = M#partisan_proc_mon_in.monitor,
                del_proc_mon_in_idx(partisan:node(Monitor), Mref);
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
    partisan_remote_ref:is_local_pid(Process, self())
        %% Or is this a remote partisan_monitor server?
        orelse partisan_remote_ref:is_name(Process, ?MODULE).


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
-spec cast_signal(
    Dest :: partisan:remote_pid() | partisan:remote_name(),
    Tag  :: term(),
    Mref :: reference() | partisan:remote_reference(),
    Monitored :: partisan:remote_pid() | partisan:remote_name(),
    Reason :: any(),
    Opts :: [tuple()]
    ) -> ok.

cast_signal(Dest, Tag, Mref0, Monitored, Reason, Opts)
when is_reference(Mref0) ->
    Mref = partisan_remote_ref:from_term(Mref0),
    %% eqwalizer:ignore Mref
    cast_signal(Dest, Tag, Mref, Monitored, Reason, Opts);

cast_signal(Dest, Tag, Mref, {Name, Node}, Reason, Opts)
when is_atom(Name), is_atom(Node) ->
    Ref = partisan_remote_ref:from_term(Name, Node),
    %% eqwalizer:ignore Ref
    cast_signal(Dest, Tag, Mref, Ref, Reason, Opts);

cast_signal(Dest, Tag, Mref, Monitored, Reason, Opts) ->
    Node = partisan_remote_ref:node(Dest),

    Down = {
        Tag,
        Mref,
        process,
        Monitored,
        Reason
    },

    try
        partisan_gen_server:cast({?MODULE, Node}, Down, Opts)
    catch
        exit:noproc ->
            ok
    end.


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
%% @doc We send the process DOWN signal to all local processes monitoring
%% remote processes on Node and remove the cache.
%% @end
%% -----------------------------------------------------------------------------
notify_proc_mon_out(Node, Reason) ->
    lists:foreach(
        fun
            ({_, Mref}) ->
                case proc_mon_out(Mref) of
                    [] ->
                        ok;
                    [#partisan_proc_mon_out{} = M] ->
                        Monitored = M#partisan_proc_mon_out.monitored,
                        Monitor = M#partisan_proc_mon_out.monitor,

                        ok = del_proc_mon_out(M),
                        ok = del_proc_mon_out_idx(Node, Mref),

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
        proc_mon_out_indices(Node)
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
    Type = case lists:keyfind(node_type, 1, Opts0) of
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

new_process_mon(Mref, Monitored, Monitor, Channel)
when is_reference(Mref) andalso
(is_pid(Monitored) orelse is_atom(Monitored)) andalso
is_atom(Channel) ->
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
    %% associated with a node
    add_proc_mon_in_idx(Node, Mref).


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
-spec new_proc_mon_in_idx(Node :: node(), Ref :: reference()) ->
    proc_mon_in_idx().

new_proc_mon_in_idx(Node, Mref) when is_atom(Node), is_reference(Mref) ->
    {Node, Mref}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
add_proc_mon_in_idx(Node, Mref) when is_reference(Mref) ->
    _ = ets:insert(?PROC_MON_IN_IDX, new_proc_mon_in_idx(Node, Mref)),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_proc_mon_in_idx(node(), reference()) -> ok.

del_proc_mon_in_idx(Node, Mref) when is_reference(Mref) ->
    _ = ets:delete_object(?PROC_MON_IN_IDX, {Node, Mref}),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec proc_mon_in_indices(node()) -> [proc_mon_in_idx()].

proc_mon_in_indices(Node) ->
    ets:lookup(?PROC_MON_IN_IDX, Node).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_proc_mon_out(
    Mref :: partisan:remote_reference(),
    Monitored :: partisan:remote_pid() | partisan:remote_name(),
    Monitor :: pid()
 ) -> ok.

add_proc_mon_out(Mref, Monitored, Monitor)
when is_pid(Monitor); is_atom(Monitor) ->
    Obj = #partisan_proc_mon_out{
        ref = Mref,
        monitored = Monitored,
        monitor = Monitor
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
-spec add_proc_mon_out_idx(node(), partisan:remote_reference()) -> ok.

add_proc_mon_out_idx(Node, Mref) when is_atom(Node), not is_reference(Mref) ->
    _ = ets:insert(?PROC_MON_OUT_IDX, {Node, Mref}),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_proc_mon_out_idx(node(), partisan:remote_reference()) -> ok.

del_proc_mon_out_idx(Node, Mref) when is_atom(Node), not is_reference(Mref) ->
    _ = ets:delete_object(?PROC_MON_OUT_IDX, {Node, Mref}),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec proc_mon_out_indices(node()) -> [proc_mon_out_idx()].

proc_mon_out_indices(Node) when is_atom(Node) ->
    ets:lookup(?PROC_MON_OUT_IDX, Node).




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
