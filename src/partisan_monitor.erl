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
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_monitor).

-behaviour(partisan_gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-define(REF_KEY, {?MODULE, ref}).
-define(DUMMY_MREF_KEY, {?MODULE, monitor_ref}).
-define(IS_ENABLED, persistent_term:get({?MODULE, enabled})).

%% stores process_monitor()
%% local process mrefs held on behalf of remote processes
-define(PROC_MON_TAB, partisan_monitor_process_mon).
%% refs grouped by node, used to notify/cleanup on a nodedown signal
%% stores process_monitor_idx()
-define(PROC_MON_NODE_IDX_TAB, partisan_monitor_process_mon_node_idx).
%% local cache for a remote process monitor
-define(PROC_MON_CACHE_TAB, partisan_monitor_process_mon_cache).
%% Local pids that are monitoring individual node
%% stores node_monitor()
-define(NODE_MON_TAB, partisan_monitor_node_mon).
%% Local pids that are monitoring all nodes
%% stores nodes_monitor()
-define(NODES_MON_TAB, partisan_monitor_nodes_mon).

-record(state, {
    %% whether monitoring is enabled,
    %% depends on partisan_peer_service_manager offering support for on_up/down
    enabled                     ::  boolean(),
    %% We cache a snapshot of the nodes, so that if we are terminated we can
    %% notify the subscriptions. This is the set of nodes we are currently
    %% connected to. Also this might be a partial view of the whole cluster,
    %% dependending on the peer_service_manager backend topology.
    nodes                       ::  sets:set(node())
}).


%% TODO add channel
-type process_monitor()         ::  {
                                        Mref :: reference(),
                                        MPid :: pid(),
                                        Owner :: partisan_remote_ref:p()
                                                | partisan_remote_ref:n()
                                    }.
-type process_monitor_idx()     ::  {node(), reference()}.
-type process_monitor_cache()   ::  {
                                        Node :: node(),
                                        Mref :: reference(),
                                        MPid :: partisan_remote_ref:p()
                                                | partisan_remote_ref:n(),
                                        Owner :: pid()

                                    }.
-type node_monitor()            ::  {node(), pid()}.
-type nodes_monitor()           ::  {
                                        {
                                            Owner :: pid(),
                                            Hash :: integer()
                                        },
                                        nodes_monitor_opts()
                                    }.
-type nodes_monitor_opts()      ::  {
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
%% receiving the message are message loss, tree reconfiguration and the node
%% is no longer reachable.
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
    RemoteRef :: partisan_remote_ref:p() | partisan_remote_ref:n(),
    Opts :: partisan:monitor_opts()) -> partisan_remote_ref:r() | no_return().

monitor(RemoteRef, Opts) when is_list(Opts) ->
    partisan_remote_ref:is_pid(RemoteRef)
        orelse partisan_remote_ref:is_name(RemoteRef)
        orelse error(badarg),

    %% This might be a circular call, in which case we need to skip the
    %% monitoring request. This occurs because this server implements the
    %% partisan_gen_server behaviour and both partisan_gen_server and
    %% partisan_gen use this server for monitoring.
    %% To solve the issue, we return a static dummy reference,
    %% We also need to skip the request when somebody else is trying to monitor
    %% this server or a remote partisan_monitor server (saving the roundtrip).
    Skip =
        %% Is this a circular call?
        {registered_name, ?MODULE} ==
            erlang:process_info(self(), registered_name)
        %% Is this a remote partisan_monitor server?
        orelse partisan_remote_ref:is_name(RemoteRef, ?MODULE)
        %% Is somebody else trying to monitor us?
        orelse is_self(RemoteRef),


    case Skip of
        true ->
            %% Return a static dummy reference and ignore the request
            persistent_term:get(?DUMMY_MREF_KEY);

        false ->
            Channel = get_option(channel, Opts, nochannel),
            %% Wethere we fallback to the default channel if the requested
            %% channel is not connected
            Fallback = get_option(channel_fallback, Opts, true),

            case partisan_remote_ref:is_local(RemoteRef) of
                true ->
                    PidOrRegName = partisan_remote_ref:to_term(RemoteRef),
                    %% partisan:monitor while coerce Opts to erlang monitor opts
                    partisan:monitor(process, PidOrRegName, Opts);

                false ->
                    Node = partisan_remote_ref:node(RemoteRef),
                    Channel = get_option(channel, Opts, ?DEFAULT_CHANNEL),
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

                    monitor(RemoteRef, Opts, {connection, IsConnected})

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
    RemoteRef :: partisan_remote_ref:r(), Opts :: partisan:demonitor_opts()) ->
    boolean() | no_return().

demonitor(RemoteRef, Opts) ->
    partisan_remote_ref:is_reference(RemoteRef) orelse error(badarg),

    Skip =
        %% Is this a dummy reference or a cicular call?
        RemoteRef == persistent_term:get(?DUMMY_MREF_KEY)
        orelse {registered_name, ?MODULE} ==
            erlang:process_info(self(), registered_name),

    case Skip of
        true ->
            true;

        false ->
            %% We remove the local cache
            ok = del_process_monitor_cache(RemoteRef),

            %% We call the remote node to demonitor
            Node = partisan_remote_ref:node(RemoteRef),
            Cmd = {demonitor, RemoteRef, Opts},

            case call({?MODULE, Node}, Cmd) of
                {ok, Bool} ->
                    case lists:member(flush, Opts) of
                        true ->
                            receive
                                {_, RemoteRef, _, _, _} ->
                                    Bool
                            after
                                0 ->
                                    Bool
                            end;
                        false ->
                            Bool
                    end;
                {error, timeout} ->
                    true;
                {error, noproc} ->
                    true;
                {error, {nodedown, _}} ->
                    true;
                {error, badarg} ->
                    error(badarg)
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
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_node(node() | partisan:node_spec(), boolean()) -> true.

monitor_node(#{name := Node}, Flag) ->
    monitor_node(Node, Flag);

monitor_node(Node, Flag) when is_atom(Node) ->
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
                    add_nodes_mon(self(), Opts);
                Opts when Flag == false ->
                    del_nodes_mon(self(), Opts)
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
    _ = persistent_term:put(?REF_KEY, partisan_remote_ref:from_term(?MODULE)),
    _ = persistent_term:put(?DUMMY_MREF_KEY, partisan:make_ref()),

    TabOpts = [
        named_table,
        public,
        {keypos, 1},
        {write_concurrency, true},
        {read_concurrency, true}
    ],

    %% Table to record remote processes monitoring a local processes
    %% contains process_monitor() records
    ?PROC_MON_TAB = ets:new(?PROC_MON_TAB, [set | TabOpts]),

    %% An index over ?PROC_MON_TAB
    %% contains process_monitor_idx() records
    ?PROC_MON_NODE_IDX_TAB = ets:new(?PROC_MON_NODE_IDX_TAB, [bag | TabOpts]),

    %% Table to record local processes monitoring a remote processes
    %% For every record in ?PROC_MON_TAB we a companion record in this table,
    %% this is to be able to send a signal to the local monitoring process a
    %% remote process when the remote node crashes.
    %% contains process_monitor_cache() records
    ?PROC_MON_CACHE_TAB = ets:new(?PROC_MON_CACHE_TAB, [bag | TabOpts]),

    %% Table to record local processes monitoring nodes
    %% contains node_monitor() records
    ?NODE_MON_TAB = ets:new(?NODE_MON_TAB, [duplicate_bag | TabOpts]),

    %% contains nodes_monitor() records
    ?NODES_MON_TAB = ets:new(?NODES_MON_TAB, [set | TabOpts]),

    State = #state{
        enabled = Enabled,
        %% A snapshot of the membership view
        nodes = sets:new([{version, 2}])
    },

    {ok, State}.


handle_call(_, _, #state{enabled = false} = State) ->
    %% The peer service manager does not implement support for remote monitoring
    %% Instead of failing we return the dummy ref
    Reply = {ok, persistent_term:get(?DUMMY_MREF_KEY)},
    {reply, Reply, State};

handle_call({monitor, RemoteRef, _}, {RemoteRef, _}, State) ->
    %% A circular call (partisan_gen)
    Reply = {ok, persistent_term:get(?DUMMY_MREF_KEY)},
    {reply, Reply, State};

handle_call({monitor, RemoteRef, _Opts}, {RemotePid, _}, State) ->
    %% A remote process (RemotePid) wants to monitor a process (RemoteRef) on
    %% this node.

    %% We did this check in monitor/2, but we double check again in case
    %% someone is calling partisan_gen_server:call directly.
    Reply =
        case is_self(RemoteRef) of
            true ->
                %% The case for a process monitoring this server, this server
                %% monitoring itself or another node's monitor server monitoring
                %% this one.
                {ok, persistent_term:get(?DUMMY_MREF_KEY)};

            false ->
                %% TODO Implement options
                %%  {tag, UserDefinedTag} option
                try
                    Node = partisan_remote_ref:node(RemotePid),

                    %% RemoteRef can be a pid or registered name for a local
                    %% process
                    PidOrRegName = partisan_remote_ref:to_term(RemoteRef),

                    %% We monitor the process on behalf of the remote caller.
                    %% We will handle the EXIT signal and forward it to
                    %% RemotePid when it occurs.
                    Mref = partisan:monitor(process, PidOrRegName),

                    %% We track the Mref to match the 'DOWN' signal
                    ok = add_process_monitor(
                        Node, Mref, PidOrRegName, RemotePid
                    ),

                    %% We reply with the encoded monitor reference
                    {ok, partisan_remote_ref:from_term(Mref)}

                catch
                    error:badarg ->
                        {error, badarg}
                end
        end,

    {reply, Reply, State};

handle_call({demonitor, RemoteRef, _}, {RemoteRef, _}, State) ->
    %% A circular call: this server monitoring itself (partisan_gen)
    {reply, true, State};

handle_call({demonitor, RemoteRef, Opts}, {_RemotePid, _}, State) ->
    %% A remote process is requesting a demonitor
    case RemoteRef == persistent_term:get(?DUMMY_MREF_KEY) of
        true ->
            %% We skip.
            %% The case for a process monitoring this server or another node's
            %% monitor server monitoring this one.
            {reply, true, State};
        false ->
            Reply = do_demonitor(RemoteRef, Opts),
            {reply, Reply, State}
    end;

handle_call(_Msg, _From, State) ->
    {reply, {error, unsupported_call}, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_, #state{enabled = false} = State) ->
    %% Functionality disabled
    {noreply, State};

handle_info({'DOWN', Mref, process, _ServerRef, Reason}, State) ->
    case take_process_mon(Mref) of
        {Mref, PidOrName, OwnerRef} ->
            %% Local process down signal
            Node = partisan:node(OwnerRef),
            del_process_monitor_idx(Node, Mref),
            ok = send_process_down(OwnerRef, 'DOWN', Mref, PidOrName, Reason);
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

    ok = send_nodeup(Node),

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

    ok = send_nodedown(Node, connection_closed),

    State = State0#state{
        nodes = sets:del_element(Node, State0#state.nodes)
    },

    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.


terminate(shutdown, State) ->
    send_self_down(shutdown, State);


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
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec monitor(
    RemoteRef :: partisan_remote_ref:p() | partisan_remote_ref:n(),
    Opts :: [partisan:monitor_opt()],
    Reason :: atom() | {connection, boolean()}) ->
    partisan_remote_ref:r() | no_return().

monitor(RemoteRef, Opts, {connection, true}) ->
    %% We call the remote partisan_monitor process to
    %% request a monitor
    Node = partisan_remote_ref:node(RemoteRef),
    Cmd = {monitor, RemoteRef, Opts},
    case call({?MODULE, Node}, Cmd) of
        {ok, Mref} ->
            %% We add a local reference to the remote Mref so that we can
            %% simulate a process DOWN signal when node is down
            ok = add_process_monitor_cache(Node, Mref, RemoteRef, self()),
            Mref;
        {error, timeout} ->
            monitor(RemoteRef, Opts, timeout);
        {error, noproc} ->
            monitor(RemoteRef, Opts, noproc);
        {error, {nodedown, _}} ->
            monitor(RemoteRef, Opts, noconnection);
        {error, Reason} ->
            ErrOpts = [
                {error_info, #{
                    cause => Reason,
                    module => ?MODULE,
                    function => monitor
                }}
            ],
            erlang:error(Reason, [RemoteRef, Opts], ErrOpts)
    end;

monitor(RemoteRef, Opts, {connection, false}) ->
    monitor(RemoteRef, Opts, noconnection);

monitor(RemoteRef, _, Reason) ->
    %% We reply a transient ref but we do not store the request as we are
    %% immediately sending a DOWN signal
    Mref = partisan:make_ref(),
    ok = send_process_down(self(), 'DOWN', Mref, {ref, RemoteRef}, Reason),
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

        case take_process_mon(Mref) of
            {Mref, _, OwnerRef} ->
                del_process_monitor_idx(partisan:node(OwnerRef), Mref);
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
    try
        partisan_gen_server:call(ServerRef, Message)
    catch
        exit:noproc ->
            exit(notalive)
    end.


%% @private
is_self(Pid) when is_pid(Pid) ->
    Pid == self();

is_self(RemoteRef) when is_tuple(RemoteRef); is_binary(RemoteRef) ->
    SelfRef = persistent_term:get(?REF_KEY),
    partisan_remote_ref:is_identical(RemoteRef, SelfRef).


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
-spec send_process_down(
    Dest :: partisan_remote_ref:p() | partisan_remote_ref:n() | pid() | atom(),
    Tag  :: term(),
    Mref :: reference() | partisan_remote_ref:r(),
    Term :: {ref, partisan_remote_ref:p() | partisan_remote_ref:n()}
    | pid() | atom(),
    Reason :: any()) -> ok.

send_process_down(Dest, Tag, Mref0, {ref, PRef}, Reason)
when is_reference(Mref0) ->
    Mref = partisan_remote_ref:from_term(Mref0),
    send_process_down(Dest, Tag, Mref, {ref, PRef}, Reason);

send_process_down(Dest, Tag, Mref, {ref, PRef}, Reason) ->
    Down = {
        Tag,
        Mref,
        process,
        PRef,
        Reason
    },
    partisan:forward_message(Dest, Down);

send_process_down(Dest, Tag, Mref, Term, Reason)
when is_pid(Term) orelse is_atom(Term) ->
    Ref = partisan_remote_ref:from_term(Term),
    send_process_down(Dest, Tag, Mref, {ref, Ref}, Reason);

send_process_down(Dest, Tag, Mref, {Name, Node}, Reason)
when is_atom(Name), is_atom(Node) ->
    Ref = partisan_remote_ref:from_term(Name, Node),
    send_process_down(Dest, Tag, Mref, {ref, Ref}, Reason).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
send_nodeup(Node) when is_atom(Node) ->
    Msg = {nodeup, Node},
    ExtMsg = {nodeup, Node, []},

    ets:foldl(
        fun
            ({{Pid, _Hash}, {_Type, true}}, ok) ->
                partisan:forward_message(Pid, ExtMsg);
            ({{Pid, _Hash}, {_Type, false}}, ok) ->
                partisan:forward_message(Pid, Msg)
        end,
        ok,
        ?NODES_MON_TAB
    ).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
send_nodedown(Node, Reason) when is_atom(Node) ->
    %% We assume Node has crashed.

    %% 1. So we send the process DOWN signal to all local processes monitoring
    %% remote processes on Node and remove the cache.
    lists:foreach(
        fun
            ({_, Mref, MPid, Owner}) ->
                %% A local index to a remote monitor
                ok = del_process_monitor_cache(Mref),
                send_process_down(
                    Owner, 'DOWN', Mref, {ref, MPid}, noconnection
                )
        end,
        process_monitor_caches(Node)
    ),

    %% 2. We demonitor all monitors with owners in Node (monitors originating
    %% in Node with targets in this node)
    %% REVIEW we could keep them in case the node comes back
    lists:foreach(
        fun({_, Ref}) -> do_demonitor(Ref) end,
        process_monitor_indices(Node)
    ),

    %% 3. We send a nodedown signal to the local processes monitoring Node.
    %% We then demonitor.
    Msg = {nodedown, Node},
    ExtMsg = {nodedown, Node, [{nodedown_reason, Reason}]},
    Pids = take_node_monitors(Node),
    [partisan:forward_message(Pid, Msg) || {_, Pid} <- Pids],


    %% 4. We send the nodedown signal to all processes monitoring ALL nodes
    ets:foldl(
        fun
            ({{Pid, _Hash}, {_Type, true}}, ok) ->
                partisan:forward_message(Pid, ExtMsg);
            ({{Pid, _Hash}, {_Type, false}}, ok) ->
                partisan:forward_message(Pid, Msg)
        end,
        ok,
        ?NODES_MON_TAB
    ).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
send_self_down(Reason, State) ->
    %% We are shutting down or crashing so we do not cleanup ets, it will be
    %% done automatically.

    %% 1. We send the process DOWN signal to all local processes monitoring
    %% remote processes on remote nodes.
    ets:foldl(
        fun
            ({_, Mref, MPid, Owner}, ok) ->
                %% A local index to a remote monitor
                send_process_down(
                    Owner, 'DOWN', Mref, {ref, MPid}, noconnection
                )
        end,
        ok,
        ?PROC_MON_CACHE_TAB
    ),

    %% 2. We send a nodedown message to the local processes monitoring the
    %% individual nodes
    ets:foldl(
        fun({Node, Pid}, ok) ->
            partisan:forward_message(Pid, {nodedown, Node}),
            ok
        end,
        ok,
        ?NODE_MON_TAB
    ),

    %% 3. We send the nodedown signal to all processes monitor ALL nodes
    ExtReason = [{nodedown_reason, Reason}],

    Acc0 = ok,

    ets:foldl(
        fun
            ({{Pid, _Hash}, {_Type, true}}, Acc) ->
                sets:fold(
                    fun(N, IAcc) ->
                        partisan:forward_message(Pid, {nodedown, N, ExtReason}),
                        IAcc
                    end,
                    Acc,
                    State#state.nodes
                );

            ({{Pid, _Hash}, {_Type, false}}, Acc) ->
                sets:fold(
                    fun(N, IAcc) ->
                        partisan:forward_message(Pid, {nodedown, N}),
                        IAcc
                    end,
                    Acc,
                    State#state.nodes
                )
        end,
        Acc0,
        ?NODES_MON_TAB
    ).



%% =============================================================================
%% PRIVATE: TYPES
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_process_mon
    (reference(), pid(), partisan_remote_ref:p()) ->
        process_monitor();
    (reference(), partisan_remote_ref:p(), pid()) ->
        process_monitor().

new_process_mon(Mref, MPid, Owner) when is_pid(MPid) orelse is_atom(MPid) ->
    {Mref, MPid, Owner};

new_process_mon(Mref, MPid, Owner) when is_pid(Owner) orelse is_atom(MPid) ->
    {Mref, MPid, Owner}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_process_monitor_idx(Node :: node(), Ref :: reference()) ->
    process_monitor_idx().

new_process_monitor_idx(Node, Mref) ->
    {Node, Mref}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_process_monitor_cache(
    Node :: node(),
    Mref :: partisan_remote_ref:r(),
    RPid :: partisan_remote_ref:p() | partisan_remote_ref:n(),
    Owner :: pid()) -> process_monitor_cache().

new_process_monitor_cache(Node, Mref, RPid, Owner)
when (is_pid(Owner) orelse is_atom(Owner)) ->
    %% This for tracking a remote monitor locally, so that if node goes down we
    %% have a way to send a process DOWN signal
    {Node, Mref, RPid, Owner}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_node_monitor(node(), pid()) -> node_monitor().

new_node_monitor(Node, Pid) ->
    {Node, Pid}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new_nodes_mon(pid(), nodes_monitor_opts()) -> nodes_monitor().

new_nodes_mon(Pid, Opts) when is_tuple(Opts) ->
    {{Pid, erlang:phash2(Opts)}, Opts}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec parse_nodemon_opts([partisan:monitor_nodes_opt()]) ->
    nodes_monitor_opts().

parse_nodemon_opts(Opts0) ->
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
add_process_monitor(Node, Mref, LocalProcess, RemoteOwner) ->
    %% 1. monitor ref -> {monitored pid, caller}
    _ = ets:insert_new(
        ?PROC_MON_TAB, new_process_mon(Mref, LocalProcess, RemoteOwner)
    ),

    %% 2. an index to fetch all refs associated with a remote node
    add_process_monitor_idx(Node, Mref).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
take_process_mon(Mref) ->
    case ets:take(?PROC_MON_TAB, Mref) of
        [{Mref, _, _} = Existing] ->
            Existing;
        [] ->
            error
    end.



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
add_process_monitor_idx(Node, Mref) when is_reference(Mref) ->
    _ = ets:insert_new(
        ?PROC_MON_NODE_IDX_TAB, new_process_monitor_idx(Node, Mref)
    ),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_process_monitor_idx(node(), reference()) -> ok.

del_process_monitor_idx(Node, Mref) when is_reference(Mref) ->
    _ = ets:delete_object(?PROC_MON_NODE_IDX_TAB, {Node, Mref}),
    ok.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec process_monitor_indices(node()) -> [process_monitor_idx()].

process_monitor_indices(Node) ->
    ets:lookup(?PROC_MON_NODE_IDX_TAB, Node).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
add_process_monitor_cache(Node, Mref, RPid, Owner) ->
    Obj = new_process_monitor_cache(Node, Mref, RPid, Owner),
    _ = ets:insert_new(?PROC_MON_CACHE_TAB, Obj),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_process_monitor_cache(partisan_remote_ref:r()) -> ok.

del_process_monitor_cache(Mref) ->
    Node = partisan_remote_ref:node(Mref),
    Pattern = {Node, Mref, '_', '_'},
    true = ets:match_delete(?PROC_MON_CACHE_TAB, Pattern),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec process_monitor_caches(node()) -> [process_monitor_idx()].

process_monitor_caches(Node) ->
    ets:lookup(?PROC_MON_CACHE_TAB, Node).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_node_monitor(node(), pid()) -> true.

add_node_monitor(Node, Pid) ->
    _ = ets:insert(?NODE_MON_TAB, new_node_monitor(Node, Pid)),
    true.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_node_monitor(node(), pid()) -> true.

del_node_monitor(Node, Pid) ->
    _ = ets:delete_object(?NODE_MON_TAB, new_node_monitor(Node, Pid)),
    true.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec take_node_monitors(node()) -> [node_monitor()].

take_node_monitors(Node) ->
    ets:take(?NODE_MON_TAB, Node).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_nodes_mon(pid(), nodes_monitor_opts()) -> ok.

add_nodes_mon(Pid, Opts) ->
    Obj = new_nodes_mon(Pid, Opts),
    _ = ets:insert_new(?NODES_MON_TAB, Obj),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec del_nodes_mon(pid(), nodes_monitor_opts()) -> ok.

del_nodes_mon(Pid, Opts) ->
    Obj = new_nodes_mon(Pid, Opts),
    _ = ets:delete_object(?NODES_MON_TAB, Obj),
    ok.
