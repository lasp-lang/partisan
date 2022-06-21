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
%%
%% @doc This module is responsible for monitoring processes on remote nodes.
-module(partisan_monitor).

-behaviour(partisan_gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-record(state, {
    enabled                 ::  boolean(),
    %% process monitor refs held on behalf of remote processes
    refs = #{}              ::  #{reference() => remote_ref(process_ref())},
    %% refs grouped by node, used to notify/cleanup on a nodedown signal
    refs_by_node = #{}      ::  #{node() => [reference()]},
    %% Local pids that are monitoring a remote node
    pids_by_node = #{}      ::  #{node() => [pid()]},
    %% Local pids that are monitoring all nodes
    subscriptions = #{}     ::  #{pid() => {
                                    Type :: all | visible,
                                    InclReason :: boolean()
                                }},
    %% We cache the nodes that are up, so that if we are terminated we can
    %% notify the subscriptions
    up_nodes                 ::  sets:set(node())
}).


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
-compile({no_auto_import, [demonitor/2]}).



%% =============================================================================
%% API
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
start_link() ->
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% -----------------------------------------------------------------------------
%% @doc when you attempt to monitor a partisan_remote_reference, it is not
%% guaranteed that you will receive the DOWN message. A few reasons for not
%% receiving the message are message loss, tree reconfiguration and the node
%% is no longer reachable.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor(partisan_remote_ref:p() | partisan_remote_ref:node(), list()) ->
    partisan_remote_ref:r() | no_return().

monitor(RemoteRef, Opts) ->
    partisan_remote_ref:is_pid(RemoteRef)
        orelse partisan_remote_ref:is_name(RemoteRef)
        orelse error(badarg),

    Node = partisan_remote_ref:node(RemoteRef),

    case partisan_peer_connections:is_connected(Node) of
        true ->
            %% We call the remote partisan_monitor process to request a monitor
            Cmd1 = {monitor, RemoteRef, Opts},
            case partisan_gen_server:call({?MODULE, Node}, Cmd1) of
                {ok, MRef} ->
                    MRef;
                {error, badarg} ->
                    error(badarg)
            end;
        false ->
            %% We reply a ref but we do not record the request as we are
            %% immediatly sending a DOWN signal
            MRef = make_ref(),
            ok = send_process_down(self(), MRef, {id, RemoteRef}, noconnection),
            partisan_remote_ref:from_term(MRef)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(
    RemoteRef :: partisan_remote_ref:r(),
    OptionList :: [flush | info]) -> boolean() | no_return().

demonitor(RemoteRef, Opts) ->

    partisan_remote_ref:is_reference(RemoteRef) orelse error(badarg),

    Node = partisan_remote_ref:node(RemoteRef),

    Cmd1 = {demonitor, RemoteRef, Opts},

    case partisan_gen_server:call({?MODULE, Node}, Cmd1) of
        {ok, Res} ->
            case lists:member(flush, Opts) of
                true ->
                    MRef = decode_ref(RemoteRef),
                    receive
                        {_, MRef, _, _, _} ->
                            Res
                    after
                        0 ->
                            Res
                    end;
                false ->
                    Res
            end;
        {error, badarg} ->
            error(badarg)
    end.


%% -----------------------------------------------------------------------------
%% @doc Monitor the status of the node `Node'. If Flag is true, monitoring is
%% turned on. If `Flag' is `false', monitoring is turned off.
%%
%% Making several calls to `monitor_node(Node, true)' for the same `Node' from
%% is not an error; it results in as many independent monitoring instances as
%% the number of different calling processes i.e. If a process has made two
%% calls to `monitor_node(Node, true)' and `Node' terminates, only one
%% `nodedown' message is delivered to the process (this differs from {@link
%% erlang:monitor_node/2}).
%%
%% If `Node' fails or does not exist, the message `{nodedown, Node}' is
%% delivered to the calling process. If there is no connection to Node, a
%% `nodedown' message is delivered. As a result when using a membership
%% strategy that uses a partial view, you can not monitor nodes that are not
%% members of the view.
%%
%% If `Node' is the caller's node, the function returns `false'.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_node(node() | node_spec(), boolean()) -> boolean().

monitor_node(#{name := Node}, Flag) ->
    monitor_node(Node, Flag);

monitor_node(Node, Flag) ->
    case partisan_peer_connections:is_connected(Node) of
        true ->
            partisan_gen_server:call(?MODULE, {monitor_node, Node, Flag});
        false ->
            %% We reply true but we do not record the request as we are
            %% immediatly sending a nodedown signal
            self() ! {nodedown, Node},
            true
    end.


%% -----------------------------------------------------------------------------
%% @doc The calling process subscribes or unsubscribes to node status change
%% messages. A nodeup message is delivered to all subscribing processes when a
%% new node is connected, and a nodedown message is delivered when a node is
%% disconnected.
%% If Flag is true, a new subscription is started. If Flag is false, all
%% previous subscriptions started with the same Options are stopped. Two option
%% lists are considered the same if they contain the same set of options.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_nodes(Flag :: boolean(), [partisan:monitor_nodes_opt()]) ->
    ok | error | {error, term()}.

monitor_nodes(Flag, Opts0) when is_boolean(Flag), is_list(Opts0) ->
    case parse_monitor_nodes_opts(Opts0) of
        {hidden, _} ->
            %% Do nothing as we do not have hidden nodes in Partisan
            ok;
        Opts when Flag == true ->
            partisan_gen_server:call(
                ?MODULE, {monitor_nodes, Opts}
            );
        Opts when Flag == false ->
            partisan_gen_server:call(
                ?MODULE, {demonitor_nodes, Opts}
            )
    end.



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init([]) ->
    %% We subscribe to node status to implement node monitoring
    Enabled = subscribe_to_node_status(),

    %% We trap exist so that we get a call to terminate w/reason shutdown when
    %% the supervisor terminates us when the partisan_peer_service:manager() is
    %% terminated.
    erlang:process_flag(trap_exit, true),

    State = #state{
        enabled = Enabled,
        up_nodes = sets:new([{version, 2}])
    },
    {ok, State}.

handle_call(_, _, #state{enabled = false} = State) ->
    %% The peer service manager does not implement on_up/down calls which we ]
    %% require to implement monitors
    {reply, {error, not_implemented}, State};

handle_call({monitor, RemoteRef, _Opts}, {RemotePid, _}, State0) ->
    %% A remote process wants to monitor Ref on this node
    %% TODO Implement {tag, UserDefinedTag} option
    try
        Node = partisan_remote_ref:node(RemotePid),
        PidOrName = partisan_remote_ref:to_term(RemoteRef),

        %% We monitor the process on behalf of the remote caller
        MRef = erlang:monitor(process, PidOrName),

        %% We track the ref to match the 'DOWN' signal
        State = add_process_monitor(
            Node, MRef, {PidOrName, RemotePid}, State0
        ),

        %% We reply the encoded monitor reference
        Reply = {ok, partisan_remote_ref:from_term(MRef)},

        {reply, Reply, State}
    catch
        error:badarg ->
            {reply, {error, badarg}, State0}
    end;

handle_call({demonitor, RemoteRef, Opts}, _From, State0) ->
    %% Remote process is requesting a demonitor
    {Res, State} = do_demonitor(RemoteRef, Opts, State0),
    {reply, Res, State};


handle_call({monitor_node, Node, true}, {Pid, _}, State0) ->
    %% Pid is always local but encoded by partisan_gen
    State = add_node_monitor(Node, decode_pid(Pid), State0),
    {reply, true, State};

handle_call({monitor_node, Node, false}, {Pid, _}, State0) ->
    %% Pid is always local but encoded by partisan_gen
    State = remove_node_monitor(Node, decode_pid(Pid), State0),
    {reply, true, State};

handle_call({monitor_nodes, {_, _} = Opts}, {Pid, _}, State0) ->
    %% Call by monitor_nodes/2
    Key = {Pid, erlang:phash2(Opts)},
    %% TODO monitor Pid so that we can cleanup when it exits
    State = State0#state{
        subscriptions = maps:put(Key, Opts, State0#state.subscriptions)
    },
    {reply, ok, State};

handle_call({demonitor_nodes, {_, _} = Opts}, {Pid, _}, State0) ->
    %% Call by monitor_nodes/2
    Key = {Pid, erlang:phash2(Opts)},
    State = case maps:find(Key, State0#state.subscriptions) of
        {ok, Opts} ->
            State0#state{
                subscriptions = maps:remove(Key, State0#state.subscriptions)
            };
        error ->
            State0
    end,
    {reply, ok, State};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'DOWN', MRef, process, Pid, Reason}, State0) ->
    State = case take_process_monitor(MRef, State0) of
        {{Pid, SubscriberRPid}, State1} ->
            Node = partisan:node(SubscriberRPid),
            State2 = remove_ref_by_node(Node, MRef, State1),
            ok = send_process_down(SubscriberRPid, MRef, Pid, Reason),
            State2;
        error ->
            State0
    end,

    {noreply, State};

handle_info({nodeup, Node} = Msg, State0) ->
    ?LOG_DEBUG(#{
        description => "Got nodeup signal",
        node => Node
    }),
    %% Either a net_kernel or Partisan signal
    %% We notify all subscribers
    ExtMsg = erlang:append_element(Msg, [{nodedown_reason, disconnect}]),

    ok = maps:foreach(
        fun
            ({Pid, _}, {_, true}) ->
                partisan:forward_message(Pid, ExtMsg);
            ({Pid, _}, {_, false}) ->
                partisan:forward_message(Pid, Msg)
        end,
        State0#state.subscriptions
    ),

    State = State0#state{
        up_nodes = sets:add_element(Node, State0#state.up_nodes)
    },

    {noreply, State};

handle_info({nodedown, Node} = Msg, State0) ->
    ?LOG_DEBUG(#{
        description => "Got nodedown signal",
        node => Node
    }),
    %% Either a net_kernel or Partisan signal

    %% We need to notify all local processes monitoring Node
    %% See monitor_nodes/2.
    {Pids, State1} = take_node_monitors(Node, State0),

    %% There are all local pids but encoded by partisan_gen, so we use
    %% partisan:forward_message/2
    [partisan:forward_message(Pid, Msg) || Pid <- Pids],

    %% We need to demonitor all monitors associated with Node
    Refs = refs_by_node(Node, State1),

    State2 = lists:foldl(
        fun(Ref, Acc0) -> {_, Acc} = do_demonitor(Ref, Acc0), Acc end,
        State1,
        Refs
    ),

    State = State2#state{
        up_nodes = sets:del_element(Node, State2#state.up_nodes)
    },

    %% Finally we need to notify all subscribers
    ExtMsg = erlang:append_element(Msg, [{nodedown_reason, connection_closed}]),

    ok = maps:foreach(
        fun
            ({Pid, _}, {_, true}) ->
                partisan:forward_message(Pid, ExtMsg);
            ({Pid, _}, {_, false}) ->
                partisan:forward_message(Pid, Msg)
        end,
        State#state.subscriptions
    ),

    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.


terminate(shutdown, State) ->
    %% We need to notify all local process monitoring a node that the node is
    %% down
    ok = maps:foreach(
        fun(Node, Pids) ->
            [partisan:forward_message(Pid, {nodedown, Node}) || Pid <- Pids]
        end,
        State#state.pids_by_node
    ),

    %% We need to notify all local process monitoring a all nodes that all
    %% nodes are down
    Nodes = sets:to_list(State#state.up_nodes),

    ok = maps:foreach(
        fun(Pid, {_, _}) ->
            [partisan:forward_message(Pid, {nodedown, Node}) || Node <- Nodes]
        end,
        State#state.subscriptions
    ),

    ok = maps:foreach(
        fun
            ({Pid, _}, {_, true}) ->
                [
                    begin
                        Msg = {nodedown, Node, [{nodedown_reason, disconnect}]},
                        partisan:forward_message(Pid, Msg)
                    end || Node <- Nodes
                ];
            ({Pid, _}, {_, false}) ->
                [
                    partisan:forward_message(Pid, {nodedown, Node})
                    || Node <- Nodes
                ]
        end,
        State#state.subscriptions
    ),

    ok;

terminate(_Reason, _State) ->
    ok.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private We subscribe to either net_kernel's or Partisan's node status
%% signals to update an ets table that tracks each node status.
subscribe_to_node_status() ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            case net_kernel:monitor_nodes(true) of
                ok ->
                    ok;
                error ->
                    error({monitor_nodes_failed, unknown});
                {error, Reason} ->
                    error({monitor_nodes_failed, Reason})
            end;

        false ->
            Me = self(),

            OnUp = fun(Node) -> Me ! {nodeup, Node} end,
            Res1 = partisan_peer_service:on_up('_', OnUp),

            OnDown = fun(Node) -> Me ! {nodedown, Node} end,
            Res2 = partisan_peer_service:on_down('_', OnDown),

            %% Not all service managers implement this capability so the result
            %% can be an error.
            Res1 =:= ok andalso Res2 =:= ok
    end.


%% @private
-spec send_process_down(
    remote_ref(process_ref()) | pid(),
    reference(),
    {id, remote_ref() | {remote_ref(registered_name_ref()), node()}}
    | pid() | atom() | {atom(), node()},
    any()) -> ok.

send_process_down(Pid, MRef, {id, PRef}, Reason) ->
    Down = {
        'DOWN',
        partisan_remote_ref:from_term(MRef),
        process,
        PRef,
        Reason
    },
    partisan:forward_message(Pid, Down);

send_process_down(Pid, MRef, Term, Reason) when is_pid(Term) ->
    Id = {id, partisan_remote_ref:from_term(Term)},
    send_process_down(Pid, MRef, Id, Reason);

send_process_down(Pid, MRef, Name, Reason) when is_atom(Name) ->
    Id = {id, partisan_remote_ref:from_term(Name)},
    send_process_down(Pid, MRef, Id, Reason);

send_process_down(Pid, MRef, {Name, Node}, Reason)
when is_atom(Name), is_atom(Node) ->
    Id = {id, partisan_remote_ref:from_term(Name, Node)},
    send_process_down(Pid, MRef, Id, Reason).


%% @private
do_demonitor(Term, State0) ->
    do_demonitor(Term, [], State0).


%% @private
do_demonitor(Term, Opts, State0) ->
    try
        MRef = decode_ref(Term),
        Res = erlang:demonitor(MRef, Opts),

        State = case take_process_monitor(MRef, State0) of
            {{_, RemotePid}, State1} ->
                Node = partisan:node(RemotePid),
                remove_ref_by_node(Node, MRef, State1);
            error ->
                State0
        end,

        {{ok, Res}, State}

    catch
        _:_ ->
            {{error, badarg}, State0}
    end.


%% @private
add_process_monitor(Nodename, MRef, {_, _} = Pids, State) ->
    %% we store two mappings:
    %% 1. monitor ref -> {monitored pid, caller}
    %% 2. caller's nodename -> [monitor ref] - an index to fecth all refs
    %% associated with a remote node
    Refs = maps:put(MRef, Pids, State#state.refs),
    Index = partisan_util:maps_append(
        Nodename, MRef, State#state.refs_by_node
    ),
    State#state{refs = Refs, refs_by_node = Index}.


%% @private
take_process_monitor(MRef, State) ->
    case maps:take(MRef, State#state.refs) of
        {Existing, Map} ->
            {Existing, State#state{refs = Map}};
        error ->
            error
    end.


%% @private
remove_ref_by_node(Node, MRef, State) ->
    case maps:find(Node, State#state.refs_by_node) of
        {ok, Refs0} ->
            Map = case lists:delete(MRef, Refs0) of
                [] ->
                    maps:remove(Node, State#state.refs_by_node);
                Refs ->
                    maps:put(Node, Refs, State#state.refs_by_node)
            end,
            State#state{refs_by_node = Map};
        error ->
            State
    end.


%% @private
refs_by_node(Node, State) ->
    case maps:find(Node, State#state.refs_by_node) of
        {ok, Refs} ->
            Refs;
        error ->
            []
    end.


%% @private
add_node_monitor(Node, Pid, State) ->
    Map = partisan_util:maps_append(Node, Pid, State#state.pids_by_node),
    State#state{pids_by_node = Map}.


%% @private
remove_node_monitor(Node, Pid, State) ->
    Map0 = State#state.pids_by_node,
    case maps:find(Node, Map0) of
        {ok, Pids} when is_list(Pids) ->
            Map1 = maps:put(Node, lists:delete(Pid, Pids), Map0),
            State#state{pids_by_node = Map1};
        error ->
            State
    end.


%% @private
take_node_monitors(Node, State) ->
    case maps:take(Node, State#state.pids_by_node) of
        {Existing, Map} ->
            {Existing, State#state{pids_by_node = Map}};
        error ->
            {[], State}
    end.


%% @private
decode_ref(Ref) when is_reference(Ref) ->
    Ref;

decode_ref(RemoteRef) ->
    partisan_remote_ref:to_term(RemoteRef).


%% @private
decode_pid(Pid) when is_pid(Pid) ->
    Pid;

decode_pid(RemoteRef) ->
    partisan_remote_ref:to_term(RemoteRef).


%% @private
parse_monitor_nodes_opts(Opts0) ->
    Type = case lists:keyfind(node_type, 1, Opts0) of
        {node_type, Val} ->
            Val;
        false ->
            all
    end,
    InclReason = lists:member(nodedown_reason, Opts0),
    {Type, InclReason}.
