%% @doc This module is responsible for monitoring processes on remote nodes.
-module(partisan_monitor).

-behaviour(partisan_gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-record(state, {
    %% process monitor refs held on behalf of remote processes
    refs = #{}              ::  #{reference() => remote_ref(process_ref())},
    %% Process monitor refs held on behalf of remote processes, grouped by node,
    %% used to cleanup when we get a nodedown signal for a node
    refs_by_node = #{}      ::  #{node() => [reference()]},
    %% Local pids that are monitoring a remote node
    pids_by_node = #{}      ::  #{node() => [pid()]}
}).


% API
-export([demonitor/2]).
-export([monitor/1]).
-export([monitor_node/3]).
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
monitor({partisan_remote_reference, Node,
         {partisan_process_reference, PidAsList}}) ->
    partisan_gen_server:call({?MODULE, Node}, {monitor, PidAsList}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(
    MonitorRef :: remote_ref(encoded_ref()),
    OptionList :: [flush | info]) -> boolean().

demonitor(MRef, Opts) when is_reference(MRef) ->
    erlang:demonitor(MRef, Opts);

demonitor({partisan_remote_reference, Node,
           {partisan_encoded_reference, _}} = RemoteRef, Opts) ->
    Res = partisan_gen_server:call(
        {?MODULE, Node}, {demonitor, RemoteRef, Opts}
    ),
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
-spec monitor_node(node() | node_spec(), boolean(), [flush | info]) ->
    boolean().

monitor_node(#{name := Node}, Flag, Opts) ->
    monitor_node(Node, Flag, Opts);

monitor_node(Node, Flag, Opts) ->
    case partisan_config:get(connect_disterl) of
        true ->
            erlang:monitor_node(Node, Flag, Opts);
        false ->
            case Node == partisan:node() of
                true ->
                    true;
                false when Flag == true ->
                    partisan_gen_server:call(?MODULE, {monitor_node, Node});
                false when Flag == false ->
                    partisan_gen_server:call(?MODULE, {demonitor_node, Node})
            end
    end.



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init([]) ->
    Me = self(),

    %% Every time a node goes down we will get a {nodedown, Node} message
    Fun = fun(Node) -> Me ! {nodedown, Node} end,
    ok = partisan_peer_service:on_down('_', Fun),

    {ok, #state{}}.


handle_call({monitor, PidAsList}, {RemotePid, _RemoteRef}, State0) ->
    {partisan_remote_reference, Nodename, _} = RemotePid,
    Pid = list_to_pid(PidAsList),

    %% We monitor the process on behalf of the remote caller
    MRef = erlang:monitor(process, Pid),

    State = add_process_monitor(Nodename, MRef, {Pid, RemotePid}, State0),

    %% We reply the encoded monitor reference
    Reply = partisan_util:ref(MRef),

    {reply, Reply, State};

handle_call({demonitor, PartisanRef, Opts}, _From, State0) ->
    {Res, State} = do_demonitor(PartisanRef, Opts, State0),
    {reply, Res, State};

handle_call({monitor_node, Node}, {Pid, _} = From, State0) ->
    case is_connected(Node) of
        true ->
            %% Pid is always local but encoded by partisan_gen
            State = add_node_monitor(Node, decode_pid(Pid), State0),
            {reply, true, State};
        false ->
            %% We reply true but we do not record the request as we are
            %% immediatly sending a nodedown signal
            ok = partisan_gen_server:reply(From, true),
            ok = partisan:forward_message(Pid, {nodedown, Node}),
            {noreply, State0}
    end;

handle_call({demonitor_node, Node}, {Pid, _}, State0) ->
    State = remove_node_monitor(Node, decode_pid(Pid), State0),
    {reply, true, State};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.



handle_cast(_Msg, State) ->
    {noreply, State}.



handle_info({'DOWN', MRef, process, Pid, Reason}, State0) ->
    State = case take_process_monitor(MRef, State0) of
        {{Pid, RemotePid}, State1} ->
            Node = partisan:node(RemotePid),
            State2 = remove_ref_by_node(Node, MRef, State1),
            ok = send_process_down(RemotePid, MRef, Pid, Reason),
            State2;
        error ->
            State0
    end,

    {noreply, State};

handle_info({nodedown, Node} = Msg, State0) ->
    %% We need to notify all local processes monitoring Node
    {Pids, State1} = take_node_monitors(Node, State0),
    [partisan:forward_message(Pid, Msg) || Pid <- Pids],

    %% We need to demonitor all monitors associated with Node
    Refs = refs_by_node(Node, State1),

    State = lists:foldl(
        fun(Ref, Acc0) -> {true, Acc} = do_demonitor(Ref, Acc0), Acc end,
        State1,
        Refs
    ),

    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.



terminate(_Reason, _State) ->
    ok.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
send_process_down(RemotePid, MRef, Pid, Reason) ->
    Down = {
        'DOWN',
        partisan_util:ref(MRef),
        process,
        partisan_util:pid(Pid),
        Reason
    },
    partisan:forward_message(RemotePid, Down).


%% @private
do_demonitor(Term, State0) ->
    do_demonitor(Term, [], State0).


%% @private
do_demonitor(Term, Opts, State0) ->
    MRef = decode_ref(Term),
    Res = erlang:demonitor(MRef, Opts),

    State = case take_process_monitor(MRef, State0) of
        {{_, RemotePid}, State1} ->
            Node = partisan:node(RemotePid),
            remove_ref_by_node(Node, MRef, State1);
        error ->
            State0
    end,

    {Res, State}.


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
decode_ref(
    {partisan_remote_reference, _, {partisan_encoded_reference, RefAsList}}) ->
    erlang:list_to_ref(RefAsList);

decode_ref(Ref) when is_reference(Ref) ->
    Ref.


decode_pid(
    {partisan_remote_reference, _, {partisan_process_reference, PidAsList}}) ->
    erlang:list_to_pid(PidAsList);

decode_pid(Pid) when is_pid(Pid) ->
    Pid.
