%% @doc This module is responsible for monitoring processes on remote nodes.
-module(partisan_monitor).

-behaviour(partisan_gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").

% API
-export([start_link/0]).
-export([monitor/1]).
-export([demonitor/1]).
-export([demonitor/2]).
-export([monitor_node/1]).
-export([demonitor_node/1]).
-export([monitor_node/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-compile({no_auto_import, [monitor_node/2]}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc when you attempt to monitor a partisan_remote_reference, it is not
%% guaranteed that you will receive the DOWN message. A few reasons for not
%% receiving the message are message loss, tree reconfiguration and the node
%% is no longer reachable.
monitor(Pid) when is_pid(Pid) ->
    erlang:monitor(process, Pid);

monitor({partisan_remote_reference, Node,
         {partisan_process_reference, PidAsList}}) ->
    partisan_gen_server:call({?MODULE, Node}, {monitor, PidAsList}).


demonitor(Ref) when is_reference(Ref) ->
    erlang:demonitor(Ref);

demonitor({partisan_remote_reference, Node,
           {partisan_encoded_reference, _}} = PartisanRef) ->
    partisan_gen_server:call({?MODULE, Node}, {demonitor, PartisanRef}).


demonitor(Ref, Opts) when is_reference(Ref) ->
    erlang:demonitor(Ref, Opts);

demonitor({partisan_remote_reference, Node,
           {partisan_encoded_reference, _}} = PartisanRef, _Opts) ->
    partisan_gen_server:call({?MODULE, Node}, {demonitor, PartisanRef}).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_node(node() | node_spec()) -> true.

monitor_node(Node) when is_atom(Node) ->
    monitor_node(Node, true).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor_node(node() | node_spec()) -> true.

demonitor_node(Node) when is_atom(Node) ->
    monitor_node(Node, false).


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

monitor_node(Node, Flag) when is_atom(Node), is_boolean(Flag) ->
    partisan_gen_server:call(?MODULE, {monitor_node, Node, Flag}).




%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    %% Every time a node goes down we will get a {nodedown, Node} message
    Fun = fun(Node) -> self() ! {nodedown, Node} end,
    Mod = partisan_peer_service:manager(),
    ok = Mod:on_down('_', Fun),
    {ok, #{}}.

%% @private
handle_call({monitor, PidAsList}, {PartisanRemote, _PartisanRemoteRef}, State0) ->
    %% We monitor the process
    Pid = list_to_pid(PidAsList),
    Ref = erlang:monitor(process, Pid),

    PartisanRef = partisan_util:ref(Ref),
    State1 = maps:put(Ref, {PartisanRef, PartisanRemote}, State0),
    State2 = maps:put(PartisanRef, Ref, State1),

    %% We store a mapping from remote Node name so that we can cleanup monitors
    %% when the PartisanRemote node goes down
    {partisan_remote_reference, Name, _} = PartisanRemote,
    StateFinal = append_ref_by_node(Name, PartisanRef, State2),

    {reply, PartisanRef, StateFinal};

handle_call({demonitor, PartisanRef}, _From, State) ->
    StateFinal = do_demonitor(PartisanRef, State),
    {reply, true, StateFinal};

handle_call({monitor_node, Node, true}, {PartisanRemote, _} = From, State0) ->
    %% The caller is always a local process
    {partisan_remote_reference, MyNode,
         {partisan_process_reference, PidAsList}} = PartisanRemote,

    case MyNode == Node of
        true ->
            {reply, false, State0};
        false ->
            Pid = list_to_pid(PidAsList),
            Mod = partisan_peer_service:manager(),

            case Mod:member(Node) of
                true ->
                    StateFinal = append_pid_by_node(Node, Pid, State0),
                    {reply, true, StateFinal};
                false ->
                    ok = partisan_gen_server:reply(From, true),
                    Pid ! {nodedown, Node},
                    {noreply, State0}
            end
    end;

handle_call({monitor_node, Node, false}, From, State) ->
    StateFinal = remove_pid_by_node(Node, From, State),
    {reply, true, StateFinal};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({'DOWN', Ref, process, Pid, Reason}, State) ->
    {PartisanRef, PartisanRemote} = maps:get(Ref, State),
    State1 = maps:remove(Ref, State),
    StateFinal = maps:remove(PartisanRef, State1),
    Resp = {'DOWN', PartisanRef, process, partisan_util:pid(Pid), Reason},
    partisan_peer_service_manager:forward_message(PartisanRemote, Resp),
    {noreply, StateFinal};

handle_info({nodedown, Node}, State0) ->
    %% We need to notify all local processes monitoring Node
    {Pids, State1} = take_pids_by_node(Node, State0),
    [Pid ! {nodedown, Node} || Pid <- Pids],

    %% We need to demonitor all process monitors requested by the remote Node
    StateFinal = demonitor_all(Node, State1),

    {noreply, StateFinal};

handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
do_demonitor(PartisanRef, State) ->
    case maps:find(PartisanRef, State) of
        {ok, Ref} ->
            erlang:demonitor(Ref, [flush]),
            State1 = maps:remove(PartisanRef, State),
            maps:remove(Ref, State1);
        error ->
            ?LOG_DEBUG(#{
                description => "Monitor reference unknown",
                reference => PartisanRef
            }),
            State
    end.


demonitor_all(Node, State0) ->
    {Refs, State1} = take_refs_by_node(Node, State0),

    lists:foldl(
        fun(PartisanRef, Acc) ->
            do_demonitor(PartisanRef, Acc)
        end,
        State1,
        Refs
    ).


append_pid_by_node(Node, Pid, State) ->
    Key = {pid_by_node, Node},

    case maps:find(Key, State) of
        {ok, Existing} ->
            State#{Key => [Pid|Existing]};
        error ->
            State#{Key => [Pid]}
    end.


remove_pid_by_node(Node, Pid, State) ->
    Key = {pid_by_node, Node},

    case maps:find(Key, State) of
        {ok, Existing} ->
            State#{Key => lists:delete(Pid, Existing)};
        error ->
            State
    end.


take_pids_by_node(Node, State) ->
    case maps:take({pid_by_node, Node}, State) of
        error ->
            {[], State};
        Tuple ->
            Tuple
    end.


append_ref_by_node(Node, PartisanRef, State) ->
    Key = {ref_by_node, Node},

    case maps:find(Key, State) of
        {ok, Existing} ->
            State#{Key => [PartisanRef|Existing]};
        error ->
            State#{Key => [PartisanRef]}
    end.


take_refs_by_node(Node, State) ->
    case maps:take({ref_by_node, Node}, State) of
        error ->
            {[], State};
        Tuple ->
            Tuple
    end.