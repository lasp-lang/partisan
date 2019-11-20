-module(partisan_monitor).

-behaviour(partisan_gen_server).

% API
-export([start_link/0, monitor/1, demonitor/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

monitor({partisan_remote_reference, Node,
         {partisan_process_reference, PidAsList}}) ->
    partisan_gen_server:call({?MODULE, Node}, {monitor, PidAsList}).

demonitor({partisan_remote_reference, Node,
           {partisan_encoded_reference, _}} = PartisanRef) ->
    partisan_gen_server:call({?MODULE, Node}, {demonitor, PartisanRef}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    {ok, #{}}.

%% @private
handle_call({monitor, PidAsList}, {PartisanRemote, _PartisanRemoteRef}, State) ->
    Ref = erlang:monitor(process, list_to_pid(PidAsList)),
    PartisanRef = partisan_util:ref(Ref),
    State1 = maps:put(Ref, {PartisanRef, PartisanRemote}, State),
    StateFinal = maps:put(PartisanRef, Ref, State1),
    {reply, PartisanRef, StateFinal};
handle_call({demonitor, PartisanRef}, _From, State) ->
    Ref = maps:get(PartisanRef, State),
    erlang:demonitor(Ref),
    State1 = maps:remove(PartisanRef, State),
    StateFinal = maps:remove(Ref, State1),
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
handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
