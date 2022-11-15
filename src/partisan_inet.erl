%% -------------------------------------------------------------------
%%
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
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_inet).
-behaviour(gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-define(CHECK_MSECS, timer:seconds(2)).

-record(state, {
    atomics_ref             ::  atomics:atomics_ref(),
    pids = #{}              ::  #{pid() := reference()}
}).


%% API
-export([monitor/1]).
-export([net_status/0]).
-export([net_status/1]).
-export([start_link/0]).


%% gen_server callbacks
-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([terminate/2]).

-compile({no_auto_import, [monitor/1]}).


%% =============================================================================
%% API
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).



%% -----------------------------------------------------------------------------
%% @doc Returns `connected' if the host has at least one non-loopback network
%% interface address. Otherwise returns `disconnected'.
%% @end
%% -----------------------------------------------------------------------------
-spec net_status() -> connected | disconnected.

net_status() ->
    case persistent_term:get({?MODULE, atomics_ref}) of
        undefined ->
            status_name(check_net_status());
        Ref ->
            status_name(atomics:get(Ref, 1))
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns `connected' if the host has at least one non-loopback network
%% interface address. Otherwise returns `disconnected'.
%% @end
%% -----------------------------------------------------------------------------
-spec net_status([nocache]) -> connected | disconnected.

net_status([nocache]) ->
    status_name(check_net_status());

net_status(_) ->
    net_status().



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec monitor(Flag :: boolean()) -> ok.

monitor(true) ->
    gen_server:call(?MODULE, monitor);

monitor(false) ->
    gen_server:call(?MODULE, demonitor).



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



init([]) ->
    Ref = atomics:new(1, [{signed, false}]),
    _ = persistent_term:put({?MODULE, atomics_ref}, Ref),
    State = #state{atomics_ref = Ref},
    ok = atomics:put(State#state.atomics_ref, 1, check_net_status()),
    ok = schedule_check(),
    {ok, State}.

handle_call(monitor, {Pid, _Tag}, State0) ->
    Pids0 = State0#state.pids,
    State = case maps:find(Pid, Pids0) of
        {ok, _} ->
            %% Already monitoring
            State0;
        error ->
            Ref = partisan:monitor(process, Pid),
            Pids = maps:put(Pid, Ref, Pids0),
            State0#state{pids = Pids}
    end,
    {reply, ok, State};

handle_call(demonitor, {Pid, _Tag}, State0) ->
    Pids0 = State0#state.pids,
    State = case maps:find(Pid, Pids0) of
        {ok, _} ->
            Pids = maps:remove(Pid, Pids0),
            State0#state{pids = Pids};
        error ->
            State0
    end,
    {reply, ok, State};

handle_call(_, _From, State) ->
    {reply, {error, unknown}, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'DOWN', _Ref, process, Pid, _Reason}, State0) ->
    Pids0 = State0#state.pids,
    Pids = maps:remove(Pid, Pids0),
    State = State0#state{pids = Pids},
    {noreply, State};

handle_info(check, State) ->
    Status = check_net_status(),
    case atomics:exchange(State#state.atomics_ref, 1, Status) of
        Status ->
            %% No change, we do nothing
            ok;
        0 ->
            %% We were disconnected before, now connected
            notify(connected, State);
        1 ->
            %% We were connected before, now disconnected
            notify(disconnected, State)
    end,
    ok = schedule_check(),
    {noreply, State, hibernate};

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
status_name(0) -> disconnected;
status_name(1) -> connected.


%% @private
check_net_status() ->
    {ok, L} = net:getifaddrs(
        fun
            (#{addr  := #{family := Family}, flags := Flags})
            when Family == inet ->
            % when Family == inet orelse Family == inet6 ->
			    not lists:member(loopback, Flags);
            (_) ->
                false
            end
    ),

    case L == [] of
        true -> 0;
        false -> 1
    end.


%% @private
schedule_check() ->
    _ = erlang:send_after(?CHECK_MSECS, self(), check),
    ok.


%% @private
notify(Status, State) ->
    maps:foreach(
        fun(Pid, Ref) ->
            case Status of
                connected ->
                    Pid ! {network_connected, Ref};
                disconnected ->
                    Pid ! {network_disconnected, Ref}
            end
        end,
        State#state.pids
    ).