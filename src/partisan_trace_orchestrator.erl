%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(partisan_trace_orchestrator).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

-include("partisan.hrl").

%% API
-export([start_link/0,
         start_link/1,
         record/4,
         reset/0,
         identify/1,
         print/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {trace=[], identifier=undefined}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%% @doc Record message for trace.
record(SourceNode, DestinationNode, Type, Message) ->
    gen_server:call(?MODULE, {record, {SourceNode, DestinationNode, Type, Message}}, infinity).

%% @doc Reset trace.
reset() ->
    gen_server:call(?MODULE, reset, infinity).

%% @doc Print trace.
print() ->
    gen_server:call(?MODULE, print, infinity).

%% @doc Identify trace.
identify(Identifier) ->
    gen_server:call(?MODULE, {identify, Identifier}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    lager:info("Test orchestrator started on node: ~p", [node()]),
    {ok, #state{trace=[]}}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
handle_call({record, {_SourceNode, _DestinationNode, _Type, _Message} = TraceMessage}, _From, #state{trace=Trace0}=State) ->
    %% lager:info("~p: recording trace message: ~p", [?MODULE, TraceMessage]),
    {reply, ok, State#state{trace=Trace0++[TraceMessage]}};
handle_call(reset, _From, State) ->
    lager:info("~p: resetting trace.", [?MODULE]),
    {reply, ok, State#state{trace=[], identifier=undefined}};
handle_call({identify, Identifier}, _From, State) ->
    lager:info("~p: identifying trace: ~p", [?MODULE, Identifier]),
    {reply, ok, State#state{identifier=Identifier}};
handle_call(print, _From, #state{trace=Trace}=State) ->
    lists:foreach(fun({SourceNode, DestinationNode, Type, Message}) ->
        case Type of
            receive_message ->
                lager:info("~p: ~p <- ~p: ~p", [?MODULE, DestinationNode, SourceNode, Message]);
            forward_message ->
                lager:info("~p: ~p => ~p: ~p", [?MODULE, SourceNode, DestinationNode, Message]);
            _ ->
                lager:info("~p: Unknown message type.", [?MODULE])
        end
    end, Trace),

    {reply, ok, State};
handle_call(Msg, _From, State) ->
    lager:warning("Unhandled call messages at module ~p: ~p", [?MODULE, Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(Msg, State) ->
    lager:warning("Unhandled info messages at module ~p: ~p", [?MODULE, Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================