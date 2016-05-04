%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Helium Systems, Inc.  All Rights Reserved.
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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

-module(partisan_peer_service_gossip).

-behavior(gen_server).

-export([start_link/0,
         stop/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("partisan.hrl").

%%%==================================================================
%%% gen_server api
%%%==================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop).

%%%===============================================================
%%% gen_server callbacks
%%%===============================================================

init([]) ->
    %% Seed the process at initialization.
    random:seed(erlang:phash2([node()]),
                erlang:monotonic_time(),
                erlang:unique_integer()),

    erlang:send_after(?GOSSIP_INTERVAL, ?MODULE, gossip),
    {ok, []}.

handle_call(stop, _From, State) ->
    {stop, normal, State}.

handle_cast({receive_state, PeerState}, State) ->
    {ok, LocalState} = partisan_peer_service_manager:get_local_state(),
    case ?SET:equal(PeerState, LocalState) of
        true ->
            %% do nothing
            {noreply, State};
        false ->
            Merged = ?SET:merge(PeerState, LocalState),
            partisan_peer_service_manager:update_state(Merged),
            partisan_peer_service_events:update(Merged),
            {noreply, State}
    end.

handle_info(gossip, State) ->
    _ = do_gossip(),
    erlang:send_after(?GOSSIP_INTERVAL, self(), gossip),
    {noreply, State};

handle_info(_Info, State) ->
    lager:info("Unexpected: ~p,~p.~n", [_Info, State]),
    {noreply, State}.

terminate(Reason, State) ->
    lager:info("Terminate ~p, ~p.~n", [Reason, State]),
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===============================================================
%%% private functions
%%%===============================================================

%% @doc initiate gossip on local node
do_gossip() ->
    {ok, Local} = partisan_peer_service_manager:get_local_state(),
    case get_peers(Local) of
        [] ->
            {error, singleton};
        Peers ->
            {ok, Peer} = random_peer(Peers),
            gen_server:cast({?MODULE, Peer}, {receive_state, Local})
    end.

%% @doc returns a list of peer nodes
get_peers(Local) ->
    Members = ?SET:value(Local),
    Peers = [X || {X, _, _} <- Members, X /= node()],
    Peers.

%% @doc return random peer from nodelist
random_peer(Peers) ->
    Idx = random:uniform(length(Peers)),
    Peer = lists:nth(Idx, Peers),
    {ok, Peer}.
