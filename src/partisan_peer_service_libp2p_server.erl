%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(partisan_peer_service_libp2p_server).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

%% API
-export([start_link/0,
         start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% State record.
-record(state, {swarm}).

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

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    %% Get our information.
    #{listen_addrs := ListenAddrs} = partisan_peer_service_manager:myself(),
    #{ip := MyAddress, port := MyPort} = hd(ListenAddrs),
    OurListenAddr = "/ip4/" ++ inet:ntoa(MyAddress) ++ "/tcp/" ++ integer_to_list(MyPort),

    %% Initialize the connection cache supervised by the supervisor.
    ?LIBP2P_SERVER = ets:new(?LIBP2P_SERVER, [public, named_table, set, {read_concurrency, true}]),

    %% Bind a listener.
    case libp2p_swarm:start(OurListenAddr) of
        {ok, Swarm} ->
            true = ets:insert(?LIBP2P_SERVER, {local_swarm, Swarm}),

            Channels = partisan_config:get(channels, []),

            lists:foreach(fun(Channel) ->
                Name = atom_to_list(Channel),
                libp2p_swarm:add_stream_handler(Swarm, Name, {libp2p_framed_stream, server, [partisan_peer_service_libp2p_framed_stream]})
            end, Channels),

            {ok, #state{swarm=Swarm}};
        Error ->
            lager:error("Unable to init ~p due to ~p", [OurListenAddr, Error]),
            {stop, normal}
    end.

%% @private
handle_call(Msg, _From, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_cast(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
handle_info(Msg, State) ->
    _ = lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) ->
    {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================