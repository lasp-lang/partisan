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

-module(partisan_rpc_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0,
         call/5]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-include("partisan.hrl").

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

call(Name, Module, Function, Arguments, Timeout) ->
    %% Make call.
    Manager = partisan_config:get(partisan_peer_service_manager),
    Self = self(),
    Options = options(),
    RpcChannel = rpc_channel(),
    OurName = partisan_peer_service_manager:mynode(),
    Manager:forward_message(Name, RpcChannel, ?MODULE, {call, Module, Function, Arguments, Timeout, {origin, OurName, Self}}, Options),

    %% Wait for response.
    receive
        {response, Response} ->
            Response
    after
        Timeout ->
            {badrpc, timeout}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([]) ->
    {ok, #state{}}.

%% @private
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({call, Module, Function, Arguments, _Timeout, {origin, Name, Self}}, State) ->
    %% Execute function.
    Response = try 
        erlang:apply(Module, Function, Arguments)
    catch
         Error ->
             {badrpc, Error}
    end,

    %% Send the response to execution.
    Manager = partisan_config:get(partisan_peer_service_manager),
    Options = options(),
    RpcChannel = rpc_channel(),
    ok = Manager:forward_message(Name, RpcChannel, Self, {response, Response}, Options),

    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

options() ->
    ForwardOptions = partisan_config:get(forward_options),
    Transitive = partisan_config:get(broadcast, false),
    [{transitive, Transitive}, {forward_options, ForwardOptions}].

rpc_channel() ->
    Channels = partisan_config:get(channels),
    case lists:member(rpc, Channels) of
        true ->
            ?RPC_CHANNEL;
        false ->
            ?DEFAULT_CHANNEL
    end.