%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Christopher S. Meiklejohn. All Rights Reserved.
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

-module(partisan_rpc).

-include("partisan.hrl").

-type error_reason()    ::  timeout | any().

%% API
-export([call/4]).
-export([call/5]).
-export([prepare_opts/1]).

-dialyzer([{nowarn_function, call/4}, no_return]).
-dialyzer([{nowarn_function, call/5}, no_return]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec call(
    Node :: node(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [any()]) -> Reply :: any() | {badrpc, error_reason()}.

call(Node, Module, Function, Arguments) ->
    call(Node, Module, Function, Arguments, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec call(
    Node :: node(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [any()],
    Timeout :: timeout() | partisan_peer_service_manager:forward_opts()) ->
    Reply :: any() | {badrpc, error_reason()}.

call(Node, Module, Function, Arguments, Timeout) when ?IS_VALID_TMO(Timeout) ->
    call(Node, Module, Function, Arguments, #{timeout => Timeout});

call(Node, Module, Function, Arguments, Opts) when is_list(Opts) ->
    call(Node, Module, Function, Arguments, maps:from_list(Opts));

call(Node, Module, Function, Arguments, Opts0) when is_map(Opts0) ->
    Self = partisan:self(),
    Timeout = maps:get(timeout, Opts0, ?DEFAULT_TIMEOUT),

    ?IS_VALID_TMO(Timeout) orelse error({?MODULE, badarg}),

    Opts = prepare_opts(
        partisan_config:get(forward_options, maps:without([timeout], Opts0))
    ),

    Msg = {call, Module, Function, Arguments, Timeout, {origin, Self}},

    case partisan:forward_message(Node, partisan_rpc_backend, Msg, Opts) of
        ok ->
            receive
                {rpc_response, Response} ->
                    Response
            after
                Timeout ->
                    {badrpc, timeout}
            end;
        {error, Reason} ->
            {badrpc, Reason}
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec prepare_opts(list() | map()) -> map().

prepare_opts(L) when is_list(L) ->
    prepare_opts(maps:from_list(L));

prepare_opts(#{channel := _} = Opts) ->
    Opts;

prepare_opts(Opts) when is_map(Opts) ->
    Opts#{channel => ?DEFAULT_CHANNEL}.



%% =============================================================================
%% PRIVATE
%% =============================================================================

