%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Christopher S. Meiklejohn.  All Rights Reserved.
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
%% @doc TODO validate this module, align vs partisan_gen and if useful add
%% gen_statem API, deprecating fsm API.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_otp_adapter).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").


-export([gen_server_cast/3]).
-export([gen_server_call/4]).
-export([gen_fsm_send_event/3]).
-export([gen_fsm_send_all_state_event/3]).
-export([gen_fsm_sync_send_event/4]).
-export([gen_fsm_sync_send_all_state_event/4]).



%% =============================================================================
%% GEN_SERVER API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
gen_server_cast(Node, Dest, Request) ->
    partisan:forward_message(
        Node,
        Dest,
        {'$gen_cast', Request},
        #{channel => ?DEFAULT_CHANNEL}
    ).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
gen_server_call(Node, Dest, Request, Opts) when is_list(Opts) ->
    call(
        gen_server_call, Node, Dest, '$gen_call', Request, Opts
    );

gen_server_call(Node, Dest, Request, Timeout) ->
    call(
        gen_server_call, Node, Dest, '$gen_call', Request, [{timeout, Timeout}]
    ).





%% =============================================================================
%% GEN_FSM_API (DEPRECATED)
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Deprecated
%% @end
%% -----------------------------------------------------------------------------
gen_fsm_send_event(Node, Dest, Request) ->
    partisan:forward_message(
        Node,
        Dest,
        {'$gen_event', Request},
        #{channel => ?DEFAULT_CHANNEL}
    ).

gen_fsm_send_all_state_event(Node, Dest, Request) ->
    partisan:forward_message(
        Node,
        Dest,
        {'$gen_all_state_event', Request},
        #{channel => ?DEFAULT_CHANNEL}
    ).

gen_fsm_sync_send_event(Node, Dest, Request, Timeout) ->
    call(
        gen_fsm_sync_send_event, Node, Dest, '$gen_sync_event', Request, Timeout
    ).

gen_fsm_sync_send_all_state_event(Node, Dest, Request, Timeout) ->
    call(
        gen_fsm_sync_send_all_state_event, Node, Dest,
        '$gen_sync_all_state_event', Request, Timeout
    ).



%% =============================================================================
%% PRIVATE
%% =============================================================================



call(Type, Node, Dest, Label, Request, Opts0) ->
    %% Make reference.
    Mref = make_ref(),

    %% Get our pid.
    Self = partisan:self(),

    {Timeout, Opts} =
        case lists:keytake(timeout, 1, Opts0) of
            {value, {timeout, Val}, Opts1} ->
                {Val, Opts1};
            false ->
                {5000, Opts0}
        end,

    %% Send message.
    partisan:forward_message(
        Node,
        Dest,
        {Label, {Self, Mref}, Request},
        Opts
    ),

    %% Don't timeout earlier than the timeout -- Distributed Erlang would if
    %% the net_ticktime fired and determined that the node is down.  However,
    %% this adds nondeterminism into the execution, so wait until
    %% the timeout. This is still nondeterministic, but less so.
    receive
        {Mref, Reply} ->
            Reply
    after
        Timeout ->
            exit({timeout, {?MODULE, Type, [Dest, Request, Timeout]}})
    end.