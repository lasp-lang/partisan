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

-module(partisan_otp_adapter).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([
         gen_server_cast/3,
         gen_server_call/4,
         gen_fsm_send_event/3,
         gen_fsm_send_all_state_event/3,
         gen_fsm_sync_send_event/4,
         gen_fsm_sync_send_all_state_event/4
        ]).

%%%===================================================================
%%% gen_server API
%%%===================================================================

gen_server_cast(Node, Dest, Request) ->
    partisan_pluggable_peer_service_manager:forward_message(Node, undefined, Dest, {'$gen_cast', Request}, []).

gen_server_call(Node, Dest, Request, Timeout) ->
    call(gen_server_call, Node, Dest, '$gen_call', Request, Timeout).

%%%===================================================================
%%% gen_fsm API
%%%===================================================================

gen_fsm_send_event(Node, Dest, Request) ->
    partisan_pluggable_peer_service_manager:forward_message(Node, undefined, Dest, {'$gen_event', Request}, []).

gen_fsm_send_all_state_event(Node, Dest, Request) ->
    partisan_pluggable_peer_service_manager:forward_message(Node, undefined, Dest, {'$gen_all_state_event', Request}, []).

gen_fsm_sync_send_event(Node, Dest, Request, Timeout) ->
    call(gen_fsm_sync_send_event, Node, Dest, '$gen_sync_event', Request, Timeout).

gen_fsm_sync_send_all_state_event(Node, Dest, Request, Timeout) ->
    call(gen_fsm_sync_send_all_state_event, Node, Dest, '$gen_sync_all_state_event', Request, Timeout).

%%%===================================================================
%%% Internal functions
%%%===================================================================

call(Type, Node, Dest, Label, Request, Timeout) ->
    %% Make reference.
    Mref = make_ref(),

    %% Get our pid.
    Self = partisan:self(),

    %% Send message.
    partisan_pluggable_peer_service_manager:forward_message(Node, undefined, Dest, {Label, {Self, Mref}, Request}, []),

    %% Don't timeout earlier than the timeout -- Distributed Erlang would if the net_ticktime fired and
    %% determined that the node is down.  However, this adds nondeterminism into the execution, so wait until
    %% the timeout.  This is still nondeterministic, but less so.
    receive
        {Mref, Reply} ->
            Reply
    after
        Timeout ->
            exit({timeout, {?MODULE, Type, [Dest, Request, Timeout]}})
    end.