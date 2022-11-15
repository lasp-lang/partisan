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

-module(partisan_membership_strategy).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-type state() :: term().
-type outgoing_message() :: {node(), message()}.
-type outgoing_messages() :: [outgoing_message()].
-type membership_list() :: [node_spec()].

-callback init(actor()) -> {ok, membership_list(), state()}.

-callback join(state(), node_spec(), state()) ->
    {ok, membership_list(), outgoing_messages(), state()}.

-callback leave(state(), node_spec()) ->
    {ok, membership_list(), outgoing_messages(), state()}.

-callback prune(state(), [node_spec()]) ->
    {ok, membership_list(), state()}.

-callback periodic(state()) ->
    {ok, membership_list(), outgoing_messages(), state()}.

-callback handle_message(state(), message()) ->
    {ok, membership_list(), outgoing_messages(), state()}.