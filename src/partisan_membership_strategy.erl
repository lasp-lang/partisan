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

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_membership_strategy).

-include("partisan.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-type outgoing_message()    ::  {node(), partisan:message()}.
-type outgoing_messages()   ::  [outgoing_message()].
-type membership_list()     ::  [partisan:node_spec()].

-export_type([outgoing_message/0]).
-export_type([outgoing_messages/0]).
-export_type([membership_list/0]).

%% API
-export([init/2]).
-export([join/4]).
-export([leave/3]).
-export([prune/3]).
-export([periodic/2]).
-export([handle_message/3]).



%% =============================================================================
%% CALLBACKS
%% =============================================================================



-callback init(partisan:actor()) ->
    {ok, membership_list(), State :: any()}.

-callback join(
    NodeSpec :: partisan:node_spec(),
    PeerState :: any(),
    LocalState :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

-callback periodic(State :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

-callback compare(Members :: membership_list(),  State :: any()) ->
    {Joiners :: membership_list(), Leavers :: membership_list()}.

-callback handle_message(partisan:message(), State :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

-callback leave(NodeSpec :: partisan:node_spec(), State :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

-callback prune([NodeSpec :: partisan:node_spec()], State :: any()) ->
    {ok, membership_list(), NewState :: any()}.



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec init(Mod :: module(), partisan:actor()) ->
    {ok, membership_list(), State :: any()}.

init(Mod, Actor) ->
    Mod:init(Actor).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec join(
    Mod :: module(),
    NodeSpec :: partisan:node_spec(),
    PeerState :: any(),
    LocalState :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

join(Mod, NodeSpec, PeerState, LocalState) ->
    Mod:join(NodeSpec, PeerState, LocalState).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec periodic(Mod :: module(), State :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

periodic(Mod, State) ->
    Mod:periodic(State).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec handle_message(Mod :: module(), partisan:message(), State :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

handle_message(Mod, Msg, State) ->
    Mod:handle_message(Msg, State).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec leave(Mod :: module(), partisan:node_spec(), State :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

leave(Mod, NodeSpec, State) ->
    Mod:leave(NodeSpec, State).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec prune(Mod :: module(), [partisan:node_spec()], State :: any()) ->
    {ok, membership_list(), NewState :: any()}.

prune(Mod, NodeSpecs, State) ->
    Mod:prune(NodeSpecs, State).




