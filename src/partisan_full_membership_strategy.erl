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
%% @doc This module implements the full-mesh membership strategy to be used
%% with {link partisan_pluggable_peer_service_manager}.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_full_membership_strategy).

-behaviour(partisan_membership_strategy).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").


-record(full_v1, {
    actor           ::  partisan:actor(),
    membership      ::  partisan_membership_set:t()
}).

-type t()                   :: #full_v1{}.
-type membership_list()     :: partisan_membership_strategy:membership_list().
-type outgoing_messages()   :: partisan_membership_strategy:outgoing_messages().


%% PARTISAN_MEMBERSHIP_STRATEGY CALLBACKS
-export([init/1]).
-export([join/3]).
-export([leave/2]).
-export([compare/2]).
-export([periodic/1]).
-export([prune/2]).
-export([handle_message/2]).



%% =============================================================================
%% PARTISAN_MEMBERSHIP_STRATEGY CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Initialize the strategy state.
%% @end
%% -----------------------------------------------------------------------------
-spec init(partisan:actor()) ->
    {ok, membership_list(), State :: any()}.

init(Actor) ->
    State = maybe_load_state_from_disk(Actor),
    Members = members(State),
    ok = persist_state(State),
    {ok, Members, State}.


%% -----------------------------------------------------------------------------
%% @doc When a node is connected, return the state, membership and outgoing
%% message queue to be transmitted.
%% @end
%% -----------------------------------------------------------------------------
-spec join(partisan:node_spec(), PeerState :: any(), State :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

join(_Node, #full_v1{} = PeerState, #full_v1{} = State0) ->
    M0 = State0#full_v1.membership,
    PeerM = PeerState#full_v1.membership,

    M = partisan_membership_set:merge(M0, PeerM),
    State = State0#full_v1{membership = M},
    ok = persist_state(State),

    Members = members(State),
    OutgoingMessages = gossip_messages(State),

    {ok, Members, OutgoingMessages, State}.


%% -----------------------------------------------------------------------------
%% @doc Periodic protocol maintenance.
%% @end
%% -----------------------------------------------------------------------------
-spec periodic(State :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

periodic(State) ->
    Members = members(State),
    OutgoingMessages = gossip_messages(State),
    persist_state(State),

    {ok, Members, OutgoingMessages, State}.


%% -----------------------------------------------------------------------------
%% @doc Returns the tuple `{Joiners, Leavers}' where `Joiners' is the list of
%% node specifications that are elements of `List' but are not in the
%% membership set, and `Leavers' are the node specifications for the current
%% members that are not elements in `List'.
%% @end
%% -----------------------------------------------------------------------------
-spec compare(Members :: [partisan:node_spec()], t()) ->
    {Joiners :: [partisan:node_spec()], Leavers :: [partisan:node_spec()]}.

compare(Members, State) ->
    partisan_membership_set:compare(Members, State#full_v1.membership).


%% -----------------------------------------------------------------------------
%% @doc Handling incoming protocol message.
%% @end
%% -----------------------------------------------------------------------------
-spec handle_message(partisan:message(), State :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

handle_message({#{name := From}, #full_v1{} = State1}, #full_v1{} = State0) ->

    M0 = State0#full_v1.membership,
    M1 = State1#full_v1.membership,

    ?LOG_DEBUG(
        fun([Node, M]) ->
            #{
                description => "Received membership_strategy",
                from => Node,
                membership => partisan_membership_set:to_list(M)
            }
        end,
        [From, M1]
    ),

    case partisan_membership_set:equal(M0, M1) of
        true ->
            %% Convergence of gossip at this node.
            Members = partisan_membership_set:to_list(M0),
            OutgoingMessages = [],
            {ok, Members, OutgoingMessages, State0};
        false ->
            %% Merge, persist, reforward to peers.
            M = partisan_membership_set:merge(M0, M1),
            State = State0#full_v1{membership = M},
            ok = persist_state(State),

            Members = partisan_membership_set:to_list(M),
            OutgoingMessages = gossip_messages(State),

            {ok, Members, OutgoingMessages, State}
    end.


%% -----------------------------------------------------------------------------
%% @doc Leave a node from the cluster.
%% @end
%% -----------------------------------------------------------------------------
-spec leave(partisan:node_spec(), State :: any()) ->
    {ok, membership_list(), outgoing_messages(), NewState :: any()}.

leave(#{name := NameLeaving}, #full_v1{} = State0) ->
    Actor = State0#full_v1.actor,
    Membership0 = State0#full_v1.membership,

    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    Membership = lists:foldl(
        fun
            (#{name := Name} = N, Acc0) when Name == NameLeaving ->
                partisan_membership_set:remove(N, Actor, Acc0);
            (_, Acc0) ->
                Acc0
        end,
        Membership0,
        members(State0)
    ),

    %% Self-leave removes our own state and resets it.
    StateToGossip = State0#full_v1{membership = Membership},

    State = case partisan:node() of
        NameLeaving ->
            %% Reset our state, store this, but gossip the state with us
            %% removed to the remainder of the members.
            new_state(Actor);
        _ ->
            %% Gossip state with member removed.
            StateToGossip
    end,

    ok = persist_state(State),

    Members = members(State),

    %% Gossip new membership to existing members, so they remove themselves.
    OutgoingMessages = gossip_messages(State0, StateToGossip),

    {ok, Members, OutgoingMessages, State}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec prune([partisan:node_spec()], State :: any()) ->
    {ok, membership_list(), NewState :: any()}.

prune([H|T], #full_v1{membership = M0} = State0) ->
    Actor = State0#full_v1.actor,
    M = partisan_membership_set:remove(H, Actor, M0),
    State = State0#full_v1{membership = M},
    prune(T, State);

prune([], State) ->
    {ok, members(State), State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
members(#full_v1{membership = M}) ->
    partisan_membership_set:to_list(M).


%% @private
gossip_messages(State) ->
    gossip_messages(State, State).


%% @private
gossip_messages(State0, #full_v1{} = State) ->
    case partisan_config:get(gossip, true) of
        true ->
            %% All nodes excluding myself
            M = State0#full_v1.membership,
            Peers = partisan_membership_set:to_peer_list(M),
            gossip_messages(State, Peers);
        _ ->
            []
    end;

gossip_messages(_, []) ->
    [];

gossip_messages(State, Peers) when is_list(Peers) ->
    Msg = {membership_strategy, {partisan:node_spec(), State}},
    [{Peer, Msg} || Peer <- Peers].


%% @private
maybe_load_state_from_disk(Actor) ->
    case data_root() of
        undefined ->
            new_state(Actor);
        Dir ->
            case filelib:is_regular(filename:join(Dir, "cluster_state")) of
                true ->
                    {ok, Bin} = file:read_file(filename:join(Dir, "cluster_state")),
                    binary_to_term(Bin);
                false ->
                    new_state(Actor)
            end
    end.


%% @private
new_state(Actor) ->
    Membership = partisan_membership_set:add(
        partisan:node_spec(), Actor, partisan_membership_set:new()
    ),
    State = #full_v1{actor = Actor, membership = Membership},
    ok = persist_state(State),
    State.


%% @private
persist_state(State) ->
    case partisan_config:get(persist_state, true) of
        true ->
            try
                write_state_to_disk(State)
            catch
                Class:Reason:Stacktrace ->
                    ?LOG_ERROR(#{
                        description => "Error while persisting membership",
                        class => Class,
                        reason => Reason,
                        stacktrace => Stacktrace
                    }),
                    ok
            end;
        false ->
            ok
    end.


%% @private
write_state_to_disk(State) ->
    case data_root() of
        undefined ->
            ok;
        Dir ->
            File = filename:join(Dir, "cluster_state"),
            ok = filelib:ensure_dir(File),
            ok = file:write_file(File, term_to_binary(State))
    end.


%% @private
data_root() ->
    case application:get_env(partisan, partisan_data_dir) of
        {ok, PRoot} ->
            filename:join(PRoot, "default_peer_service");
        undefined ->
            undefined
    end.
