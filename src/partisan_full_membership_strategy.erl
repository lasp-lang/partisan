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

-module(partisan_full_membership_strategy).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(partisan_membership_strategy).

-include("partisan_logger.hrl").

-export([init/1,
         join/3,
         leave/2,
         periodic/1,
         handle_message/2]).

-define(SET, partisan_membership_set).

-record(full_v1, {actor, membership}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Initialize the strategy state.
init(Identity) ->
    State = maybe_load_state_from_disk(Identity),
    MembershipList = membership_list(State),
    persist_state(State),
    {ok, MembershipList, State}.

%% @doc When a node is connected, return the state, membership and outgoing message queue to be transmitted.
join(#full_v1{membership=Membership0} = State0, _Node, #full_v1{membership=NodeMembership}) ->
    Membership = ?SET:merge(Membership0, NodeMembership),
    State = State0#full_v1{membership=Membership},
    MembershipList = membership_list(State),
    OutgoingMessages = gossip_messages(State),
    persist_state(State),
    {ok, MembershipList, OutgoingMessages, State}.

%% @doc Leave a node from the cluster.
leave(#full_v1{}=State0, #{name := NameToRemove}) ->
    Membership0 = State0#full_v1.membership,
    Actor = State0#full_v1.actor,

    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    Membership = lists:foldl(
        fun
            (#{name := Name} = N, Acc0) when Name == NameToRemove ->
                {ok, Acc} = ?SET:remove(N, Actor, Acc0),
                Acc;
            (_, Acc0) ->
                Acc0
        end,
        Membership0,
        membership_list(State0)
    ),

    %% Self-leave removes our own state and resets it.
    StateToGossip = State0#full_v1{membership = Membership},

    State = case partisan:node() of
        NameToRemove ->
            %% Reset our state, store this, but gossip the state with us
            %% removed to the remainder of the members.
            new_state(Actor);
        _ ->
            %% Gossip state with member removed.
            StateToGossip
    end,

    MembershipList = membership_list(State),

    %% Gossip new membership to existing members, so they remove themselves.
    OutgoingMessages = gossip_messages(State0, StateToGossip),

    persist_state(State),

    {ok, MembershipList, OutgoingMessages, State}.


%% @doc Periodic protocol maintenance.
periodic(State) ->
    MembershipList = membership_list(State),
    OutgoingMessages = gossip_messages(State),

    {ok, MembershipList, OutgoingMessages, State}.

%% @doc Handling incoming protocol message.
handle_message(
    #full_v1{membership = Membership0} = State0,
    {#{name := From}, #full_v1{membership = Membership1}}) ->

    ?LOG_DEBUG(#{
        description => "Received membership_strategy",
        from => From,
        membership => ?SET:to_list(Membership1)
    }),

    case ?SET:equal(Membership0, Membership1) of
        true ->
            %% Convergence of gossip at this node.
            MembershipList = membership_list(State0),
            OutgoingMessages = [],
            {ok, MembershipList, OutgoingMessages, State0};
        false ->
            %% Merge, persist, reforward to peers.
            Membership = ?SET:merge(Membership0, Membership1),
            State = State0#full_v1{membership = Membership},
            MembershipList = membership_list(State),
            OutgoingMessages = gossip_messages(State),
            persist_state(State),

            {ok, MembershipList, OutgoingMessages, State}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
membership_list(#full_v1{membership=Membership}) ->
    ?SET:to_list(Membership).

%% @private
gossip_messages(State) ->
    gossip_messages(State, State).

%% @private
gossip_messages(State0, State) ->
    MembershipList = without_me(membership_list(State0)),

    case partisan_config:get(gossip, true) of
        true ->
            case MembershipList of
                [] ->
                    [];
                AllPeers ->
                    lists:map(
                        fun(Peer) ->
                            Message = {membership_strategy, {myself(), State}},
                            {Peer, Message}
                        end,
                        AllPeers
                    )
            end;
        _ ->
            []
    end.

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
data_root() ->
    case application:get_env(partisan, partisan_data_dir) of
        {ok, PRoot} ->
            filename:join(PRoot, "default_peer_service");
        undefined ->
            undefined
    end.

%% @private
new_state(Actor) ->
    {ok, Membership} = ?SET:add(myself(), Actor, ?SET:new()),
    LocalState = #full_v1{membership=Membership, actor=Actor},
    persist_state(LocalState),
    LocalState.

%% @private
myself() ->
    partisan:node_spec().

%% @private
persist_state(State) ->
    case partisan_config:get(persist_state, true) of
        true ->
            write_state_to_disk(State);
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
without_me(MembershipList) ->
    MyNode = partisan:node(),
    lists:filter(fun(#{name := Name}) -> Name =/= MyNode end, MembershipList).
