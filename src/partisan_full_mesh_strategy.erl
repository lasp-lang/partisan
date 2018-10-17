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

-module(partisan_full_mesh_strategy).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(partisan_membership_strategy).

-export([init/1,
         join/3,
         leave/2,
         periodic/1,
         handle_message/2]).

-define(SET, state_orset).

-record(full_mesh_v1, {actor, membership}).

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
join(#full_mesh_v1{membership=Membership0} = State0, _Node, #full_mesh_v1{membership=NodeMembership}) ->
    Membership = ?SET:merge(Membership0, NodeMembership),
    State = State0#full_mesh_v1{membership=Membership},
    MembershipList = membership_list(State),
    OutgoingMessages = gossip_messages(State),
    persist_state(State),
    {ok, MembershipList, OutgoingMessages, State}.

%% @doc Leave a node from the cluster.
leave(#full_mesh_v1{membership=Membership0, actor=Actor}=State0, #{name := NameToRemove}) ->
    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    Membership = lists:foldl(fun(#{name := Name} = N, M0) ->
                        case NameToRemove of
                            Name ->
                                {ok, M} = ?SET:mutate({rmv, N}, Actor, M0),
                                M;
                            _ ->
                                M0
                        end
                end, Membership0, membership_list(State0)),

    %% Self-leave removes our own state and resets it.
    State = case partisan_peer_service_manager:mynode() of
        NameToRemove ->
            new_state(Actor);
        _ ->
            State0#full_mesh_v1{membership=Membership}
    end,

    MembershipList = membership_list(State),

    %% Gossip new membership to existing members, so they remove themselves.
    OutgoingMessages = gossip_messages(State0, State),
    persist_state(State),

    {ok, MembershipList, OutgoingMessages, State}.

%% @doc Periodic protocol maintenance.
periodic(State) ->
    MembershipList = membership_list(State),
    OutgoingMessages = gossip_messages(State),

    {ok, MembershipList, OutgoingMessages, State}.

%% @doc Handling incoming protocol message.
handle_message(#full_mesh_v1{membership=Membership0}=State0, {#{name := _From}, #full_mesh_v1{membership=NodeMembership}}) ->
    case ?SET:equal(Membership0, NodeMembership) of
        true ->
            %% Convergence of gossip at this node.
            MembershipList = membership_list(State0),
            OutgoingMessages = [],

            {ok, MembershipList, OutgoingMessages, State0};
        false ->
            %% Merge, persist, reforward to peers.
            Membership = ?SET:merge(Membership0, NodeMembership),
            State = State0#full_mesh_v1{membership=Membership},
            MembershipList = membership_list(State),
            OutgoingMessages = gossip_messages(State),
            persist_state(State),

            {ok, MembershipList, OutgoingMessages, State}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
membership_list(#full_mesh_v1{membership=Membership}) ->
    sets:to_list(?SET:query(Membership)).

%% @private
gossip_messages(State) ->
    gossip_messages(State, State).

%% @private
gossip_messages(State0, State) ->
    MembershipList = membership_list(State0),

    case partisan_config:get(gossip, true) of
        true ->
            case MembershipList of
                [] ->
                    [];
                AllPeers ->
                    lists:map(fun(Peer) -> {Peer, {protocol, {myself(), State}}} end, AllPeers)
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
    {ok, Membership} = ?SET:mutate({add, myself()}, Actor, ?SET:new()),
    LocalState = #full_mesh_v1{membership=Membership, actor=Actor},
    persist_state(LocalState),
    LocalState.

%% @private
myself() ->
    partisan_peer_service_manager:myself().

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