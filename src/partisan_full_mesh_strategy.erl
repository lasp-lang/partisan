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

%% -behaviour(membership_strategy).

-export([init/1,
         join/3,
         leave/2,
         periodic/1]).

-define(SET, state_orset).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Initialize the strategy state.
init(Identity) ->
    State = maybe_load_state_from_disk(Identity),
    Membership = sets:to_list(?SET:query(State)),
    persist_state(State),
    {ok, Membership, State}.

%% @doc When a node is connected, return the state, membership and outgoing message queue to be transmitted.
join(State0, _Node, NodeState) ->
    State = ?SET:merge(NodeState, State0),
    Membership = sets:to_list(?SET:query(State)),
    OutgoingMessages = gossip_messages(State),
    persist_state(State),
    {ok, Membership, OutgoingMessages, State}.

%% @doc
leave(State0, Node) ->
    %% TODO: Actor needs to be part of the state!
    Actor = gen_actor(),

    %% Node may exist in the membership on multiple ports, so we need to
    %% remove all.
    State1 = lists:foldl(fun(#{name := Name} = N, M0) ->
                        case Node of
                            Name ->
                                {ok, M} = ?SET:mutate({rmv, N}, Actor, M0),
                                M;
                            _ ->
                                M0
                        end
                end, State0, sets:to_list(?SET:query(State0))),

    %% Self-leave removes our own state and resets it.
    State = case partisan_peer_service_manager:mynode() of
        Node ->
            empty_membership(Actor);
        _ ->
            State1
    end,

    Membership = sets:to_list(?SET:query(State)),

    %% Gossip new membership to existing members, so they remove themselves.
    OutgoingMessages = gossip_messages(State0, State),
    persist_state(State),

    {ok, Membership, OutgoingMessages, State}.

%% @doc
periodic(State) ->
    Membership = sets:to_list(?SET:query(State)),
    OutgoingMessages = gossip_messages(State),

    {ok, Membership, OutgoingMessages, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
gossip_messages(State) ->
    gossip_messages(State, State).

%% @private
gossip_messages(State0, State) ->
    Membership = sets:to_list(?SET:query(State0)),

    case partisan_config:get(gossip, true) of
        true ->
            case Membership of
                [] ->
                    [];
                AllPeers ->
                    Members = [N || #{name := N} <- Membership],
                    lager:debug("Sending state with updated membership: ~p", [Members]),
                    lists:map(fun(Peer) -> {Peer, {protocol, myself(), State}} end, AllPeers)
            end;
        _ ->
            []
    end.

%% @private
maybe_load_state_from_disk(Actor) ->
    case data_root() of
        undefined ->
            empty_membership(Actor);
        Dir ->
            case filelib:is_regular(filename:join(Dir, "cluster_state")) of
                true ->
                    {ok, Bin} = file:read_file(filename:join(Dir, "cluster_state")),
                    ?SET:decode(erlang, Bin);
                false ->
                    empty_membership(Actor)
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
empty_membership(Actor) ->
    {ok, LocalState} = ?SET:mutate({add, myself()}, Actor, ?SET:new()),
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
            ok = file:write_file(File, ?SET:encode(erlang, State))
    end.

%% @private
gen_actor() ->
    Node = atom_to_list(partisan_peer_service_manager:mynode()),
    Unique = erlang:unique_integer([positive]),
    TS = integer_to_list(Unique),
    Term = Node ++ TS,
    crypto:hash(sha, Term).