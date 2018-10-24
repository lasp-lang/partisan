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

%% @reference https://people.maths.bris.ac.uk/~maajg/scamp-ngc.pdf

-module(partisan_scamp_v1_membership_strategy).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(partisan_membership_strategy).

-export([init/1,
         join/3,
         leave/2,
         periodic/1,
         handle_message/2]).

-include("partisan.hrl").

-record(scamp_v1, {actor, membership, last_message_time}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Initialize the strategy state.
%%      Start with an empty state with only ourselves known.
init(Identity) ->
    Membership = sets:add_element(myself(), sets:new()),
    State = #scamp_v1{membership=Membership, actor=Identity},
    MembershipList = membership_list(State),
    {ok, MembershipList, State}.

%% @doc When a remote node is connected, notify that node to add us.  Then, perform forwarding, if necessary.
join(#scamp_v1{membership=Membership0}=State0, Node, _NodeState) ->
    OutgoingMessages0 = [],

    %% 1. Add node to our state.
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Adding node ~p to our membership.", [node(), Node]);
        false ->
            ok
    end,
    Membership = sets:add_element(Node, Membership0),

    %% 2. Notify node to add us to its state. 
    %%    This is lazily done to ensure we can setup the TCP connection both ways, first.
    Myself = partisan_peer_service_manager:myself(),
    OutgoingMessages1 = OutgoingMessages0 ++ [{Node, {protocol, {forward_subscription, Myself}}}],

    %% 3. Notify all members we know about to add node to their membership.
    OutgoingMessages2 = sets:fold(fun(N, OM) ->
        case partisan_config:get(tracing, ?TRACING) of 
            true ->
                lager:info("~p: Forwarding subscription for ~p to node: ~p", [node(), Node, N]);
            false ->
                ok
        end,

        OM ++ [{N, {protocol, {forward_subscription, Node}}}]

        end, OutgoingMessages1, Membership0),

    %% 4. Use 'c' (failure tolerance value) to send forwarded subscriptions for node.
    C = partisan_config:get(scamp_c, ?SCAMP_C_VALUE),
    ForwardMessages = lists:map(fun(N) ->
        case partisan_config:get(tracing, ?TRACING) of 
            true ->
                lager:info("~p: Forwarding additional subscription for ~p to node: ~p", [node(), Node, N]);
            false ->
                ok
        end,

        {N, {protocol, {forward_subscription, Node}}}

        end, select_random_sublist(State0, C)),
    OutgoingMessages = OutgoingMessages2 ++ ForwardMessages,

    State = State0#scamp_v1{membership=Membership},
    MembershipList = membership_list(State),
    {ok, MembershipList, OutgoingMessages, State}.

%% @doc Leave a node from the cluster.
leave(#scamp_v1{membership=Membership0}=State0, Node) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Issuing remove_subscription for node ~p.", [node(), Node]);
        false ->
            ok
    end,

    %% Remove node.
    Membership = sets:del_element(Node, Membership0),
    MembershipList0 = membership_list(State0),

    %% Gossip to existing cluster members.
    Message = {remove_subscription, Node},
    OutgoingMessages = lists:map(fun(Peer) -> {Peer, {protocol, Message}} end, MembershipList0),

    %% Return updated membership.
    State = State0#scamp_v1{membership=Membership},
    MembershipList = membership_list(State),

    {ok, MembershipList, OutgoingMessages, State}.

%% @doc Periodic protocol maintenance.
periodic(#scamp_v1{last_message_time=LastMessageTime} = State) ->
    SourceNode = myself(),
    MembershipList = membership_list(State),

    %% Isolation detection: 
    %%
    %% Since we do not know the rate of message transmission by other nodes in the system,
    %% periodically transmit a message to all known nodes.  Each node will keep track of the 
    %% last message received, and if we don't receive one after X interval, then we know 
    %% we are isolated.
    OutgoingPingMessages = lists:map(fun(Peer) -> 
        {Peer, {protocol, {ping, SourceNode}}}
    end, MembershipList),

    Difference = case LastMessageTime of 
        undefined ->
            0;
        _ ->
            CurrentTime = erlang:timestamp(),
            timer:now_diff(CurrentTime, LastMessageTime)
    end,

    OutgoingSubscriptionMessages = case Difference > (?PERIODIC_INTERVAL * ?SCAMP_MESSAGE_WINDOW) of 
        true ->
            %% Node is isolated.
            case partisan_config:get(tracing, ?TRACING) of 
                true ->
                    lager:info("~p: Node is possibily isolated.", [node()]);
                false ->
                    ok
            end,

            Myself = myself(),

            lists:map(fun(N) ->
                case partisan_config:get(tracing, ?TRACING) of 
                    true ->
                        lager:info("~p: Forwarding additional subscription for ~p to node: ~p", [node(), Myself, N]);
                    false ->
                        ok
                end,
                
                {N, {protocol, {forward_subscription, Myself}}}
            end, select_random_sublist(State, 1));
        false ->
            %% Node is not isolated.
            []
    end,

    {ok, MembershipList, OutgoingSubscriptionMessages ++ OutgoingPingMessages, State}.

%% @doc Handling incoming protocol message.
handle_message(State, {ping, SourceNode}) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Received ping from node ~p.", [node(), SourceNode]);
        false ->
            ok
    end,

    MembershipList = membership_list(State),
    LastMessageTime = erlang:timestamp(),
    OutgoingMessages = [],
    {ok, MembershipList, OutgoingMessages, State#scamp_v1{last_message_time=LastMessageTime}};
%% @doc Handling incoming protocol message.
handle_message(#scamp_v1{membership=Membership0}=State0, {remove_subscription, Node}) ->
    lager:info("~p: Received remove_subscription for node ~p.", [node(), Node]),
    MembershipList0 = membership_list(State0),

    case sets:is_element(Node, Membership0) of 
        true ->
            %% Remove.
            Membership = sets:del_element(Membership0, Node),

            %% Gossip removals.
            Message = {remove_subscription, Node},
            OutgoingMessages = lists:map(fun(Peer) -> {Peer, {protocol, Message}} end, MembershipList0),

            %% Update state.
            State = State0#scamp_v1{membership=Membership},
            MembershipList = membership_list(State),

            {ok, MembershipList, OutgoingMessages, State};
        false ->
            OutgoingMessages = [],
            {ok, MembershipList0, OutgoingMessages, State0}
    end;
handle_message(#scamp_v1{membership=Membership0}=State0, {forward_subscription, Node}) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Received subscription for node ~p.", [node(), Node]);
        false ->
            ok
    end,

    MembershipList0 = membership_list(State0),

    %% Probability: P = 1 / (1 + sizeOf(View))
    Random = random_0_or_1(),
    Keep = trunc((sets:size(Membership0) + 1) * Random),

    case Keep =:= 0 andalso not lists:member(Node, MembershipList0) of 
        true ->
            case partisan_config:get(tracing, ?TRACING) of 
                true ->
                    lager:info("~p: Adding subscription for node: ~p", [node(), Node]);
                false ->
                    ok
            end,

            Membership = sets:add_element(Node, Membership0),
            State = State0#scamp_v1{membership=Membership},
            MembershipList = membership_list(State),
            OutgoingMessages = [],
            {ok, MembershipList, OutgoingMessages, State};
        false ->
            OutgoingMessages = lists:map(fun(N) ->
                case partisan_config:get(tracing, ?TRACING) of 
                    true ->
                        lager:info("~p: Forwarding subscription for ~p to node: ~p", [node(), Node, N]);
                    false ->
                        ok
                end,

                {N, {protocol, {forward_subscription, Node}}}
                end, select_random_sublist(State0, 1)),
            {ok, MembershipList0, OutgoingMessages, State0}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
membership_list(#scamp_v1{membership=Membership}) ->
    sets:to_list(Membership).

%% @private
select_random_sublist(State, K) ->
    List = membership_list(State),
    lists:sublist(shuffle(List), K).

%% @reference http://stackoverflow.com/questions/8817171/shuffling-elements-in-a-list-randomly-re-arrange-list-elements/8820501#8820501
shuffle(L) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), N} || N <- L])].

%% @private
random_0_or_1() ->
    Rand = rand:uniform(10),
    case Rand >= 5 of
        true ->
            1;
        false ->
            0
    end.

%% @private
myself() ->
    partisan_peer_service_manager:myself().