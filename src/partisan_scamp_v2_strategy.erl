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

%% @reference https://people.maths.bris.ac.uk/~maajg/hiscamp-sigops.pdf

%% @todo Join of InView.
%% @todo Node unsubscript.

-module(partisan_scamp_v2_strategy).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(partisan_membership_strategy).

-export([init/1,
         join/3,
         leave/2,
         periodic/1,
         handle_message/2]).

%% @todo Setup isolation handling properly.

-include("partisan.hrl").

-record(scamp_v2, {actor, partial_view, in_view}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Initialize the strategy state.
%%      Start with an empty state with only ourselves known.
init(Identity) ->
    Membership = sets:add_element(myself(), sets:new()),
    State = #scamp_v2{partial_view=Membership, actor=Identity},
    PartialViewList = partial_view_list(State),
    {ok, PartialViewList, State}.

%% @doc When a remote node is connected, notify that node to add us.  Then, perform forwarding, if necessary.
join(#scamp_v2{partial_view=PartialView0}=State0, Node, _NodeState) ->
    OutgoingMessages0 = [],

    %% 1. Add node to our state.
    lager:info("~p: Adding node ~p to our partial_view.", [node(), Node]),
    Membership = sets:add_element(Node, PartialView0),

    %% 2. Notify node to add us to its state. 
    %%    This is lazily done to ensure we can setup the TCP connection both ways, first.
    Myself = partisan_peer_service_manager:myself(),
    OutgoingMessages1 = OutgoingMessages0 ++ [{Node, {protocol, {forward_subscription, Myself}}}],

    %% 3. Notify all members we know about to add node to their partial_view.
    OutgoingMessages2 = sets:fold(fun(N, OM) ->
        lager:info("~p: Forwarding subscription for ~p to node: ~p", [node(), Node, N]),
        OM ++ [{N, {protocol, {forward_subscription, Node}}}]
        end, OutgoingMessages1, PartialView0),

    %% 4. Use 'c - 1' (failure tolerance value) to send forwarded subscriptions for node.
    %% 
    %% @todo: Ambiguity here: "These forwarded subscriptions may be kept by the neighbours or 
    %%                        "forwarded, but are not destroyed until some node keeps them?"
    %% 
    %% What does this mean?
    %% 
    C = partisan_config:get(scamp_c, ?SCAMP_C_VALUE),
    ForwardMessages = lists:map(fun(N) ->
        lager:info("~p: Forwarding additional subscription for ~p to node: ~p", [node(), Node, N]),
        {N, {protocol, {forward_subscription, Node}}}
        end, select_random_sublist(State0, C - 1)), %% Important difference from scamp_v1: (c - 1) additional copies instead of c!
    OutgoingMessages = OutgoingMessages2 ++ ForwardMessages,

    State = State0#scamp_v2{partial_view=Membership},
    PartialViewList = partial_view_list(State),
    {ok, PartialViewList, OutgoingMessages, State}.

%% @doc Leave a node from the cluster.
leave(#scamp_v2{partial_view=PartialView0}=State0, Node) ->
    lager:info("~p: Issuing remove_subscription for node ~p.", [node(), Node]),

    %% Remove node.
    Membership = sets:del_element(Node, PartialView0),
    PartialViewList0 = partial_view_list(State0),

    %% Gossip to existing cluster members.
    Message = {remove_subscription, Node},
    OutgoingMessages = lists:map(fun(Peer) -> {Peer, {protocol, Message}} end, PartialViewList0),

    %% Return updated membership.
    State = State0#scamp_v2{partial_view=Membership},
    PartialViewList = partial_view_list(State),

    {ok, PartialViewList, OutgoingMessages, State}.

%% @doc Periodic protocol maintenance.
periodic(State) ->
    PartialViewList = partial_view_list(State),
    OutgoingMessages = [],
    {ok, PartialViewList, OutgoingMessages, State}.

%% @doc Handling incoming protocol message.
handle_message(#scamp_v2{partial_view=PartialView0}=State0, {remove_subscription, Node}) ->
    lager:info("~p: Received remove_subscription for node ~p.", [node(), Node]),
    PartialViewList0 = partial_view_list(State0),

    case sets:is_element(Node, PartialView0) of 
        true ->
            %% Remove.
            Membership = sets:del_element(PartialView0, Node),

            %% Gossip removals.
            Message = {remove_subscription, Node},
            OutgoingMessages = lists:map(fun(Peer) -> {Peer, {protocol, Message}} end, PartialViewList0),

            %% Update state.
            State = State0#scamp_v2{partial_view=Membership},
            PartialViewList = partial_view_list(State),

            {ok, PartialViewList, OutgoingMessages, State};
        false ->
            OutgoingMessages = [],
            {ok, PartialViewList0, OutgoingMessages, State0}
    end;
handle_message(#scamp_v2{partial_view=PartialView0}=State0, {forward_subscription, Node}) ->
    lager:info("~p: Received subscription for node ~p.", [node(), Node]),
    PartialViewList0 = partial_view_list(State0),

    %% Probability: P = 1 / (1 + sizeOf(View))
    Random = random_0_or_1(),
    Keep = trunc((sets:size(PartialView0) + 1) * Random),

    case Keep =:= 0 andalso not lists:member(Node, PartialViewList0) of 
        true ->
            lager:info("~p: Adding subscription for node: ~p", [node(), Node]),
            Membership = sets:add_element(Node, PartialView0),
            State = State0#scamp_v2{partial_view=Membership},
            PartialViewList = partial_view_list(State),
            OutgoingMessages = [],
            {ok, PartialViewList, OutgoingMessages, State};
        false ->
            OutgoingMessages = lists:map(fun(N) ->
                lager:info("~p: Forwarding subscription for ~p to node: ~p", [node(), Node, N]),
                {N, {protocol, {forward_subscription, Node}}}
                end, select_random_sublist(State0, 1)),
            {ok, PartialViewList0, OutgoingMessages, State0}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
partial_view_list(#scamp_v2{partial_view=Membership}) ->
    sets:to_list(Membership).

%% @private
select_random_sublist(State, K) ->
    List = partial_view_list(State),
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