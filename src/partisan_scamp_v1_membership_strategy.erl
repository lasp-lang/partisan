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

-include("partisan.hrl").
-include("partisan_logger.hrl").

-record(scamp_v1, {
    actor               ::  partisan:actor(),
    membership          ::  sets:set(partisan:node_spec()),
    last_message_time   ::  erlang:timestamp() | undefined
}).

-type t()               ::  #scamp_v1{}.

-export([init/1]).
-export([join/3]).
-export([leave/2]).
-export([compare/2]).
-export([prune/2]).
-export([periodic/1]).
-export([handle_message/2]).



%%%===================================================================
%%% API
%%%===================================================================

%% @doc Initialize the strategy state.
%%      Start with an empty state with only ourselves known.
init(Identity) ->
    Membership = sets:add_element(
        partisan:node_spec(),
        sets:new([{version, 2}])
    ),
    State = #scamp_v1{
        membership = Membership,
        actor = Identity
    },

    {ok, members(State), State}.

%% @doc When a remote node is connected, notify that node to add us.  Then, perform forwarding, if necessary.
join(Node, #scamp_v1{} = _NodeState, #scamp_v1{} = State0) ->
    Membership0 = State0#scamp_v1.membership,
    OutgoingMessages0 = [],

    %% 1. Add node to our state.
    ?LOG_TRACE(
        "~p: Adding node ~p to our membership.", [partisan:node(), Node]
    ),
    Membership = sets:add_element(Node, Membership0),

    %% 2. Notify node to add us to its state.
    %%    This is lazily done to ensure we can setup the TCP connection both
    %%    ways, first.
    Myself = partisan:node_spec(),
    OutgoingMessages1 = OutgoingMessages0 ++ [{Node, {membership_strategy, {forward_subscription, Myself}}}],

    %% 3. Notify all members we know about to add node to their membership.
    OutgoingMessages2 =
        sets:fold(
            fun(N, OM) ->
                ?LOG_TRACE(
                    "~p: Forwarding subscription for ~p to node: ~p",
                    [partisan:node(), Node, N]
                ),

                OM ++ [{N, {membership_strategy, {forward_subscription, Node}}}]
            end,
            OutgoingMessages1,
            Membership0
        ),

    %% 4. Use 'c' (failure tolerance value) to send forwarded subscriptions for node.
    C = partisan_config:get(scamp_c, ?SCAMP_C_VALUE),
    ForwardMessages = lists:map(
        fun(N) ->
            ?LOG_TRACE(
                "~p: Forwarding additional subscription for ~p to node: ~p",
                [partisan:node(), Node, N]
            ),

            {N, {membership_strategy, {forward_subscription, Node}}}

        end,
        select_random_sublist(State0, C)
    ),

    OutgoingMessages = OutgoingMessages2 ++ ForwardMessages,

    State = State0#scamp_v1{membership = Membership},

    {ok, members(State), OutgoingMessages, State}.

%% @doc Leave a node from the cluster.
leave(Node, #scamp_v1{membership=Membership0}=State0) ->
    ?LOG_TRACE(
        "~p: Issuing remove_subscription for node ~p.",
        [partisan:node(), Node]
    ),

    %% Remove node.
    Membership = sets:del_element(Node, Membership0),

    %% Gossip to existing cluster members.
    Message = {remove_subscription, Node},
    OutgoingMessages = lists:map(
        fun(Peer) -> {Peer, {membership_strategy, Message}} end,
        members(State0)
    ),

    %% Return updated membership.
    State = State0#scamp_v1{membership = Membership},
    Members = members(State),

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

compare(_Members, #scamp_v1{}) ->
    %% TODO at the momebr this is called only when peer_service:jupdate_members
    %% is called. We need to define what happens in this case as we maintain a
    %% partial view of the cluster and Members could be the complete cluster
    %% view as discovered by partisan_peer_discovery_agent or manually by the
    %% user.
    {[], []}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
prune(_Nodes, #scamp_v1{} = State) ->
    %% Not implemented
    {ok, members(State), State}.


%% @doc Periodic protocol maintenance.
periodic(#scamp_v1{last_message_time=LastMessageTime} = State) ->
    SourceNode = partisan:node_spec(),
    Members = members(State),

    %% Isolation detection:
    %%
    %% Since we do not know the rate of message transmission by other nodes in the system,
    %% periodically transmit a message to all known nodes.  Each node will keep track of the
    %% last message received, and if we don't receive one after X interval, then we know
    %% we are isolated.
    OutgoingPingMessages = lists:map(fun(Peer) ->
        {Peer, {membership_strategy, {ping, SourceNode}}}
    end, Members),

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
            ?LOG_TRACE("~p: Node is possibly isolated.", [partisan:node()]),

            Myself = partisan:node_spec(),

            lists:map(fun(N) ->
                ?LOG_TRACE(
                    "~p: Forwarding additional subscription for ~p to node: ~p",
                    [partisan:node(), Myself, N]
                ),

                {N, {membership_strategy, {forward_subscription, Myself}}}
            end, select_random_sublist(State, 1));
        false ->
            %% Node is not isolated.
            []
    end,

    {ok, Members, OutgoingSubscriptionMessages ++ OutgoingPingMessages, State}.

%% @doc Handling incoming protocol message.
handle_message({ping, SourceNode}, State) ->
    ?LOG_TRACE(
        "~p: Received ping from node ~p.", [partisan:node(), SourceNode]
    ),

    Members = members(State),
    LastMessageTime = erlang:timestamp(),
    OutgoingMessages = [],
    {ok, Members, OutgoingMessages, State#scamp_v1{last_message_time=LastMessageTime}};

%% @doc Handling incoming protocol message.
handle_message({remove_subscription, Node}, #scamp_v1{} = State0) ->

    ?LOG_INFO(
        "~p: Received remove_subscription for node ~p.",
        [partisan:node(), Node]
    ),

    Membership0 = State0#scamp_v1.membership,

    case sets:is_element(Node, Membership0) of
        true ->
            %% Remove.
            Membership = sets:del_element(Membership0, Node),
            %% eqwalizer:ignore
            State = State0#scamp_v1{membership = Membership},
            Members = members(State),

            %% Gossip removals.
            Message = {remove_subscription, Node},

            OutgoingMessages = lists:map(
                fun(Peer) ->
                    {Peer, {membership_strategy, Message}}
                end,
                Members
            ),

            {ok, Members, OutgoingMessages, State};

        false ->
            OutgoingMessages = [],
            {ok, members(State0), OutgoingMessages, State0}
    end;

handle_message({forward_subscription, Node}, #scamp_v1{} = State0) ->
    ?LOG_TRACE(
        "~p: Received subscription for node ~p.", [partisan:node(), Node]
    ),

    Membership0 = State0#scamp_v1.membership,
    Members0 = members(State0),

    %% Probability: P = 1 / (1 + sizeOf(View))
    Random = random_0_or_1(),
    Keep = trunc((sets:size(Membership0) + 1) * Random),

    case Keep =:= 0 andalso not lists:member(Node, Members0) of
        true ->
            ?LOG_TRACE(
                "~p: Adding subscription for node: ~p", [partisan:node(), Node]
            ),

            Membership = sets:add_element(Node, Membership0),
            State = State0#scamp_v1{membership=Membership},
            Members = members(State),
            OutgoingMessages = [],
            {ok, Members, OutgoingMessages, State};
        false ->
            OutgoingMessages = lists:map(fun(N) ->
                ?LOG_TRACE(
                    "~p: Forwarding subscription for ~p to node: ~p",
                    [partisan:node(), Node, N]
                ),

                {N, {membership_strategy, {forward_subscription, Node}}}
                end, select_random_sublist(State0, 1)),
            {ok, Members0, OutgoingMessages, State0}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
members(#scamp_v1{membership = Membership}) ->
    sets:to_list(Membership).


%% @private
select_random_sublist(State, K) ->
    List = members(State),
    lists:sublist(shuffle(List), K).

%% -----------------------------------------------------------------------------
%% @doc
%% http://stackoverflow.com/questions/8817171/shuffling-elements-in-a-list-randomly-re-arrange-list-elements/8820501#8820501
%% @end
%% -----------------------------------------------------------------------------
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
