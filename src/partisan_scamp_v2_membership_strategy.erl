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

-module(partisan_scamp_v2_membership_strategy).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(partisan_membership_strategy).

-export([init/1,
         join/3,
         leave/2,
         periodic/1,
         handle_message/2]).

%% Isolation prevention.
%%
%% Protocol has no specific description of how detection of recovery or isolation
%% is performed, so we use the protocol specification for that taken from the
%% scamp_v1 membership strategy.

-include("partisan.hrl").

-record(scamp_v2, {actor :: term(), 
                   partial_view :: [node_spec()], 
                   in_view :: [node_spec()],
                   last_message_time :: term()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Initialize the strategy state.
%%      Start with an empty state with only ourselves known.
init(Identity) ->
    PartialView = [myself()],
    InView = [],
    State = #scamp_v2{in_view=InView, partial_view=PartialView, actor=Identity},
    {ok, PartialView, State}.

%% @doc When a remote node is connected, notify that node to add us.  Then, perform forwarding, if necessary.
join(#scamp_v2{partial_view=PartialView0}=State0, Node, _NodeState) ->
    OutgoingMessages0 = [],

    %% 1. Add node to our state.
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Adding node ~p to our partial_view.", [node(), Node]);
        false ->
            ok
    end,
    PartialView = [Node|PartialView0],

    %% 2. Notify node to add us to its state. 
    %%    This is lazily done to ensure we can setup the TCP connection both ways, first.
    Myself = partisan_peer_service_manager:myself(),
    OutgoingMessages1 = OutgoingMessages0 ++ [{Node, {membership_strategy, {forward_subscription, Myself}}}],

    %% 3. Notify all members we know about to add node to their partial_view.
    OutgoingMessages2 = lists:foldl(fun(N, OM) ->
        case partisan_config:get(tracing, ?TRACING) of 
            true ->
                lager:info("~p: Forwarding subscription for ~p to node: ~p", [node(), Node, N]);
            false ->
                ok
        end,

        OM ++ [{N, {membership_strategy, {forward_subscription, Node}}}]
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
        case partisan_config:get(tracing, ?TRACING) of 
            true ->
                lager:info("~p: Forwarding additional subscription for ~p to node: ~p", [node(), Node, N]);
            false ->
                ok
        end,

        {N, {membership_strategy, {forward_subscription, Node}}}
        end, select_random_sublist(State0, C - 1)), %% Important difference from scamp_v1: (c - 1) additional copies instead of c!
    OutgoingMessages = OutgoingMessages2 ++ ForwardMessages,

    {ok, PartialView, OutgoingMessages, State0#scamp_v2{partial_view=PartialView}}.

%% @doc Leave a node from the cluster.
leave(#scamp_v2{partial_view=PartialView}=State0, Node) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Issuing remove_subscription for node ~p.", [node(), Node]);
        false ->
            ok
    end,

    %% Begin unsubcription process: send a bootstrap message to the node that is being removed.
    Message = {bootstrap_remove_subscription, Node},
    OutgoingMessages = lists:map(fun(Peer) -> {Peer, {membership_strategy, Message}} end, PartialView),
    {ok, PartialView, OutgoingMessages, State0}.

%% @doc Periodic protocol maintenance.
periodic(#scamp_v2{partial_view=PartialView, last_message_time=LastMessageTime}=State) ->
    SourceNode = myself(),

    %% Isolation detection: 
    %%
    %% Since we do not know the rate of message transmission by other nodes in the system,
    %% periodically transmit a message to all known nodes.  Each node will keep track of the 
    %% last message received, and if we don't receive one after X interval, then we know 
    %% we are isolated.
    OutgoingPingMessages = lists:map(fun(Peer) -> 
        {Peer, {membership_strategy, {ping, SourceNode}}}
    end, PartialView),

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
                    lager:info("~p: Node is possibly isolated.", [node()]);
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

                {N, {membership_strategy, {forward_subscription, Myself}}}
            end, select_random_sublist(State, 1));
        false ->
            %% Node is not isolated.
            []
    end,

    {ok, PartialView, OutgoingSubscriptionMessages ++ OutgoingPingMessages, State}.

%% @doc Handling incoming protocol message.
handle_message(#scamp_v2{partial_view=PartialView0}=State, {ping, SourceNode}) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Received ping from node ~p.", [node(), SourceNode]);
        false ->
            ok
    end,

    LastMessageTime = erlang:timestamp(),
    OutgoingMessages = [],
    {ok, PartialView0, OutgoingMessages, State#scamp_v2{last_message_time=LastMessageTime}};
handle_message(#scamp_v2{partial_view=PartialView0, in_view=InView0}=State0, {bootstrap_remove_subscription, Node}) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Received bootstrap_remove_subscription from node ~p.", [node(), Node]);
        false ->
            ok
    end,

    Myself = myself(),
    C = partisan_config:get(scamp_c, ?SCAMP_C_VALUE),

    case Node of
        %% Remove ourselves, but attempt to preserve the scaling relation.
        Myself ->
            %% 1. Notify InView[0 - (L - C - 1)] to replace with PartialView[0 - (L - C - 1)]
            NumToIterate = length(InView0) - (C - 1),

            ReplacementMessages = case NumToIterate > 0 of
                true ->
                    lists:map(fun(N) ->
                        Nth = lists:nth(N, InView0),
                        Replacement = lists:nth(N div length(PartialView0), PartialView0),
                        {Nth, {membership_strategy, {replace_subscription, Node, Replacement}}}
                    end, lists:seq(1, NumToIterate));
                false ->
                    []
            end,

            %% 2. Notify InView[(L - C - 1) - ] to remove.
            RemainderToIterate = length(InView0) - NumToIterate,
            
            RemovalMessages = case RemainderToIterate > 0 of
                true ->
                    lists:map(fun(N) ->
                        Nth = lists:nth(N, InView0),
                        {Nth, {membership_strategy, {remove_subscription, Node}}}
                    end, lists:seq(1, RemainderToIterate));
                false ->
                    []
            end,

            %% Reset our state.
            {ok, [], ReplacementMessages ++ RemovalMessages, State0#scamp_v2{in_view=[], partial_view=[]}};
        _ ->
            %% Not us, do nothing.
            {ok, PartialView0, [], State0}
    end;
handle_message(#scamp_v2{partial_view=PartialView0}=State0, {replace_subscription, Node, Replacement}) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Received replace_subscription for node ~p => ~p.", [node(), Node, Replacement]);
        false ->
            ok
    end,

    %% Replacement reorganizes the graphs so that the removed nodes parents connect to 
    %% its children; but, this doesn't update in links, right?  Is that missing in the 
    %% protocol description?

    PartialView = lists:map(fun(N) ->
            case N of
                Node ->
                    Replacement;
                _ ->
                    N
            end
        end, PartialView0),

    {ok, PartialView, [], State0#scamp_v2{partial_view=PartialView}};
handle_message(#scamp_v2{partial_view=PartialView0}=State0, {remove_subscription, Node}) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Received remove_subscription for node ~p.", [node(), Node]);
        false ->
            ok
    end,

    case lists:member(Node, PartialView0) of 
        true ->
            %% Remove.
            PartialView = PartialView0 -- [Node],

            %% Gossip removals.
            Message = {remove_subscription, Node},
            OutgoingMessages = lists:map(fun(Peer) -> {Peer, {membership_strategy, Message}} end, PartialView0),

            %% Update state.
            {ok, PartialView, OutgoingMessages, State0#scamp_v2{partial_view=PartialView}};
        false ->
            OutgoingMessages = [],
            {ok, PartialView0, OutgoingMessages, State0}
    end;
handle_message(#scamp_v2{partial_view=PartialView0}=State0, {forward_subscription, Node}) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Received subscription for node ~p.", [node(), Node]);
        false ->
            ok
    end,

    %% Probability: P = 1 / (1 + sizeOf(View))
    Random = random_0_or_1(),
    Keep = trunc((length(PartialView0) + 1) * Random),

    case Keep =:= 0 andalso not lists:member(Node, PartialView0) of 
        true ->
            case partisan_config:get(tracing, ?TRACING) of 
                true ->
                    lager:info("~p: Adding subscription for node: ~p", [node(), Node]);
                false ->
                    ok
            end,
            PartialView = [Node|PartialView0],

            %% Respond to the node that's joining and tell them to keep us.
            case partisan_config:get(tracing, ?TRACING) of 
                true ->
                    lager:info("~p: Notifying ~p to keep us: ~p", [node(), Node, node()]);
                false ->
                    ok
            end,
            OutgoingMessages = [{Node, {membership_strategy, {keep_subscription, myself()}}}],

            {ok, PartialView, OutgoingMessages, State0#scamp_v2{partial_view=PartialView}};
        false ->
            OutgoingMessages = lists:map(fun(N) ->
                case partisan_config:get(tracing, ?TRACING) of 
                    true ->
                        lager:info("~p: Forwarding subscription for ~p to node: ~p", [node(), Node, N]);
                    false ->
                        ok
                end,
                {N, {membership_strategy, {forward_subscription, Node}}}
            end, select_random_sublist(State0, 1)),
            {ok, PartialView0, OutgoingMessages, State0}
    end;
handle_message(#scamp_v2{partial_view=PartialView0, in_view=InView0}=State, {keep_subscription, Node}) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Received keep_subscription for node ~p.", [node(), Node]);
        false ->
            ok
    end,

    InView = [Node|InView0],
    OutgoingMessages = [],
    {ok, PartialView0, OutgoingMessages, State#scamp_v2{in_view=InView}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
select_random_sublist(#scamp_v2{partial_view=PartialView}, K) ->
    lists:sublist(shuffle(PartialView), K).

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