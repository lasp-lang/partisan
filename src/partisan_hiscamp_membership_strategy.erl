-module(partisan_hiscamp_membership_strategy).
-author("Hana Frluckaj <hanafrla@cmu.edu>").
-behavior(partisan_membership_strategy).
%%import Heap stuff
%change size to heaps:size()
% change export list
%-export([init/1, distance/2, hierarchial_clustering/2]).
-include("partisan.hrl").

%fix this
-record(hiScamp, {membership, actor, heap, level1, level2}).

%%initialize state
init(Identity) -> 
    Heap = heaps:new(),
    L1 = sets:new(),
    L2 = sets:new(),
    %dataset and data size
    %gold standard
    Membership = sets:add_element(myself(), sets:new()),
    State = #hiScamp{membership=Membership, actor=Identity, heap=Heap, level1 = L1, level2 = L2},
    MembershipList = membership_list(State),
    {ok, MembershipList, State}.

%% remote node connects, notify node to us. perform forwarding?
%% Membership0 is a set of node elements
join(#hiScamp{membership=Membership0, level1=L1, level2=L2}=State0, Node, _NodeState) ->
    OutgoingMessages0 = [],
    case partisan_config:get(tracing, ?TRACING) of
        true ->
            lager:info("~p: Adding node ~p to our membership.", [node90, Node]);
        false ->
            ok
    end,
    Membership = sets:add_element(Node, Membership0),
%% notify node to add us to its state.
    Myself = partisan_peer_service_manager:myself(),
    OutgoingMessages1 = OutgoingMessages0 ++ [{Node, {protocol, {forward_subscription, Myself}}}],
    OutgoingMessages2 = sets:fold(fun(N, OM) ->
        %% tracing is a macro
        case partisan_config:get(tracing, ?TRACING) of
            true ->
                lager:info("~p: Forwarding subscription for ~p to node: ~p", [node(), Node, N]);
            false ->
                ok
            end,
            OM ++ [{N, {protocol, {forward_subscription, Node}}}]
            end, OutgoingMessages1, Membership0),
    C = partisan_config:get(scamp_c, ?SCAMP_C_VALUE),
    ForwardMessages = lists:map(fun(N) ->
        case partisan_config:get(tracing, ?TRACING) of
            true ->
                lager:info("~p: Forwarding additional subs. for ~p to node: ~p", [node(), Node, N]);
            false ->
                ok
        end,
        {N, {protocol, {forward_subscription, Node}}}
        end, select_random_sublist(state0, C)),
    OutgoingMessages = OutgoingMessages2 ++ ForwardMessages,
    State = State0#hiScamp{membership=Membership},
    MembershipList = membership_list(State),
    {ok, Membership, OutgoingMessages, State}.

distance(#hiScamp{membership=Membership0, heap=Heap, level1=L1, level2=L2}=_State0, Node) ->
    % A node j joins the system by sending a subscription request to the node s which is closest to it
    Threshold = 3,
    Dict = get(distance_metrics),
    try dict:find(Node, Dict) of
        {ok, Dist} -> Dist
    catch
         error:E -> {"Error computing distance"}
    end,
    case Threshold > Dist of
        true -> 
            % process this case using Scamp within this cluster
            % subscription at level 1
            %need to send copies out to other nodes -> gossip protocol
            sets:add(Node, L1);
        false ->
                       % search through set to find another set
                       % store two sets in the record
            % subscription at level 2
                       %input Cluster0
            sets:add(Node, L2)
    end,
    heaps:add(Dist, Heap).

get_centroid_two_clusters(#hiScamp{membership=Membership0, heap=Heap, level1=L1, level2=L2} =State0) ->
    Min1 = heaps:min(Heap),
    Heap = heaps:delete_min(Heap),
    Min2 = heaps:min(Heap),
    case (sets:is_element(Min1, L1) andalso sets:is_element(Min2, L1) orelse 
         (sets:is_element(Min1, L2) andalso sets:is_element(Min2, L2))) of
        true ->
            % can group nodes as theyre the closest within the same threshold
        %% NODE CONNECTIONS
            ok;
        false -> 
            Heap = heaps:merge(heaps:from_list(sets:to_list(L2), sets:to_list(L1)))
    end.

hierarchial_clustering(#hiScamp{membership=Memberhsip0, heap=Heap, level1=L1, level2=L2}=State0) ->
    ok.

leave(#hiScamp{membership=Membership0}=State0, Node) ->
    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("~p: Issuing remove_subscription for node ~p.", [node(), Node]);
        false ->
            ok
    end,
    Membership = sets:del_element(Node, Membership0),
    MembershipList0 = membership_list(State0),
%% Gossip
    Message = {remove_subscription, Node},
    OutgoingMessages = lists:map(fun(Peer) -> {Peer, {protocol, Message}} end, MembershipList0),
    State = State0#hiScamp{membership=Membership},
    MembershipList = membership_list(State),
    {ok, MembershipList, OutgoingMessages, State}.

periodic(#hiScamp{last_message_time = LastMessageTime} = State) ->
    SourceNode = myself(),
    MembershipList = membership_list(State),
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
            case partisan_config: get(tracing, ?TRACING) of
                true ->
                    lager:info("~p: Node is possibly isolated.", [node()]);
                false ->
                    ok
            end,
            Myself = myself(),
            lists:map(fun(N) ->
                case partisan_config:get(tracing, ?TRACING) of
                    true ->
                        lager:info("~p: Forwarding add'l subs for ~p to node: ~p", [node(), Myself, N]);
                    false ->
                        ok
                end,
                {N, {protocol, {forward_subscription, Myself}}}
            end, select_random_sublist(State, 1));
        false ->
            []
    end,
    {ok, MemberhsipList, OutgoingSubscriptionMessages + OutgoingPingMessages, State}.

handle_message(#hiScamp{membership=Membership0}=State0, {remove_subscription, Node}) ->
    lager:info("~p: Recieved remove_subscription for node ~p.", [node(), Node]),
    MembershipList0 = membership_list(state0),
    case sets:is_element(Node, Membership0) of 
        true ->
            Membership = sets:del_element(Membership0, Node),
            Message = {remove_subscription, Node},
            OutgoingMessages = lists:map(fun(Peer) -> {Peer, {protocol, Message}} end, MembershipList0),
            State = State0#hiScamp{membership=Membership},
            MembershipList = membership_list(state),
            {ok, MembershipList, OutgoingMessages, State};
        false ->
            OutgoingMessages = [],
            {ok, MembershipList0, OutgoingMessages, State0}
    end;

handle_message(#hiScamp{membership=Membership0}=State0, {forward_subscription, Node}) ->
    case partisan_config:get(tracing, ?TRACING) of
        true ->
            lager:info("~p: Recieved subscription for node ~p.", [node(), Node]);
        false ->
            ok
    end,
    MembershipList0 = membership_list(state0),
    Random = random_0_or_1(),
    Keep = trunc((sets:size(Membership0) + 1)*Random),
    case Keep =:= 0 andalso not lists:member(node, MembershipList0) of
        true ->
            case partisan_config:get(tracing, ?TRACING) of
                true ->
                    lager:info("~p:Adding subscription for node: ~p", [node(), Node]);
                false -> 
                    ok
            end,
            Membership = sets:add_element(Node, Membership0),
            State = State0#hiScamp{membership=Membership},
            MembershipList = membership_list(State),
            OutgoingMessages = [],
            {ok, MembershipList, OutgoingMessages, State};
        false ->
            OutgoingMessages = lists:map(fun(N) ->
                case partisan_config:get(tracing, ?TRACING) of
                    true -> 
                        lager:info("~p: Forwarding subs for ~p to node: ~p", [node(), Node, N]);
                    false ->
                        ok
                end,
                {N, {protocol, {forward_subscription, Node}}}
                end, select_random_sublist(State0, 1)),
            {ok, MembershipList0, OutgoingMessages, State0}
    end.

%%HELPERS
    
%% @private
membership_list(#hiScamp{membership=Membership}) ->
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

insert(Table, Key, Value) ->
    ets:insert(Table, {Key, Value}).

lookup(Table, Key) ->
    [{Key, Value}] = ets:lookup(Table, Key),
    Value.
