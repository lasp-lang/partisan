-module(partisan_hiscamp_membership_strategy).
-author("Hana Frluckaj <hanafrla@cmu.edu>").
-behavior(partisan_membership_strategy).
%%import Heap stuff
-import(heaps, [add/2, delete_min/1, from_list/1, empty/1, merge/2, new/0, min/1,
                sort/1, to_list/1]).
%change size to heaps:size()
% change export list
%-export([init/1, distance/2, hierarchial_clustering/2]).
-include("partisan.hrl").

%fix this
-record(hiScamp, {membership, actor, heap, clusters}).

% how is the data inserted?

%%initialize state
init(Identity) -> 
    Heap = heaps:new(),
    Clusters = sets:new(),
    %dataset and data size
    %gold standard
    Membership = sets:add_element(myself(), sets:new()),
    State = #hiScamp{membership=Membership, actor=Identity, heap=Heap, clusters=Clusters},
    MembershipList = membership_list(State),
    {ok, MembershipList, State}.

%% remote node connects, notify node to us. perform forwarding?
%% Membership0 is a set of node elements
join(#hiScamp{membership=Membership0, clusters=Clusters0}=State0, Node, _NodeState) ->
    OutgoingMessages0 = [],
    case partisan_config:get(tracing, ?TRACING) of
        true ->
            lager:info("~p: Adding node ~p to our membership.", [node90, Node]);
        false ->
            ok
    end,
    Membership = sets:add_element(Node, Membership0),
    % have 2 levels of clusters, those within threshold and those outside
    Clusters1 = sets:add_element(Node, Clusters0),
    Clusters = sets:add_element(undefined, Clusters1),
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
    State = State0#hiScamp{membership=Membership, clusters = Clusters},
    MembershipList = membership_list(State),
    {ok, Membership, OutgoingMessages, State}.

distance(#hiScamp{membership=Membership0, heap=Heap, clusters=Clusters}=_State0, Node) ->
    % A node j joins the system by sending a subscription request to the node s which is closest to it
    Dict = get(Membership0, Node),
    Threshold = 3,
    Dist = dict:fetch(Node, Dict),
    case Threshold > Dist of
        true -> 
            % process this case using Scamp within this cluster
            % subscription at level 1
            %need to send copies out to other nodes -> gossip protocol
            sets:add(Node, Cluster0);
        false ->
            % subscription at level 2
            sets:add(Node, Cluster1)
    end,
    heaps:add(Dist, Heap),
    Clusters.

get_centroid_two_clusters(#hiScamp{membership=Membership0, heap=Heap, clusters=Clusters} =State0) ->
    Min1 = heaps:min(Heap),
    Heap = heaps:delete_min(Heap),
    Min2 = heaps:min(Heap),
    case sets:is_element(Min1, Clusters0) andalso sets:is_element(Min2, Clusters0) of
        true ->
            % can group nodes as theyre the closest within the same threshold
            erlang:setcookie(Min1, Min2);
        false -> 
            case sets:is_element(Min2, Clusters1) andalso sets:is_element(Min2, Clusters1) of
                true ->
                    % group nodes as theyre in the same threshold
                    erlang:setcookie(Min1, Min2);
                false ->
                    %merge the two different clusters, and reflect that in second level
                    Level2 = heaps:merge(from_list(sets:to_list(Clusters0), sets:to_list(Clusters1))),
                    Heap = heaps:merge(Heap, Level2)
            end
    end,
    Clusters.

%do I use Membership as Data?
insert_data(#hiScamp{membership=Membership0}=State0, Data, Table) ->
    Folding = fun(Item, N) ->
                      insert(Table, N, Item),
                      N = N+1
    end, 
    lists:foldl(Folding, 1, DataItems).

level1(N, P, L, _M) ->
    insert(P, N+1, N+1),
    insert(L, N+1, infinity).

level2(N, _P, _L, M, Data, DistFun) ->
    Folding = fun(I)->
                      % fix line 22
                      Dist = distance(#hiScamp{membership=Membership}, Node, Heap, Clusters),
                      insert(M, I, Dist)
    end,
    lists:foreach(Folding, lists:seq(1, N)).
            
level3(N, P, L, M) ->
    Folding = fun(I) ->
                      PI = lookup(P, I),
                      LI = lookup(L, I),
                      MI = lookup(M, I),
                      case MI < LI of
                          true ->
                              insert(M, PI, min(lookup(M, PI), LI)),
                              insert(L, I, lookup(M, I)),
                              insert(P, I, N + 1);
                          false ->
                              insert(M, PI, min(lookup(M, PI), MI))
                      end
    end, 
    lists:foreach(Folding, lists:seq(1, N)).

%set Data = Membership??
cluster(#hiScamp{membership=Membership0}=State0, Data) ->
    cluster(Data, fetch(Node, get(Membership0, Node))).
cluster(#hiScamp{membership=Membership0}=State0, Data, DistFun) ->
    DataTable = ets:new(dc_d, [set, private]),
    insert_data(Data, DataTable),
    P = ets:new(dc_p, [set, private]),
    insert(P,1,1),
    L = ets:new(dc_l, [set, private]),
    insert(L, 1, infinity),
    M = ets:new(dc_m, [set, private]),
    Folding = fun(N) ->
                      level1(N, P, L, M),
                      level2(N, P, DataTable, DistFun),
                      level3(N, P, L, M)
    end,
    lists:foreach(Folding, lists:seq(1, length(Data)-1)),
    Cluster = hierarchial_clustering(L, P, DataTable),
    [ets:delete(T)||T<-[DataTable,P,L,M]],
    Cluster.

hierarchial_clustering(#hiScamp{membership=Membership0}=State0, Clusters) ->
    DataList = ets:tab2list(DataTable),
    DataNoDist = dict:from_list(lists:map(fun({I, {_V, Data}}) -> {I, Data} end, DataList)),
    %iterate over indices by distance
    DistIndexMap = lists:sort([{Dist, I} || {I, Dist} <- ets:tab2list(LDict)]),
    MapFun = fun({Distance, Index}, Clusters) ->
                     case Distance =:= infinity of
                         true -> Clusters;
                         false ->
                             Item = dict:fetch(Index, Clusters),
                             IndexSibling = lookup(PDict, Index),
                             Sibling = dict:fetch(IndexSibling, Clusters),
                             Clusters1 = dict:erase(Index, Clusters),
                             NewCluster = {Distance, [Item, Sibling]},
                             dict:store(IndexSibling, NewCluster, Clusters1)
                     end
    end,
    ClusterAsDict = lists:foldl(MapFun, DataNoVectors, DistIndexMap),
    [{_, ClusterAsList}] = dict:to_list(ClusterAsDict),
    ClusterAsList.

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
