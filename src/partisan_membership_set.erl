%% -----------------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher S. Meiklejohn.  All Rights Reserved.
%% Copyright (c) 2022 Alejandro M. Ramallo.  All Rights Reserved.
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
%% -----------------------------------------------------------------------------

%% -----------------------------------------------------------------------------
%% @doc This module represents the cluster membership view for this node.
%%
%% When a node joins the cluster it is added to the set. Conversely when a node
%% leaves the cluster it is removed from the set. A node that crashes or gets
%% disconnected will remain in the set so that Partisan can try to re-connect
%% with the node when it restarts or becomes reachable again.
%%
%% == Implementation ==
%% The set is implemented as a CRDT set of `partisan:node_spec()' objects. More
%% specifically a state_orset.
%%
%% Notice that because the set stores `partisan:node_spec()' objects and not
%% `node()',
%% the set can have multiple `partisan:node_spec()' objects for the same node.
%%
%% This can occur when the set contains one or more
%% <em>stale specifications</em>.
%%
%% == Stale Specifications ==
%% A stale specification exists due to the following reasons:
%%
%% <ul>
%% <li>
%% A node crashes (without leaving the cluster) and returns bearing
%% <em>different IP Addresses</em> (the value of the node specification's
%% `listen_addrs' property). This is common in cloud orchestration
%% scenarios where instances have dynamic IP addresses.
%% </li>
%% <li>
%% A node crashes (without leaving the cluster) and returns bearing
%% different values for the node specification properties `channels' and/or
%% `parallelism'. For example, this can happen in the case the Partisan
%% configuration has changed when using a rolling update strategy i.e. a
%% gradual update process that allows you to update a cluster one node at a
%% time to minimise downtime.
%% </li>
%% </ul>
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_membership_set).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-opaque t()   ::  state_orset:state_orset().

-export_type([t/0]).

%% API
-export([add/3]).
-export([compare/2]).
-export([decode/1]).
-export([encode/1]).
-export([equal/2]).
-export([merge/2]).
-export([new/0]).
-export([remove/3]).
-export([to_list/1]).
-export([to_peer_list/1]).


-eqwalizer({nowarn_function, add_remove_test/0}).
-eqwalizer({nowarn_function, one_side_updates_test/0}).
-eqwalizer({nowarn_function, concurrent_updates_test/0}).
-eqwalizer({nowarn_function, compare_test/0}).
-eqwalizer({nowarn_function, concurrent_remove_update_test/0}).
-eqwalizer({nowarn_function, assoc_test/0}).
-eqwalizer({nowarn_function, clock_test/0}).
-eqwalizer({nowarn_function, remfield_test/0}).
-eqwalizer({nowarn_function, present_but_removed_test/0}).
-eqwalizer({nowarn_function, no_dots_left_test/0}).
-eqwalizer({nowarn_function, equals_test/0}).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec new() -> t().

new() ->
    state_orset:new().


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add(partisan:node_spec(), Actor :: partisan:actor(), t()) -> t().

add(#{name := _} = NodeSpec, Actor, T0) ->
    {ok, T} = state_orset:mutate({add, NodeSpec}, Actor, T0),
    T.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec remove(partisan:node_spec(), Actor :: partisan:actor(), t()) -> t().

remove(#{name := _} = NodeSpec, Actor, T) ->
    {ok, T1} = state_orset:mutate({rmv, NodeSpec}, Actor, T),
    T1.


%% -----------------------------------------------------------------------------
%% @doc Returns the tuple `{Joiners, Leavers}' where `Joiners' is the list of
%% node specifications that are elements of `List' but are not in the
%% membership set, and `Leavers' are the node specifications for the current
%% members that are not elements in `List'.
%% @end
%% -----------------------------------------------------------------------------
-spec compare([partisan:node_spec()], t()) ->
    {Joiners :: [partisan:node_spec()], Leavers :: [partisan:node_spec()]}.

compare([], _) ->
    {[], []};

compare(List, T) when is_list(List) ->
    Set = sets:from_list(List),
    Members = state_orset:query(T),
    Intersection = sets:intersection(Set, Members),
    Joiners = sets:to_list(sets:subtract(Set, Intersection)),
    Leavers = sets:to_list(sets:subtract(Members, Intersection)),
    %% eqwalizer:ignore these are [partisan:node_spec()]
    {Joiners, Leavers}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec merge(t(), t()) -> t().

merge(T1, T2) ->
    state_orset:merge(T1, T2).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec equal(t(), t()) -> boolean().

equal(T1, T2) ->
    state_orset:equal(T1, T2).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec to_list(t()) -> [partisan:node_spec()].

to_list(T) ->
    %% eqwalizer:ignore state_orset:query returns [partisan:node_spec()]
    lists:sort(sets:to_list(state_orset:query(T))).


%% -----------------------------------------------------------------------------
%% @doc Returns a list of node specifications but omitting the specification
%% for the local node.
%% No sorting is applied, so the sorting is undefined.
%% @end
%% -----------------------------------------------------------------------------
-spec to_peer_list(t()) -> [partisan:node_spec()].

to_peer_list(T) ->
    Myself = partisan:node(),
    Peers = sets:fold(
        fun
            (#{name := Node} = Spec, Acc) when Node =/= Myself ->
                [Spec | Acc];
            (_, Acc) ->
                Acc
        end,
        [],
        state_orset:query(T)
    ),
    %% eqwalizer:ignore Peers :: [partisan:node_spec()]
    lists:reverse(Peers).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode(t()) -> binary().

encode(T) ->
    Opts = partisan_config:get('$membership_encoding_opts', [compressed]),
    erlang:term_to_binary(T, Opts).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec decode(binary()) -> t().

decode(Binary) ->
    erlang:binary_to_term(Binary).



%% =============================================================================
%% EUNIT TESTS
%% =============================================================================



-ifdef(TEST).




node_spec(Nodename) ->
    node_spec(Nodename, {127,0,0,1}).


node_spec(Nodename, IP) ->
    #{
        name => Nodename,
        listen_addrs => [#{ip => IP, port => 62823}],
        channels => #{
            ?DEFAULT_CHANNEL => #{
                parallelism => 1,
                monotonic => false
            }
        }
    }.


add_remove_test() ->
    Nodename = 'node1@127.0.0.1',
    Node = node_spec(Nodename),

    A = add(Node, a, new()),

    ?assertEqual(
        [Node],
        to_list(A)
    ),

    A1 = remove(Node, a, A),

    ?assertEqual(
        [],
        to_list(A1)
    ).

one_side_updates_test() ->
    Nodename = 'node1@127.0.0.1',
    Node1 = node_spec(Nodename, {127, 0, 0, 1}),

    A = B = new(),

    A1 = add(Node1, a, A),


    ?assertEqual(
        [Node1],
        to_list(A1)
    ),

    ?assertEqual(
        [Node1],
        to_list(merge(A1, B))
    ).

concurrent_updates_test() ->
    Nodename = 'node1@127.0.0.1',
    Node1 = node_spec(Nodename, {127, 0, 0, 1}),
    Node2 = node_spec(Nodename, {192, 168, 0, 1}),

    A = B = new(),

    A1 = add(Node1, a, A),

    B1 = add(Node2, b, B),


    ?assertEqual(
        [Node1],
        to_list(A1)
    ),

    ?assertEqual(
        [Node2],
        to_list(B1)
    ),

    ?assertEqual(
        [Node1, Node2],
        to_list(merge(A1, B1))
    ).

compare_test() ->
    Node1 = node_spec('node1@127.0.0.1', {192, 168, 0, 1}),
    Node2 = node_spec('node2@127.0.0.1', {192, 168, 0, 2}),
    Node3 = node_spec('node3@127.0.0.1', {192, 168, 0, 3}),

    S0 = new(),
    S1 = add(Node1, a, S0),
    S2 = add(Node2, a, S1),


    ?assertEqual(
        {[], []},
        compare([], S2)
    ),

    ?assertEqual(
        {[], []},
        compare([Node1, Node2], S2)
    ),

    ?assertEqual(
        {[Node3], []},
        compare([Node1, Node2, Node3], S2)
    ),

    ?assertEqual(
        {[Node3], [Node2]},
        compare([Node1, Node3], S2)
    ).


concurrent_remove_update_test() ->
    Nodename = 'node1@127.0.0.1',
    Node1 = node_spec(Nodename, {127, 0, 0, 1}),
    Node2 = node_spec(Nodename, {192, 168, 0, 1}),

    A = add(Node1, a, new()),
    A1 = remove(Node1, a, A),

    B = A, %% not A1
    B1 = add(Node2, b, B),


    ?assertEqual(
        [Node1],
        to_list(A)
    ),

    ?assertEqual(
        [],
        to_list(A1)
    ),

    ?assertEqual(
        [Node1, Node2],
        to_list(B1)
    ),
    %% Replicate to A
    ?assertEqual(
        [Node2],
        to_list(merge(A1, B1))
    ).


%% This fails on previous version of riak_dt_map
assoc_test() ->
    Nodename = 'node1@127.0.0.1',
    Node = node_spec(Nodename),

    A = add(Node, a, new()),
    B = add(Node, b, new()),
    B2 = remove(Node, b, B),

    C = A,

    C3 = remove(Node, c, C),

    ?assertEqual(
        merge(A, merge(B2, C3)),
        merge(merge(A, B2), C3)
    ),

    ?assertEqual(
        to_list(merge(merge(A, C3), B2)),
        to_list(merge(merge(A, B2), C3))
    ),

    ?assertEqual(
        merge(merge(A, C3), B2),
        merge(merge(A, B2), C3)
    ).


clock_test() ->
    Nodename = 'node1@127.0.0.1',
    Node1 = node_spec(Nodename),
    Node2 = node_spec(Nodename, {192, 168, 0, 1}),

    A = add(Node1, a, new()),
    B = A,
    B2 = add(Node2, b, B),

    A2 = remove(Node1, a, A),
    A4 = add(Node2, a, A2),
    AB = merge(A4, B2),

    %% LWW
    ?assertEqual([Node2], to_list(AB)).


remfield_test() ->
    Name = 'node1@127.0.0.1',
    Node1 = node_spec(Name),
    Node2 = node_spec(Name, {192, 168, 0, 1}),

    A = add(Node1, a, new()),
    B = A,
    A2 = remove(Node1, a, A),
    A4 = add(Node2, a, A2),
    AB = merge(A4, B),
    ?assertEqual([Node2], to_list(AB)).

%% Bug found by EQC, not dropping dots in merge when an element is
%% present in both Maos leads to removed items remaining after merge.
present_but_removed_test() ->
    Name = 'node1@127.0.0.1',
    Node1 = node_spec(Name),
    Node2 = node_spec(Name, {192, 168, 0, 1}),
    %% Add Z to A
    A = add(Node1, a, new()),
    %% Replicate it to C so A has 'Z'->{a, 1}
    C = A,
    %% Remove Z from A
    A2 = remove(Node1, a, A),
    %% Add Z to B, a new replica
    B = add(Node2, b, new()),
    %%  Replicate B to A, so now A has a Z, the one with a Dot of
    %%  {b,1} and clock of [{a, 1}, {b, 1}]
    A3 = merge(B, A2),
    %% Remove the 'Z' from B replica
    B2 = remove(Node2, b, B),
    %% Both C and A have a 'Z', but when they merge, there should be
    %% no 'Z' as C's has been removed by A and A's has been removed by
    %% C.
    Merged = lists:foldl(fun(Set, Acc) ->
                                 merge(Set, Acc) end,
                         %% the order matters, the two replicas that
                         %% have 'Z' need to merge first to provoke
                         %% the bug. You end up with 'Z' with two
                         %% dots, when really it should be removed.
                         A3,
                         [C, B2]),
    ?assertEqual([], to_list(Merged)).


%% A bug EQC found where dropping the dots in merge was not enough if
%% you then store the value with an empty clock (derp).
no_dots_left_test() ->
    Name = 'node1@127.0.0.1',
    Node1 = node_spec(Name),
    Node2 = node_spec(Name, {192, 168, 0, 1}),
    A =  add(Node1, a, new()),
    B =  add(Node2, b, new()),
    C = A, %% replicate A to empty C
    A2 = remove(Node1, a, A),
    %% replicate B to A, now A has B's 'Z'
    A3 = merge(A2, B),
    %% Remove B's 'Z'
    B2 = remove(Node2, b, B),
    %% Replicate C to B, now B has A's old 'Z'
    B3 = merge(B2, C),
    %% Merge everytyhing, without the fix You end up with 'Z' present,
    %% with no dots
    Merged = lists:foldl(fun(Set, Acc) ->
                                 merge(Set, Acc) end,
                         A3,
                         [B3, C]),
    ?assertEqual([], to_list(Merged)).


equals_test() ->
    Name1 = 'node1@127.0.0.1',
    Node1 = node_spec(Name1),
    A = add(Node1, a, new()),
    B = add(Node1, b, new()),
    ?assert(not equal(A, B)),
    C = merge(A, B),
    D = merge(B, A),
    ?assert(equal(C, D)),
    ?assert(equal(A, A)).

-endif.


