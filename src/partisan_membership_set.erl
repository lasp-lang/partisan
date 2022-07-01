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
%% @doc Actually not a set but an Add-Wins Multi-value Register Map Causal CRDT.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_membership_set).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type t()   ::  state_awmap:state_awmap().

%% API
-export([add/3]).
-export([decode/1]).
-export([encode/1]).
-export([equal/2]).
-export([merge/2]).
-export([new/0]).
-export([remove/3]).
-export([to_list/1]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
new() ->
    state_awmap:new([state_mvregister]).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
add(#{name := Node} = NodeSpec, Actor, T) ->
    {ok, T1} = state_awmap:mutate({apply, Node, {set, 0, NodeSpec}}, Actor, T),
    T1.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
remove(#{name := Node}, Actor, T) ->
    {ok, T1} = state_awmap:mutate({rmv, Node}, Actor, T),
    T1.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
merge({state_awmap, _} = T1, {state_awmap, _} = T2) ->
    state_awmap:merge(T1, T2);

merge({state_awmap, _} = T1, _) ->
    %% Maybe the other node is using a diff version of this module
    %% TODO implement versioning and migrations to enable two diff versions to
    %% form a cluster.
    %% At the moment we just pick the local.
    T1.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
equal(T1, T2) ->
    state_awmap:equal(T1, T2).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
to_list(T) ->
    lists:usort(
        lists:append(
            [sets:to_list(Values) || {_, Values} <- state_awmap:query(T)]
        )
    ).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec encode(t()) -> binary().

encode(T) ->
    Opts = case partisan_config:get(membership_binary_compression, 1) of
        true ->
            [compressed];
        N when N >= 0, N =< 9 ->
            [{compressed, N}];
        _ ->
            []
    end,

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
        channels => [undefined],
        listen_addrs => [#{ip => IP, port => 62823}],
        name => Nodename,
        parallelism => 1
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
        [Node2],
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


