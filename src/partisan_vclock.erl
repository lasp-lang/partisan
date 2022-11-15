%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc A simple Erlang implementation of vector clocks as inspired by Lamport
%% logical clocks. Taken from Riak.
%%
%% @reference Leslie Lamport (1978). "Time, clocks, and the ordering of events
%% in a distributed system". Communications of the ACM 21 (7): 558-565.
%% [http://research.microsoft.com/en-us/um/people/lamport/pubs/time-clocks.pdf]
%%
%% @reference Friedemann Mattern (1988). "Virtual Time and Global States of
%% Distributed Systems". Workshop on Parallel and Distributed Algorithms:
%% pp. 215-226
%% [http://homes.cs.washington.edu/~arvind/cs425/doc/mattern89virtual.pdf]

-module(partisan_vclock).

-export([fresh/0, descends/2, merge/1, get_counter/2, subtract_dots/2,
         increment/2, all_nodes/1, equal/2,
         to_binary/1, from_binary/1, dominates/2, glb/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([vclock/0, vclock_node/0, binary_vclock/0]).

-type vclock() :: [vc_entry()].
-type binary_vclock() :: binary().
% The timestamp is present but not used, in case a client wishes to inspect it.
-type vc_entry() :: {vclock_node(), counter()}.

% Nodes can have any term() as a name, but they must differ from each other.
-type   vclock_node() :: term().
-type   counter() :: integer().

% @doc Create a brand new vclock.
-spec fresh() -> vclock().
fresh() ->
    [].

% @doc Return true if Va is a direct descendant of Vb, else false -- remember, a vclock is its own descendant!
-spec descends(Va :: vclock()|[], Vb :: vclock()|[]) -> boolean().
descends(_, []) ->
    % all vclocks descend from the empty vclock
    true;
descends(Va, Vb) ->
    [{NodeB, CtrB} |RestB] = Vb,
    case lists:keyfind(NodeB, 1, Va) of
        false ->
            false;
        {_, CtrA} ->
            (CtrA >= CtrB) andalso descends(Va, RestB)
    end.

-spec dominates(vclock(), vclock()) -> boolean().
dominates(A, B) ->
    descends(A, B) andalso not descends(B, A).

%% @doc subtract the VClock from the DotList.
%% what this means is that any `{actor(), count()}' pair in
%% DotList that is &lt;= an entry in  VClock is removed from DotList
%% Example [{a, 3}, {b, 2}, {d, 14}, {g, 22}] -
%%         [{a, 4}, {b, 1}, {c, 1}, {d, 14}, {e, 5}, {f, 2}] =
%%         [{{b, 2}, {g, 22}]
-spec subtract_dots(vclock(), vclock()) -> vclock().
subtract_dots(DotList, VClock) ->
    drop_dots(DotList, VClock, []).

drop_dots([], _Clock, NewDots) ->
    lists:sort(NewDots);
drop_dots([{Actor, Count}=Dot | Rest], Clock, Acc) ->
    case get_counter(Actor, Clock) of
        Cnt when Cnt >= Count ->
            %% Dot is dominated by clock, drop it
            drop_dots(Rest, Clock, Acc);
        _ ->
            drop_dots(Rest, Clock, [Dot | Acc])
    end.

% @doc Combine all VClocks in the input list into their least possible
%      common descendant.
-spec merge(VClocks :: [vclock()]) -> vclock() | [].
merge([])             -> [];
merge([SingleVclock]) -> SingleVclock;
merge([First|Rest])   -> merge(Rest, lists:keysort(1, First)).

merge([], NClock) -> NClock;
merge([AClock|VClocks], NClock) ->
    merge(VClocks, merge(lists:keysort(1, AClock), NClock, [])).

merge([], [], AccClock) -> lists:reverse(AccClock);
merge([], Left, AccClock) -> lists:reverse(AccClock, Left);
merge(Left, [], AccClock) -> lists:reverse(AccClock, Left);
merge(V=[{Node1, Ctr1}=NCT1|VClock],
      N=[{Node2, Ctr2}=NCT2|NClock], AccClock) ->
    case compare(Node1, Node2) of
        lt ->
            merge(VClock, N, [NCT1|AccClock]);
        gt ->
            merge(V, NClock, [NCT2|AccClock]);
        eq ->
            CT = case compare(Ctr1, Ctr2) of
                lt ->
                    Ctr2;
                _ ->
                    Ctr1
            end,
            merge(VClock, NClock, [{Node1, CT}|AccClock])
    end.

compare(A, B) when A < B -> lt;
compare(A, B) when A > B -> gt;
compare(_, _) -> eq.

% @doc Get the counter value in VClock set from Node.
-spec get_counter(Node :: vclock_node(), VClock :: vclock()) -> counter().
get_counter(Node, VClock) ->
    case lists:keyfind(Node, 1, VClock) of
        {_, Ctr} -> Ctr;
        false           -> 0
    end.

% @doc Increment VClock at Node.
-spec increment(Node :: vclock_node(),
                VClock :: vclock()) -> vclock().
increment(Node, VClock) ->
    {Ctr, NewV} = case lists:keytake(Node, 1, VClock) of
                                false ->
                                    {1, VClock};
                                {value, {_N, C}, ModV} ->
                                    {C + 1, ModV}
                            end,
    [{Node, Ctr}|NewV].


% @doc Return the list of all nodes that have ever incremented VClock.
-spec all_nodes(VClock :: vclock()) -> [vclock_node()].
all_nodes(VClock) ->
    [X || {X, _} <- sort(VClock)].

% @doc Compares two VClocks for equality.
-spec equal(VClockA :: vclock(), VClockB :: vclock()) -> boolean().
equal(VA, VB) ->
    lists:sort(VA) =:= lists:sort(VB).

%% @doc sorts the vclock by actor
-spec sort(vclock()) -> vclock().
sort(Clock) ->
    lists:sort(Clock).

%% @doc an efficient format for disk / wire.
%5 @see from_binary/1
-spec to_binary(vclock()) -> binary_vclock().
to_binary(Clock) ->
    term_to_binary(sort(Clock)).

%% @doc takes the output of `to_binary/1' and returns a vclock
-spec from_binary(binary_vclock()) -> vclock().
from_binary(Bin) ->
    sort(binary_to_term(Bin)).

%% @doc take two vclocks and return a vclock that summerizes only the
%% events both have seen.
-spec glb(vclock(), vclock()) -> vclock().
glb(Clock1, Clock2) ->
    Clock = lists:foldl(fun({Actor, Cnt}, GLB) ->
                                case lists:keyfind(Actor, 1, Clock2) of
                                    false ->
                                        GLB;
                                    {Actor, Cnt2} when Cnt2 >= Cnt ->
                                        [{Actor, Cnt} | GLB];
                                    {Actor, Cnt2} ->
                                        [{Actor, Cnt2} | GLB]
                                end
                        end,
                        fresh(),
                        Clock1),
    lists:sort(Clock).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

% doc Serves as both a trivial test and some example code.
example_test() ->
    A = ?MODULE:fresh(),
    B = ?MODULE:fresh(),
    A1 = ?MODULE:increment(a, A),
    B1 = ?MODULE:increment(b, B),
    true = ?MODULE:descends(A1, A),
    true = ?MODULE:descends(B1, B),
    false = ?MODULE:descends(A1, B1),
    A2 = ?MODULE:increment(a, A1),
    C = ?MODULE:merge([A2, B1]),
    C1 = ?MODULE:increment(c, C),
    true = ?MODULE:descends(C1, A2),
    true = ?MODULE:descends(C1, B1),
    false = ?MODULE:descends(B1, C1),
    false = ?MODULE:descends(B1, A1),
    ok.

accessor_test() ->
    VC = [{<<"1">>, 1},
          {<<"2">>, 2}],
    ?assertEqual(1, get_counter(<<"1">>, VC)),
    ?assertEqual(2, get_counter(<<"2">>, VC)),
    ?assertEqual(0, get_counter(<<"3">>, VC)),
    ?assertEqual([<<"1">>, <<"2">>], all_nodes(VC)).

merge_test() ->
    VC1 = [{<<"1">>, 1},
           {<<"2">>, 2},
           {<<"4">>, 4}],
    VC2 = [{<<"3">>, 3},
           {<<"4">>, 3}],
    ?assertEqual([], merge(?MODULE:fresh())),
    ?assertEqual([{<<"1">>, 1}, {<<"2">>, 2}, {<<"3">>, 3}, {<<"4">>, 4}],
                 merge([VC1, VC2])).

merge_less_left_test() ->
    VC1 = [{<<"5">>, 5}],
    VC2 = [{<<"6">>, 6}, {<<"7">>, 7}],
    ?assertEqual([{<<"5">>, 5}, {<<"6">>, 6}, {<<"7">>, 7}],
                 ?MODULE:merge([VC1, VC2])).

merge_less_right_test() ->
    VC1 = [{<<"6">>, 6}, {<<"7">>, 7}],
    VC2 = [{<<"5">>, 5}],
    ?assertEqual([{<<"5">>, 5}, {<<"6">>, 6}, {<<"7">>, 7}],
                 ?MODULE:merge([VC1, VC2])).

merge_same_id_test() ->
    VC1 = [{<<"1">>, 1}, {<<"2">>, 1}],
    VC2 = [{<<"1">>, 1}, {<<"3">>, 1}],
    ?assertEqual([{<<"1">>, 1}, {<<"2">>, 1}, {<<"3">>, 1}],
                 ?MODULE:merge([VC1, VC2])).

-endif.
