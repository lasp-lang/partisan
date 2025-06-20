%% =============================================================================
%%  partisan_interval_sets -
%%
%%  Copyright (c) 2022 Alejandro M. Ramallo. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% =============================================================================
%% -----------------------------------------------------------------------------
%% @doc An implementation of a set of bounded open intervals.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_interval_sets).

-type interval()    ::  {integer(), integer()}.
-type element()     ::  integer() | interval().
-type t()           ::  [element()].

-export_type([interval/0]).
-export_type([t/0]).


-export([add_element/2]).
-export([del_element/2]).
-export([filter/2]).
-export([fold/3]).
-export([from_list/1]).
-export([intersection/1]).
-export([intersection/2]).
-export([is_disjoint/2]).
-export([is_element/2]).
-export([is_empty/1]).
-export([is_subset/2]).
-export([is_type/1]).
-export([new/0]).
-export([size/1]).
-export([flat_size/1]).
-export([subtract/2]).
-export([to_list/1]).
-export([to_flat_list/1]).
-export([union/1]).
-export([union/2]).
-export([min/1]).
-export([max/1]).


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-export([element_union/2]).
-export([element_intersection/2]).
-export([element_overlaps/2]).
-export([element_starts_before/2]).
-export([element_meets/2]).
-export([element_merges/2]).
-export([element_begins/2]).
-export([element_ends/2]).
-export([element_succeeds/2]).
-export([element_precedes/2]).
-export([element_includes/2]).
-export([element_included/2]).
-export([element_subtract/2]).

-endif.




%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Return a new empty offset.
%% -----------------------------------------------------------------------------
-spec new() -> [].

new() ->
    [].


%% -----------------------------------------------------------------------------
%% @doc Return 'true' if Set is an ordered set of elements, else 'false'.
%% -----------------------------------------------------------------------------
-spec is_type(t()) -> boolean().

is_type([E|Es]) ->
    is_element_type(E) andalso is_type(Es, E);

is_type([]) ->
    true;

is_type(_) ->
    false.


%% -----------------------------------------------------------------------------
%% @doc Return the number of elements in OrdSet.
%% -----------------------------------------------------------------------------
-spec size(t()) -> non_neg_integer().

size(S) ->
    length(S).


%% -----------------------------------------------------------------------------
%% @doc Return the number of points in the Ordset.
%% -----------------------------------------------------------------------------
-spec flat_size(t()) -> non_neg_integer().

flat_size(S) ->
    lists:foldl(
        fun
            ({H, T}, Cnt) -> Cnt + 1 + T - H;
            (_, Cnt) -> Cnt + 1
        end,
        0,
        S
    ).


%% -----------------------------------------------------------------------------
%% @doc Return 'true' if OrdSet is an empty set, otherwise 'false'.
%% -----------------------------------------------------------------------------
-spec is_empty(t()) -> boolean().

is_empty(S) ->
    S =:= [].


%% -----------------------------------------------------------------------------
%% @doc Returns the minimum integer value contained in the set.
%% -----------------------------------------------------------------------------
-spec min(t()) -> integer().

min([{H, _}|_]) ->
    H;

min([N|_]) ->
    N.


%% -----------------------------------------------------------------------------
%% @doc Returns the maximum integer value contained in the set.
%% -----------------------------------------------------------------------------
-spec max(t()) -> integer().

max(S) ->
    case lists:last(S) of
        {_, T} -> T;
        N -> N
    end.


%% -----------------------------------------------------------------------------
%% @doc Return the elements in OrdSet as a list.
%% -----------------------------------------------------------------------------
-spec to_list(t()) -> [element()].

to_list(S) ->
    S.


%% -----------------------------------------------------------------------------
%% @doc Return the points in OrdSet as a list.
%% -----------------------------------------------------------------------------
-spec to_flat_list(t()) -> [integer()].

to_flat_list(Set) ->
    List = lists:foldl(
        fun
            ({H, T}, Acc) ->
                [lists:seq(H, T)|Acc];
            (N, Acc) ->
                [N|Acc]
        end,
        [],
        Set
    ),
    lists:flatten(lists:reverse(List)).



%% -----------------------------------------------------------------------------
%% @doc Build an ordered set from the elements in List.
%% -----------------------------------------------------------------------------
-spec from_list(List :: [element()]) -> Sets :: t().

from_list(L0) ->
    L1 = lists:usort(
        fun
            ({V1, V2} = E1, {V3, _} = E2) ->
                ok = validate_element(E1),
                ok = validate_element(E2),
                V2 =< V3 orelse V1 =< V3;

            ({V1, _} = E1, E2) when is_integer(E2) ->
                ok = validate_element(E1),
                V1 =< E2;

            (E1, {V1, _} = E2) when is_integer(E1) ->
                ok = validate_element(E2),
                E1 =< V1;

            (E1, E2) when is_integer(E1), is_integer(E2), E1 < E2 ->
                true;

            (E1, E2) ->
                ok = validate_element(E1),
                ok = validate_element(E2),
                false

        end,
        L0
    ),
    compact(L1).


%% -----------------------------------------------------------------------------
%% @doc Return 'true' if Element is an element of Sets, else 'false'.
%% -----------------------------------------------------------------------------
-spec is_element(Element :: element(), Sets :: t()) -> boolean().

is_element(Element, Sets) ->
    ok = validate_element(Element),
    do_is_element(Element, Sets).


%% @private
do_is_element(A, [B|Es]) ->
    (not element_starts_before(A, B) andalso not element_precedes(A, B))
    andalso (
        element_included(A, B)
        orelse (
            element_succeeds(A, B) andalso do_is_element(A, Es)
        )
    );

do_is_element(_, []) ->
    false.



%% -----------------------------------------------------------------------------
%% @doc Return OrdSet with Element inserted in it.
%% -----------------------------------------------------------------------------
-spec add_element(Element :: element(), Set1 :: t()) -> Set2 :: t().

add_element(A, [B|Es] = Set) ->
    ok = validate_element(A),

    case equal(A, B) of
        true ->
            Set;
        false ->
            case element_meets(A, B) of
                true ->
                    E = unsafe_element_union(A, B),
                    add_element(E, Es);
                false ->
                    case element_precedes(A, B) of
                        true ->
                            [simplify(A)|Set];
                        false ->
                            case element_succeeds(A, B) of
                                true ->
                                    [B|add_element(A, Es)];
                                false ->
                                    case element_overlaps(A, B) of
                                        true ->
                                            E = unsafe_element_union(A, B),
                                            add_element(E, Es);
                                        false ->
                                            error(badarg)
                                    end
                            end
                    end
            end
    end;

add_element(E, []) ->
    ok = validate_element(E),
    [E].



%% -----------------------------------------------------------------------------
%% @doc Return OrdSet but with Element removed.
%% -----------------------------------------------------------------------------
-spec del_element(Element :: element(), Set1 :: t()) -> Set2 :: t().

del_element(A, [B|Es] = Set) ->
    ok = validate_element(A),

    case equal(A, B) of
        true ->
            Es;
        false ->
            case element_precedes(A, B) of
                true ->
                    Set;
                false ->
                    case element_succeeds(A, B) of
                        true ->
                            [B|del_element(A, Es)];
                        false ->
                            case element_overlaps(A, B) of
                                true ->
                                    I = element_intersection(A, B),
                                    New = [
                                        simplify(X)
                                        || X <- element_subtract(B, I)
                                    ],
                                    R = element_subtract(A, I),
                                    New ++ del_element(R, Es);
                                false ->
                                    error(badarg)
                            end
                    end
            end
    end;

del_element(_, []) ->
    [].


%% -----------------------------------------------------------------------------
%% @doc Return the union of IntervalSet1 and IntervalSet2.
%% -----------------------------------------------------------------------------
-spec union(Set1 :: t(), Set2 :: t()) -> Set3 :: t().

union(Set1, Set2) ->
    compact(ordsets:union(to_flat_list(Set1), to_flat_list(Set2))).

% union([E1|Es1], [E2|_]=Set2) when E1 < E2 ->
%     [E1|union(Es1, Set2)];
% union([E1|_]=Set1, [E2|Es2]) when E1 > E2 ->
%     [E2|union(Es2, Set1)];            % switch arguments!
% union([E1|Es1], [_E2|Es2]) ->         %E1 == E2
%     [E1|union(Es1, Es2)];
% union([], Es2) -> Es2;
% union(Es1, []) -> Es1.


%% -----------------------------------------------------------------------------
%% @doc Return the union of the list of interval sets.
%% -----------------------------------------------------------------------------
-spec union(SetsList :: [t()]) -> Set :: t().

union(SetsList) ->
    compact(lists:umerge([to_flat_list(Set) || Set <- SetsList])).


%% -----------------------------------------------------------------------------
%% @doc Return the intersection of IntervalSet1 and IntervalSet2.
%% -----------------------------------------------------------------------------
-spec intersection(Set1 :: t(), Set2 :: t()) -> Set3 :: t().

intersection(Set1, Set2) ->
    compact(ordsets:intersection(to_flat_list(Set1), to_flat_list(Set2))).


% intersection([E1|Es1], [E2|_]=Set2) when E1 < E2 ->
%     intersection(Es1, Set2);
% intersection([E1|_]=Set1, [E2|Es2]) when E1 > E2 ->
%     intersection(Es2, Set1);          % switch arguments!
% intersection([E1|Es1], [_E2|Es2]) ->      %E1 == E2
%     [E1|intersection(Es1, Es2)];
% intersection([], _) ->
%     [];
% intersection(_, []) ->
%     [].


%% -----------------------------------------------------------------------------
%% @doc Return the intersection of the list of interval sets.
%% -----------------------------------------------------------------------------
-spec intersection(SetsList :: [t()]) -> Set :: t().

intersection([S1,S2|Ss]) ->
    intersection1(intersection(S1, S2), Ss);

intersection([S]) ->
    S.

intersection1(S1, [S2|Ss]) ->
    intersection1(intersection(S1, S2), Ss);

intersection1(S1, []) ->
    S1.


%% -----------------------------------------------------------------------------
%% @doc Check whether IntervalSet1 and IntervalSet2 are disjoint.
%% -----------------------------------------------------------------------------
-spec is_disjoint(Set1 :: t(), Set2 :: t()) -> boolean().

is_disjoint(Set1, Set2) ->
    ordsets:is_disjoint(to_flat_list(Set1), to_flat_list(Set2)).

% is_disjoint([E1|Es1], [E2|_]=Set2) when E1 < E2 ->
%     is_disjoint(Es1, Set2);
% is_disjoint([E1|_]=Set1, [E2|Es2]) when E1 > E2 ->
%     is_disjoint(Es2, Set1);           % switch arguments!
% is_disjoint([_E1|_Es1], [_E2|_Es2]) ->        %E1 == E2
%     false;
% is_disjoint([], _) ->
%     true;
% is_disjoint(_, []) ->
%     true.


%% -----------------------------------------------------------------------------
%% @doc Return all and only the elements of IntervalSet1 which are not also in
%% IntervalSet2.
%% -----------------------------------------------------------------------------
-spec subtract(Set1 :: t(), Set2 :: t()) -> Set3 :: t().

subtract(Set1, Set2) ->
    compact(ordsets:subtract(to_flat_list(Set1), to_flat_list(Set2))).

% subtract([E1|Es1], [E2|_]=Set2) when E1 < E2 ->
%     [E1|subtract(Es1, Set2)];

% subtract([E1|_]=Set1, [E2|Es2]) when E1 > E2 ->
%     subtract(Set1, Es2);

% subtract([_E1|Es1], [_E2|Es2]) ->     %E1 == E2
%     subtract(Es1, Es2);

% subtract([], _) ->
%     [];

% subtract(Es1, []) ->
%     Es1.


%% -----------------------------------------------------------------------------
%% @doc Return 'true' when every element of IntervalSet1 is also a member of
%%  IntervalSet2, else 'false'.
%% -----------------------------------------------------------------------------
-spec is_subset(Set1 :: t(), Set2 :: t()) -> boolean().

is_subset(Set1, Set2) ->
    ordsets:is_subset(to_flat_list(Set1), to_flat_list(Set2)).

% is_subset([E1|_], [E2|_]) when E1 < E2 -> %E1 not in Set2
%     false;
% is_subset([E1|_]=Set1, [E2|Es2]) when E1 > E2 ->
%     is_subset(Set1, Es2);
% is_subset([_E1|Es1], [_E2|Es2]) ->        %E1 == E2
%     is_subset(Es1, Es2);
% is_subset([], _) -> true;
% is_subset(_, []) -> false.


%% -----------------------------------------------------------------------------
%% @doc Fold function Fun over all elements in OrdSet and return Accumulator.
%% -----------------------------------------------------------------------------
-spec fold(Function, Acc0, Sets) -> Acc1 when
      Function :: fun((Element :: element(), AccIn :: term()) -> AccOut :: term()),
      Sets :: t(),
      Acc0 :: term(),
      Acc1 :: term().

fold(F, Acc, Set) ->
    lists:foldl(F, Acc, Set).


%% -----------------------------------------------------------------------------
%% @doc Filter OrdSet with Fun.
%% -----------------------------------------------------------------------------
-spec filter(Pred, Set1) -> Set2 when
      Pred :: fun((Element :: element()) -> boolean()),
      Set1 :: t(),
      Set2 :: t().

filter(F, Set) ->
    lists:filter(F, Set).



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
interval({_, _} = E) ->
    E;

interval(N) ->
    {N, N}.


%% @private
simplify({N, N}) ->
    N;

simplify(E) ->
    E.


%% @private
equal({_, _} = A, A) ->
    true;

equal(N, {N, N}) ->
    true;

equal({N, N}, N) ->
    true;

equal(N, N) ->
    true;

equal(_, _) ->
    false.


%% @private
is_type([{E3, _} = E|Es], {_, E2}) when E2 =< E3 ->
    is_type(Es, E);

is_type([{_, _}|_], {_, _}) ->
    false;

is_type([{E2, _} = E|Es], E1) when E1 =< E2 ->
    is_type(Es, E);

is_type([{_, _}|_], _) ->
    false;

is_type([E3|Es], {_, E2}) when is_integer(E3), E2 =< E3 ->
    is_type(Es, E2);

is_type([_|_], {_, _}) ->
    false;

is_type([E2|Es], E1) when E1 < E2 ->
    is_type(Es, E2);

is_type([_|_], _) ->
    false;

is_type([], _) ->
    true.


%% @private
is_element_type(X) when is_integer(X) ->
    true;

is_element_type({X, Y}) when is_integer(X), is_integer(Y), X =< Y ->
    true;

is_element_type(_) ->
    false.


%% @private
validate_element(E) ->
    is_element_type(E) orelse error({badarg, E}),
    ok.


%% @private
compact(L) ->
    compact(L, []).


%% @private
compact([], _Acc) ->
    [];
compact([E1|Es], Acc) ->
    compact(Es, Acc, E1).


%% @private
compact([E|Es], Acc, E) ->
    compact(Es, Acc, E);

compact([{V3, V4}|Es], Acc, {V1, V2}) when V2 + 1 >= V3, V2 =< V4 ->
    E = {V1, V4},
    compact(Es, Acc, E);

compact([{V3, V4}|Es], Acc, {V1, V2}) when V2 + 1 >= V3, V2 > V4 ->
    E = {V1, V2},
    compact(Es, Acc, E);

compact([{_, _} = E2|Es], Acc, {_, _} = E1) ->
    %% There is a gap between E1 and E2
    compact(Es, [simplify(E1)|Acc], E2);

compact([{V1, V2}|Es], Acc, E1) when E1 + 1 >= V1, E1 =< V2 ->
    E = {E1, V2},
    compact(Es, Acc, E);

compact([{_, _} = E2|Es], Acc, E1) ->
    %% There is a gap between E1 and E2
    compact(Es, [simplify(E1)|Acc], E2);

compact([E2|Es], Acc, {V1, V2}) when E2 - 1 =< V2 ->
    %% E2 is covered by the interval or is next(V2)
    E = {V1, max(E2, V2)},
    compact(Es, Acc, E);

compact([E2|Es], Acc, {_, V2} = E1) when E2 =< V2 ->
    %% There is a gap between E1 and E2
    compact(Es, [simplify(E1)|Acc], E2);

compact([E2|Es], Acc, E1) when E1 + 1 == E2 ->
    E = {E1, E2},
    compact(Es, Acc, E);

compact([E2|Es], Acc, E1) ->
    %% There is a gap between E1 and E2
    compact(Es, [simplify(E1)|Acc], E2);

compact([], Acc, E) ->
    lists:reverse([simplify(E)|Acc]).


%% @private
element_includes({H1, T1}, {H2, T2}) ->
    H1 =< H2 andalso T1 >= T2;

element_includes({H, T}, N) ->
    H =< N andalso T >= N;

element_includes(N, {N, N}) ->
    true;

element_includes(_, {_, _}) ->
    false;

element_includes(N, M) ->
    N =:= M.


%% @private
element_included(A, B) ->
    element_includes(B, A).


%% @private
element_precedes({_, T1}, {H2, _}) ->
    T1 < H2;

element_precedes({_, T}, N) ->
    T < N;

element_precedes(N, {H, _}) ->
    N < H;

element_precedes(N, M) ->
    N < M.


%% @private
element_succeeds(A, B) ->
    element_precedes(B, A).


%% @private
element_starts_before({H1, _}, {H2, _}) ->
    H1 < H2;

element_starts_before({H, _}, N) ->
    H < N;

element_starts_before(N, {H, _}) ->
    N < H;

element_starts_before(N, M) ->
    N < M.


%% @private
element_overlaps({H1, T1}, {H2, T2}) ->
    H1 =< T2 andalso H2 =< T1;

element_overlaps({_, _} = A, N) ->
    element_overlaps(A, interval(N));

element_overlaps(N, {_, _} = B) ->
    element_overlaps(interval(N), B);

element_overlaps(A, B) ->
    A =:= B.


%% @private
element_meets({H1, T1} = A, {H2, T2} = B) ->
    (element_precedes(A, B) andalso H2 =:= T1 + 1)
        orelse (element_precedes(B, A) andalso H1 =:= T2 + 1);

element_meets({_, _} = A, N) ->
    element_meets(A, interval(N));

element_meets(N, {_, _} = B) ->
    element_meets(interval(N), B);

element_meets(A, B) ->
    abs(A - B) == 1.


%% private
unsafe_element_union({H1, T1}, {H2, T2}) ->
    {min(H1, H2), max(T1, T2)};

unsafe_element_union({_, _} = A, N) ->
    unsafe_element_union(A, interval(N));

unsafe_element_union(N, {_, _} = B) ->
    unsafe_element_union(interval(N), B);

unsafe_element_union(N, B) ->
    unsafe_element_union(interval(N), B).


%% private
element_intersection({_, _} = A, {_, _} = B) ->
    case element_overlaps(A, B) of
        true ->
            unsafe_element_intersection(A, B);
        false ->
            error(badarg)
    end;

element_intersection({_, _} = A, N) ->
    element_intersection(A, interval(N));

element_intersection(N, {_, _} = B) ->
    element_intersection(interval(N), B);

element_intersection(N, B) ->
    element_intersection(interval(N), B).


%% private
unsafe_element_intersection({H1, T1}, {H2, T2}) ->
    {max(H1, H2), min(T1, T2)};

unsafe_element_intersection({_, _} = A, N) ->
    unsafe_element_intersection(A, interval(N));

unsafe_element_intersection(N, {_, _} = B) ->
    unsafe_element_intersection(interval(N), B);

unsafe_element_intersection(N, B) ->
    unsafe_element_intersection(interval(N), B).


%% private
element_subtract(A, B) ->
    Empty =
        element_precedes(A, B)
        orelse element_included(A, B)
        orelse element_succeeds(A, B),

    case Empty of
        true ->
            [];
        false ->
            do_element_subtract(A, B)
    end.

do_element_subtract({H1, T1}, {H2, T2}) when H1 >= H2, T1 > T2 ->
    [{max(T2 + 1, H1), T1}];

do_element_subtract({H1, T1}, {H2, T2}) when H1 < H2, T1 =< T2 ->
    [{H1, min(H2 - 1, T1)}];

do_element_subtract({H1, T1}, {H2, T2}) when H1 < H2, T1 > T2 ->
    %% A includes B
    [{H1, H2 - 1}, {T2 + 1, T1}];

do_element_subtract({_, _} = A, N) when is_integer(N) ->
    do_element_subtract(A, interval(N));

do_element_subtract(N, {_, _} = B) when is_integer(N) ->
    do_element_subtract(interval(N), B);

do_element_subtract(_, _) ->
    error(badarg).



-ifdef(TEST).
%% DISABLED FOR NOW

%% @private
element_merges(A, B) ->
    element_overlaps(A, B) orelse element_meets(A, B).

%% @private
element_begins({H1, T1}, {H2, _} = B) ->
    H1 =:= H2 andalso is_element(T1, [B]);

element_begins({_, _} = A, N) ->
    element_begins(A, interval(N));

element_begins(N, {_, _} = B) ->
    element_begins(interval(N), B);

element_begins(_, _) ->
    false.


%% @private
element_ends({H1, T1}, {_, T2} = B) ->
    T1 =:= T2 andalso is_element(H1, [B]);

element_ends({_, _} = A, N) ->
    element_ends(A, interval(N));

element_ends(N, {_, _} = B) ->
    element_ends(interval(N), B);

element_ends(_, _) ->
    false.


%% private
element_union({_, _} = A, {_, _} = B) ->
    case element_merges(A, B) of
        true ->
            unsafe_element_union(A, B);
        false ->
            error(badarg)
    end;

element_union({_, _} = A, N) ->
    element_union(A, interval(N));

element_union(N, {_, _} = B) ->
    element_union(interval(N), B);

element_union(N, B) ->
    element_union(interval(N), B).

-endif.

%% =============================================================================
%% EUNIT
%% =============================================================================

-ifdef(TEST).


%% @private
element_merges(A, B) ->
    element_overlaps(A, B) orelse element_meets(A, B).

%% @private
element_begins({H1, T1}, {H2, _} = B) ->
    H1 =:= H2 andalso is_element(T1, [B]);

element_begins({_, _} = A, N) ->
    element_begins(A, interval(N));

element_begins(N, {_, _} = B) ->
    element_begins(interval(N), B);

element_begins(_, _) ->
    false.


%% @private
element_ends({H1, T1}, {_, T2} = B) ->
    T1 =:= T2 andalso is_element(H1, [B]);

element_ends({_, _} = A, N) ->
    element_ends(A, interval(N));

element_ends(N, {_, _} = B) ->
    element_ends(interval(N), B);

element_ends(_, _) ->
    false.


%% private
element_union({_, _} = A, {_, _} = B) ->
    case element_merges(A, B) of
        true ->
            unsafe_element_union(A, B);
        false ->
            error(badarg)
    end;

element_union({_, _} = A, N) ->
    element_union(A, interval(N));

element_union(N, {_, _} = B) ->
    element_union(interval(N), B);

element_union(N, B) ->
    element_union(interval(N), B).



from_list_test_() ->
    Expected = [{1, 2}, 4, {6, 10}],
    [
        ?_assertEqual(Expected, from_list([1, 2, 4, 6, 7, 8, 9, 10])),
        ?_assertEqual(Expected, from_list([{1, 2}, 4, 6, 7, 8, 9, 10])),
        ?_assertEqual(Expected, from_list([{1, 2}, 4, {6, 7}, 8, 9, 10])),
        ?_assertEqual(Expected, from_list([{1, 2}, 4, {6, 7}, 8, {9, 10}])),
        ?_assertEqual(Expected, from_list(Expected))
    ].


to_flat_list_test_() ->
    Expected = [1, 2, 4, 6, 7, 8, 9, 10],
    [
        ?_assertEqual(Expected, to_flat_list(Expected)),
        ?_assertEqual(Expected, to_flat_list([{1, 2}, 4, 6, 7, 8, 9, 10])),
        ?_assertEqual(Expected, to_flat_list([{1, 2}, 4, {6, 7}, 8, 9, 10])),
        ?_assertEqual(Expected, to_flat_list([{1, 2}, 4, {6, 10}]))
    ].


is_type_test_() ->
    [
        ?_assert(true =:= is_type([1, 2, 4, 6, 7, 8, 9, 10])),
        ?_assert(true =:= is_type([{1, 2}, 4, 6, 7, 8, 9, 10])),
        ?_assert(true =:= is_type([{1, 2}, 4, {6, 7}, 8, 9, 10])),
        ?_assert(true =:= is_type([{1, 2}, 4, {6, 7}, 8, {9, 10}])),
        ?_assert(true =:= is_type([{1, 2}, 4, {6, 10}])),
        ?_assert(false =:= is_type([0.23])),
        ?_assert(false =:= is_type([atom])),
        ?_assert(false =:= is_type([<<>>]))
    ].


is_element_test_() ->
    [
        ?_assert(true =:= is_element(1, [{1, 2}, 4, {6, 10}])),
        ?_assert(true =:= is_element(2, [{1, 2}, 4, {6, 10}])),
        ?_assert(false =:= is_element(3, [{1, 2}, 4, {6, 10}])),
        ?_assert(true =:= is_element(4, [{1, 2}, 4, {6, 10}])),
        ?_assert(false =:= is_element(5, [{1, 2}, 4, {6, 10}])),
        ?_assert(true =:= is_element(6, [{1, 2}, 4, {6, 10}])),
        ?_assert(true =:= is_element(7, [{1, 2}, 4, {6, 10}])),
        ?_assert(true =:= is_element(8, [{1, 2}, 4, {6, 10}])),
        ?_assert(true =:= is_element(9, [{1, 2}, 4, {6, 10}])),
        ?_assert(true =:= is_element(10, [{1, 2}, 4, {6, 10}])),
        ?_assert(false =:= is_element(11, [{1, 2}, 4, {6, 10}])),
        ?_assert(false =:= is_element({1,6}, [{1, 2}, 4, {6, 10}])),
        ?_assert(true =:= is_element({6,7}, [{1, 2}, 4, {6, 10}])),
        ?_assert(true =:= is_element({7,10}, [{1, 2}, 4, {6, 10}])),
        ?_assert(false =:= is_element({8,11}, [{1, 2}, 4, {6, 10}]))
    ].


flat_size_test_() ->
    [
        ?_assert(8 =:= flat_size([1, 2, 4, 6, 7, 8, 9, 10])),
        ?_assert(8 =:= flat_size([{1, 2}, 4, 6, 7, 8, 9, 10])),
        ?_assert(8 =:= flat_size([{1, 2}, 4, {6, 7}, 8, 9, 10])),
        ?_assert(8 =:= flat_size([{1, 2}, 4, {6, 7}, 8, {9, 10}])),
        ?_assert(8 =:= flat_size([{1, 2}, 4, {6, 10}]))
    ].

min_test_() ->
    [
        ?_assert(1 =:= min([1, 2, 4, 6, 7, 8, 9, 10])),
        ?_assert(1 =:= min([{1, 2}, 4, 6, 7, 8, 9, 10])),
        ?_assert(1 =:= min([{1, 2}, 4, {6, 7}, 8, 9, 10])),
        ?_assert(1 =:= min([{1, 2}, 4, {6, 7}, 8, {9, 10}])),
        ?_assert(1 =:= min([{1, 2}, 4, {6, 10}]))
    ].

max_test_() ->
    [
        ?_assert(10 =:= max([1, 2, 4, 6, 7, 8, 9, 10])),
        ?_assert(10 =:= max([{1, 2}, 4, 6, 7, 8, 9, 10])),
        ?_assert(10 =:= max([{1, 2}, 4, {6, 7}, 8, 9, 10])),
        ?_assert(10 =:= max([{1, 2}, 4, {6, 7}, 8, {9, 10}])),
        ?_assert(10 =:= max([{1, 2}, 4, {6, 10}]))
    ].

element_precedes_test_() ->
    [
        ?_assert(element_precedes(0, {2, 3})),
        ?_assert(element_precedes(1, {2, 3})),
        ?_assert(element_precedes({0, 1}, {2, 3})),
        ?_assert(false =:= element_precedes({1, 1}, 1)),
        ?_assert(false =:= element_precedes({0, 1}, {0, 1})),
        ?_assert(false =:= element_precedes({0, 3}, {2, 3})),
        ?_assert(false =:= element_precedes({3, 4}, {2, 3})),
        ?_assert(false =:= element_precedes({4, 5}, {2, 3}))
    ].

element_meets_test_() ->
    [
        ?_assert(false =:= element_meets({1, 1}, 1)),
        ?_assert(false =:= element_meets({0, 1}, {0, 1})),
        ?_assert(false =:= element_meets({0, 3}, {2, 3})),
        ?_assert(false =:= element_meets({3, 4}, {2, 3})),
        ?_assert(element_meets({0, 1}, {2, 3})),
        ?_assert(element_meets({4, 5}, {2, 3}))
    ].

element_subtract_test_() ->
    [
        ?_assertEqual([], element_subtract(16, 16)),
        ?_assertEqual([], element_subtract({0, 16}, {0, 16})),
        ?_assertEqual([], element_subtract({4, 16}, {0, 16})),
        ?_assertEqual([], element_subtract({0, 5}, {0, 10})),
        ?_assertEqual([], element_subtract({5, 10}, {3, 20})),

        ?_assertEqual([{0, 1}], element_subtract({0, 16}, {2, 16})),
        ?_assertEqual([{9, 16}], element_subtract({0, 16}, {0, 8})),
        ?_assertEqual([{9, 16}], element_subtract({4, 16}, {4, 8})),
        ?_assertEqual([{9, 16}], element_subtract({4, 16}, {4, 8})),
        ?_assertEqual([{11, 20}], element_subtract({3, 20}, {0, 10})),
        ?_assertEqual([{0, 7}], element_subtract({0, 16}, {8, 20})),

        ?_assertEqual([{0, 1}, {9, 16}], element_subtract({0, 16}, {2, 8})),
        ?_assertEqual([{0, 3}, {9, 16}], element_subtract({0, 16}, {4, 8})),
        ?_assertEqual([{3, 4}, {11, 20}], element_subtract({3, 20}, {5, 10}))
    ].

add_element_test_() ->
    Cases = [
        % {Expected, Element, Set}
        {[{0, 1}, {3, 4}], {0, 1}, [{3, 4}]},
        {[0, {3, 4}], 0, [{3, 4}]},
        {[1, {3, 4}], 1, [{3, 4}]},
        {[{0, 3}], {0, 1}, [{2, 3}]},
        {[{0, 3}], {0, 2}, [{2, 3}]},
        {[{0, 3}], {0, 3}, [{2, 3}]},
        {[{0, 4}], {0, 4}, [{2, 3}]},
        {[{0, 4}], {0, 4}, [{0, 3}]},
        {[{0, 4}], {0, 4}, [{0, 4}]},
        {[{2, 10}], {3, 10}, [{2, 3}]},
        {[{2, 3}, {20, 30}], {20, 30}, [{2, 3}]}
    ],
    lists:append([
        [
            ?_assertEqual(Expected, add_element(Element, Set)),
            ?_assertEqual(
                ordsets:union(to_flat_list([Element]), to_flat_list(Set)),
                to_flat_list(add_element(Element, Set))
            )
        ]
        || {Expected, Element, Set} <- Cases
    ]).


del_element_test_() ->
    Cases = [
        {[2], 1, [2]},
        {[{2, 3}], 1, [{2, 3}]},
        {[], 1, [1]},
        {[], 1, [{1, 1}]},
        {[], {1, 2}, [{1, 2}]},
        {[{3, 4}], {0, 1}, [{3, 4}]},
        {[{2, 4}], {0, 1}, [{0, 4}]},
        {[{0, 2}, {15, 16}], {3, 14}, [{0, 16}]}
    ],
    lists:append([
        [
            ?_assertEqual(Expected, del_element(Element, Set)),
            ?_assertEqual(
                ordsets:subtract(to_flat_list(Set), to_flat_list([Element])),
                to_flat_list(del_element(Element, Set))
            )
        ]
        || {Expected, Element, Set} <- Cases
    ]).


-endif.