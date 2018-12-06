% Copyright (c) 2010-2014, Lars Buitinck
% May be used, redistributed and modified under the terms of the
% GNU Lesser General Public License (LGPL), version 2.1 or later

-module(heaps).
-export([add/2, delete_min/1, from_list/1, empty/1, merge/2, new/0, min/1,
         size/1, sort/1, to_list/1]).
-import(lists).

% Public interface: priority queues

% Add element X to priority queue
add(X, {prioq, Heap, N}) ->
    {prioq, meld(Heap, {X, []}), N + 1}.

% Return new priority queue with minimum element removed
delete_min({prioq, {_, Sub}, N}) ->
    {prioq, pair(Sub), N - 1}.

% Construct priority queue from list
from_list(L) ->
    lists:foldl(fun(X, Q) -> add(X, Q) end, new(), L).

% True iff argument is an empty priority queue
empty({prioq, nil, 0}) -> true;
empty(_) -> false.

% Merge two priority queues
merge({prioq, Heap0, M}, {prioq, Heap1, N}) ->
    {prioq, meld(Heap0, Heap1), M + N}.

% Get minimum element
min({prioq, Heap, _}) -> heap_min(Heap).

% Returns new, empty priority queue
new() -> {prioq, nil, 0}.

% Number of elements in queue
size({prioq, _, N}) -> N.

% Heap sort a list
sort(L) -> to_list(from_list(L)).

% List elements in priority queue in sorted order
to_list({prioq, nil, _}) -> [];
to_list(Q) ->
    [min(Q) | to_list(delete_min(Q))].

heap_min({X, _}) -> X.

meld(nil, Q) -> Q;
meld(Q, nil) -> Q;
meld(L = {X, SubL}, R = {Y, SubR}) ->
    if
        X < Y ->
            {X, [R|SubL]};
        true ->
            {Y, [L|SubR]}
    end.

% "Pair up" (recursively meld) a list of pairing heaps.
pair([]) -> nil;
pair([Q]) -> Q;
pair([Q0, Q1 | Qs]) ->
    Q2 = meld(Q0, Q1),
    meld(Q2, pair(Qs)).
