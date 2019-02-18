#!/usr/bin/env escript
%%! -pa ./_build/test/lib/partisan/ebin -Wall

main(_Args) ->
    %% Generate and print the powerset.
    io:format("Powerset:~n", []),
    Powerset = powerset(schedule()),
    lists:foreach(fun(N) ->
        io:format("~p~n", [N]),

        %% Generate permutations.
        Perms = perms(N),
        lists:foreach(fun(M) ->
            io:format("  ~p~n", [M])
        end, Perms),

        io:format("~n", [])
    end, Powerset),

    %% TODO: Static partial order reduction.

    %% TODO: Dynamic partial order reduction.

    ok.

schedule() ->
    [1,2,3].

%% @doc Generate the powerset of messages.
powerset([]) -> 
    [[]];

powerset([H|T]) -> 
    PT = powerset(T),
    [ [H|X] || X <- PT ] ++ PT.

%% @doc Generate all possible permutations.
perms([]) -> 
    [[]];

perms(L) -> 
    [[H|T] || H <- L, T <- perms(L--[H])].