#!/usr/bin/env escript

-define(TIMEOUT, 1000).

main([TraceFile, ReplayTraceFile, CounterexampleConsultFile, RebarCounterexampleConsultFile]) ->
    %% Open the trace file.
    {ok, TraceLines} = file:consult(TraceFile),

    %% Write out a new replay trace from the last used trace.
    {ok, TraceIo} = file:open(ReplayTraceFile, [write, {encoding, utf8}]),
    [io:format(TraceIo, "~p.~n", [TraceLine]) || TraceLine <- TraceLines],
    ok = file:close(TraceIo),

    %% Open the counterexample consult file:
    %% - we make an assumption that there will only be a single counterexample here.
    {ok, [{TestModule, TestFunction, [Commands]}]} = file:consult(CounterexampleConsultFile),

    %% Rewrite partition commands to delay commands.
    FinalCommands = lists:foldr(fun(Command, FC) ->
        case Command of 
            {set, Var, {call, prop_partisan, resolve_sync_partition, Args}} ->
                io:format("Removing the following command from the trace:~n", []),
                io:format(" ~p~n", [Command]),

                NewCommand = {set, Var, {call, prop_partisan, delayed_resolve_sync_partition, [?TIMEOUT] ++ Args}},
                io:format(" Replacing with the following command in the trace:~n", []),
                io:format("  ~p~n", [NewCommand]),

                [NewCommand] ++ FC;
            _ ->
                [Command] ++ FC
        end
    end, [], Commands),

    %% Write the schedule out.
    {ok, CounterexampleIo} = file:open(RebarCounterexampleConsultFile, [write, {encoding, utf8}]),
    io:format(CounterexampleIo, "~p.~n", [{TestModule, TestFunction, [FinalCommands]}]),
    ok = file:close(CounterexampleIo),

    %% Print out final schedule.
    %% lists:foreach(fun(Command) -> io:format("~p~n", [Command]) end, AlteredCommands),

    %% Move the 
    ok.