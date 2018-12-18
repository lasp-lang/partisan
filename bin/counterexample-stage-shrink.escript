#!/usr/bin/env escript

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

    %% Alter the schedule -- remove first partitioning command.
    {_, AlteredCommands} = lists:foldr(fun(Command, {Removed, Acc}) ->
        case Removed of 
            false ->
                case Command of 
                    {set, _Var, {call, prop_partisan, _Function, _Args}} ->
                        io:format("Removing the following command from the trace:~n", []),
                        io:format(" ~p~n", [Command]),
                        {true, Acc};
                    _ ->
                        {false, [Command] ++ Acc}
                end;
            true ->
                {Removed, [Command] ++ Acc}
        end
    end, {false, []}, Commands),

    %% Write the schedule out.
    {ok, CounterexampleIo} = file:open(RebarCounterexampleConsultFile, [write, {encoding, utf8}]),
    io:format(CounterexampleIo, "~p.~n", [{TestModule, TestFunction, [AlteredCommands]}]),
    ok = file:close(CounterexampleIo),

    %% Print out final schedule.
    %% lists:foreach(fun(Command) -> io:format("~p~n", [Command]) end, AlteredCommands),

    %% Move the 
    ok.