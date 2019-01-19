#!/usr/bin/env escript

-define(CRASH_MODULE, prop_partisan_crash_fault_model).

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

    io:format("Loading commands...~n", []),
    [io:format("~p.~n", [Command]) || Command <- Commands],

    %% Find command to remove.
    {_RemovedYet, RemovedCommand, AlteredCommands} = lists:foldr(fun(Command, {RY, RC, AC}) ->
        case RY of 
            false ->
                case Command of 
                    {set, _Var, {call, ?CRASH_MODULE, _Command, _Args}} ->
                        io:format("Removing the following command from the trace:~n", []),
                        io:format(" ~p~n", [Command]),
                        {true, Command, AC};
                    _ ->
                        {false, RC, [Command] ++ AC}
                end;
            true ->
                {RY, RC, [Command] ++ AC}
        end
    end, {false, undefined, []}, Commands),

    %% If we removed a partition resolution command, then we need to remove
    %% the partition introduction itself.
    FinalCommands = case RemovedCommand of 
        {set, _Var, {call, ?CRASH_MODULE, end_send_omission, Args}} ->
            io:format(" Found elimination of partition, looking for introduction call...~n", []),

            {_, _, FC} = lists:foldr(fun(Command, {RY, RC, AC}) ->
                case RY of 
                    false ->
                        case Command of 
                            {set, _Var1, {call, ?CRASH_MODULE, begin_send_omission, Args}} ->
                                io:format(" Removing the ASSOCIATED command from the trace:~n", []),
                                io:format("  ~p~n", [Command]),
                                {true, Command, AC};
                            _ ->
                                {false, RC, [Command] ++ AC}
                        end;
                    true ->
                        {RY, RC, [Command] ++ AC}
                end
            end, {false, undefined, []}, AlteredCommands),
            FC;
        _ ->
            AlteredCommands
    end,

    %% Write the schedule out.
    {ok, CounterexampleIo} = file:open(RebarCounterexampleConsultFile, [write, {encoding, utf8}]),
    io:format(CounterexampleIo, "~p.~n", [{TestModule, TestFunction, [FinalCommands]}]),
    ok = file:close(CounterexampleIo),

    %% Print out final schedule.
    %% lists:foreach(fun(Command) -> io:format("~p~n", [Command]) end, AlteredCommands),

    %% Move the 
    ok.