#!/usr/bin/env escript

-define(CRASH_MODULE, prop_partisan_crash_fault_model).

main([TraceFile, ReplayTraceFile, CounterexampleConsultFile, RebarCounterexampleConsultFile]) ->
    %% Open the trace file.
    {ok, TraceLines} = file:consult(TraceFile),

    %% Write out a new replay trace from the last used trace.
    {ok, TraceIo} = file:open(ReplayTraceFile, [write, {encoding, utf8}]),

    %% Open the counterexample consult file:
    %% - we make an assumption that there will only be a single counterexample here.
    {ok, [{TestModule, TestFunction, [TestCommands]}]} = file:consult(CounterexampleConsultFile),

    io:format("Loading commands...~n", []),
    [io:format("~p.~n", [TestCommand]) || TestCommand <- TestCommands],

    %% Find command to remove.
    {_TestRemovedYet, TestRemovedCommand, TestAlteredCommands} = lists:foldr(fun(TestCommand, {RY, RC, AC}) ->
        case RY of 
            false ->
                case TestCommand of 
                    {set, _Var, {call, ?CRASH_MODULE, _Command, _Args}} ->
                        io:format("Removing the following command from the trace:~n", []),
                        io:format(" ~p~n", [TestCommand]),
                        {true, TestCommand, AC};
                    _ ->
                        {false, RC, [TestCommand] ++ AC}
                end;
            true ->
                {RY, RC, [TestCommand] ++ AC}
        end
    end, {false, undefined, []}, TestCommands),

    %% If we removed a partition resolution command, then we need to remove
    %% the partition introduction itself.
    {TestSecondRemovedCommand, TestFinalCommands} = case TestRemovedCommand of 
        {set, _Var, {call, ?CRASH_MODULE, end_receive_omission, Args}} ->
            io:format(" Found elimination of partition, looking for introduction call...~n", []),

            {_, SRC, FC} = lists:foldr(fun(TestCommand, {RY, RC, AC}) ->
                case RY of 
                    false ->
                        case TestCommand of 
                            {set, _Var1, {call, ?CRASH_MODULE, begin_receive_omission, Args}} ->
                                io:format(" Removing the ASSOCIATED command from the trace:~n", []),
                                io:format("  ~p~n", [TestCommand]),
                                {true, TestCommand, AC};
                            _ ->
                                {false, RC, [TestCommand] ++ AC}
                        end;
                    true ->
                        {RY, RC, [TestCommand] ++ AC}
                end
            end, {false, undefined, []}, TestAlteredCommands),
            {SRC, FC};
        {set, _Var, {call, ?CRASH_MODULE, end_send_omission, Args}} ->
            io:format(" Found elimination of partition, looking for introduction call...~n", []),

            {_, SRC, FC} = lists:foldr(fun(TestCommand, {RY, RC, AC}) ->
                case RY of 
                    false ->
                        case TestCommand of 
                            {set, _Var1, {call, ?CRASH_MODULE, begin_send_omission, Args}} ->
                                io:format(" Removing the ASSOCIATED command from the trace:~n", []),
                                io:format("  ~p~n", [TestCommand]),
                                {true, TestCommand, AC};
                            _ ->
                                {false, RC, [TestCommand] ++ AC}
                        end;
                    true ->
                        {RY, RC, [TestCommand] ++ AC}
                end
            end, {false, undefined, []}, TestAlteredCommands),
            {SRC, FC};
        _ ->
            {undefined, TestAlteredCommands}
    end,

    %% Create list of commands to remove from the trace schedule.
    CommandsToRemoveFromTrace = 
        lists:flatmap(fun({set, _Var2, {call, ?CRASH_MODULE, Function, [Node|Rest]}}) ->
            [{exit_command, {Node, [Function] ++ Rest}},
             {enter_command, {Node, [Function] ++ Rest}}]
        end, lists:filter(fun(X) -> X =/= undefined end, [TestSecondRemovedCommand, TestRemovedCommand])),
    io:format("To be removed from trace: ~n", []),
    [io:format(" ~p.~n", [CommandToRemoveFromTrace]) || CommandToRemoveFromTrace <- CommandsToRemoveFromTrace],

    %% Prune out commands from the trace.
    {FinalTraceLines, []} = lists:foldr(fun(TraceLine, {FTL, TR}) ->
        case length(TR) > 0 of
            false ->
                {[TraceLine] ++ FTL, TR};
            true ->
                ToRemove = hd(TR),
                case TraceLine of 
                    ToRemove ->
                        {FTL, tl(TR)};
                    _ ->
                        {[TraceLine] ++ FTL, TR}
                end
        end
    end, {[], CommandsToRemoveFromTrace}, hd(TraceLines)),

    %% Write the schedule out.
    {ok, CounterexampleIo} = file:open(RebarCounterexampleConsultFile, [write, {encoding, utf8}]),
    io:format(CounterexampleIo, "~p.~n", [{TestModule, TestFunction, [TestFinalCommands]}]),
    ok = file:close(CounterexampleIo),

    %% Write new trace out.
    [io:format(TraceIo, "~p.~n", [TraceLine]) || TraceLine <- [FinalTraceLines]],
    ok = file:close(TraceIo),

    %% Print out final schedule.
    %% lists:foreach(fun(Command) -> io:format("~p~n", [Command]) end, AlteredCommands),

    ok.