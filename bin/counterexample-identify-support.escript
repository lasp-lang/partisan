#!/usr/bin/env escript
%%! -pa ./_build/default/lib/jsx/ebin -Wall

%% TODO: Store results in ETS table.
%% TODO: Sort omissions ascending by size.
%% TODO: If we have a fail result for a prefix, fail result should propagate forward to bigger 
%%       sets of traces: dynamic partial order reduction.
%% TODO: All subsequent messages, after omitted messages, need to be removed from trace
%%       (optional delivery, not reified in the trace.)

main([TraceFile, ReplayTraceFile, CounterexampleConsultFile, RebarCounterexampleConsultFile, PreloadOmissionFile]) ->
    %% Keep track of when test started.
    StartTime = os:timestamp(),

    %% Open ets table.
    ?MODULE = ets:new(?MODULE, [named_table, set]),

    %% Open the trace file.
    {ok, TraceLines} = file:consult(TraceFile),

    %% Open the counterexample consult file:
    %% - we make an assumption that there will only be a single counterexample here.
    {ok, [{TestModule, TestFunction, [TestCommands]}]} = file:consult(CounterexampleConsultFile),

    io:format("Loading commands...~n", []),
    [io:format(" ~p.~n", [TestCommand]) || TestCommand <- TestCommands],

    %% Drop last command -- forced failure.
    TestFinalCommands = lists:reverse(tl(lists:reverse(TestCommands))),

    io:format("Rewritten commands...~n", []),
    [io:format(" ~p.~n", [TestFinalCommand]) || TestFinalCommand <- TestFinalCommands],

    %% Write the schedule out.
    {ok, CounterexampleIo} = file:open(RebarCounterexampleConsultFile, [write, {encoding, utf8}]),
    io:format(CounterexampleIo, "~p.~n", [{TestModule, TestFunction, [TestFinalCommands]}]),
    ok = file:close(CounterexampleIo),

    %% Remove forced failure from the trace.
    TraceLinesWithoutFailure = lists:filter(fun(Command) ->
        case Command of 
            {_, {_, [forced_failure]}} ->
                io:format("Removing command from trace: ~p~n", [Command]),
                false;
            _ ->
                true
        end
    end, hd(TraceLines)),

    %% Filter the trace into message trace lines.
    MessageTraceLines = lists:filter(fun({Type, Message}) ->
        case Type =:= pre_interposition_fun of 
            true ->
                {_TracingNode, InterpositionType, _OriginNode, _MessagePayload} = Message,

                case InterpositionType of 
                    forward_message ->
                        true;
                    _ -> 
                        false
                end;
            false ->
                false
        end
    end, TraceLinesWithoutFailure),
    io:format("Number of items in message trace: ~p~n", [length(MessageTraceLines)]),

    %% Generate the powerset of tracelines.
    MessageTraceLinesPowerset = powerset(MessageTraceLines),
    io:format("Number of message sets in powerset: ~p~n", [length(MessageTraceLinesPowerset)]),

    %% Traces to iterate.
    SortedPowerset = lists:sort(fun(A, B) -> length(A) =< length(B) end, MessageTraceLinesPowerset),

    TracesToIterate = case os:getenv("SUBLIST") of 
        false ->
            lists:reverse(SortedPowerset);
        Other ->
            lists:reverse(lists:sublist(SortedPowerset, list_to_integer(Other)))
    end,

    %% For each trace, write out the preload omission file.
    {_, FailedOmissions, PassedOmissions} = lists:foldl(fun(Omissions, {Iteration, InvalidOmissions, ValidOmissions}) ->
            %% Write out a new omission file from the previously used trace.
            io:format("Writing out new preload omissions file!~n", []),
            {ok, PreloadOmissionIo} = file:open(PreloadOmissionFile, [write, {encoding, utf8}]),
            [io:format(PreloadOmissionIo, "~p.~n", [O]) || O <- [Omissions]],
            ok = file:close(PreloadOmissionIo),

            %% Generate a new trace.
            io:format("Generating new trace based on message omissions (~p omissions): ~n", 
                    [length(Omissions)]),

            {FinalTraceLines, _, _} = lists:foldl(fun({Type, Message} = Line, {FinalTrace0, FaultsStarted0, AdditionalOmissions0}) ->
                case Type =:= pre_interposition_fun of 
                    true ->
                        {TracingNode, InterpositionType, OriginNode, MessagePayload} = Message,

                        case FaultsStarted0 of 
                            true ->
                                %% Once we start omitting, omit everything after that's a message
                                %% send because we don't know what might be coming. In 2PC, if we
                                %% have a successful trace and omit a prepare -- we can't be guaranteed
                                %% to ever see a prepare vote or commmit.
                                {FinalTrace0, FaultsStarted0, AdditionalOmissions0};
                            false ->
                                %% Otherwise, find just the targeted commands to remove.
                                case InterpositionType of 
                                    forward_message ->
                                        case lists:member(Line, Omissions) of 
                                            true ->
                                                %% Sort of hack, just deal with it for now.
                                                ReceiveOmission = {Type, {OriginNode, receive_message, TracingNode, {forward_message, implementation_module(), MessagePayload}}},
                                                {FinalTrace0, true, AdditionalOmissions0 ++ [ReceiveOmission]};
                                            false ->
                                                {FinalTrace0 ++ [Line], FaultsStarted0, AdditionalOmissions0}
                                        end;
                                    receive_message -> 
                                        case lists:member(Line, AdditionalOmissions0) of 
                                            true ->
                                                {FinalTrace0, FaultsStarted0, AdditionalOmissions0 -- [Line]};
                                            false ->
                                                {FinalTrace0 ++ [Line], FaultsStarted0, AdditionalOmissions0}
                                        end
                                end
                        end;
                    false ->
                        {FinalTrace0 ++ [Line], FaultsStarted0, AdditionalOmissions0}
                end
            end, {[], false, []}, TraceLinesWithoutFailure),

            %% Write out replay trace.
            io:format("Writing out new replay trace file!~n", []),
            {ok, TraceIo} = file:open(ReplayTraceFile, [write, {encoding, utf8}]),
            [io:format(TraceIo, "~p.~n", [TraceLine]) || TraceLine <- [FinalTraceLines]],
            ok = file:close(TraceIo),

            %% Run the trace.
            Command = "SHRINKING=true REPLAY=true PRELOAD_OMISSIONS_FILE=" ++ PreloadOmissionFile ++ " REPLAY_TRACE_FILE=" ++ ReplayTraceFile ++ " TRACE_FILE=" ++ TraceFile ++ " ./rebar3 proper --retry | tee /tmp/partisan.output",
            io:format("Executing command for iteration ~p of ~p (~p invalid, ~p valid):", 
                        [Iteration, length(TracesToIterate), length(InvalidOmissions), length(ValidOmissions)]),
            io:format(" ~p~n", [Command]),
            Output = os:cmd(Command),

            %% Store set of omissions as omissions that didn't invalidate the execution.
            case string:find(Output, "{postcondition,false}") of 
                nomatch ->
                    %% This passed.
                    io:format("Test passed, adding omissions to set of supporting omissions!~n", []),

                    %% Insert result into the ETS table.
                    true = ets:insert(?MODULE, {Iteration, {Omissions, true}}),

                    %% Increment iteration, update omission accumulators.
                    {Iteration + 1, InvalidOmissions, ValidOmissions ++ [Omissions]};
                _ ->
                    %% This failed.
                    io:format("Test FAILED!~n", []),

                    %% Insert result into the ETS table.
                    true = ets:insert(?MODULE, {Iteration, {Omissions, false}}),

                    %% Increment iteration, update omission accumulators.
                    {Iteration + 1, InvalidOmissions ++ [Omissions], ValidOmissions}
            end
    end, {1, [], []}, TracesToIterate),

    io:format("Out of ~p traces found ~p that supported the execution where ~p didn't.~n", 
              [length(TracesToIterate), length(PassedOmissions), length(FailedOmissions)]),

    %% Test finished time.
    EndTime = os:timestamp(),
    Difference = timer:now_diff(EndTime, StartTime),
    DifferenceMs = Difference / 1000,
    DifferenceSec = DifferenceMs / 1000,

    io:format("Test started: ~p~n", [StartTime]),
    io:format("Test ended: ~p~n", [EndTime]),
    io:format("Test took: ~p seconds.~n", [DifferenceSec]),

    ok.

%% @doc Generate the powerset of messages.
powerset([]) -> 
    [[]];

powerset([H|T]) -> 
    PT = powerset(T),
    [ [H|X] || X <- PT ] ++ PT.

%% @private
implementation_module() ->
    case os:getenv("IMPLEMENTATION_MODULE") of 
        false ->
            exit({error, no_implementation_module_specified});
        Other ->
            list_to_atom(Other)
    end.