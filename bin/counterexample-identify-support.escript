#!/usr/bin/env escript
%%! -pa ./_build/default/lib/jsx/ebin -Wall

-define(RESULTS, results).
-define(SCHEDULES, schedules).

main([TraceFile, ReplayTraceFile, CounterexampleConsultFile, RebarCounterexampleConsultFile, PreloadOmissionFile]) ->
    %% Get module as string.
    ModuleString = os:getenv("IMPLEMENTATION_MODULE"),

    %% Keep track of when test started.
    StartTime = os:timestamp(),

    %% Open ets table for results.
    ?RESULTS = ets:new(?RESULTS, [named_table, set]),

    %% Open table for schedules.
    ?SCHEDULES = ets:new(?SCHEDULES, [named_table, ordered_set]),

    %% Open the trace file.
    {ok, TraceLines} = file:consult(TraceFile),

    %% Open the causality file.
    CausalityFile = "/tmp/partisan-causality-" ++ ModuleString,

    case filelib:is_file(CausalityFile) of 
        false ->
            io:format("Causality file doesn't exist: ~p~n", [CausalityFile]);
        true ->
            ok
    end,

    {ok, [RawCausality]} = file:consult(CausalityFile),
    Causality = dict:from_list(RawCausality),
    io:format("Causality loaded: ~p~n", [dict:to_list(Causality)]),

    %% Open the annotations file.
    AnnotationsFile = "/tmp/partisan-annotations-" ++ ModuleString,

    case filelib:is_file(AnnotationsFile) of 
        false ->
            io:format("Annotations file doesn't exist: ~p~n", [AnnotationsFile]);
        true ->
            ok
    end,

    {ok, [RawAnnotations]} = file:consult(AnnotationsFile),
    Annotations = dict:from_list(RawAnnotations),
    io:format("Annotations loaded: ~p~n", [dict:to_list(Annotations)]),

    %% Check that we have the necessary preconditions.
    PreconditionsPresent = ensure_preconditions_present(Causality, Annotations),
    io:format("All preconditions present: ~p~n", [PreconditionsPresent]),
    case PreconditionsPresent of 
        true ->
            ok;
        false ->
            exit({error, not_all_preconditions_present})
    end,

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

    %% Perform recursive analysis of the traces.
    PreviousIteration = 1,
    PreviousClassificationsExplored = [],
    analyze(1, PreloadOmissionFile, ReplayTraceFile, TraceFile, Causality, Annotations, PreviousIteration, PreviousClassificationsExplored, [{TraceLinesWithoutFailure, [], []}]),

    %% Should we try to find witnesses?
    case os:getenv("FIND_WITNESSES") of 
        false ->
            ok;
        _Other ->
            identify_minimal_witnesses()
    end,

    %% Test finished time.
    EndTime = os:timestamp(),
    Difference = timer:now_diff(EndTime, StartTime),
    DifferenceMs = Difference / 1000,
    DifferenceSec = DifferenceMs / 1000,

    io:format("Test started: ~p~n", [StartTime]),
    io:format("Test ended: ~p~n", [EndTime]),
    io:format("Test took: ~p seconds.~n", [DifferenceSec]),

    ok.

%% @private
analyze(_Pass, _PreloadOmissionFile, _ReplayTraceFile, _TraceFile, _Causality, _Annotations, _PreviousIteration, _PreviousClassificationsExplored, []) ->
    ok;

analyze(Pass, PreloadOmissionFile, ReplayTraceFile, TraceFile, Causality, Annotations, PreviousIteration, PreviousClassificationsExplored, [{TraceLines, BaseOmissions, _Difference}|RestTraceLines]) ->
    io:format("Beginning analyze pass: ~p with previous iteration: ~p~n", [Pass, PreviousIteration]),

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
    end, TraceLines),
    io:format("Number of items in message trace: ~p~n", [length(MessageTraceLines)]),

    %% Generate the powerset of tracelines.
    {PowersetTime, MessageTraceLinesPowerset} = timer:tc(fun() -> powerset(MessageTraceLines) end),
    io:format("Number of message sets in powerset: ~p~n", [length(MessageTraceLinesPowerset)]),
    io:format("Powerset generation took: ~p~n", [PowersetTime]),

    TracesToIterate = case os:getenv("SUBLIST") of 
        false ->
            lists:reverse(MessageTraceLinesPowerset);
        Other ->
            case Other of 
                "" ->
                    exit({error, no_sublist_provided});
                "0" ->
                    lists:reverse(MessageTraceLinesPowerset);
                _ ->
                    lists:reverse(lists:sublist(MessageTraceLinesPowerset, list_to_integer(Other)))
            end
    end,

    %% Generate schedules.
    {GenIteration, GenNumPassed, GenNumFailed, GenNumPruned, GenClassificationsExplored, GenAdditionalTraces} = lists:foldl(fun(Omissions, {GenIteration0, GenNumPassed0, GenNumFailed0, GetNumPruned0, GenClassificationsExplored0, GenAdditionalTraces0}) ->
        % io:format("~n", []),
        % io:format("Number of message trace line: ~p~n", [length(MessageTraceLines)]),
        % io:format("Number of omission lines: ~p~n", [length(Omissions)]),
        % io:format("Difference lines: ~p~n", [length(MessageTraceLines -- Omissions)]),

        %% Generate a new trace.
        % io:format("Generating new trace based on message omissions (~p omissions, iteration ~p): ~n", [length(Omissions), Iteration]),

        {FinalTraceLines, _, _, PrefixMessageTypes, OmittedMessageTypes, ConditionalMessageTypes} = lists:foldl(fun({Type, Message} = Line, {FinalTrace0, FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0, OmittedMessageTypes0, ConditionalMessageTypes0}) ->
            case Type =:= pre_interposition_fun of 
                true ->
                    {TracingNode, InterpositionType, OriginNode, MessagePayload} = Message,

                    case FaultsStarted0 of 
                        true ->
                            %% Once we start omitting, omit everything after that's a message
                            %% send because we don't know what might be coming. In 2PC, if we
                            %% have a successful trace and omit a prepare -- we can't be guaranteed
                            %% to ever see a prepare vote or commmit.
                            case InterpositionType of 
                                forward_message ->
                                    case lists:member(Line, Omissions) of 
                                        true ->
                                            ReceiveOmission = {Type, {OriginNode, receive_message, TracingNode, {forward_message, implementation_module(), MessagePayload}}},
                                            {FinalTrace0, FaultsStarted0, AdditionalOmissions0 ++ [ReceiveOmission], PrefixMessageTypes0, OmittedMessageTypes0 ++ [message_type(Message)], ConditionalMessageTypes0};
                                        false ->
                                            {FinalTrace0, FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0, OmittedMessageTypes0, ConditionalMessageTypes0 ++ [message_type(Message)]}
                                    end;
                                receive_message ->
                                    case lists:member(Line, AdditionalOmissions0) of 
                                        true ->
                                            {FinalTrace0, FaultsStarted0, AdditionalOmissions0 -- [Line], PrefixMessageTypes0, OmittedMessageTypes0 ++ [message_type(Message)], ConditionalMessageTypes0};
                                        false ->
                                            {FinalTrace0, FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0, OmittedMessageTypes0, ConditionalMessageTypes0 ++ [message_type(Message)]}
                                    end
                            end;
                        false ->
                            %% Otherwise, find just the targeted commands to remove.
                            case InterpositionType of 
                                forward_message ->
                                    case lists:member(Line, Omissions) of 
                                        true ->
                                            % io:format("fault started with line: ~p~n", [Line]),

                                            %% Sort of hack, just deal with it for now.
                                            ReceiveOmission = {Type, {OriginNode, receive_message, TracingNode, {forward_message, implementation_module(), MessagePayload}}},
                                            {FinalTrace0, true, AdditionalOmissions0 ++ [ReceiveOmission], PrefixMessageTypes0, OmittedMessageTypes0 ++ [message_type(Message)], ConditionalMessageTypes0};
                                        false ->
                                            {FinalTrace0 ++ [Line], FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0 ++ [message_type(Message)], OmittedMessageTypes0, ConditionalMessageTypes0}
                                    end;
                                receive_message -> 
                                    case lists:member(Line, AdditionalOmissions0) of 
                                        true ->
                                            {FinalTrace0, FaultsStarted0, AdditionalOmissions0 -- [Line], PrefixMessageTypes0, OmittedMessageTypes0 ++ [message_type(Message)], ConditionalMessageTypes0};
                                        false ->
                                            {FinalTrace0 ++ [Line], FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0 ++ [message_type(Message)], OmittedMessageTypes0, ConditionalMessageTypes0}
                                    end
                            end
                    end;
                false ->
                    {FinalTrace0 ++ [Line], FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0, OmittedMessageTypes0, ConditionalMessageTypes0}
            end
        end, {[], false, [], [], [], []}, TraceLines),

        % io:format("PrefixMessageTypes: ~p~n", [PrefixMessageTypes]),
        % io:format("OmittedMessageTypes: ~p~n", [OmittedMessageTypes]),
        % io:format("ConditionalMessageTypes: ~p~n", [ConditionalMessageTypes]),
        % io:format("length(MessageTypes): ~p~n", [length(PrefixMessageTypes ++ OmittedMessageTypes ++ ConditionalMessageTypes)]),

        %% Is this schedule valid for these omissions?
        OmissionsSet = sets:from_list(Omissions),
        BaseOmissionsSet = sets:from_list(BaseOmissions),

        ScheduleValidOmissions = case sets:is_subset(BaseOmissionsSet, OmissionsSet) orelse BaseOmissionsSet =:= OmissionsSet of
            true ->
                true;
            false ->
                false
        end,

        ScheduleValidCausality = schedule_valid_causality(Causality, PrefixMessageTypes, OmittedMessageTypes, ConditionalMessageTypes),
        % ScheduleValidCausality = true,

        ScheduleValid = ScheduleValidCausality andalso ScheduleValidOmissions,

        case ScheduleValid of
            true ->
            %    io:format("schedule_valid_omissions: ~p~n", [ScheduleValidOmissions]),
            %    io:format("schedule_valid_causality: ~p~n", [ScheduleValidCausality]),
            %    io:format("schedule_valid: ~p~n", [ScheduleValid]),
               ok;
            false ->
                ok
        end,

        ClassifySchedule = classify_schedule(3, Annotations, PrefixMessageTypes, OmittedMessageTypes, ConditionalMessageTypes),

        case os:getenv("PRELOAD_SCHEDULES") of 
            false ->
                case GenIteration0 rem 1000 == 0 of 
                    true ->
                        io:format("Executed ~p schedules.~n", [GenIteration0]);
                    false ->
                        ok
                end,

                case execute_schedule(PreloadOmissionFile, ReplayTraceFile, TraceFile, TraceLines, {GenIteration0, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}, GenClassificationsExplored0, GenAdditionalTraces0) of
                    pruned ->
                        {GenIteration0 + 1, GenNumPassed0, GenNumFailed0, GetNumPruned0 + 1, GenClassificationsExplored0, GenAdditionalTraces0};
                    {passed, ClassificationsExplored, NewTraces} ->
                        {GenIteration0 + 1, GenNumPassed0 + 1, GenNumFailed0, GetNumPruned0, ClassificationsExplored, NewTraces};
                    {failed, ClassificationsExplored, NewTraces} ->
                        {GenIteration0 + 1, GenNumPassed0, GenNumFailed0 + 1, GetNumPruned0, ClassificationsExplored, NewTraces};
                    invalid ->
                        {GenIteration0 + 1, GenNumPassed0, GenNumFailed0, GetNumPruned0 + 1, GenClassificationsExplored0, GenAdditionalTraces0}
                end;
            "false" ->
                case GenIteration0 rem 1000 == 0 of 
                    true ->
                        io:format("Executed ~p schedules.~n", [GenIteration0]);
                    false ->
                        ok
                end,

                case execute_schedule(PreloadOmissionFile, ReplayTraceFile, TraceFile, TraceLines, {GenIteration0, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}, GenClassificationsExplored0, GenAdditionalTraces0) of
                    pruned ->
                        {GenIteration0 + 1, GenNumPassed0, GenNumFailed0, GetNumPruned0 + 1, GenClassificationsExplored0, GenAdditionalTraces0};
                    {passed, ClassificationsExplored, NewTraces} ->
                        {GenIteration0 + 1, GenNumPassed0 + 1, GenNumFailed0, GetNumPruned0, ClassificationsExplored, NewTraces};
                    {failed, ClassificationsExplored, NewTraces} ->
                        {GenIteration0 + 1, GenNumPassed0, GenNumFailed0 + 1, GetNumPruned0, ClassificationsExplored, NewTraces};
                    invalid ->
                        {GenIteration0 + 1, GenNumPassed0, GenNumFailed0, GetNumPruned0 + 1, GenClassificationsExplored0, GenAdditionalTraces0}
                end;
            _Other ->
                %% Store generated schedule.
                true = ets:insert(?SCHEDULES, {GenIteration0, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}),

                {GenIteration0 + 1, GenNumPassed0, GenNumFailed0, GetNumPruned0, GenClassificationsExplored0, GenAdditionalTraces0}
        end
    end, {PreviousIteration, 0, 0, 0, PreviousClassificationsExplored, RestTraceLines}, TracesToIterate),

    %% Run generated schedules stored in the ETS table.
    {PreloadNumPassed, PreloadNumFailed, PreloadNumPruned, PreloadClassificationsExplored, PreloadAdditionalTraces} = ets:foldl(fun({Iteration, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}, {PreloadNumPassed0, PreloadNumFailed0, PreloadNumPruned0, PreloadClassificationsExplored0, PreloadAdditionalTraces0}) ->
        case execute_schedule(PreloadOmissionFile, ReplayTraceFile, TraceFile, TraceLines, {Iteration, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}, PreloadClassificationsExplored0, PreloadAdditionalTraces0) of
            pruned ->
                {PreloadNumPassed0, PreloadNumFailed0, PreloadNumPruned0 + 1, PreloadClassificationsExplored0, PreloadAdditionalTraces0};
            {passed, ClassificationsExplored, NewTraces} ->
                {PreloadNumPassed0 + 1, PreloadNumFailed0, PreloadNumPruned0, ClassificationsExplored, NewTraces};
            {failed, ClassificationsExplored, NewTraces} ->
                {PreloadNumPassed0, PreloadNumFailed0 + 1, PreloadNumPruned0, ClassificationsExplored, NewTraces};
            invalid ->
                {PreloadNumPassed0, PreloadNumFailed0, PreloadNumPruned0 + 1, PreloadClassificationsExplored0, PreloadAdditionalTraces0}
        end
    end, {GenNumPassed, GenNumFailed, GenNumPruned, GenClassificationsExplored, GenAdditionalTraces}, ?SCHEDULES),

    io:format("Pass: ~p, Iterations: ~p - ~p, Results: ~p, Failed: ~p, Pruned: ~p~n", [Pass, PreviousIteration, GenIteration, PreloadNumPassed, PreloadNumFailed, PreloadNumPruned]),

    %% Should we explore any new schedules we have discovered?
    case os:getenv("RECURSIVE") of 
        false ->
            analyze(Pass + 1, PreloadOmissionFile, ReplayTraceFile, TraceFile, Causality, Annotations, GenIteration, PreloadClassificationsExplored, RestTraceLines);
        "false" ->
            analyze(Pass + 1, PreloadOmissionFile, ReplayTraceFile, TraceFile, Causality, Annotations, GenIteration, PreloadClassificationsExplored, RestTraceLines);
        _Other ->
            analyze(Pass + 1, PreloadOmissionFile, ReplayTraceFile, TraceFile, Causality, Annotations, GenIteration, PreloadClassificationsExplored, PreloadAdditionalTraces)
    end.

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

%% @private
is_supertrace(Trace1, Trace2) ->
    %% Trace1 is a supertrace if Trace2 is a prefix of Trace1.
    %% Contiguous, ordered.
    lists:prefix(Trace2, Trace1).

%% @private
is_subtrace(Trace1, Trace2) ->
    %% Remove all elements from T2 not in T1 to make traces comparable.
    FilteredTrace2 = lists:filter(fun(T2) -> lists:member(T2, Trace1) end, Trace2),

    %% Now, is the first trace a prefix?  If so, it's a subtrace.
    lists:prefix(Trace1, FilteredTrace2).

%% @private
message_type(Message) ->
    {_TracingNode, InterpositionType, _OriginNode, MessagePayload} = Message,

    case InterpositionType of 
        forward_message ->
            MessageType1 = element(1, MessagePayload),
            {forward_message, MessageType1};
        receive_message ->
            {forward_message, _Module, Payload} = MessagePayload,
            MessageType1 = element(1, Payload),
            {receive_message, MessageType1}
    end.

%% @private
schedule_valid_causality(Causality, PrefixSchedule, _OmittedSchedule, ConditionalSchedule) ->
    DerivedSchedule = PrefixSchedule ++ ConditionalSchedule,
    % io:format("derived_schedule: ~p~n", [length(DerivedSchedule)]),
    % io:format("prefix_schedule: ~p~n", [length(PrefixSchedule)]),
    % io:format("omitted_schedule: ~p~n", [length(OmittedSchedule)]),
    % io:format("conditional_schedule: ~p~n", [length(ConditionalSchedule)]),

    RequirementsMet = lists:foldl(fun(Type, Acc) ->
        % io:format("=> Type: ~p~n", [Type]),

        All = lists:foldl(fun({K, V}, Acc1) ->
            % io:format("=> V: ~p~n", [V]),

            case lists:member(Type, V) of 
                true ->
                    % io:format("=> Presence of ~p requires: ~p~n", [Type, K]),

                    case lists:member(K, DerivedSchedule) of 
                        true ->
                            % io:format("=> Present!~n", []),
                            true andalso Acc1;
                        false ->
                            % io:format("=> NOT Present!~n", []),
                            false andalso Acc1
                    end;
                false ->
                    % io:format("=> * fallthrough: ~p not in ~p.~n", [Type, V]),
                    true andalso Acc1
            end
        end, Acc, dict:to_list(Causality)),

        All andalso Acc
    end, true, DerivedSchedule),

    case RequirementsMet of 
        true ->
            % io:format("Causality verified.~n", []),
            ok;
        false ->
            % io:format("Schedule does not represent a valid schedule!~n", []),
            ok
    end,

    RequirementsMet.

%% @private
identify_minimal_witnesses() ->
    io:format("Identifying witnesses...~n", []),

    %% Once we finished, we need to compute the minimals.
    Witnesses = ets:foldl(fun({_, {_Iteration, FinalTraceLines, _Omissions, Status} = Candidate}, Witnesses1) ->
        % io:format("=> looking at iteration ~p~n", [Iteration]),

        %% For each trace that passes.
        case Status of 
            true ->
                %% Ensure all supertraces also pass.
                AllSupertracesPass = ets:foldl(fun({_, {_Iteration1, FinalTraceLines1, _Omissions1, Status1}}, AllSupertracesPass1) ->
                    case is_supertrace(FinalTraceLines1, FinalTraceLines) of
                        true ->
                            % io:format("=> => found supertrace, status: ~p~n", [Status1]),
                            Status1 andalso AllSupertracesPass1;
                        false ->
                            AllSupertracesPass1
                    end
                end, true, ?RESULTS),

                % io:format("=> ~p, all_super_traces_passing? ~p~n", [Iteration, AllSupertracesPass]),

                case AllSupertracesPass of 
                    true ->
                        % io:format("=> witness found!~n", []),
                        Witnesses1 ++ [Candidate];
                    false ->
                        Witnesses1
                end;
            false ->
                %% If it didn't pass, it can't be a witness.
                Witnesses1
        end
    end, [], ?RESULTS),

    io:format("Checking for minimal witnesses...~n", []),

    %% Identify minimal.
    MinimalWitnesses = lists:foldl(fun({Iteration, FinalTraceLines, _Omissions, _Status} = Witness, MinimalWitnesses) ->
        % io:format("=> looking at iteration ~p~n", [Iteration]),

        %% See if any of the traces are subtraces of this.
        StillMinimal = lists:foldl(fun({Iteration1, FinalTraceLines1, _Omissions1, _Status1}, StillMinimal1) ->
            %% Is this other trace a subtrace of me?  If so, discard us
            case is_subtrace(FinalTraceLines1, FinalTraceLines) andalso Iteration =/= Iteration1 of 
                true ->
                    % io:format("=> => found subtrace in iteration: ~p, status: ~p~n", [Iteration1, Status1]),
                    StillMinimal1 andalso false;
                false ->
                    StillMinimal1
            end
        end, true, Witnesses),

        % io:format("=> ~p, still_minimal? ~p~n", [Iteration, StillMinimal]),

        case StillMinimal of 
            true ->
                % io:format("=> minimal witness found!~n", []),
                MinimalWitnesses ++ [Witness];
            false ->
                MinimalWitnesses
        end
    end, [], Witnesses),

    %% Output.
    io:format("Witnesses found: ~p~n", [length(Witnesses)]),
    io:format("Minimal witnesses found: ~p~n", [length(MinimalWitnesses)]),

    ok.

%% @private
classify_schedule(_N, Annotations, PrefixSchedule, _OmittedSchedule, ConditionalSchedule) ->
    DerivedSchedule = PrefixSchedule ++ ConditionalSchedule,

    Classification = lists:foldl(fun({Type, Preconditions}, Dict0) ->
        Result = lists:foldl(fun(Precondition, Acc) ->
            case Precondition of 
                {PreconditionType, N} ->
                    Num = length(lists:filter(fun(T) -> T =:= PreconditionType end, DerivedSchedule)),
                    % io:format("=> * found ~p messages of type ~p~n", [Num, PreconditionType]),
                    Acc andalso Num >= N;
                true ->
                    Acc andalso true
            end
        end, true, Preconditions),

        % io:format("=> type: ~p, preconditions: ~p~n", [Type, Result]),
        dict:store(Type, Result, Dict0)
    end, dict:new(), dict:to_list(Annotations)),

    % io:format("classification: ~p~n", [dict:to_list(Classification)]),

    Classification.

%% @private
ensure_preconditions_present(Causality, Annotations) ->
    %% Get all messages that are the result of other messages.
    CausalMessages = lists:foldl(fun({_, Messages}, Acc) ->
        Messages ++ Acc
    end, [], dict:to_list(Causality)),

    lists:foldl(fun(Message, Acc) ->
        case lists:keymember(Message, 1, dict:to_list(Annotations)) of 
            true ->
                Acc andalso true;
            false ->
                io:format("Precondition not found for message type: ~p~n", [Message]),
                Acc andalso false
        end
    end, true, CausalMessages).

%% @private
message_types(TraceLines) ->
    %% Filter the trace into message trace lines.
    lists:flatmap(fun({Type, Message}) ->
        case Type =:= pre_interposition_fun of 
            true ->
                [message_type(Message)];
            false ->
                []
        end
    end, TraceLines).

%% @privae
execute_schedule(PreloadOmissionFile, ReplayTraceFile, TraceFile, TraceLines, {Iteration, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}, ClassificationsExplored0, NewTraces0) ->
    Classification = dict:to_list(ClassifySchedule),

    case ScheduleValid of 
        false ->
            invalid;
        true ->
            case lists:member(Classification, ClassificationsExplored0) of 
                true ->
                    pruned;
                false ->
                    %% Write out a new omission file from the previously used trace.
                    io:format("Writing out new preload omissions file!~n", []),
                    {ok, PreloadOmissionIo} = file:open(PreloadOmissionFile, [write, {encoding, utf8}]),
                    [io:format(PreloadOmissionIo, "~p.~n", [O]) || O <- [Omissions]],
                    ok = file:close(PreloadOmissionIo),

                    %% Write out replay trace.
                    io:format("Writing out new replay trace file!~n", []),
                    {ok, TraceIo} = file:open(ReplayTraceFile, [write, {encoding, utf8}]),
                    [io:format(TraceIo, "~p.~n", [TraceLine]) || TraceLine <- [FinalTraceLines]],
                    ok = file:close(TraceIo),

                    ClassificationsExplored = ClassificationsExplored0 ++ [Classification],
                    io:format("=> Classification for this test: ~p~n", [Classification]),

                    %% Run the trace.
                    Command = "rm -rf priv/lager; IMPLEMENTATION_MODULE=" ++ os:getenv("IMPLEMENTATION_MODULE") ++ " SHRINKING=true REPLAY=true PRELOAD_OMISSIONS_FILE=" ++ PreloadOmissionFile ++ " REPLAY_TRACE_FILE=" ++ ReplayTraceFile ++ " TRACE_FILE=" ++ TraceFile ++ " ./rebar3 proper --retry | tee /tmp/partisan.output",
                    io:format("Executing command for iteration ~p:~n", [Iteration]),
                    io:format("~p~n", [Command]),
                    Output = os:cmd(Command),

                    ClassificationsExplored = ClassificationsExplored0 ++ [Classification],
                    io:format("=> Classification now: ~p~n", [ClassificationsExplored]),

                    %% New trace?
                    {ok, NewTraceLines} = file:consult(TraceFile),
                    io:format("=> Executed test and test contained ~p lines compared to original trace with ~p lines.~n", [length(hd(NewTraceLines)), length(TraceLines)]),

                    MessageTypesFromTraceLines = message_types(TraceLines),
                    io:format("=> MessageTypesFromTraceLines: ~p~n", [MessageTypesFromTraceLines]),
                    MessageTypesFromNewTraceLines = message_types(hd(NewTraceLines)),
                    io:format("=> MessageTypesFromNewTraceLines: ~p~n", [MessageTypesFromNewTraceLines]),

                    Difference = lists:usort(MessageTypesFromNewTraceLines) -- lists:usort(MessageTypesFromTraceLines),
                    io:format("=> Difference: ~p~n", [Difference]),

                    NewTraces = case length(Difference) > 0 of
                        true ->
                            io:format("=> * Adding trace to list to explore.~n", []),

                            case lists:keymember(Difference, 3, NewTraces0) of 
                                true ->
                                    io:format("=> * Similar trace already exists, ignoring!~n", []),
                                    NewTraces0;
                                false ->
                                    NewTraces0 ++ [{hd(NewTraceLines), Omissions, Difference}]
                            end;
                        false ->
                            NewTraces0
                    end,

                    %% Store set of omissions as omissions that didn't invalidate the execution.
                    case string:find(Output, "{postcondition,false}") of 
                        nomatch ->
                            %% This passed.
                            io:format("Test passed.~n", []),

                            %% Insert result into the ETS table.
                            true = ets:insert(?RESULTS, {Iteration, {Iteration, FinalTraceLines, Omissions, true}}),

                            {passed, ClassificationsExplored, NewTraces};
                        _ ->
                            %% This failed.
                            io:format("Test FAILED!~n", []),
                            % io:format("Failing test contained the following omitted mesage types: ~p~n", [Omissions]),

                            case os:getenv("EXIT_ON_COUNTEREXAMPLE") of 
                                false ->
                                    ok;
                                "false" ->
                                    ok;
                                _Other ->
                                    exit({error, counterexample_found})
                            end,

                            %% Insert result into the ETS table.
                            true = ets:insert(?RESULTS, {Iteration, {Iteration, FinalTraceLines, Omissions, false}}),

                            {failed, ClassificationsExplored, NewTraces}
                    end
            end
    end.