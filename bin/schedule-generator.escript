#!/usr/bin/env escript
%%! -pa ./_build/test/lib/partisan/ebin -Wall

main(_Args) ->
    TraceFile = "/tmp/partisan-latest.trace",

    %% Open the trace file.
    {ok, TraceLines} = file:consult(TraceFile),

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
    end, hd(TraceLines)),
    io:format("Number of items in message trace: ~p~n",
              [length(MessageTraceLines)]),

    %% Generate the powerset of tracelines.
    MessageTraceLinesPowerset = powerset(MessageTraceLines),

    %% For each powerset, generate a trace that only allows those messages.
    lists:foreach(fun(Set) ->
        %% TODO: Allow non-pre-interposition through.
        ConcreteTrace = lists:flatmap(fun({T, M} = Full) -> 
            case T of 
                pre_interposition_fun ->
                    {_TracingNode, InterpositionType, _OriginNode, _MessagePayload} = M,
                    
                    case InterpositionType of
                        forward_message ->
                            case lists:member(Full, Set) of 
                                %% If powerset contains message, allow.
                                %% This means that the message should be allowed during this execution.
                                true ->
                                    [{T, M}];
                                %% If powerset doesn't contain the message, remap message to an omission.
                                %% This is the omission we want to test.
                                false ->
                                    [{{omit, T}, M}]
                            end;
                        _ ->
                            %% Pass anything but a forward_message through.
                            %% We only test partitions on the sender side.
                            [{T, M}]
                    end;
                _ ->
                    %% Pass non-pre-interposition-funs through.
                    %% Other commands are not subject to fault-injection.
                    [{T, M}]
            end
        end, hd(TraceLines)),

        io:format("Materializing concrete trace: trace is ~p lines~n", [length(ConcreteTrace)]),

        % Print concrete trace.
        lists:foreach(fun(X) -> 
            io:format("~p~n", [X])
        end, ConcreteTrace),

        %% TODO: Execute trace.

        %% TODO: Perform dynamic partial order reduction, if possible.

        ok
    %% TODO: Remove hd when debugged.
    end, [hd(MessageTraceLinesPowerset)]),

    ok.

%% @doc Generate the powerset of messages.
powerset([]) -> 
    [[]];

powerset([H|T]) -> 
    PT = powerset(T),
    [ [H|X] || X <- PT ] ++ PT.