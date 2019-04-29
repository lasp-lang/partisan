%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Christopher S. Meiklejohn.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%%

-module(filibuster_SUITE).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

%% Counterexamples
-define(COUNTEREXAMPLE_FILE, "rebar3_proper-counterexamples.consult").

%% General test configuration
-define(ETS, prop_partisan).
-define(CLUSTER_NODES, true).
-define(MANAGER, partisan_pluggable_peer_service_manager).

%% Model checker configuration
-define(RESULTS, results).
-define(SCHEDULES, schedules).

%% Helpers.
-define(NAME, fun(Name) -> [{_, NodeName}] = ets:lookup(?ETS, Name), NodeName end).

%% Debug.
-define(DEBUG, true).
-define(INITIAL_STATE_DEBUG, false).
-define(PRECONDITION_DEBUG, true).
-define(POSTCONDITION_DEBUG, true).

%% Partisan connection and forwarding settings.
-define(EGRESS_DELAY, 0).                           %% How many milliseconds to delay outgoing messages?
-define(INGRESS_DELAY, 0).                          %% How many millisconds to delay incoming messages?
-define(VNODE_PARTITIONING, false).                 %% Should communication be partitioned by vnode identifier?

%% common_test callbacks
-export([%% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0,
         groups/0,
         init_per_group/2]).

%% tests
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

%% ==================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(_Config) ->
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, Config) ->
    ct:pal("Beginning test case ~p", [Case]),

    [{hash, erlang:phash2({Case, Config})}|Config].

end_per_testcase(Case, _Config) ->
    ct:pal("Ending test case ~p", [Case]),

    _Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

all() ->
    [model_checker_test].

groups() ->
    [].

%% ===================================================================
%% Tests.
%% ===================================================================

model_checker_test(_Config) ->
    lager:info("~p: starting nodes!", [?MODULE]),

    %% Get self.
    Self = self(),

    %% Special configuration for the cluster.
    Config = [{partisan_dispatch, true},
              {parallelism, ?PARALLELISM},
              {tls, false},
              {binary_padding, false},
              {channels, ?CHANNELS},
              {vnode_partitioning, ?VNODE_PARTITIONING},
              {causal_labels, ?CAUSAL_LABELS},
              {pid_encoding, false},
              {sync_join, false},
              {forward_options, []},
              {broadcast, false},
              {disterl, false},
              {hash, undefined},
              {egress_delay, ?EGRESS_DELAY},
              {ingress_delay, ?INGRESS_DELAY},
              {membership_strategy_tracing, false},
              {periodic_enabled, false},
              {distance_enabled, false},
              {replaying, true},
              {shrinking, true},
              {disable_fast_forward, true},
              {disable_fast_receive, true},
              {membership_strategy, partisan_full_membership_strategy}],

    %% Cluster and start options.
    Options = [{partisan_peer_service_manager, ?MANAGER}, 
                {num_nodes, ?TEST_NUM_NODES}, 
                {cluster_nodes, ?CLUSTER_NODES}],

    %% Initialize a cluster.
    Nodes = ?SUPPORT:start(prop_partisan, Config, Options),

    %% Create an ets table for test configuration.
    ?ETS = ets:new(?ETS, [named_table]),

    %% Insert all nodes into group for all nodes.
    true = ets:insert(?ETS, {nodes, Nodes}),

    %% Insert name to node mappings for lookup.
    %% Caveat, because sometimes we won't know ahead of time what FQDN the node will
    %% come online with when using partisan.
    lists:foreach(fun({Name, Node}) ->
        true = ets:insert(?ETS, {Name, Node})
    end, Nodes),

    debug("~p started nodes: ~p", [Self, Nodes]),

    %% Open up the counterexample file.
    {ok, Base} = file:get_cwd(),
    CounterexampleFilePath = filename:join([Base, "../../", ?COUNTEREXAMPLE_FILE]),
    case file:consult(CounterexampleFilePath) of
        {ok, [Counterexample]} ->
            %% Test execution begins here.

            %% Initialize the checker.
            TraceFile = "/tmp/partisan-latest.trace",
            ReplayTraceFile = "/tmp/partisan-replay.trace",
            CounterexampleConsultFile = "/tmp/partisan-counterexample.consult",
            RebarCounterexampleConsultFile = CounterexampleFilePath,
            PreloadOmissionFile = "/tmp/partisan-preload.trace",
            init(Nodes, Counterexample, TraceFile, ReplayTraceFile, CounterexampleConsultFile, RebarCounterexampleConsultFile, PreloadOmissionFile),

            debug("finished with test, beginning teardown...", []),

            %% Teardown begins here.

            %% Get list of nodes that were started at the start
            %% of the test.
            [{nodes, Nodes}] = ets:lookup(?ETS, nodes),

            %% Stop nodes.
            ?SUPPORT:stop(Nodes),

            %% Delete the table.
            ets:delete(?ETS),

            ok;
        {error, _} ->
            debug("no counterexamples to run.", []),
            ok
    end.

%% ===================================================================
%% Test Runner
%% ===================================================================

execute(Nodes, {M, F, A}) ->
    %% Ensure replaying option is set.
    lists:foreach(fun({ShortName, _}) ->
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [replaying, true])
    end, Nodes),

    %% Ensure shrinking option is set.
    lists:foreach(fun({ShortName, _}) ->
        ok = rpc:call(?NAME(ShortName), partisan_config, set, [shrinking, true])
    end, Nodes),

    %% Deterministically seed the random number generator.
    partisan_config:seed(),

    %% Start tracing infrastructure.
    partisan_trace_orchestrator:start_link(),

    %% Reset trace.
    ok = partisan_trace_orchestrator:reset(),

    %% Start tracing.
    ok = partisan_trace_orchestrator:enable(),

    %% Set replay.
    partisan_config:set(replaying, true),

    %% Set shrinking.
    partisan_config:set(shrinking, true),

    %% Perform preloads.
    ok = partisan_trace_orchestrator:perform_preloads(Nodes),

    %% Identify trace.
    TraceRandomNumber = rand:uniform(100000),
    %% lager:info("~p: trace random generated: ~p", [?MODULE, TraceRandomNumber]),
    TraceIdentifier = atom_to_list(system_model()) ++ "_" ++ integer_to_list(TraceRandomNumber),
    ok = partisan_trace_orchestrator:identify(TraceIdentifier),

    %% Add send and receive pre-interposition functions to enforce message ordering.
    PreInterpositionFun = fun({Type, OriginNode, OriginalMessage}) ->
        %% TODO: This needs to be fixed: replay and trace need to be done
        %% atomically otherwise processes will race to write trace entry when
        %% they are unblocked from retry: this means that under replay the trace
        %% file might generate small permutations of messages which means it's
        %% technically not the same trace.

        %% Under replay ensure they match the trace order (but only for pre-interposition messages).
        ok = partisan_trace_orchestrator:replay(pre_interposition_fun, {node(), Type, OriginNode, OriginalMessage}),

        %% Record message incoming and outgoing messages.
        ok = partisan_trace_orchestrator:trace(pre_interposition_fun, {node(), Type, OriginNode, OriginalMessage}),

        ok
    end, 
    lists:foreach(fun({_Name, Node}) ->
        rpc:call(Node, ?MANAGER, add_pre_interposition_fun, ['$tracing', PreInterpositionFun])
    end, Nodes),

    %% Add send and receive post-interposition functions to perform tracing.
    PostInterpositionFun = fun({Type, OriginNode, OriginalMessage}, {Type, OriginNode, RewrittenMessage}) ->
        %% Record outgoing message after transformation.
        ok = partisan_trace_orchestrator:trace(post_interposition_fun, {node(), OriginNode, Type, OriginalMessage, RewrittenMessage})
    end, 
    lists:foreach(fun({_Name, Node}) ->
        rpc:call(Node, ?MANAGER, add_post_interposition_fun, ['$tracing', PostInterpositionFun])
    end, Nodes),

    %% Enable tracing.
    lists:foreach(fun({_Name, Node}) ->
        rpc:call(Node, partisan_config, set, [tracing, false])
    end, Nodes),

    %% Run proper.
    Result = proper:check(M:F(), A, []),
    debug("execute result: ~p", [Result]),

    %% Print trace.
    partisan_trace_orchestrator:print(),

    %% Stop tracing infrastructure.
    partisan_trace_orchestrator:stop(),
    
    Result.

%% ===================================================================
%% Internal Functions
%% ===================================================================

debug(Line, Args) ->
    case ?DEBUG of
        true ->
            lager:info("~p: " ++ Line, [?MODULE] ++ Args);
        false ->
            ok
    end.

%% @private
system_model() ->
    case os:getenv("SYSTEM_MODEL") of
        false ->
            exit({error, no_system_model_specified});
        SystemModel ->
            list_to_atom(SystemModel)
    end.

%% ===================================================================
%% Model Checker
%% ===================================================================

init(Nodes, _Counterexample, TraceFile, ReplayTraceFile, CounterexampleConsultFile, RebarCounterexampleConsultFile, PreloadOmissionFile) ->
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

    {ok, Base} = file:get_cwd(),
    BasePath = Base ++ "/../../../../",

    %% Open the causality file.
    CausalityFile = BasePath ++ "/analysis/partisan-causality-" ++ ModuleString,

    case filelib:is_file(CausalityFile) of 
        false ->
            debug("Causality file doesn't exist: ~p~n", [CausalityFile]),
            ct:fail({notfound, CausalityFile});
        true ->
            ok
    end,

    {ok, [RawCausality]} = file:consult(CausalityFile),
    Causality = dict:from_list(RawCausality),
    debug("Causality loaded: ~p~n", [dict:to_list(Causality)]),

    %% Open the annotations file.
    AnnotationsFile = BasePath ++ "/annotations/partisan-annotations-" ++ ModuleString,

    case filelib:is_file(AnnotationsFile) of 
        false ->
            debug("Annotations file doesn't exist: ~p~n", [AnnotationsFile]),
            ct:fail({notfound, AnnotationsFile});
        true ->
            ok
    end,

    {ok, [RawAnnotations]} = file:consult(AnnotationsFile),
    debug("Raw annotations loaded: ~p~n", [RawAnnotations]),
    AllAnnotations = dict:from_list(RawAnnotations),
    debug("Annotations loaded: ~p~n", [dict:to_list(AllAnnotations)]),

    {ok, RawCausalityAnnotations} = dict:find(causality, AllAnnotations),
    debug("Raw causality annotations loaded: ~p~n", [RawCausalityAnnotations]),

    CausalityAnnotations = dict:from_list(RawCausalityAnnotations),
    debug("Causality annotations loaded: ~p~n", [dict:to_list(CausalityAnnotations)]),

    {ok, BackgroundAnnotations} = dict:find(background, AllAnnotations),
    debug("Background annotations loaded: ~p~n", [BackgroundAnnotations]),

    %% Check that we have the necessary preconditions.
    PreconditionsPresent = ensure_preconditions_present(Causality, CausalityAnnotations, BackgroundAnnotations),
    debug("All preconditions present: ~p~n", [PreconditionsPresent]),
    case PreconditionsPresent of 
        true ->
            ok;
        false ->
            exit({error, not_all_preconditions_present})
    end,

    %% Open the counterexample consult file:
    %% - we make an assumption that there will only be a single counterexample here.
    {ok, [{TestModule, TestFunction, [TestCommands]}]} = file:consult(CounterexampleConsultFile),

    debug("Loading commands...~n", []),
    [debug(" ~p.~n", [TestCommand]) || TestCommand <- TestCommands],

    %% Drop last command -- forced failure.
    TestFinalCommands = lists:reverse(tl(lists:reverse(TestCommands))),

    debug("Rewritten commands...~n", []),
    [debug(" ~p.~n", [TestFinalCommand]) || TestFinalCommand <- TestFinalCommands],

    %% Write the schedule out.
    {ok, CounterexampleIo} = file:open(RebarCounterexampleConsultFile, [write, {encoding, utf8}]),
    io:format(CounterexampleIo, "~p.~n", [{TestModule, TestFunction, [TestFinalCommands]}]),
    ok = file:close(CounterexampleIo),

    %% Get counterexample.
    {ok, Base} = file:get_cwd(),
    CounterexampleFilePath = filename:join([Base, "../../", ?COUNTEREXAMPLE_FILE]),
    Counterexample = case file:consult(CounterexampleFilePath) of
        {ok, [C]} ->
            C;
        {error, _} ->
            ct:fail("no counterexamples to run.", []),
            error
    end,

    %% Remove forced failure from the trace.
    TraceLinesWithoutFailure = lists:filter(fun(Command) ->
        case Command of 
            {_, {_, [forced_failure]}} ->
                debug("Removing command from trace: ~p~n", [Command]),
                false;
            _ ->
                true
        end
    end, hd(TraceLines)),

    %% Perform recursive analysis of the traces.
    PreviousIteration = 1,
    PreviousClassificationsExplored = [],
    analyze(StartTime, Nodes, Counterexample, 1, 0, 0, 0, PreloadOmissionFile, ReplayTraceFile, TraceFile, Causality, CausalityAnnotations, BackgroundAnnotations, PreviousIteration, PreviousClassificationsExplored, [{TraceLinesWithoutFailure, [], [], []}]),

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

    debug("Test started: ~p~n", [StartTime]),
    debug("Test ended: ~p~n", [EndTime]),
    debug("Test took: ~p seconds.~n", [DifferenceSec]),

    ok.

filter_trace_lines(_, TraceLines, BackgroundAnnotations) ->
    %% Filter the trace into message trace lines.
    MessageTraceLines = lists:filter(fun({Type, Message}) ->
        case Type =:= pre_interposition_fun of 
            true ->
                {_TracingNode, InterpositionType, _OriginNode, _MessagePayload} = Message,

                case InterpositionType of 
                    forward_message ->
                        MessageType = message_type(Message),
                        case lists:member(element(2, MessageType), BackgroundAnnotations) of 
                            true ->
                                false;
                            false ->
                                true
                        end;
                    _ -> 
                        false
                end;
            false ->
                false
        end
    end, TraceLines),

    % debug("Number of items in message trace: ~p~n", [length(MessageTraceLines)]),

    MessageTraceLines.

%% @private
analyze(_StartTime, _Nodes, _Counterexample, _Pass, _NumPassed0, _NumFailed0, _NumPruned0, _PreloadOmissionFile, _ReplayTraceFile, _TraceFile, _Causality, _CausalityAnnotations, _BackgroundAnnotations, _PreviousIteration, _PreviousClassificationsExplored, []) ->
    ok;

analyze(StartTime, Nodes, Counterexample, Pass, NumPassed0, NumFailed0, NumPruned0, PreloadOmissionFile, ReplayTraceFile, TraceFile, Causality, CausalityAnnotations, BackgroundAnnotations, PreviousIteration, PreviousClassificationsExplored, [{TraceLines, BaseOmissions, DifferenceTypes, DifferenceTraceLines}|RestTraceLines]) ->
    debug("Beginning analyze pass: ~p with previous iteration: ~p, traces remaining: ~p~n", [Pass, PreviousIteration, length(RestTraceLines)]),

    %% Generate the powerset of tracelines.
    {MessageTraceLines, MessageTraceLinesPowerset} = case length(DifferenceTraceLines) > 0 of 
        true ->
            %% Generate powerset offset to reduce redundant schedule exploration.
            % debug("length(DifferenceTraceLines): ~p~n", [length(DifferenceTraceLines)]),
            % debug("length(DifferenceTypes): ~p~n", [length(DifferenceTypes)]),
            debug("Using difference types for powerset generation: ~p~n", [DifferenceTypes]),

            %% Use message trace.
            FilteredTraceLines = filter_trace_lines(implementation_module(), TraceLines, BackgroundAnnotations),

            %% Generate powerset using *only* the new messages.
            FilteredDifferenceTraceLines = filter_trace_lines(implementation_module(), DifferenceTraceLines, BackgroundAnnotations),
            {Time, L} = timer:tc(fun() -> powerset(FilteredDifferenceTraceLines) end),

            % debug("FilteredDifferenceTraceLines: ~p~n", [FilteredDifferenceTraceLines]),
            % debug("DistanceTraceLines: ~p~n", [DifferenceTraceLines]),
            debug("Number of messages in DifferenceTraceLines: ~p~n", [length(DifferenceTraceLines)]),
            debug("Number of message sets in powerset: ~p~n", [length(L)]),
            debug("Powerset generation took: ~p~n", [Time]),

            %% Prefix the trace with base omissions.
            PrefixedPowersetTraceLines = lists:map(fun(X) -> BaseOmissions ++ X end, L),

            %% TODO: Remove me.
            FinalPowerset = [lists:nth(1, PrefixedPowersetTraceLines)] ++ [lists:nth(length(PrefixedPowersetTraceLines), PrefixedPowersetTraceLines)],
            % FinalPowerset = PrefixedPowersetTraceLines,

            {FilteredTraceLines, FinalPowerset};
        false ->
            %% Generate all.
            FilteredTraceLines = filter_trace_lines(implementation_module(), TraceLines, BackgroundAnnotations),
            debug("Beginning powerset generation, length(FilteredTraceLines): ~p~n", [length(FilteredTraceLines)]),
            {Time, FinalPowerset} = timer:tc(fun() -> powerset(FilteredTraceLines) end),
            debug("Number of message sets in powerset: ~p~n", [length(FinalPowerset)]),
            debug("Powerset generation took: ~p~n", [Time]),
            {FilteredTraceLines, FinalPowerset}
    end,

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
        % debug("~n", []),
        % debug("Number of message trace line: ~p~n", [length(MessageTraceLines)]),
        % debug("Number of omission lines: ~p~n", [length(Omissions)]),
        % debug("Difference lines: ~p~n", [length(MessageTraceLines -- Omissions)]),

        %% Validate the causality earlier, using a speculative validation to reduce
        %% the cost of schedule materialization.
        CandidateTrace = MessageTraceLines -- Omissions,
        EarlyCausality = schedule_valid_causality(Causality, CandidateTrace),

        EarlyClassification0 = classify_schedule(3, CausalityAnnotations, CandidateTrace),
        EarlyClassification = dict:to_list(EarlyClassification0),

        %% Is this schedule valid for these omissions?
        OmissionsSet = sets:from_list(Omissions),
        BaseOmissionsSet = sets:from_list(BaseOmissions),

        EarlyOmissions = case sets:is_subset(BaseOmissionsSet, OmissionsSet) orelse BaseOmissionsSet =:= OmissionsSet of
            true ->
                true;
            false ->
                false
        end,

        % debug("OmissionTypes: ~p~n", [message_types(Omissions)]),
        % DifferenceOmissionTypes = message_types(Omissions) -- message_types(BaseOmissions),
        % debug("DifferenceOmissionTypes: ~p~n", [DifferenceOmissionTypes]),

        case EarlyOmissions andalso EarlyCausality andalso not lists:member(EarlyClassification, GenClassificationsExplored0) of
            true ->
                debug("~n", []),
                % debug("Entering generation pass.~n", []),
                % debug("length(CandidiateTrace): ~p~n", [length(CandidateTrace)]),
                % debug("EarlyCausality: ~p~n", [EarlyCausality]),
                % debug("EarlyClassification: ~p~n", [EarlyClassification]),
                % debug("EarlyOmissions: ~p~n", [EarlyOmissions]),

                %% Generate a new trace.
                debug("Generating new trace based on message omissions (~p omissions, iteration ~p): ~n", [length(Omissions), GenIteration0]),

                {FinalTraceLines, _, _, PrefixMessageTypes, OmittedMessageTypes, ConditionalMessageTypes, _, _BackgroundOmissions} = lists:foldl(fun({Type, Message} = Line, {FinalTrace0, FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0, OmittedMessageTypes0, ConditionalMessageTypes0, FaultedNodes0, BackgroundOmissions0}) ->
                    case Type =:= pre_interposition_fun of 
                        true ->
                            {TracingNode, InterpositionType, OriginNode, MessagePayload} = Message,

                            % debug("Looking at message type: ~p~n", [message_type(Message)]),

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
                                                    %% Another fault: this should be added to the list of faulted nodes.
                                                    FaultedNodes = update_faulted_nodes(TraceLines, {Type, Message}, Omissions, BackgroundAnnotations, FaultedNodes0),

                                                    %% Generate matching receive omission.
                                                    ReceiveOmission = {Type, {OriginNode, receive_message, TracingNode, {forward_message, implementation_module(), MessagePayload}}},

                                                    {FinalTrace0, FaultsStarted0, AdditionalOmissions0 ++ [ReceiveOmission], PrefixMessageTypes0, OmittedMessageTypes0 ++ [message_type(Message)], ConditionalMessageTypes0, FaultedNodes, BackgroundOmissions0};
                                                false ->
                                                    % debug("found a message after the faults started of type: ~p not omission.~n", [message_type(Message)]),

                                                    %% If the node is faulted, but the message isn't a specific omission, then we probably have a background message that also needs to be omitted.
                                                    case lists:member(element(2, message_type(Message)), BackgroundAnnotations) of
                                                        true ->
                                                            % debug("=> found background message from ~p to node ~p~n", [TracingNode, OriginNode]),
                                                            % debug("=> faulted nodes: ~p~n", [dict:to_list(FaultedNodes0)]),

                                                            case dict:find(TracingNode, FaultedNodes0) of 
                                                                {ok, true} ->
                                                                    % debug("Found background message for FAULTED node: ~p~n", [TracingNode]),

                                                                    %% Generate receive omission.
                                                                    ReceiveOmission = {Type, {OriginNode, receive_message, TracingNode, {forward_message, implementation_module(), MessagePayload}}},

                                                                    %% Add additional background omissions.
                                                                    {FinalTrace0, FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0, OmittedMessageTypes0, ConditionalMessageTypes0 ++ [message_type(Message)], FaultedNodes0, BackgroundOmissions0 ++ [Line, ReceiveOmission]};
                                                                _ ->
                                                                    {FinalTrace0, FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0, OmittedMessageTypes0, ConditionalMessageTypes0 ++ [message_type(Message)], FaultedNodes0, BackgroundOmissions0}
                                                            end;
                                                        false ->
                                                            {FinalTrace0, FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0, OmittedMessageTypes0, ConditionalMessageTypes0 ++ [message_type(Message)], FaultedNodes0, BackgroundOmissions0}
                                                    end
                                            end;
                                        receive_message ->
                                            case lists:member(Line, AdditionalOmissions0) of 
                                                true ->
                                                    {FinalTrace0, FaultsStarted0, AdditionalOmissions0 -- [Line], PrefixMessageTypes0, OmittedMessageTypes0 ++ [message_type(Message)], ConditionalMessageTypes0, FaultedNodes0, BackgroundOmissions0};
                                                false ->
                                                    {FinalTrace0, FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0, OmittedMessageTypes0, ConditionalMessageTypes0 ++ [message_type(Message)], FaultedNodes0, BackgroundOmissions0}
                                            end
                                    end;
                                false ->
                                    %% Otherwise, find just the targeted commands to remove.
                                    case InterpositionType of 
                                        forward_message ->
                                            case lists:member(Line, Omissions) of 
                                                true ->
                                                    % debug("fault started with line: ~p~n", [Line]),

                                                    %% First, fault.  This should be added to the list of faulted nodes.
                                                    FaultedNodes = update_faulted_nodes(TraceLines, {Type, Message}, Omissions, BackgroundAnnotations, FaultedNodes0),

                                                    %% Write out the reverse side of the receive omission.
                                                    ReceiveOmission = {Type, {OriginNode, receive_message, TracingNode, {forward_message, implementation_module(), MessagePayload}}},

                                                    {FinalTrace0, true, AdditionalOmissions0 ++ [ReceiveOmission], PrefixMessageTypes0, OmittedMessageTypes0 ++ [message_type(Message)], ConditionalMessageTypes0, FaultedNodes, BackgroundOmissions0};
                                                false ->
                                                    case lists:member(element(2, message_type(Message)), BackgroundAnnotations) of 
                                                        true ->
                                                            % debug("forward_message message outside of fault schedule, not adding to replay trace: ~p~n", [message_type(Message)]),
                                                            {FinalTrace0, FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0 ++ [message_type(Message)], OmittedMessageTypes0, ConditionalMessageTypes0, FaultedNodes0, BackgroundOmissions0};
                                                        false ->
                                                            {FinalTrace0 ++ [Line], FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0 ++ [message_type(Message)], OmittedMessageTypes0, ConditionalMessageTypes0, FaultedNodes0, BackgroundOmissions0}
                                                    end
                                            end;
                                        receive_message -> 
                                            case lists:member(Line, AdditionalOmissions0) of 
                                                true ->
                                                    {FinalTrace0, FaultsStarted0, AdditionalOmissions0 -- [Line], PrefixMessageTypes0, OmittedMessageTypes0 ++ [message_type(Message)], ConditionalMessageTypes0, FaultedNodes0, BackgroundOmissions0};
                                                false ->
                                                    case lists:member(element(2, message_type(Message)), BackgroundAnnotations) of 
                                                        true ->
                                                            % debug("receive_message message outside of fault schedule, not adding to replay trace: ~p~n", [message_type(Message)]),
                                                            {FinalTrace0, FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0 ++ [message_type(Message)], OmittedMessageTypes0, ConditionalMessageTypes0, FaultedNodes0, BackgroundOmissions0};
                                                        false ->
                                                            {FinalTrace0 ++ [Line], FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0 ++ [message_type(Message)], OmittedMessageTypes0, ConditionalMessageTypes0, FaultedNodes0, BackgroundOmissions0}
                                                    end
                                            end
                                    end
                            end;
                        false ->
                            {FinalTrace0 ++ [Line], FaultsStarted0, AdditionalOmissions0, PrefixMessageTypes0, OmittedMessageTypes0, ConditionalMessageTypes0, FaultedNodes0, BackgroundOmissions0}
                    end
                end, {[], false, [], [], [], [], dict:new(), []}, TraceLines),

                % debug("PrefixMessageTypes: ~p~n", [PrefixMessageTypes]),
                % debug("OmittedMessageTypes: ~p~n", [OmittedMessageTypes]),
                % debug("ConditionalMessageTypes: ~p~n", [ConditionalMessageTypes]),
                % debug("length(MessageTypes): ~p~n", [length(PrefixMessageTypes ++ OmittedMessageTypes ++ ConditionalMessageTypes)]),

                %% Is this schedule valid for these omissions?
                ScheduleValidOmissions = case sets:is_subset(BaseOmissionsSet, OmissionsSet) orelse BaseOmissionsSet =:= OmissionsSet of
                    true ->
                        true;
                    false ->
                        false
                end,

                ScheduleValidCausality = schedule_valid_causality(Causality, PrefixMessageTypes, OmittedMessageTypes, ConditionalMessageTypes),

                % debug("ScheduleValidOmissions: ~p~n", [ScheduleValidOmissions]),
                % debug("ScheduleValidCausality: ~p~n", [ScheduleValidCausality]),

                ScheduleValid = ScheduleValidCausality andalso ScheduleValidOmissions,

                case ScheduleValid of
                    true ->
                        % debug("schedule_valid_omissions: ~p~n", [ScheduleValidOmissions]),
                        % debug("schedule_valid_causality: ~p~n", [ScheduleValidCausality]),
                        % debug("schedule_valid: ~p~n", [ScheduleValid]),
                        ok;
                    false ->
                        ok
                end,

                ClassifySchedule = classify_schedule(3, CausalityAnnotations, PrefixMessageTypes, OmittedMessageTypes, ConditionalMessageTypes),
                % debug("Classification: ~p~n", [dict:to_list(ClassifySchedule)]),

                % debug("=> length(BackgroundOmissions): ~p~n", [length(BackgroundOmissions)]),
                % debug("=> length(lists:usort(BackgroundOmissions)): ~p~n", [length(lists:usort(BackgroundOmissions))]),

                case os:getenv("PRELOAD_SCHEDULES") of 
                    "true" ->
                        debug("Inserting into generated schedule list.~n", []),

                        %% Store generated schedule.
                        true = ets:insert(?SCHEDULES, {GenIteration0, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}),

                        {GenIteration0 + 1, GenNumPassed0, GenNumFailed0, GetNumPruned0, GenClassificationsExplored0, GenAdditionalTraces0};
                    _Other ->
                        case GenIteration0 rem 10000 == 0 of 
                            true ->
                                debug(".", []);
                            false ->
                                ok
                        end,

                        case execute_schedule(StartTime, Nodes, Counterexample, PreloadOmissionFile, ReplayTraceFile, TraceFile, TraceLines, {GenIteration0, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}, GenClassificationsExplored0, GenAdditionalTraces0) of
                            pruned ->
                                debug("Schedule pruned.~n", []),
                                {GenIteration0 + 1, GenNumPassed0, GenNumFailed0, GetNumPruned0 + 1, GenClassificationsExplored0, GenAdditionalTraces0};
                            {passed, ClassificationsExplored, NewTraces} ->
                                debug("Schedule passed.~n", []),
                                {GenIteration0 + 1, GenNumPassed0 + 1, GenNumFailed0, GetNumPruned0, ClassificationsExplored, NewTraces};
                            {failed, ClassificationsExplored, NewTraces} ->
                                debug("Schedule failed.~n", []),
                                {GenIteration0 + 1, GenNumPassed0, GenNumFailed0 + 1, GetNumPruned0, ClassificationsExplored, NewTraces};
                            invalid ->
                                debug("Schedule invalid.~n", []),
                                {GenIteration0 + 1, GenNumPassed0, GenNumFailed0, GetNumPruned0 + 1, GenClassificationsExplored0, GenAdditionalTraces0}
                        end
                end;
            false ->
                case GenIteration0 rem 10000 == 0 of 
                    true ->
                        debug(".", []);
                    false ->
                        ok
                end,

                % debug("Pruned through early causality and annotation checks.~n", []),
                {GenIteration0 + 1, GenNumPassed0, GenNumFailed0, GetNumPruned0 + 1, GenClassificationsExplored0, GenAdditionalTraces0}
        end
    end, {PreviousIteration, NumPassed0, NumFailed0, NumPruned0, PreviousClassificationsExplored, RestTraceLines}, TracesToIterate),

    %% Run generated schedules stored in the ETS table.
    {PreloadNumPassed, PreloadNumFailed, PreloadNumPruned, PreloadClassificationsExplored, PreloadAdditionalTraces} = ets:foldl(fun({Iteration, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}, {PreloadNumPassed0, PreloadNumFailed0, PreloadNumPruned0, PreloadClassificationsExplored0, PreloadAdditionalTraces0}) ->
        case execute_schedule(StartTime, Nodes, Counterexample, PreloadOmissionFile, ReplayTraceFile, TraceFile, TraceLines, {Iteration, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}, PreloadClassificationsExplored0, PreloadAdditionalTraces0) of
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

    debug("~n", []),
    debug("Pass: ~p, Iterations: ~p - ~p, Aggregate: Passed: ~p, Failed: ~p, Pruned: ~p~n", [Pass, PreviousIteration, GenIteration, PreloadNumPassed, PreloadNumFailed, PreloadNumPruned]),

    %% Should we explore any new schedules we have discovered?
    case os:getenv("RECURSIVE") of 
        "true" ->
            analyze(StartTime, Nodes, Counterexample, Pass + 1, PreloadNumPassed, PreloadNumFailed, PreloadNumPruned, PreloadOmissionFile, ReplayTraceFile, TraceFile, Causality, CausalityAnnotations, BackgroundAnnotations, 1, [], PreloadAdditionalTraces);
        _ ->
            analyze(StartTime, Nodes, Counterexample, Pass + 1, PreloadNumPassed, PreloadNumFailed, PreloadNumPruned, PreloadOmissionFile, ReplayTraceFile, TraceFile, Causality, CausalityAnnotations, BackgroundAnnotations, GenIteration, PreloadClassificationsExplored, RestTraceLines)
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

            ActualType = case MessageType1 of 
                '$gen_cast' ->
                    CastMessage = element(2, MessagePayload),
                    element(1, CastMessage);
                _ ->
                    MessageType1
            end,

            {forward_message, ActualType};
        receive_message ->
            {forward_message, _Module, Payload} = MessagePayload,
            MessageType1 = element(1, Payload),

            ActualType = case MessageType1 of 
                '$gen_cast' ->
                    CastMessage = element(2, Payload),
                    element(1, CastMessage);
                _ ->
                    MessageType1
            end,

            {receive_message, ActualType}
    end.

%% @private
schedule_valid_causality(Causality, PrefixSchedule, _OmittedSchedule, ConditionalSchedule) ->
    DerivedSchedule = PrefixSchedule ++ ConditionalSchedule,
    % debug("derived_schedule: ~p~n", [length(DerivedSchedule)]),
    % debug("prefix_schedule: ~p~n", [length(PrefixSchedule)]),
    % debug("omitted_schedule: ~p~n", [length(OmittedSchedule)]),
    % debug("conditional_schedule: ~p~n", [length(ConditionalSchedule)]),

    RequirementsMet = lists:foldl(fun(Type, Acc) ->
        % debug("=> Type: ~p~n", [Type]),

        All = lists:foldl(fun({K, V}, Acc1) ->
            % debug("=> V: ~p~n", [V]),

            case lists:member(Type, V) of 
                true ->
                    % debug("=> Presence of ~p requires: ~p~n", [Type, K]),

                    case lists:member(K, DerivedSchedule) of 
                        true ->
                            % debug("=> Present!~n", []),
                            true andalso Acc1;
                        false ->
                            % debug("=> NOT Present!~n", []),
                            false andalso Acc1
                    end;
                false ->
                    % debug("=> * fallthrough: ~p not in ~p.~n", [Type, V]),
                    true andalso Acc1
            end
        end, Acc, dict:to_list(Causality)),

        All andalso Acc
    end, true, DerivedSchedule),

    case RequirementsMet of 
        true ->
            % debug("Causality verified.~n", []),
            ok;
        false ->
            % debug("Schedule does not represent a valid schedule!~n", []),
            ok
    end,

    RequirementsMet.

%% @private
identify_minimal_witnesses() ->
    debug("Identifying witnesses...~n", []),

    %% Once we finished, we need to compute the minimals.
    Witnesses = ets:foldl(fun({_, {_Iteration, FinalTraceLines, _Omissions, Status} = Candidate}, Witnesses1) ->
        % debug("=> looking at iteration ~p~n", [Iteration]),

        %% For each trace that passes.
        case Status of 
            true ->
                %% Ensure all supertraces also pass.
                AllSupertracesPass = ets:foldl(fun({_, {_Iteration1, FinalTraceLines1, _Omissions1, Status1}}, AllSupertracesPass1) ->
                    case is_supertrace(FinalTraceLines1, FinalTraceLines) of
                        true ->
                            % debug("=> => found supertrace, status: ~p~n", [Status1]),
                            Status1 andalso AllSupertracesPass1;
                        false ->
                            AllSupertracesPass1
                    end
                end, true, ?RESULTS),

                % debug("=> ~p, all_super_traces_passing? ~p~n", [Iteration, AllSupertracesPass]),

                case AllSupertracesPass of 
                    true ->
                        % debug("=> witness found!~n", []),
                        Witnesses1 ++ [Candidate];
                    false ->
                        Witnesses1
                end;
            false ->
                %% If it didn't pass, it can't be a witness.
                Witnesses1
        end
    end, [], ?RESULTS),

    debug("Checking for minimal witnesses...~n", []),

    %% Identify minimal.
    MinimalWitnesses = lists:foldl(fun({Iteration, FinalTraceLines, _Omissions, _Status} = Witness, MinimalWitnesses) ->
        % debug("=> looking at iteration ~p~n", [Iteration]),

        %% See if any of the traces are subtraces of this.
        StillMinimal = lists:foldl(fun({Iteration1, FinalTraceLines1, _Omissions1, _Status1}, StillMinimal1) ->
            %% Is this other trace a subtrace of me?  If so, discard us
            case is_subtrace(FinalTraceLines1, FinalTraceLines) andalso Iteration =/= Iteration1 of 
                true ->
                    % debug("=> => found subtrace in iteration: ~p, status: ~p~n", [Iteration1, Status1]),
                    StillMinimal1 andalso false;
                false ->
                    StillMinimal1
            end
        end, true, Witnesses),

        % debug("=> ~p, still_minimal? ~p~n", [Iteration, StillMinimal]),

        case StillMinimal of 
            true ->
                % debug("=> minimal witness found!~n", []),
                MinimalWitnesses ++ [Witness];
            false ->
                MinimalWitnesses
        end
    end, [], Witnesses),

    %% Output.
    debug("Witnesses found: ~p~n", [length(Witnesses)]),
    debug("Minimal witnesses found: ~p~n", [length(MinimalWitnesses)]),

    ok.

%% @private
classify_schedule(_N, CausalityAnnotations, PrefixSchedule, _OmittedSchedule, ConditionalSchedule) ->
    DerivedSchedule = PrefixSchedule ++ ConditionalSchedule,

    Classification = lists:foldl(fun({Type, Preconditions}, Dict0) ->
        Result = lists:foldl(fun(Precondition, Acc) ->
            case Precondition of 
                {PreconditionType, N} ->
                    Num = length(lists:filter(fun(T) -> T =:= PreconditionType end, DerivedSchedule)),
                    % debug("=> * found ~p messages of type ~p~n", [Num, PreconditionType]),
                    Acc andalso Num >= N;
                true ->
                    Acc andalso true
            end
        end, true, Preconditions),

        % debug("=> type: ~p, preconditions: ~p~n", [Type, Result]),
        dict:store(Type, Result, Dict0)
    end, dict:new(), dict:to_list(CausalityAnnotations)),

    % debug("classification: ~p~n", [dict:to_list(Classification)]),

    Classification.

%% @private
classify_schedule(_N, CausalityAnnotations, CandidateTrace0) ->
    CandidateTrace = message_types(CandidateTrace0),

    Classification = lists:foldl(fun({Type, Preconditions}, Dict0) ->
        Result = lists:foldl(fun(Precondition, Acc) ->
            case Precondition of 
                {{_, PreconditionType}, N} ->
                    Num = length(lists:filter(fun(T) -> T =:= {forward_message, PreconditionType} end, CandidateTrace)),
                    % debug("=> * found ~p messages of type ~p~n", [Num, PreconditionType]),
                    Acc andalso Num >= N;
                true ->
                    Acc andalso true
            end
        end, true, Preconditions),

        % debug("=> type: ~p, preconditions: ~p~n", [Type, Result]),
        dict:store(Type, Result, Dict0)
    end, dict:new(), dict:to_list(CausalityAnnotations)),

    % debug("classification: ~p~n", [dict:to_list(Classification)]),

    Classification.

%% @private
ensure_preconditions_present(Causality, CausalityAnnotations, BackgroundAnnotations) ->
    %% Get all messages that are the result of other messages.
    CausalMessages = lists:foldl(fun({_, Messages}, Acc) ->
        Messages ++ Acc
    end, [], dict:to_list(Causality)),

    lists:foldl(fun(Message, Acc) ->
        case lists:keymember(Message, 1, dict:to_list(CausalityAnnotations)) of 
            true ->
                Acc andalso true;
            false ->
                case lists:member(element(2, Message), BackgroundAnnotations) of 
                    true ->
                        Acc andalso true;
                    false ->
                        debug("Precondition not found for message type: ~p~n", [Message]),
                        Acc andalso false
                end
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
execute_schedule(StartTime, Nodes, Counterexample, PreloadOmissionFile, ReplayTraceFile, TraceFile, TraceLines, {Iteration, {Omissions, FinalTraceLines, ClassifySchedule, ScheduleValid}}, ClassificationsExplored0, NewTraces0) ->
    Classification = dict:to_list(ClassifySchedule),

    case ScheduleValid of 
        false ->
            invalid;
        true ->
            case lists:member(Classification, ClassificationsExplored0) andalso pruning() of  
                true ->
                    debug("Classification: ~p~n", [Classification]),
                    debug("Classifications explored: ~p~n", [ClassificationsExplored0]),

                    pruned;
                false ->
                    %% Write out a new omission file from the previously used trace.
                    debug("Writing out new preload omissions file!~n", []),
                    {ok, PreloadOmissionIo} = file:open(PreloadOmissionFile, [write, {encoding, utf8}]),
                    [io:format(PreloadOmissionIo, "~p.~n", [O]) || O <- [Omissions]],
                    ok = file:close(PreloadOmissionIo),

                    %% Write out replay trace.
                    debug("Writing out new replay trace file!~n", []),
                    {ok, TraceIo} = file:open(ReplayTraceFile, [write, {encoding, utf8}]),
                    [io:format(TraceIo, "~p.~n", [TraceLine]) || TraceLine <- [FinalTraceLines]],
                    ok = file:close(TraceIo),

                    ClassificationsExplored = ClassificationsExplored0 ++ [Classification],
                    % debug("=> Classification for this test: ~p~n", [Classification]),

                    % MessageTypes = message_types(FinalTraceLines),
                    % debug("=> MessageTypes for this test: ~p~n", [MessageTypes]),

                    % OmissionTypes = message_types(Omissions),
                    % debug("=> OmissionTypes for this test: ~p~n", [OmissionTypes]),

                    %% Run the trace.
                    CommandFun = fun() ->
                        execute(Nodes, Counterexample)
                    end,
                    {CTime, ReturnValue} = timer:tc(CommandFun),
                    debug("Time: ~p ms. ~n", [CTime / 1000]),
                    % Command = "rm -rf priv/lager; SYSTEM_MODEL=" ++ os:getenv("SYSTEM_MODEL", "false") ++ " USE_STARTED_NODES=" ++ os:getenv("USE_STARTED_NODES", "false") ++ " RESTART_NODES=" ++ os:getenv("RESTART_NODES", "true") ++ " NOISE=" ++ os:getenv("NOISE", "false") ++ " IMPLEMENTATION_MODULE=" ++ os:getenv("IMPLEMENTATION_MODULE") ++ " SHRINKING=true REPLAY=true PRELOAD_OMISSIONS_FILE=" ++ PreloadOmissionFile ++ " REPLAY_TRACE_FILE=" ++ ReplayTraceFile ++ " TRACE_FILE=" ++ TraceFile ++ " ./rebar3 proper --retry | tee /tmp/partisan.output",
                    % debug("Executing command for iteration ~p:~n", [Iteration]),
                    % debug("~p~n", [Command]),
                    % CommandFun = fun() -> os:cmd(Command) end,
                    % {CTime, Output} = timer:tc(CommandFun),

                    ClassificationsExplored = ClassificationsExplored0 ++ [Classification],
                    % debug("=> Classification now: ~p~n", [ClassificationsExplored]),

                    %% New trace?
                    {ok, NewTraceLines} = file:consult(TraceFile),
                    debug("=> Executed test and test contained ~p lines compared to original trace with ~p lines.~n", [length(hd(NewTraceLines)), length(TraceLines)]),

                    MessageTypesFromTraceLines = message_types(TraceLines),
                    % debug("=> MessageTypesFromTraceLines: ~p~n", [MessageTypesFromTraceLines]),
                    MessageTypesFromNewTraceLines = message_types(hd(NewTraceLines)),
                    % debug("=> MessageTypesFromNewTraceLines: ~p~n", [MessageTypesFromNewTraceLines]),
                    DifferenceTraceLines = hd(NewTraceLines) -- TraceLines,
                    % debug("=> DifferenceTraceLines: ~p~n", [DifferenceTraceLines]),

                    DifferenceTypes = lists:usort(MessageTypesFromNewTraceLines) -- lists:usort(MessageTypesFromTraceLines),
                    debug("=> DifferenceTypes: ~p~n", [DifferenceTypes]),

                    NewTraces = case length(DifferenceTypes) > 0 of
                        true ->
                            debug("=> * Adding trace to list to explore.~n", []),

                            case lists:keymember(DifferenceTypes, 3, NewTraces0) of 
                                true ->
                                    debug("=> * Similar trace already exists, ignoring!~n", []),
                                    NewTraces0;
                                false ->
                                    NewTraces0 ++ [{hd(NewTraceLines), Omissions, DifferenceTypes, DifferenceTraceLines}]
                            end;
                        false ->
                            NewTraces0
                    end,

                    %% Store set of omissions as omissions that didn't invalidate the execution.
                    case ReturnValue of
                        true ->
                            %% This passed.
                            debug("Test passed.~n", []),

                            %% Insert result into the ETS table.
                            true = ets:insert(?RESULTS, {Iteration, {Iteration, FinalTraceLines, Omissions, true}}),

                            {passed, ClassificationsExplored, NewTraces};
                        false ->
                            %% This failed.
                            debug("Test FAILED!~n", []),
                            % debug("Failing test contained the following omitted mesage types: ~p~n", [Omissions]),

                            OmissionTypes = message_types(Omissions),
                            debug("=> OmissionTypes: ~p~n", [OmissionTypes]),

                            case os:getenv("EXIT_ON_COUNTEREXAMPLE") of 
                                false ->
                                    ok;
                                "true" ->
                                    EndTime = os:timestamp(),
                                    Difference = timer:now_diff(EndTime, StartTime),
                                    DifferenceMs = Difference / 1000,
                                    DifferenceSec = DifferenceMs / 1000,
                                    debug("Counterexample identified in ~p seconds.~n", [DifferenceSec]),
                                    exit({error, counterexample_found});
                                _Other ->
                                    ok
                            end,

                            %% Insert result into the ETS table.
                            true = ets:insert(?RESULTS, {Iteration, {Iteration, FinalTraceLines, Omissions, false}}),

                            {failed, ClassificationsExplored, NewTraces}
                    end
            end
    end.

%% @private
schedule_valid_causality(Causality, CandidateTrace0) ->
    CandidateTrace = message_types(CandidateTrace0),

    % debug("derived_schedule: ~p~n", [length(DerivedSchedule)]),
    % debug("prefix_schedule: ~p~n", [length(PrefixSchedule)]),
    % debug("omitted_schedule: ~p~n", [length(OmittedSchedule)]),
    % debug("conditional_schedule: ~p~n", [length(ConditionalSchedule)]),

    RequirementsMet = lists:foldl(fun(Type, Acc) ->
        % debug("=> Type: ~p~n", [Type]),

        All = lists:foldl(fun({K, V}, Acc1) ->
            % debug("=> V: ~p~n", [V]),

            case lists:member(Type, V) of 
                true ->
                    % debug("=> Presence of ~p requires: ~p~n", [Type, K]),

                    SearchType = case K of 
                        {receive_message, T} ->
                            {forward_message, T};
                        Other ->
                            Other
                    end,

                    % debug("=> Rewriting to look for send of same message type: ~p~n", [SearchType]),

                    case lists:member(SearchType, CandidateTrace) of 
                        true ->
                            % debug("=> Present!~n", []),
                            true andalso Acc1;
                        false ->
                            % debug("=> NOT Present!~n", []),
                            false andalso Acc1
                    end;
                false ->
                    % debug("=> * fallthrough: ~p not in ~p.~n", [Type, V]),
                    true andalso Acc1
            end
        end, Acc, dict:to_list(Causality)),

        All andalso Acc
    end, true, CandidateTrace),

    case RequirementsMet of 
        true ->
            % debug("Causality verified.~n", []),
            ok;
        false ->
            % debug("Schedule does not represent a valid schedule!~n", []),
            ok
    end,

    RequirementsMet.

%% @private
update_faulted_nodes(TraceLines, {_Type, Message} = Line, Omissions, BackgroundAnnotations, FaultedNodes0) ->
    %% Destructure message.
    {TracingNode, InterpositionType, _OriginNode, _MessagePayload} = Message,

    %% Filter omissions for the tracing node.
    OmissionsFilterFun = fun({Type1, Message1}) ->
        case Type1 of 
            pre_interposition_fun ->
                {TracingNode1, InterpositionType1, _OriginNode1, _MessagePayload1} = Message1,
                case InterpositionType1 of 
                    forward_message ->
                        TracingNode =:= TracingNode1;
                    _ ->
                        false
                end;
            _ ->
                false
        end
    end, 
    OmissionsForTracingNode = lists:filter(OmissionsFilterFun, Omissions),

    %% Get last omission for node.
    LastOmissionForTracingNode = lists:last(OmissionsForTracingNode),

    %% Get last non-background message for node.
    TraceFilterFun = fun({Type1, Message1}) ->
        case Type1 of 
            pre_interposition_fun ->
                {TracingNode1, InterpositionType1, _OriginNode1, _MessagePayload1} = Message1,

                case InterpositionType1 of 
                    forward_message ->
                        case TracingNode =:= TracingNode1 of 
                            true ->
                                case lists:member(element(2, message_type(Message1)), BackgroundAnnotations) of 
                                    true ->
                                        % debug("=> message is background message, false: ~p~n", [Message1]),
                                        false;
                                    false ->
                                        % debug("=> message is NOT background message, true: ~p~n", [Message1]),
                                        true
                                end;
                            false ->
                                false
                        end;
                    _ ->
                        false
                end;
            _ ->
                false
        end
    end, 
    MessagesForTracingNode = lists:filter(TraceFilterFun, TraceLines),

    %% Last sent message.
    LastMessageForTracingNode = lists:last(MessagesForTracingNode),

    case InterpositionType of 
        forward_message ->
            case dict:find(TracingNode, FaultedNodes0) of 
                {ok, true} ->
                    %% Node is already faulted.
                    case LastOmissionForTracingNode =:= Line of 
                        true ->
                            %% Conditional resolve.
                            case LastMessageForTracingNode =:= Line of 
                                true ->
                                    % debug("Node ~p is already faulted, sends no more messages, keeping faulted.~n", [TracingNode]),

                                    %% Keep faulted if this is the last message the node sends.
                                    FaultedNodes0;
                                false ->
                                    % %% Get message type.
                                    % {forward_message, MessageType} = message_type(Message),

                                    % debug("Removing ~p from list of faulted nodes for message type: ~p.~n", [TracingNode, MessageType]),
                                    % debug("=> last message for tracing node: ~p~n", [LastMessageForTracingNode]),
                                    % debug("=> is last non-background send for node? ~p~n", [LastMessageForTracingNode =:= Line]),

                                    %% If successful sends are happening as part of the protocol (non-background)
                                    %% after this final omission, then set the node as recovered.
                                    dict:store(TracingNode, false, FaultedNodes0)
                            end;
                        false ->
                            %% Keep faulted.
                            FaultedNodes0
                    end;
                _ ->
                    %% Node is not already faulted.
                    case LastOmissionForTracingNode =:= Line of 
                        true ->
                            case LastMessageForTracingNode =:= Line of 
                                true ->
                                    %% Set faulted if this is the last message the node sends.
                                    % debug("Adding ~p to list of faulted nodes for message type: ~p.~n", [TracingNode, MessageType]),
                                    % debug("=> node ~p is involved in ~p omissions.~n", [TracingNode, length(OmissionsForTracingNode)]),
                                    % debug("=> is last omission for node? ~p~n", [LastOmissionForTracingNode =:= Line]),

                                    dict:store(TracingNode, true, FaultedNodes0);
                                false ->
                                    % debug("Node ~p is not faulted, sends more messages, keeping NOT faulted.~n", [TracingNode]),

                                    %% If successful sends are happening as part of the protocol (non-background)
                                    %% after this final omission, then leave the node as non-crashed.
                                    FaultedNodes0
                            end;
                        false ->
                            %% Otherwise, set as fauled.
                            % debug("Adding ~p to list of faulted nodes for message type: ~p.~n", [TracingNode, MessageType]),
                            % debug("=> node ~p is involved in ~p omissions.~n", [TracingNode, length(OmissionsForTracingNode)]),
                            % debug("=> is last omission for node? ~p~n", [LastOmissionForTracingNode =:= Line]),

                            dict:store(TracingNode, true, FaultedNodes0)
                    end
            end;
        _ ->
            FaultedNodes0
    end.

%% @private
pruning() ->
    case os:getenv("PRUNING") of 
        "false" ->
            false;
        _ ->
            true
    end.