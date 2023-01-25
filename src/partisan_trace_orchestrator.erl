%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(partisan_trace_orchestrator).
-author("Christopher Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-define(MANAGER, partisan_pluggable_peer_service_manager).

%% API
-export([start_link/0,
         start_link/1,
         stop/0,
         trace/2,
         replay/2,
         reset/0,
         enable/1,
         identify/1,
         print/0,
         perform_preloads/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {previous_trace=[],
                trace=[],
                enabled=false,
                nodes=[],
                replay=false,
                shrinking=false,
                blocked_processes=[],
                identifier=undefined}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
start_link() ->
    start_link([]).

stop() ->
    gen_server:stop({global, ?MODULE}, normal, infinity).

%% @doc Start and link to calling process.
start_link(Args) ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, Args, []).

%% @doc Record trace message.
trace(Type, Message) ->
    gen_server:cast({global, ?MODULE}, {trace, Type, Message}).

%% @doc Replay trace.
replay(Type, Message) ->
    gen_server:call({global, ?MODULE}, {replay, Type, Message}, infinity).

%% @doc Enable trace.
enable(Nodes) ->
    gen_server:call({global, ?MODULE}, {enable, Nodes}, infinity).

%% @doc Reset trace.
reset() ->
    gen_server:call({global, ?MODULE}, reset, infinity).

%% @doc Print trace.
print() ->
    gen_server:call({global, ?MODULE}, print, infinity).

%% @doc Identify trace.
identify(Identifier) ->
    gen_server:call({global, ?MODULE}, {identify, Identifier}, infinity).

%% @doc Perform preloads.
perform_preloads(Nodes) ->
    %% This is a replay, so load the preloads.
    replay_debug("loading preload omissions.", []),
    preload_omissions(Nodes),
    replay_debug("preloads finished.", []),

    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    debug("test orchestrator started on node: ~p", [partisan:node()]),
    State = initialize_state(),
    {ok, State}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
handle_call({enable, Nodes}, _From, #state{enabled=false}=State) ->
    ?LOG_INFO("enabling tracing for nodes: ~p", [Nodes]),

    %% Add send and receive pre-interposition functions to enforce message ordering.
    PreInterpositionFun = fun({Type, OriginNode, OriginalMessage}) ->
        %% TODO: This needs to be fixed: replay and trace need to be done
        %% atomically otherwise processes will race to write trace entry when
        %% they are unblocked from retry: this means that under replay the trace
        %% file might generate small permutations of messages which means it's
        %% technically not the same trace.
        ?LOG_INFO("pre interposition fired for message type: ~p", [Type]),

        %% Under replay ensure they match the trace order (but only for pre-interposition messages).
        ok = partisan_trace_orchestrator:replay(pre_interposition_fun, {node(), Type, OriginNode, OriginalMessage}),

        %% Record message incoming and outgoing messages.
        ok = partisan_trace_orchestrator:trace(pre_interposition_fun, {node(), Type, OriginNode, OriginalMessage}),

        ok
    end,
    lists:foreach(fun({_Name, Node}) ->
        ?LOG_INFO("Installing $tracing pre-interposition function on node: ~p", [Node]),
        ok = rpc:call(Node, ?MANAGER, add_pre_interposition_fun, ['$tracing', PreInterpositionFun])
    end, Nodes),

    %% Add send and receive post-interposition functions to perform tracing.
    PostInterpositionFun = fun({Type, OriginNode, OriginalMessage}, {Type, OriginNode, RewrittenMessage}) ->
        %% Record outgoing message after transformation.
    ?LOG_INFO("post interposition fired for message type: ~p", [Type]),
        ok = partisan_trace_orchestrator:trace(post_interposition_fun, {node(), OriginNode, Type, OriginalMessage, RewrittenMessage})
    end,
    lists:foreach(fun({_Name, Node}) ->
        ok = rpc:call(Node, ?MANAGER, add_post_interposition_fun, ['$tracing', PostInterpositionFun])
    end, Nodes),

    {reply, ok, State#state{nodes=Nodes, enabled=true}};
handle_call(Message, _From, #state{enabled=false}=State) ->
    replay_debug("ignoring ~p as tracing is disabled.", [Message]),
    {reply, ok, State};
handle_call({replay, Type, Message}, From, #state{previous_trace=PreviousTrace0, replay=Replay, shrinking=Shrinking, blocked_processes=BlockedProcesses0}=State) ->
    case Replay of
        true ->
            %% Should we enforce trace order during replay?
            ShouldEnforce = case Type of
                pre_interposition_fun ->
                    %% Destructure pre-interposition trace message.
                    {_TracingNode, InterpositionType, _OriginNode, MessagePayload} = Message,

                    case is_membership_strategy_message(InterpositionType, MessagePayload) of
                        true ->
                            membership_strategy_tracing();
                        false ->
                            true
                    end;
                _ ->
                    true
            end,

            case ShouldEnforce of
                false ->
                    {reply, ok, State};
                true ->
                    %% Find next message that should arrive based on the trace.
                    %% Can we process immediately?
                    case can_deliver_based_on_trace(Shrinking, {Type, Message}, PreviousTrace0, BlockedProcesses0) of
                        true ->
                            %% Deliver as much as we can.
                            {PreviousTrace, BlockedProcesses} = trace_deliver(Shrinking, {Type, Message}, PreviousTrace0, BlockedProcesses0),

                            %% Record new trace position and new list of blocked processes.
                            {reply, ok, State#state{blocked_processes=BlockedProcesses, previous_trace=PreviousTrace}};
                        false ->
                            %% If not, store message, block caller until processed.
                            BlockedProcesses = [{{Type, Message}, From} | BlockedProcesses0],

                            %% Block the process.
                            {noreply, State#state{blocked_processes=BlockedProcesses}}
                    end
            end;
        false ->
            {reply, ok, State}
    end;
handle_call(reset, _From, _State) ->
    replay_debug("resetting trace.", []),
    State = initialize_state(),
    {reply, ok, State};
handle_call({identify, Identifier}, _From, State) ->
    replay_debug("identifying trace: ~p", [Identifier]),
    {reply, ok, State#state{identifier=Identifier}};
handle_call(print, _From, #state{trace=Trace}=State) ->
    replay_debug("printing trace", []),

    lists:foreach(fun({Type, Message}) ->
        case Type of
            enter_command ->
                %% Destructure message.
                {TracingNode, Command} = Message,

                %% Format trace accordingly.
                replay_debug("~p entering command: ~p", [TracingNode, Command]);
            exit_command ->
                %% Destructure message.
                {TracingNode, Command} = Message,

                %% Format trace accordingly.
                replay_debug("~p leaving command: ~p", [TracingNode, Command]);
            pre_interposition_fun ->
                %% Destructure message.
                {_TracingNode, _InterpositionType, _OriginNode, _MessagePayload} = Message,

                %% Do nothing.
                ok;
            interposition_fun ->
                %% Destructure message.
                {TracingNode, OriginNode, InterpositionType, MessagePayload} = Message,

                %% Format trace accordingly.
                case InterpositionType of
                    receive_message ->
                        % replay_debug("~p <- ~p: ~p", [TracingNode, OriginNode, MessagePayload]),
                        ok;
                    forward_message ->
                        replay_debug("~p => ~p: ~p", [TracingNode, OriginNode, MessagePayload])
                end;
            post_interposition_fun ->
                %% Destructure message.
                {TracingNode, OriginNode, InterpositionType, MessagePayload, RewrittenMessagePayload} = Message,

                case is_membership_strategy_message(InterpositionType, MessagePayload) andalso not membership_strategy_tracing() of
                    true ->
                        %% Protocol message and we're not tracing protocol messages.
                        ok;
                    _ ->
                        %% Otherwise, format trace accordingly.
                        case MessagePayload =:= RewrittenMessagePayload of
                            true ->
                                case InterpositionType of
                                    receive_message ->
                                        % replay_debug("* ~p <- ~p: ~p", [TracingNode, OriginNode, MessagePayload]),
                                        ok;
                                    forward_message ->
                                        replay_debug("~p => ~p: ~p", [TracingNode, OriginNode, MessagePayload])
                                end;
                            false ->
                                case RewrittenMessagePayload of
                                    undefined ->
                                        case InterpositionType of
                                            receive_message ->
                                                replay_debug("* ~p <- ~p: DROPPED ~p", [TracingNode, OriginNode, MessagePayload]);
                                            forward_message ->
                                                replay_debug("~p => ~p: DROPPED ~p", [TracingNode, OriginNode, MessagePayload])
                                        end;
                                    _ ->
                                        case InterpositionType of
                                            receive_message ->
                                                replay_debug("* ~p <- ~p: REWROTE ~p to ~p", [TracingNode, OriginNode, MessagePayload, RewrittenMessagePayload]);
                                            forward_message ->
                                                replay_debug("~p => ~p: REWROTE ~p to ~p", [TracingNode, OriginNode, MessagePayload, RewrittenMessagePayload])
                                        end
                                end
                        end
                end;
            _ ->
                replay_debug("unknown message type: ~p, message: ~p", [Type, Message])
        end
    end, Trace),

    write_trace(Trace),

    {reply, ok, State};
handle_call(Msg, _From, State) ->
    replay_debug("Unhandled call messages: ~p", [Msg]),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast({trace, Type, Message}, #state{trace=Trace0}=State) ->
    {noreply, State#state{trace=Trace0++[{Type, Message}]}};
handle_cast(Msg, State) ->
    replay_debug("Unhandled cast messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(Msg, State) ->
    replay_debug("Unhandled info messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, #state{nodes=Nodes}) ->
    lists:foreach(fun({_Name, Node}) ->
        replay_debug("removing tracing pre-interposition on node: ~p", [Node]),
        rpc:call(Node, ?MANAGER, remove_pre_interposition_fun, ['$tracing'])
    end, Nodes),

    lists:foreach(fun({_Name, Node}) ->
        replay_debug("removing tracing post-interposition on node: ~p", [Node]),
        rpc:call(Node, ?MANAGER, remove_post_interposition_fun, ['$tracing'])
    end, Nodes),

    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec can_deliver_based_on_trace(term(), term(), term(), term()) -> true | false.
can_deliver_based_on_trace(Shrinking, {Type, Message}, PreviousTrace, BlockedProcesses) ->
    replay_debug("determining if message ~p: ~p can be delivered.", [Type, Message]),

    case PreviousTrace of
        [{NextType, NextMessage} | _] ->
            replay_debug("waiting for message ~p: ~p ", [NextType, NextMessage]),

            CanDeliver = case {NextType, NextMessage} of
                {Type, Message} ->
                    replay_debug("=> YES!", []),
                    true;
                _ ->
                    %% But, does the message actually exist in the trace?
                    case lists:member({Type, Message}, PreviousTrace) of
                        true ->
                            replay_debug("=> NO, waiting for message: ~p: ~p", [NextType, NextMessage]),
                            false;
                        false ->
                            %% new messages in the middle of the trace.
                            %% this *should* be the common case if we shrink from the start of the trace forward (foldl)
                            case Shrinking of
                                true ->
                                    replay_debug("=> CONDITIONAL YES, message doesn't exist in previous trace, but shrinking: ~p ~p", [Type, Message]),
                                    true;
                                false ->
                                    replay_debug("=> NO, waiting for message: ~p: ~p", [NextType, NextMessage]),
                                    false
                            end
                    end
            end,

            replay_debug("can deliver: ~p", [CanDeliver]),
            replay_debug("blocked processes: ~p", [length(BlockedProcesses)]),

            CanDeliver;
        [] ->
            %% end of trace, but we are still receiving messages.
            %% this *should* be the common case if we shrink from the back of the trace forward (foldr)
            case Shrinking of
                true ->
                    replay_debug("=> CONDITIONAL YES, message doesn't exist in previous trace, but shrinking: ~p ~p", [Type, Message]),
                    true;
                false ->
                    replay_debug("=> message should not have been delivered, blocking.", []),
                    false
            end
    end.

%% @private
trace_deliver(Shrinking, {Type, Message}, [{Type, Message} | Trace], BlockedProcesses) ->
    replay_debug("delivering single message!", []),

    %% Advance the trace, then try to flush the blocked processes.
    trace_deliver_log_flush(Shrinking, Trace, BlockedProcesses);
trace_deliver(_Shrinking, {_, _}, [{_, _} = NextMessage | Trace], BlockedProcesses) ->
    replay_debug("delivering single message (not in the trace)!", []),

    %% Advance the trace, don't flush blocked processes, since we weren't in the trace, nothing is blocked.
    {[NextMessage|Trace], BlockedProcesses};
trace_deliver(_Shrinking, {_, _}, [], BlockedProcesses) ->
    replay_debug("delivering single message (not in the trace -- end of trace)!", []),

    %% No trace to advance.
    {[], BlockedProcesses}.

%% @private
trace_deliver_log_flush(Shrinking, Trace0, BlockedProcesses0) ->
    replay_debug("attempting to flush blocked messages!", []),

    %% Iterate blocked processes in an attempt to remove one.
    {ND, T, BP} = lists:foldl(fun({{NextType, NextMessage}, Pid} = BP, {NumDelivered1, Trace1, BlockedProcesses1}) ->
        case can_deliver_based_on_trace(Shrinking, {NextType, NextMessage}, Trace1, BlockedProcesses0) of
            true ->
                replay_debug("message ~p ~p can be unblocked!", [NextType, NextMessage]),

                %% Advance the trace.
                [{_, _} | RestOfTrace] = Trace1,

                %% Advance the count of delivered messages.
                NewNumDelivered = NumDelivered1 + 1,

                %% Unblock the process.
                gen_server:reply(Pid, ok),

                %% Remove from the blocked processes list.
                NewBlockedProcesses = BlockedProcesses1 -- [BP],

                {NewNumDelivered, RestOfTrace, NewBlockedProcesses};
            false ->
                replay_debug("pid ~p CANNOT be unblocked yet, unmet dependencies!", [Pid]),

                {NumDelivered1, Trace1, BlockedProcesses1}
        end
    end, {0, Trace0, BlockedProcesses0}, BlockedProcesses0),

    %% Did we deliver something? If so, try again.
    case ND > 0 of
        true ->
            %% replay_debug("was able to deliver a message, trying again", []),
            trace_deliver_log_flush(Shrinking, T, BP);
        false ->
            replay_debug("flush attempt finished.", []),
            {T, BP}
    end.

%% @private
preload_omission_file() ->
    case os:getenv("PRELOAD_OMISSIONS_FILE") of
        false ->
            undefined;
        Other ->
            Other
    end.

%% @private
trace_file() ->
    case os:getenv("TRACE_FILE") of
        false ->
            "/tmp/partisan-latest.trace";
        Other ->
            Other
    end.

%% @private
replay_trace_file() ->
    case os:getenv("REPLAY_TRACE_FILE") of
        false ->
            "/tmp/partisan-latest.trace";
        Other ->
            Other
    end.

%% @private
initialize_state() ->
    case os:getenv("REPLAY_DEBUG", "false") of
        "true" ->
            partisan_config:set(replay_debug, true);
        _ ->
            partisan_config:set(replay_debug, false)
    end,

    case os:getenv("REPLAY") of
        false ->
            %% This is not a replay, so store the current trace.
            replay_debug("recording trace to file.", []),
            #state{enabled=false, trace=[], blocked_processes=[], identifier=undefined};
        _ ->
            %% Mark that we are in replay mode.
            partisan_config:set(replaying, true),

            %% This is a replay, so load the previous trace.
            replay_debug("loading previous trace for replay.", []),

            ReplayTraceFile = replay_trace_file(),
            {ok, Lines} = partisan_trace_file:read(ReplayTraceFile),

            lists:foreach(fun(Line) -> replay_debug("~p", [Line]) end, Lines),

            replay_debug("trace loaded.", []),

            Shrinking = case os:getenv("SHRINKING") of
                false ->
                    false;
                _ ->
                    %% Mark that we are in shrinking mode.
                    partisan_config:set(shrinking, true),

                    true
            end,

            #state{enabled=false, trace=[], previous_trace=Lines, replay=true, shrinking=Shrinking, blocked_processes=[], identifier=undefined}
    end.

%% @private
write_trace(Trace) ->
    %% Write trace.
    FilteredTrace = lists:filter(fun({Type, Message}) ->
        case Type of
            pre_interposition_fun ->
                %% Trace all entry points if protocol message, unless tracing enabled.
                {_TracingNode, InterpositionType, _OriginNode, MessagePayload} = Message,

                case is_membership_strategy_message(InterpositionType, MessagePayload) of
                    true ->
                        %% Trace protocol messages only if protocol tracing is enabled.
                        membership_strategy_tracing();
                    false ->
                        %% Always trace non-protocol messages.
                        true
                end;
            enter_command ->
                true;
            exit_command ->
                true;
            _ ->
                false
        end
    end, Trace),

    %% Write the trace file out.
    TraceFile = trace_file(),
    ok = partisan_trace_file:write(TraceFile, FilteredTrace),

    ok.

%% Should we do replay debugging?
replay_debug(Line, Args) ->
    case partisan_config:get(replay_debug) of
        true ->
            ?LOG_INFO("~p: " ++ Line, [?MODULE] ++ Args);
        _ ->
            ok
    end.

debug(Line, Args) ->
    ?LOG_INFO("~p: " ++ Line, [?MODULE] ++ Args).

%% @private
membership_strategy_tracing() ->
    partisan_config:get(membership_strategy_tracing, ?MEMBERSHIP_STRATEGY_TRACING).

%% @private
preload_omissions(Nodes) ->
    PreloadOmissionFile = preload_omission_file(),

    case PreloadOmissionFile of
        undefined ->
            replay_debug("no preload omissions file...", []),
            ok;
        _ ->
            {ok, Omissions} = partisan_trace_file:read(PreloadOmissionFile),

            %% Preload each omission at the correct node.
            lists:foldl(fun({T, Message}, OmissionNodes0) ->
                case T of
                    pre_interposition_fun ->
                        {TracingNode, forward_message, OriginNode, MessagePayload} = Message,

                        replay_debug("enabling preload omission for ~p => ~p: ~p", [TracingNode, OriginNode, MessagePayload]) ,

                        InterpositionFun = fun
                            ({forward_message, N, M}) ->
                                case N of
                                    OriginNode ->
                                        case M of
                                            MessagePayload ->
                                                ?LOG_INFO("~p: dropping packet from ~p to ~p due to preload interposition.", [partisan:node(), TracingNode, OriginNode]),

                                                case partisan_config:get(fauled_for_background) of
                                                    true ->
                                                        ok;
                                                    _ ->
                                                        ?LOG_INFO("~p: setting node ~p to faulted due to preload interposition hit on message: ~p", [partisan:node(), TracingNode, Message]),
                                                        partisan_config:set(fauled_for_background, true)
                                                end,

                                                undefined;
                                            Other ->
                                                ?LOG_INFO("~p: allowing message, doesn't match interposition payload while node matches", [partisan:node()]),
                                                ?LOG_INFO("~p: => expecting: ~p", [partisan:node(), MessagePayload]),
                                                ?LOG_INFO("~p: => got: ~p", [partisan:node(), Other]),
                                                M
                                        end;
                                    OtherNode ->
                                        ?LOG_INFO("~p: allowing message, doesn't match interposition as destination is ~p and not ~p", [partisan:node(), TracingNode, OtherNode]),
                                        M
                                end;
                            ({receive_message, _N, M}) ->
                                M
                        end,

                        %% Install function.
                        ok = rpc:call(TracingNode, ?MANAGER, add_interposition_fun, [{send_omission, OriginNode, Message}, InterpositionFun]),

                        lists:usort(OmissionNodes0 ++ [TracingNode]);
                    Other ->
                        replay_debug("unknown preload: ~p", [Other]),
                        OmissionNodes0
                end
            end, [], Omissions)
    end,

    %% Load background annotations.
    BackgroundAnnotations = background_annotations(),
    debug("background annotations are: ~p", [BackgroundAnnotations]),

    %% Install faulted tracing interposition function.
    lists:foreach(fun({_, Node}) ->
        InterpositionFun = fun({forward_message, _N, M}) ->
            case partisan_config:get(faulted) of
                true ->
                    case M of
                        undefined ->
                            undefined;
                        _ ->
                            replay_debug("~p: faulted during forward_message of background message, message ~p should be dropped.", [partisan:node(), M]),
                            undefined
                    end;
                _ ->
                    M
            end;
            ({receive_message, _N, M}) ->
                case partisan_config:get(faulted) of
                    true ->
                        case M of
                            undefined ->
                                undefined;
                            _ ->
                                replay_debug("~p: faulted during receive_message of background message, message ~p should be dropped.", [partisan:node(), M]),
                                undefined
                        end;
                    _ ->
                        M
                end
        end,

        %% Install function.
        replay_debug("installing faulted pre-interposition for node: ~p", [Node]),
        ok = rpc:call(Node, ?MANAGER, add_interposition_fun, [{faulted, Node}, InterpositionFun])
    end, Nodes),

    %% Install faulted_for_background tracing interposition function.
    lists:foreach(fun({_, Node}) ->
        InterpositionFun = fun({forward_message, _N, M}) ->
            replay_debug("~p: interposition called for message: ~p", [partisan:node(), M]),

            case partisan_config:get(faulted_for_background) of
                true ->
                    case M of
                        undefined ->
                            undefined;
                        _ ->
                            MessageType = message_type(forward_message, M),

                            case lists:member(element(2, MessageType), BackgroundAnnotations) of
                                true ->
                                    ?LOG_INFO("~p: faulted_for_background during forward_message of background message, message ~p should be dropped.", [partisan:node(), M]),
                                    undefined;
                                false ->
                                    ?LOG_INFO("~p: faulted_for_background, but forward_message payload is not background message: ~p, message_type: ~p", [partisan:node(), M, MessageType]),
                                    M
                            end
                    end;
                _ ->
                    M
            end;
            ({receive_message, _N, M}) ->
                case partisan_config:get(faulted_for_background) of
                    true ->
                        case M of
                            undefined ->
                                undefined;
                            _ ->
                                MessageType = message_type(receive_message, M),

                                case lists:member(element(2, MessageType), BackgroundAnnotations) of
                                    true ->
                                        ?LOG_INFO("~p: faulted_for_background during receive_message of background message, message ~p should be dropped.", [partisan:node(), M]),
                                        undefined;
                                    false ->
                                        ?LOG_INFO("~p: faulted_for_background, but receive_message payload is not background message: ~p, message_type: ~p", [partisan:node(), M, MessageType]),
                                        M
                                end
                        end;
                    _ ->
                        M
                end
        end,

        %% Install function.
        replay_debug("installing faulted_for_background pre-interposition for node: ~p", [Node]),
        ok = rpc:call(Node, ?MANAGER, add_interposition_fun, [{faulted_for_background, Node}, InterpositionFun])
    end, Nodes),

    ok.

%% @private
message_type(InterpositionType, MessagePayload) ->
    ?LOG_INFO("interposition_type: ~p, payload: ~p", [InterpositionType, MessagePayload]),

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
background_annotations() ->
    %% Get module as string.
    ModuleString = os:getenv("IMPLEMENTATION_MODULE"),

    %% Open the annotations file.
    AnnotationsFile = "./annotations/partisan-annotations-" ++ ModuleString,

    case filelib:is_file(AnnotationsFile) of
        false ->
            debug("Annotations file doesn't exist: ~p~n", [AnnotationsFile]),
            [];
        true ->
            {ok, [RawAnnotations]} = file:consult(AnnotationsFile),
            % debug("Raw annotations loaded: ~p~n", [RawAnnotations]),
            AllAnnotations = dict:from_list(RawAnnotations),
            % debug("Annotations loaded: ~p~n", [dict:to_list(AllAnnotations)]),

            % {ok, RawCausalityAnnotations} = dict:find(causality, AllAnnotations),
            % debug("Raw causality annotations loaded: ~p~n", [RawCausalityAnnotations]),

            % CausalityAnnotations = dict:from_list(RawCausalityAnnotations),
            % debug("Causality annotations loaded: ~p~n", [dict:to_list(CausalityAnnotations)]),

            {ok, BackgroundAnnotations} = dict:find(background, AllAnnotations),
            % debug("Background annotations loaded: ~p~n", [BackgroundAnnotations]),

            BackgroundAnnotations
    end.

%%%===================================================================
%%% Trace filtering: super hack, until we can refactor these messages.
%%%===================================================================

%% TODO: Find a way to disable distance metrics, which are non-deterministic
%% for the test execution.

%% TODO: Rename protocol messages, find a way to specify an external
%% function for filtering the trace -- do it from the harness.  Maybe
%% some sort of glob function for receives and forwards to filter.

%% TODO: Weird hack, now everything goes through the interposition mechanism
%% which means we can capture everything *good!* but we only want
%% to see a small subset of it -- acks, yes!, membership updates, sometimes!
%% but distances?  never.  so, we need some really terrible hacks here.

%% TODO: Change "protocol" tracing to "membership strategy" tracing.

%% Pre-interposition examples.
is_membership_strategy_message(receive_message, {_, _, {membership_strategy, _}}) ->
    true;

is_membership_strategy_message(forward_message, {membership_strategy, _}) ->
    true;

%% Post-interposition examples.
is_membership_strategy_message(forward_message, {_, _, {membership_strategy, _}}) ->
    true;

is_membership_strategy_message(receive_message, {membership_strategy, _}) ->
    true;

is_membership_strategy_message(_Type, _Message) ->
    false.

