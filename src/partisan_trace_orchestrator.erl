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

%% API
-export([start_link/0,
         start_link/1,
         trace/2,
         replay/2,
         reset/0,
         identify/1,
         print/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {previous_trace=[],
                trace=[], 
                replay=false,
                shrinking=false,
                blocked_processes=[],
                identifier=undefined}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start and link to calling process.
-spec start_link(list())-> {ok, pid()} | ignore | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%% @doc Record trace message.
trace(Type, Message) ->
    gen_server:call(?MODULE, {trace, Type, Message}, infinity).

%% @doc Replay trace.
replay(Type, Message) ->
    gen_server:call(?MODULE, {replay, Type, Message}, infinity).

%% @doc Reset trace.
reset() ->
    gen_server:call(?MODULE, reset, infinity).

%% @doc Print trace.
print() ->
    gen_server:call(?MODULE, print, infinity).

%% @doc Identify trace.
identify(Identifier) ->
    gen_server:call(?MODULE, {identify, Identifier}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, #state{}}.
init([]) ->
    lager:info("Test orchestrator started on node: ~p", [node()]),
    State = initialize_state(),
    {ok, State}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {reply, term(), #state{}}.
handle_call({trace, Type, Message}, _From, #state{trace=Trace0}=State) ->
    %% replay_debug("recording trace type: ~p message: ~p", [Type, Message]),
    {reply, ok, State#state{trace=Trace0++[{Type, Message}]}};
handle_call({replay, Type, Message}, From, #state{previous_trace=PreviousTrace0, replay=Replay, shrinking=Shrinking, blocked_processes=BlockedProcesses0}=State) ->
    case Replay of 
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
                        replay_debug("~p <- ~p: ~p", [TracingNode, OriginNode, MessagePayload]);
                    forward_message ->
                        replay_debug("~p => ~p: ~p", [TracingNode, OriginNode, MessagePayload])
                end;
            post_interposition_fun ->
                %% Destructure message.
                {TracingNode, OriginNode, InterpositionType, MessagePayload, RewrittenMessagePayload} = Message,

                case is_protocol_message(MessagePayload) andalso not protocol_tracing() of 
                    true ->
                        %% Protocol message and we're not tracing protocol messages.
                        ok;
                    _ ->
                        %% Otherwise, format trace accordingly.
                        case MessagePayload =:= RewrittenMessagePayload of 
                            true ->
                                case InterpositionType of
                                    receive_message ->
                                        replay_debug("* ~p <- ~p: ~p", [TracingNode, OriginNode, MessagePayload]);
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
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private 
can_deliver_based_on_trace(Shrinking, {Type, Message}, PreviousTrace, BlockedProcesses) ->
    replay_debug("determining if message ~p: ~p can be delivered.", [Type, Message]),

    case PreviousTrace of 
        [{NextType, NextMessage} | _] ->
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
    case os:getenv("REPLAY") of 
        false ->
            %% This is not a replay, so store the current trace.
            replay_debug("recording trace to file.", []),
            #state{trace=[], blocked_processes=[], identifier=undefined};
        _ ->
            %% Mark that we are in replay mode.
            partisan_config:set(replaying, true),

            %% This is a replay, so load the previous trace.
            replay_debug("loading previous trace for replay.", []),

            ReplayTraceFile = replay_trace_file(),
            {ok, [Lines]} = file:consult(ReplayTraceFile),

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

            #state{trace=[], previous_trace=Lines, replay=true, shrinking=Shrinking, blocked_processes=[], identifier=undefined}
    end.

%% @private
write_trace(Trace) ->
    %% Write trace.
    FilteredTrace = lists:filter(fun({Type, Message}) ->
        case Type of 
            pre_interposition_fun ->
                %% Trace all entry points if protocol message, unless tracing enabled.
                {_TracingNode, _InterpositionType, _OriginNode, MessagePayload} = Message,

                case is_protocol_message(MessagePayload) of 
                    true ->
                        %% Trace protocol messages only if protocol tracing is enabled.
                        protocol_tracing();
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

    TraceFile = trace_file(),
    replay_debug("writing trace.", []),
    {ok, Io} = file:open(TraceFile, [write, {encoding, utf8}]),
    io:format(Io, "~p.~n", [FilteredTrace]),
    file:close(Io),

    ok.

%% Should we do replay debugging?
replay_debug(Line, Args) ->
    lager:info("~p: " ++ Line, [?MODULE] ++ Args).

%% @private
protocol_tracing() ->
    partisan_config:get(protocol_tracing, ?PROTOCOL_TRACING).

%% @private
is_protocol_message({protocol, _}) ->
    true;
is_protocol_message({ping, _, _, _}) ->
    true;
is_protocol_message({pong, _, _, _}) ->
    true;
is_protocol_message(_Message) ->
    false.

