%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(partisan_causality_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/1,
         emit/4,
         receive_message/2,
         set_delivery_fun/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {name, label, myself, local_clock, order_buffer, buffered_messages, delivery_fun}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
start_link(Label) ->
    Name = generate_name(Label),
    gen_server:start_link({local, Name}, ?MODULE, [Label], []).

emit(Label, Node, ServerRef, Message) ->
    Name = generate_name(Label),
    gen_server:call(Name, {emit, Node, ServerRef, Message}, infinity).

receive_message(Label, Message) ->
    Name = generate_name(Label),
    gen_server:call(Name, {receive_message, Message}, infinity).

set_delivery_fun(Label, DeliveryFun) ->
    Name = generate_name(Label),
    gen_server:call(Name, {set_delivery_fun, DeliveryFun}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Label]) ->
    %% Figure out who we are.
    Myself = partisan_peer_service_manager:myself(),

    %% Generate a local clock that's used to track local dependencies.
    LocalClock = vclock:fresh(),

    %% Initiaize order buffer.
    OrderBuffer = orddict:new(),

    %% Generate message buffer.
    BufferedMessages = [],

    %% Schedule delivery attempts.
    schedule_delivery(Label),

    %% Get generated name.
    Name = generate_name(Label),

    %% Open the storage backend.
    try
        File = filename:join(application:get_env(data_root, partisan, dets), atom_to_list(Name)),
        lager:info("Trying to open backend: ~p", [File]),
        ok = filelib:ensure_dir(File),
        case dets:open_file(Name, [{file, File}]) of
            {ok, Name} ->
                case dets:lookup(Name, state) of
                    [{_, State}] ->
                        lager:info("Read state from disk: ~p", [State]),
                        {ok, State};
                    [] ->
                        lager:info("Using default state.", []),
                        {ok, #state{name=Name,
                                    myself=Myself,
                                    label=Label, 
                                    local_clock=LocalClock, 
                                    order_buffer=OrderBuffer, 
                                    buffered_messages=BufferedMessages}}
                end;
            {error, Error} ->
                {stop, Error}
        end
    catch
        _:Reason ->
            _ = lager:info("Backend initialization failed!"),
            {stop, Reason}
    end.

%% @private

%% Generate a message identifier and a payload to be transmitted on the wire.
handle_call({emit, Node, ServerRef, Message}, 
            _From, 
            #state{myself=#{name := Myself}, local_clock=LocalClock0, order_buffer=OrderBuffer0}=State) ->
    %% Bump our local clock.
    LocalClock = vclock:increment(Myself, LocalClock0),

    %% Only transmit order buffer containing single clock.
    FilteredOrderBuffer = orddict:filter(fun(Key, _Value) -> Key =:= Node end, OrderBuffer0),

    %% Return the message to be transmitted.
    FullMessage = {causal, Node, ServerRef, FilteredOrderBuffer, LocalClock, Message},

    %% Update the order buffer with node and mesage clock.
    OrderBuffer = orddict:store(Node, LocalClock, OrderBuffer0),

    lager:info("Emitting message with clock: ~p", [LocalClock]),

    {reply, {ok, FullMessage}, State#state{local_clock=LocalClock, order_buffer=OrderBuffer}};

%% Receive a causal messag off the wire; deliver or not depending on whether or not
%% the causal dependencies have been satisfied.
handle_call({receive_message, {causal, _Node, _ServerRef, _IncomingOrderBuffer, MessageClock, _Message}=FullMessage}, 
            _From, 
            #state{buffered_messages=BufferedMessages0}=State0) ->
    %% Add to the buffer and try to deliver.
    lager:info("Received message ~p and inserting into buffer.", [MessageClock]),
    BufferedMessages = BufferedMessages0 ++ [FullMessage],
    State = lists:foldl(fun(M, S) -> internal_receive_message(M, S) end, State0#state{buffered_messages=BufferedMessages}, BufferedMessages),

    %% Write state to disk.
    write_state(State),

    {reply, ok, State};

handle_call({set_delivery_fun, DeliveryFun}, _From, State) ->
    {reply, ok, State#state{delivery_fun=DeliveryFun}};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(deliver, #state{buffered_messages=BufferedMessages, label=Label}=State0) ->
    State = lists:foldl(fun(M, S) -> internal_receive_message(M, S) end, State0, BufferedMessages),

    %% Write state to disk.
    write_state(State),

    %% Schedule next set of deliveries.
    schedule_delivery(Label),

    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
deliver(#state{myself=Myself, local_clock=LocalClock, order_buffer=OrderBuffer, delivery_fun=DeliveryFun}=State0, 
        IncomingOrderBuffer, MessageClock, ServerRef, Message) ->
    %% Merge order buffers.
    MergeFun = fun(_Key, Value1, Value2) ->
        vclock:merge([Value1, Value2])
    end,
    orddict:merge(MergeFun, IncomingOrderBuffer, OrderBuffer),

    %% Merge clocks.
    MergedLocalClock = vclock:merge([LocalClock, MessageClock]),

    %% Advance our clock.
    IncrementedLocalClock = vclock:increment(Myself, MergedLocalClock),

    %% Deliver the actual message.
    case DeliveryFun of
        undefined ->
            try
                ServerRef ! Message
            catch
                _:Error ->
                    lager:debug("Error forwarding message ~p to process ~p: ~p", [Message, ServerRef, Error])
            end;
        DeliveryFun ->
            DeliveryFun(ServerRef, Message)
    end,

    %% Write and return updated state.
    State = State0#state{local_clock=IncrementedLocalClock},
    write_state(State),
    State.

%% @private
schedule_delivery(Label) ->
    Interval = partisan_config:get(redelivery_interval, 1000),
    Name = generate_name(Label),
    erlang:send_after(Interval, Name, deliver).

%% @private
internal_receive_message({causal, _Node, ServerRef, IncomingOrderBuffer, MessageClock, Message}=FullMessage, 
                         #state{myself=#{name := Myself}, local_clock=LocalClock, buffered_messages=BufferedMessages}=State0) ->
    lager:info("Attempting delivery of messages: ~p", [MessageClock]),

    case orddict:find(Myself, IncomingOrderBuffer) of
        %% No dependencies.
        error ->
            lager:info("Message ~p has no dependencies, delivering.", [MessageClock]),
            deliver(State0#state{buffered_messages=BufferedMessages -- [FullMessage]}, IncomingOrderBuffer, MessageClock, ServerRef, Message);
        %% Dependencies.
        {ok, DependencyClock} ->
            case vclock:dominates(LocalClock, DependencyClock) of
                %% Dependencies met.
                true ->
                    lager:info("Message ~p dependencies met, delivering.", [MessageClock]),
                    deliver(State0#state{buffered_messages=BufferedMessages -- [FullMessage]}, IncomingOrderBuffer, MessageClock, ServerRef, Message);
                %% Dependencies NOT met.
                false ->
                    %% Buffer, for later delivery.
                    lager:info("Message ~p dependencies ~p NOT met.", [MessageClock, DependencyClock]),
                    State0#state{buffered_messages=BufferedMessages}
            end
    end.

%% @private
generate_name(Label) ->
    list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Label)).

%% @private
write_state(#state{name=Name}=State) ->
    ok = dets:insert(Name, {state, State}),
    State.