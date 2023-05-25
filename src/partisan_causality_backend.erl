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

-include("partisan_logger.hrl").


%% API
-export([start_link/1,
         emit/4,
         reemit/2,
         receive_message/2,
         set_delivery_fun/2,
         is_causal_message/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    name                ::  atom(),
    label               ::  atom(),
    my_node             ::  node(),
    local_clock,
    order_buffer,
    buffered_messages,
    delivery_fun,
    storage
}).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Same as start_link([]).
%% @end
%% -----------------------------------------------------------------------------
-spec start_link(Label :: atom()) -> gen_server:start_ret().

start_link(Label) ->
    Name = generate_name(Label),
    gen_server:start_link({local, Name}, ?MODULE, [Label], []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
reemit(Label, {_CausalLabel, LocalClock}) ->
    Name = generate_name(Label),
    gen_server:call(Name, {reemit, LocalClock}, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
emit(Label, Node, ServerRef, Message) ->
    Name = generate_name(Label),
    gen_server:call(Name, {emit, Node, ServerRef, Message}, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
receive_message(Label, Message) ->
    Name = generate_name(Label),
    gen_server:call(Name, {receive_message, Message}, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
set_delivery_fun(Label, DeliveryFun) ->
    Name = generate_name(Label),
    gen_server:call(Name, {set_delivery_fun, DeliveryFun}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Determine is a message is being sent with causal delivery or not.
%% @end
%% -----------------------------------------------------------------------------
is_causal_message({causal, _Label, _Node, _ServerRef, _IncomingOrderBuffer, _MessageClock, _Message}) ->
    true;
is_causal_message(_) ->
    false.



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



%% @private
init([Label]) ->
    %% Figure out who we are.
    MyNode = partisan:node(),

    %% Generate a local clock that's used to track local dependencies.
    LocalClock = partisan_vclock:fresh(),

    %% Initiaize order buffer.
    OrderBuffer = orddict:new(),

    %% Generate message buffer.
    BufferedMessages = [],

    %% Schedule delivery attempts.
    schedule_delivery(Label),

    %% Get generated name.
    Name = generate_name(Label),

    %% Open the storage backend.
    Storage = ets:new(Name, [named_table]),

    %% Start server.
    {ok, #state{
        name = Name,
        my_node = MyNode,
        label = Label,
        local_clock = LocalClock,
        order_buffer = OrderBuffer,
        buffered_messages = BufferedMessages,
        storage = Storage
    }}.

%% Generate a message identifier and a payload to be transmitted on the wire.
handle_call({reemit, LocalClock},
            _From,
            #state{storage=Storage}=State) ->
    %% Lookup previously emitted message and return it's clock and message.
    [{LocalClock, CausalMessage}] = ets:lookup(Storage, LocalClock),

    {reply, {ok, LocalClock, CausalMessage}, State};

%% Generate a message identifier and a payload to be transmitted on the wire.
handle_call({emit, Node, ServerRef, Message},
            _From,
            #state{storage=Storage,
                   my_node=MyNode,
                   label=Label,
                   local_clock=LocalClock0,
                   order_buffer=OrderBuffer0}=State0) ->
    %% Bump our local clock.
    LocalClock = partisan_vclock:increment(MyNode, LocalClock0),

    %% Only transmit order buffer containing single clock.
    FilteredOrderBuffer = orddict:filter(fun(Key, _Value) -> Key =:= Node end, OrderBuffer0),

    %% Return the message to be transmitted.
    CausalMessage = {causal, Label, Node, ServerRef, FilteredOrderBuffer, LocalClock, Message},

    %% Update the order buffer with node and message clock.
    OrderBuffer = orddict:store(Node, LocalClock, OrderBuffer0),

    %% Every time we omit a message, store the clock and message so we can regenerate the message.
    true = ets:insert(Storage, {LocalClock, CausalMessage}),

    ?LOG_DEBUG(#{
        description => "Emitting message",
        clock => LocalClock
    }),

    State = State0#state{local_clock=LocalClock, order_buffer=OrderBuffer},

    {reply, {ok, LocalClock, CausalMessage}, State};

%% Receive a causal message off the wire; deliver or not depending on whether or not
%% the causal dependencies have been satisfied.
handle_call({receive_message, {causal, Label, _Node, _ServerRef, _IncomingOrderBuffer, MessageClock, _Message}=FullMessage},
            _From,
            #state{label=Label, buffered_messages=BufferedMessages0}=State0) ->
    %% Add to the buffer and try to deliver.
    ?LOG_DEBUG(#{
        description => "Received message, inserting into buffer.",
        message => MessageClock
    }),

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
    State = lists:foldl(
        fun(M, S) ->
            internal_receive_message(M, S)
        end,
        State0,
        BufferedMessages
    ),

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
deliver(#state{my_node=MyNode, local_clock=LocalClock, order_buffer=OrderBuffer, delivery_fun=DeliveryFun}=State0,
        IncomingOrderBuffer, MessageClock, ServerRef, Message) ->
    %% Merge order buffers.
    MergeFun = fun(_Key, Value1, Value2) ->
        partisan_vclock:merge([Value1, Value2])
    end,
    orddict:merge(MergeFun, IncomingOrderBuffer, OrderBuffer),

    %% Merge clocks.
    MergedLocalClock = partisan_vclock:merge([LocalClock, MessageClock]),

    %% Advance our clock.
    IncrementedLocalClock = partisan_vclock:increment(MyNode, MergedLocalClock),

    %% Deliver the actual message.
    case DeliveryFun of
        undefined ->
            try
                partisan_peer_service_manager:deliver(ServerRef, Message)
            catch
                _:Reason ->
                    ?LOG_DEBUG(#{
                        description => "Error forwarding message to process",
                        reason => Reason,
                        message => Message,
                        process => ServerRef
                    })
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
internal_receive_message({causal, _Label, _Node, ServerRef, IncomingOrderBuffer, MessageClock, Message}=FullMessage,
                         #state{my_node=MyNode, local_clock=LocalClock, buffered_messages=BufferedMessages}=State0) ->
    ?LOG_DEBUG(#{
        description => "Attempting delivery of messages",
        messages => MessageClock
    }),

    case orddict:find(MyNode, IncomingOrderBuffer) of
        %% No dependencies.
        error ->
            ?LOG_DEBUG(#{
                description => "Message has no dependencies, delivering",
                messages => MessageClock
            }),
            deliver(State0#state{buffered_messages=BufferedMessages -- [FullMessage]}, IncomingOrderBuffer, MessageClock, ServerRef, Message);
        %% Dependencies.
        {ok, DependencyClock} ->
            case partisan_vclock:dominates(LocalClock, DependencyClock) of
                %% Dependencies met.
                true ->
                    ?LOG_DEBUG(#{
                        description => "Message dependencies met, delivering",
                        messages => MessageClock
                    }),
                    deliver(State0#state{buffered_messages=BufferedMessages -- [FullMessage]}, IncomingOrderBuffer, MessageClock, ServerRef, Message);
                %% Dependencies NOT met.
                false ->
                    %% Buffer, for later delivery.
                    ?LOG_DEBUG(#{
                        description => "Message dependencies NOT met, delivering",
                        messages => MessageClock,
                        dependencies => DependencyClock
                    }),
                    State0#state{buffered_messages=BufferedMessages}
            end
    end.

%% @private
generate_name(Label) ->
    list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Label)).

%% @private
write_state(#state{storage=Storage}=State) ->
    true = ets:insert(Storage, {state, State}),
    State.
