%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Wu Shuwen
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
-module(partisan_causality_backend_test).

-include_lib("eunit/include/eunit.hrl").

received_order_buffer_is_forwarded_to_later_send_test() ->
    Local = node(),
    Destination = 'destination@host',
    Sender = 'sender@host',
    Label = causality_backend_order_buffer_test,
    OldName = partisan_config:get(name, undefined),
    ok = partisan_config:set(name, Local),
    {ok, Pid} = partisan_causality_backend:start_link(Label),
    try
        ok = partisan_causality_backend:set_delivery_fun(
            Label, fun(_ServerRef, _Message) -> ok end
        ),
        IncomingOrderBuffer = orddict:from_list([
            {Local, []},
            {Destination, [{Sender, 1}]}
        ]),
        ok = partisan_causality_backend:receive_message(
            Label,
            {causal, Label, Sender, self(), IncomingOrderBuffer,
                [{Sender, 1}], upstream}
        ),

        {ok, _Clock, {causal, Label, Destination, _ServerRef,
            ForwardedOrderBuffer, _MessageClock, downstream}} =
            partisan_causality_backend:emit(
                Label, Destination, self(), downstream
            ),
        ?assertEqual(
            [{Destination, [{Sender, 1}]}],
            ForwardedOrderBuffer
        )
    after
        gen_server:stop(Pid),
        ok = partisan_config:set(name, OldName)
    end.
