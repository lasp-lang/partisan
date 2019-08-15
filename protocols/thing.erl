-module(thing).
-export([init/0, thing/0, handle_info/2, finalthing/0]).

init() ->
    ok.

thing() ->
    Fun1 = fun() -> handle_info({message1_, 1}, undefined) end,
    Fun1(),

    Fun2 = fun() -> finalthing() end,
    Fun2(),

    lists:foreach(fun(X) -> X end, finalthing()),

    finalthing(),
    rand:seed().

handle_info({message1, _A}, _State) ->
    Fun = fun() -> 
        partisan_pluggable_peer_service_manager:forward_message(node(), undefined, ?MODULE, {error, txn1}, [])
    end,
    partisan_pluggable_peer_service_manager:forward_message(node(), undefined, ?MODULE, {prepare, txn1}, []),
    other_function(),
    Fun(),
    ok;
handle_info({message2, _A, _B}, _State) ->
    ok;
handle_info(message3, _State) ->
    Message = {some_other_message, 1},
    partisan_pluggable_peer_service_manager:forward_message(node(), undefined, ?MODULE, Message, []),
    ok;
handle_info(_, _) ->
    ok.

finalthing() ->
    [].

other_function() ->
    partisan_pluggable_peer_service_manager:forward_message(node(), undefined, ?MODULE, {abort, txn1}, []),
    ok.