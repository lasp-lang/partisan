-module(thing2).
-export([handle_info/2]).
handle_info({message1, _A}, State) ->
    ok;
handle_info({message2, _A}, State) ->
    ok.