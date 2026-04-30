-module(app_faulty).
-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _Args) ->
    app_faulty_sup:start_link().

stop(_State) ->
    ok.
