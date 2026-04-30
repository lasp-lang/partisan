-module(app_faulty_server).
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% Intentionally call an undefined function to trigger the
    %% expected `undef` failure in faulty_application_shutdown/1.
    an_undefined_module_with:an_undefined_function(argument1, argument2),
    {ok, []}.

handle_call(_Req, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State) -> {noreply, State}.
