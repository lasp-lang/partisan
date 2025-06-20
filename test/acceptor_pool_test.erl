-module(acceptor_pool_test).

-behaviour(acceptor_pool).
-behaviour(acceptor).

-export([start_link/1]).

-export([init/1]).

-export([acceptor_init/3,
         acceptor_continue/3,
         acceptor_terminate/2]).

start_link(Spec) ->
    acceptor_pool:start_link(?MODULE, Spec).

init(Spec) when is_map(Spec) ->
    {ok, {#{}, [Spec]}};
init(ignore) ->
    ignore;
init(Fun) when is_function(Fun, 0) ->
    init(Fun()).

acceptor_init(_, _, {ok, trap_exit}) ->
    _ = process_flag(trap_exit, true),
    {ok, trap};
acceptor_init(_, _, Return) ->
    Return.

acceptor_continue(_, Socket, _) ->
    loop(Socket).

acceptor_terminate(_, _) ->
    ok.

loop(Socket) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            _ = gen_tcp:send(Socket, Data),
            loop(Socket);
        {error, Reason} ->
            error(Reason, [Socket])
    end.