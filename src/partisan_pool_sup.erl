-module(partisan_pool_sup).

-behaviour(supervisor).

%% public api

-export([start_link/0]).

%% supervisor api

-export([init/1]).

%% public api

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% supervisor api

init([]) ->
    Flags = #{strategy => rest_for_one},
    Pool = pool(),
    Sockets = [socket(ListenAddr) || ListenAddr <- partisan_config:listen_addrs()],
    {ok, {Flags, lists:flatten([Pool, Sockets])}}.

%% @private
socket({Label, PeerIP, PeerPort}) ->
    #{id => partisan_socket,
      start => {partisan_socket, start_link, [Label, PeerIP, PeerPort]}}.

%% @private
pool() ->
    #{id => partisan_pool,
      start => {partisan_pool, start_link, []}}.
