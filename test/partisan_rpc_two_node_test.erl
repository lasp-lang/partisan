%% =============================================================================
%% @doc End-to-end proof for encoded reply destinations.
%%
%% Two isolated nodes join over TCP; a remote `partisan_rpc:call/5' replies to
%% the caller's one-shot alias reference.
%% @end
%% =============================================================================
-module(partisan_rpc_two_node_test).

-include_lib("eunit/include/eunit.hrl").

-export([echo/1]).

rpc_two_node_test_() ->
    {timeout, 120,
        {setup, fun setup/0, fun cleanup/1, fun(Nodes) ->
            [?_test(reply_reaches_remote_caller(Nodes))]
        end}}.

setup() ->
    P1 = start_peer(),
    P2 = start_peer(),
    S1 = peer:call(P1, partisan, node_spec, [], 10000),
    S2 = peer:call(P2, partisan, node_spec, [], 10000),
    %% Join both ways: the reply uses the callee's outgoing connection.
    ok = peer:call(P2, partisan_peer_service, join, [S1], 10000),
    ok = peer:call(P1, partisan_peer_service, join, [S2], 10000),
    ok = await_connected(P2, maps:get(name, S1), 50),
    ok = await_connected(P1, maps:get(name, S2), 50),
    {P1, P2}.

cleanup({P1, P2}) ->
    _ = catch peer:stop(P1),
    _ = catch peer:stop(P2),
    ok.

reply_reaches_remote_caller({P1, P2}) ->
    Node1 = peer:call(P1, partisan, node, [], 10000),
    Node2 = peer:call(P2, partisan, node, [], 10000),
    ?assertNotEqual(Node1, Node2),
    Reply = peer:call(
        P2,
        partisan_rpc,
        call,
        [Node1, ?MODULE, echo, [hello], 5000],
        10000
    ),
    ?assertEqual(hello, Reply).

echo(Value) ->
    Value.

start_peer() ->
    Paths = lists:append([["-pa", Dir] || Dir <- code:get_path()]),
    Opts = #{connection => standard_io, args => Paths},
    {ok, Peer, _} = peer:start_link(Opts),
    ok = peer:call(Peer, application, set_env, [
        partisan, peer_ip, {127, 0, 0, 1}
    ]),
    ok = peer:call(Peer, application, set_env, [
        partisan, peer_port, 20000 + rand:uniform(20000)
    ]),
    ok = peer:call(Peer, application, set_env, [
        partisan, peer_service_manager, partisan_hyparview_peer_service_manager
    ]),
    {ok, _} = peer:call(Peer, application, ensure_all_started, [partisan]),
    Peer.

await_connected(_Peer, _Node, 0) ->
    error(connection_timeout);
await_connected(Peer, Node, Attempts) ->
    case peer:call(Peer, partisan, is_connected, [Node], 5000) of
        true ->
            ok;
        false ->
            timer:sleep(100),
            await_connected(Peer, Node, Attempts - 1)
    end.
