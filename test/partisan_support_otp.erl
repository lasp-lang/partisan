-module(partisan_support_otp).


-compile([nowarn_export_all, export_all]).


start_node(Name) ->
    start_node(Name, []).


start_node(Name, Config) ->
    Prefix = string:join([atom_to_list(Name), "server"], "_"),
    Result = partisan_support:start(Prefix, Config, [
        {peer_service_manager, partisan_pluggable_peer_service_manager},
        {servers, partisan_support:node_list(1, Prefix, [])},
        {clients, []}
    ]),
    case Result of
        [] ->
            ct:fail("Couldn't start peer");
        [{_, PeerNode}] ->
            _ = put({?MODULE, nodes}, [PeerNode]),
            {ok, PeerNode}
    end.


stop_node(Name) ->
    stop_nodes([Name]).


stop_nodes(ToStop) ->
    case get({?MODULE, nodes}) of
        undefined ->
            ok;
        Nodes ->
            Remaining = lists:subtract(Nodes, [ToStop]),
            _ = put({?MODULE, nodes}, Remaining),
            partisan_support:stop(Nodes),
            ok
    end.


stop_all_nodes() ->
    case get({?MODULE, nodes}) of
        undefined ->
            ok;
        Nodes ->
            partisan_support:stop(Nodes),
            _ = erase({?MODULE, nodes}),
            ok
    end.

