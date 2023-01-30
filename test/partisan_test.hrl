
-define(TIMEOUT, 10000).
-define(CLIENT_NUMBER, 3).
-define(HIGH_CLIENT_NUMBER, 10).
-define(PAUSE_FOR_CLUSTERING, timer:sleep(5000)).
-define(PAUSE_FOR_CLUSTERING(T), timer:sleep(T)).

-define(PUT_NODES(L), persistent_term:put({?FUNCTION_NAME, nodes}, L)).
-define(TAKE_NODES(Case),
    case persistent_term:get({Case, nodes}, undefined) of
        undefined ->
            ok;
        Nodes ->
            _ = persistent_term:erase({Case, nodes}),
            Nodes
    end
).