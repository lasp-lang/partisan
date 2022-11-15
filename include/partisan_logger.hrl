-include_lib("kernel/include/logger.hrl").

-define(TRACING, false).

-define(LOG_TRACE(A),
    case partisan_config:get(tracing, ?TRACING) of
        true ->
            ?LOG_INFO(A, #{domain => trace});
        false ->
            ok
    end
).

-define(LOG_TRACE(A, B),
    case partisan_config:get(tracing, ?TRACING) of
        true ->
            ?LOG_INFO(A, B, #{domain => trace});
        false ->
            ok
    end
).

-define(LOG_TRACE(A, B, C),
    case partisan_config:get(tracing, ?TRACING) of
        true ->
            ?LOG_INFO(A, B, C#{domain => trace});
        false ->
            ok
    end
).


-define(LOG_TRACE_IF(F, A),
    case F of
        true ->
            ?LOG_TRACE(A);
        false ->
            ok
    end
).

-define(LOG_TRACE_IF(F, A, B),
    case F of
        true ->
            ?LOG_TRACE(A, B);
        false ->
            ok
    end
).

-define(LOG_TRACE_IF(F, A, B, C),
    case F of
        true ->
            ?LOG_TRACE(A, B, C);
        false ->
            ok
    end
).