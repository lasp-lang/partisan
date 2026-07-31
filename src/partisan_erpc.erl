%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2020-2021. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%
%%
%% Author: Rickard Green
%%

%% -----------------------------------------------------------------------------
%% @doc This module is an adaptation of Erlang `erpc' module.
%%
%% It replaces all instances of `erlang:send/2` and `erlang:monitor/2` with
%% their Partisan counterparts.
%%
%% It maintains the `erpc' API — every function `erpc' exports is exported here
%% with the same arity and the same contract (asserted by an export-parity test)
%% — and adds the following, all of which are supersets rather than changes:
%%
%% <ul>
%% <li><strong>Per-call transport options.</strong> Upstream `erpc' has no notion
%% of channels, so on its API every request would have to ride the globally
%% configured `forward_options'. `call/5' and `multicall/5' therefore accept
%% `forward_opts()' in place of a bare timeout (`timeout' is read out of the
%% map), and `send_request/5', `send_request/7', `cast/5' and `multicast/5' are
%% additional arities that do not exist upstream. Per-call keys win over the
%% global configuration; the global value fills in only what the caller omitted.
%% One consequence worth stating: on those two overloaded arities a *list* in the
%% fifth position is now read as a proplist of options rather than rejected as an
%% invalid timeout.</li>
%% <li><strong>Native transport.</strong> Upstream reaches the peer with the
%% `spawn_request/5' BIF and receives the result as a monitor's exit reason —
%% both distribution-protocol mechanisms that ride disterl. This module replaces
%% them with an explicit correlated request/response over Partisan (see
%% `partisan_rpc_backend'). The pure logic — error translation, `trim_stack/4',
%% `is_arg_error/4', `result/4' — is kept verbatim and diffable against
%% upstream; only the transport is native.</li>
%% <li><strong>Request-identifier collections.</strong> The vendored snapshot
%% predates the OTP 25+ collection API; it is implemented here natively, keyed by
%% each request's correlation reference.</li>
%% </ul>
%%
%% <strong>NOTICE:</strong>
%% At the moment this only works for `partisan_pluggable_peer_service_manager'.
%% @end
%% -----------------------------------------------------------------------------
%% -
-module(partisan_erpc).
-include("partisan.hrl").

-export([call/2]).
-export([call/3]).
-export([call/4]).
-export([call/5]).
-export([cast/2]).
-export([cast/4]).
-export([send_request/2]).
-export([send_request/4]).
-export([receive_response/1]).
-export([receive_response/2]).
-export([wait_response/1]).
-export([wait_response/2]).
-export([check_response/2]).
-export([multicall/2]).
-export([multicall/3]).
-export([multicall/4]).
-export([multicall/5]).
-export([multicast/2]).
-export([multicast/4]).
%% OTP 25+ request-identifier collection API. The module was vendored from
%% an OTP 23/24-era `erpc' and predates all of these; Partisan's floor is
%% OTP 27]). so without them idiomatic modern erpc fan-out code fails with
%% `undef'.
-export([send_request/6]).
-export([receive_response/3]).
-export([wait_response/3]).
-export([check_response/3]).
-export([reqids_new/0]).
-export([reqids_size/1]).
-export([reqids_add/3]).
-export([reqids_to_list/1]).
%% Partisan-specific arities. Upstream `erpc' has no notion of channels, so
%% every one of its surfaces would otherwise ride the globally configured
%% `forward_options' with no way to override it per call. `call/5' takes
%% `forward_opts()' in place of a bare timeout (upstream's arity, widened);
%% the surfaces below have no free argument to widen, so each gains an arity
%% that does not exist upstream. Export parity is unaffected — the module
%% only ever needs to be a *superset* of `erpc'.
-export([send_request/5]).
-export([send_request/7]).
-export([cast/5]).
-export([multicast/5]).

-export_type([request_id/0]).
-export_type([request_id_collection/0]).

%% Internal exports (also used by the 'rpc' module)
-export([execute_call/4]).
-export([execute_call/3]).
-export([execute_cast/3]).
-export([is_arg_error/4]).
-export([trim_stack/4]).
-export([call_result/4]).
%% Partisan-specific. Upstream `erpc' has no notion of channels, but
%% `partisan_rpc:call/5' accepts `forward_opts()' (`channel',
%% `partition_key', ...) and must keep honouring them now that it is a shim
%% over this module. Mirrors how OTP's `erpc' exports internals for `rpc'.
-export([call_with_opts/6]).

%% Nicer error stack trace...
-compile({inline, [{result, 4}]}).

%% Upstream `erpc' reaches the peer with the auto-imported `spawn_request/5'
%% BIF and receives the result as the exit reason of a distributed monitor.
%% Both are distribution-protocol mechanisms: they ride disterl, not the
%% Partisan transport. This module replaces them with an explicit correlated
%% request/response over Partisan (see `partisan_rpc_backend'), so any
%% remaining auto-imported call is a compile error rather than a silent
%% fallback to disterl.
-compile({no_auto_import, [spawn_request/5, spawn_request_abandon/1]}).

%% NOTE: `MAX_INT_TIMEOUT', `TIMEOUT_TYPE', `IS_VALID_TMO_INT' and
%% `IS_VALID_TMO' were defined here verbatim; they now come from
%% `partisan.hrl', which defines them identically.

%% =============================================================================
%% API
%% =============================================================================

-spec call(Node, Fun) -> Result when
    Node :: node(),
    Fun :: function(),
    Result :: term().

call(N, Fun) ->
    call(N, Fun, infinity).

-spec call(Node, Fun, Timeout) -> Result when
    Node :: node(),
    Fun :: function(),
    Timeout :: ?TIMEOUT_TYPE,
    Result :: term().

call(N, Fun, Timeout) when is_function(Fun, 0) ->
    call(N, erlang, apply, [Fun, []], Timeout);
call(_N, _Fun, _Timeout) ->
    error({?MODULE, badarg}).

-spec call(Node, Module, Function, Args) -> Result when
    Node :: node(),
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    Result :: term().

call(N, M, F, A) ->
    call(N, M, F, A, infinity).

-dialyzer([{nowarn_function, call/5}, no_return]).

-spec call(Node, Module, Function, Args, TimeoutOrOpts) -> Result when
    Node :: node(),
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    TimeoutOrOpts ::
        ?TIMEOUT_TYPE
        | partisan_peer_service_manager:forward_opts(),
    Result :: term().

call(N, M, F, A, T) when
    is_atom(M),
    is_atom(F),
    is_list(A),
    ?IS_VALID_TMO(T)
->
    %% Optimize local call
    case N == partisan:node() andalso T == infinity of
        true ->
            try
                {return, Return} = execute_call(M, F, A),
                Return
            catch
                exit:Reason ->
                    exit({exception, Reason});
                error:Reason:Stack ->
                    case is_arg_error(Reason, M, F, A) of
                        true ->
                            error({?MODULE, Reason});
                        false ->
                            ErpcStack = trim_stack(Stack, M, F, A),
                            error({exception, Reason, ErpcStack})
                    end
            end;
        false ->
            Res = new_res(),
            ReqId = send_erpc_request(N, Res, M, F, A),
            receive
                {?ERPC_REPLY, Res, Reply} ->
                    ok = finish(Res, ReqId),
                    result(down, ReqId, Res, Reply);
                {'DOWN', ReqId, process, _Pid, _Reason} ->
                    ok = release(Res),
                    result(down, ReqId, Res, noconnection)
            after T ->
                result(timeout, ReqId, Res, undefined)
            end
    end;
%% Partisan-specific overload: the fifth argument may be `forward_opts()'
%% instead of a bare timeout, mirroring `partisan_rpc:call/5'. Upstream `erpc'
%% has no notion of channels, so without this the primary RPC surface could
%% never select one and every request rode the globally configured
%% `forward_options'. `timeout' is read out of the map; everything else
%% (`channel', `partition_key', ...) is passed to the transport, with per-call
%% keys winning over the global configuration.
call(N, M, F, A, Opts) when is_list(Opts) ->
    call(N, M, F, A, maps:from_list(Opts));
call(N, M, F, A, Opts) when is_map(Opts) ->
    Timeout = maps:get(timeout, Opts, ?DEFAULT_TIMEOUT),
    ?IS_VALID_TMO(Timeout) orelse error({?MODULE, badarg}),
    call_with_opts(N, M, F, A, Timeout, partisan_rpc:forward_opts(Opts));
call(_N, _M, _F, _A, _T) ->
    error({?MODULE, badarg}).

%% Asynchronous call

%% The failure-detection monitor's reference. `partisan:monitor/2' returns a
%% plain `reference()' when the monitored process is local and an encoded remote
%% reference when it is not, so anything holding one has to admit both — a
%% request to the local node is the common case in tests and in single-node
%% deployments.
-type monitor_ref() :: reference() | partisan:remote_reference().

-opaque request_id() :: {
    Res :: partisan:remote_reference(), ReqId :: monitor_ref()
}.

-spec send_request(Node, Fun) -> RequestId when
    Node :: node(),
    Fun :: function(),
    RequestId :: request_id().

send_request(N, F) when is_function(F, 0) ->
    send_request(N, erlang, apply, [F, []]);
send_request(_N, _F) ->
    error({?MODULE, badarg}).

-spec send_request(Node, Module, Function, Args) -> RequestId when
    Node :: node(),
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    RequestId :: request_id().

send_request(N, M, F, A) when
    is_atom(N),
    is_atom(M),
    is_atom(F),
    is_list(A)
->
    Res = new_res(),
    ReqId = send_erpc_request(N, Res, M, F, A),
    {Res, ReqId};
%% Same arity, different shape: `send_request(Node, Fun, Label, Collection)'.
%% Upstream carries this clause too; the vendored snapshot predates it.
send_request(N, F, L, C) when is_atom(N), is_function(F, 0), is_map(C) ->
    send_request(N, erlang, apply, [F, []], L, C);
send_request(_N, _M, _F, _A) ->
    error({?MODULE, badarg}).

-doc false.
-spec send_request(Node, Module, Function, Args, Opts) -> RequestId when
    Node :: node(),
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    Opts :: partisan_peer_service_manager:forward_opts(),
    RequestId :: request_id().

%% Partisan-specific: as `send_request/4', but the request is forwarded with
%% caller-supplied `forward_opts()' (`channel', `partition_key', ...). Without
%% this the asynchronous surface — which is precisely the one used to issue many
%% concurrent requests, and therefore the one that most wants a dedicated
%% channel — could only ever use the globally configured `forward_options'.
send_request(N, M, F, A, Opts) when
    is_atom(N),
    is_atom(M),
    is_atom(F),
    is_list(A),
    (is_map(Opts) orelse is_list(Opts))
->
    Res = new_res(),
    ReqId = send_erpc_request(N, Res, M, F, A, forward_opts(Opts)),
    {Res, ReqId};
send_request(_N, _M, _F, _A, _Opts) ->
    error({?MODULE, badarg}).

%% @private
%% Issues a correlated request to `N' and returns a monitor reference.
%%
%% The monitor exists solely to detect that the target became unreachable — it
%% is deliberately *not* the channel the result travels on. In Partisan every
%% remote `DOWN' is relayed through the monitoring node's `partisan_monitor'
%% server (see `partisan_monitor:handle_info/2'), so carrying return values on
%% exit reasons the way upstream `erpc' does would funnel every RPC result on a
%% node through that single process. The result instead arrives as an ordinary
%% `?ERPC_REPLY' message tagged with `Res'.
send_erpc_request(N, Res, M, F, A) ->
    send_erpc_request(N, Res, M, F, A, default_forward_opts()).

%% @private
%% The transport options for a request whose caller supplied none: the globally
%% configured `forward_options', with a channel filled in if it names none.
default_forward_opts() ->
    partisan_rpc:prepare_opts(partisan_config:get(forward_options, [])).

send_erpc_request(N, Res, M, F, A, Opts) ->
    ReqId = partisan:monitor(process, {partisan_rpc_backend, N}),
    %% `Res' is an encoded process alias, so it is both the correlation
    %% reference the reply carries *and* the address the reply is sent to.
    %% Deactivating it (see `release/1') makes the runtime drop a late reply,
    %% which is how upstream `erpc' avoids leaving one in an abandoned caller's
    %% mailbox — it gets the same effect from `demonitor(_, [flush])'.
    Msg = {?ERPC_REQUEST, Res, Res, M, F, A},
    _ = partisan:forward_message(N, partisan_rpc_backend, Msg, Opts),
    ReqId.

%% @private
%% Resolves caller-supplied `forward_opts()' against the globally configured
%% `forward_options'. Per-call keys win; the global value only fills in what the
%% caller omitted. `timeout' is not a transport option and is dropped.
%%
%% Deliberately delegates to `partisan_rpc:forward_opts/1' rather than restating
%% the merge, so both RPC surfaces resolve options through exactly one function.
forward_opts(Opts) when is_list(Opts) ->
    forward_opts(maps:from_list(Opts));
forward_opts(Opts) when is_map(Opts) ->
    partisan_rpc:forward_opts(Opts).

%% @private
%% Makes a correlation reference that is also a one-shot reply address.
new_res() ->
    partisan_remote_ref:from_term(erlang:alias([explicit_unalias])).

%% @private
%% Retires a request's reply address. Any reply that arrives afterwards is
%% discarded by the runtime rather than accumulating in this process's mailbox.
release(Res) ->
    try erlang:unalias(partisan_remote_ref:to_term(Res)) of
        _ -> ok
    catch
        _:_ -> ok
    end.

%% @private
%% Terminal cleanup for a request: drop its failure-detection monitor and
%% retire its reply address.
finish(Res, ReqId) ->
    _ = partisan:demonitor(ReqId, [flush]),
    release(Res).

-doc false.
-spec call_with_opts(
    Node :: node(),
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    Timeout :: ?TIMEOUT_TYPE,
    Opts :: map()
) -> term().

%% As `call/5', but the request is forwarded with caller-supplied
%% `forward_opts()' (channel, partition key, ...). Partisan-specific; see the
%% note on the internal export list.
call_with_opts(N, M, F, A, T, Opts) when
    is_atom(M), is_atom(F), is_list(A), ?IS_VALID_TMO(T)
->
    case N == partisan:node() andalso T == infinity of
        true ->
            call(N, M, F, A, T);
        false ->
            Res = new_res(),
            ReqId = send_erpc_request(N, Res, M, F, A, Opts),
            receive
                {?ERPC_REPLY, Res, Reply} ->
                    ok = finish(Res, ReqId),
                    result(down, ReqId, Res, Reply);
                {'DOWN', ReqId, process, _Pid, _Reason} ->
                    ok = release(Res),
                    result(down, ReqId, Res, noconnection)
            after T ->
                result(timeout, ReqId, Res, undefined)
            end
    end;
call_with_opts(_N, _M, _F, _A, _T, _Opts) ->
    error({?MODULE, badarg}).

-spec receive_response(RequestId) -> Result when
    RequestId :: request_id(),
    Result :: term().

receive_response({Res, ReqId} = RId) ->
    partisan:is_reference(Res) andalso partisan:is_reference(ReqId) orelse
        error({?MODULE, badarg}),
    receive_response(RId, infinity).

-dialyzer([{nowarn_function, receive_response/2}, no_return]).

-spec receive_response(RequestId, Timeout) -> Result when
    RequestId :: request_id(),
    Timeout :: ?TIMEOUT_TYPE,
    Result :: term().

receive_response({Res, ReqId}, Tmo) when ?IS_VALID_TMO(Tmo) ->
    partisan:is_reference(Res) andalso partisan:is_reference(ReqId) orelse
        error({?MODULE, badarg}),

    receive
        {?ERPC_REPLY, Res, Reply} ->
            ok = finish(Res, ReqId),
            result(down, ReqId, Res, Reply);
        {'DOWN', ReqId, process, _Pid, _Reason} ->
            %% The target's backend is gone or the peer is unreachable. Either
            %% way the request cannot be answered; `noconnection' is the
            %% reason `erpc' specifies for this.
            ok = release(Res),
            result(down, ReqId, Res, noconnection)
    after Tmo ->
        result(timeout, ReqId, Res, undefined)
    end;
receive_response(_, _) ->
    error({?MODULE, badarg}).

-spec wait_response(RequestId) -> {'response', Result} | 'no_response' when
    RequestId :: request_id(),
    Result :: term().

wait_response({Res, ReqId} = RId) ->
    partisan:is_reference(Res) andalso partisan:is_reference(ReqId) orelse
        error(function_clause),
    wait_response(RId, 0).

-dialyzer([{nowarn_function, wait_response/2}, no_return]).

-spec wait_response(RequestId, WaitTime) ->
    {'response', Result} | 'no_response'
when
    RequestId :: request_id(),
    WaitTime :: ?TIMEOUT_TYPE,
    Result :: term().

wait_response({Res, ReqId}, WT) when ?IS_VALID_TMO(WT) ->
    partisan:is_reference(Res) andalso partisan:is_reference(ReqId) orelse
        error({?MODULE, badarg}),

    receive
        {?ERPC_REPLY, Res, Reply} ->
            ok = finish(Res, ReqId),
            {response, result(down, ReqId, Res, Reply)};
        {'DOWN', ReqId, process, _Pid, _Reason} ->
            ok = release(Res),
            {response, result(down, ReqId, Res, noconnection)}
    after WT ->
        no_response
    end;
wait_response(_, _) ->
    error({?MODULE, badarg}).

-dialyzer([{nowarn_function, check_response/2}, no_return]).

-spec check_response(Message, RequestId) ->
    {'response', Result} | 'no_response'
when
    Message :: term(),
    RequestId :: request_id(),
    Result :: term().

check_response({?ERPC_REPLY, Res, Reply}, {Res, ReqId}) ->
    partisan:is_reference(Res) andalso partisan:is_reference(ReqId) orelse
        error({?MODULE, badarg}),
    ok = finish(Res, ReqId),
    {response, result(down, ReqId, Res, Reply)};
check_response({'DOWN', ReqId, process, _Pid, _Reason}, {Res, ReqId}) ->
    partisan:is_reference(Res) andalso partisan:is_reference(ReqId) orelse
        error({?MODULE, badarg}),
    ok = release(Res),
    {response, result(down, ReqId, Res, noconnection)};
check_response(_Msg, {Res, ReqId}) ->
    partisan:is_reference(Res) andalso partisan:is_reference(ReqId) orelse
        error({?MODULE, badarg}),
    no_response;
check_response(_, _) ->
    error({?MODULE, badarg}).

%% =============================================================================
%% REQUEST IDENTIFIER COLLECTIONS (OTP 25+)
%% =============================================================================

%% Upstream represents a `request_id()' as the improper list `[Res|ReqId]' and
%% keys the collection by `ReqId'. This module keeps the vendored `{Res, ReqId}'
%% tuple and keys the collection by `Res' — the correlation reference, which is
%% what a reply carries. `DOWN' signals carry `ReqId' instead, so the receive
%% loops below build a small reverse index to guard on.

-type request_id_collection() :: #{
    Res ::
        partisan:remote_reference() => {ReqId :: monitor_ref(), Label :: term()}
}.

-spec reqids_new() -> request_id_collection().

reqids_new() ->
    maps:new().

-spec reqids_size(request_id_collection()) -> non_neg_integer().

reqids_size(ReqIdCollection) ->
    try
        maps:size(ReqIdCollection)
    catch
        _:_ ->
            error({?MODULE, badarg})
    end.

-spec reqids_add(request_id(), term(), request_id_collection()) ->
    request_id_collection().

reqids_add({Res, _ReqId}, _, ReqIdCollection) when
    is_map_key(Res, ReqIdCollection)
->
    error({?MODULE, badarg});
reqids_add({Res, ReqId}, Label, ReqIdCollection) when
    is_map(ReqIdCollection)
->
    %% `partisan:is_reference/1' is not a guard BIF (a Partisan reference is a
    %% term, not an Erlang reference), so this is checked in the body.
    partisan:is_reference(Res) andalso partisan:is_reference(ReqId) orelse
        error({?MODULE, badarg}),
    maps:put(Res, {ReqId, Label}, ReqIdCollection);
reqids_add(_, _, _) ->
    error({?MODULE, badarg}).

-spec reqids_to_list(request_id_collection()) ->
    [{request_id(), Label :: term()}].

reqids_to_list(ReqIdCollection) when is_map(ReqIdCollection) ->
    try
        maps:fold(
            fun
                (Res, {ReqId, Label}, Acc) ->
                    [{{Res, ReqId}, Label} | Acc];
                (_, _, _) ->
                    throw(badarg)
            end,
            [],
            ReqIdCollection
        )
    catch
        throw:badarg ->
            error({?MODULE, badarg})
    end;
reqids_to_list(_) ->
    error({?MODULE, badarg}).

-spec send_request(
    Node :: node(),
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    Label :: term(),
    request_id_collection()
) -> request_id_collection().

send_request(N, M, F, A, L, C) when
    is_atom(N),
    is_atom(M),
    is_atom(F),
    is_list(A),
    is_map(C)
->
    Res = new_res(),
    ReqId = send_erpc_request(N, Res, M, F, A),
    maps:put(Res, {ReqId, L}, C);
send_request(_N, _M, _F, _A, _L, _C) ->
    error({?MODULE, badarg}).

-doc false.
-spec send_request(
    Node :: node(),
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    Label :: term(),
    request_id_collection(),
    Opts :: partisan_peer_service_manager:forward_opts()
) -> request_id_collection().

%% Partisan-specific: `send_request/6' with caller-supplied `forward_opts()'.
%% This is the collection form — the shape used for high-fan-out concurrent RPC,
%% where directing the traffic at a dedicated channel matters most.
send_request(N, M, F, A, L, C, Opts) when
    is_atom(N),
    is_atom(M),
    is_atom(F),
    is_list(A),
    is_map(C),
    (is_map(Opts) orelse is_list(Opts))
->
    Res = new_res(),
    ReqId = send_erpc_request(N, Res, M, F, A, forward_opts(Opts)),
    maps:put(Res, {ReqId, L}, C);
send_request(_N, _M, _F, _A, _L, _C, _Opts) ->
    error({?MODULE, badarg}).

-dialyzer([{nowarn_function, receive_response/3}, no_return]).

-spec receive_response(
    request_id_collection(), Timeout :: ?TIMEOUT_TYPE, Delete :: boolean()
) -> {Result :: term(), Label :: term(), request_id_collection()}.

receive_response(ReqIdCol, Tmo, Del) when is_map(ReqIdCol), is_boolean(Del) ->
    Timeout = timeout_value(Tmo),
    RevIdx = reqids_rev_index(ReqIdCol),
    receive
        {?ERPC_REPLY, Res, Reply} when is_map_key(Res, ReqIdCol) ->
            collection_result(down, Res, Reply, ReqIdCol, false, Del);
        {'DOWN', ReqId, process, _Pid, _Reason} when
            is_map_key(ReqId, RevIdx)
        ->
            Res = maps:get(ReqId, RevIdx),
            ok = release(Res),
            collection_result(down, Res, noconnection, ReqIdCol, false, Del)
    after Timeout ->
        collection_result(timeout, ok, ok, ReqIdCol, false, Del)
    end;
receive_response(_, _, _) ->
    error({?MODULE, badarg}).

-dialyzer([{nowarn_function, wait_response/3}, no_return]).

-spec wait_response(
    request_id_collection(), WaitTime :: ?TIMEOUT_TYPE, Delete :: boolean()
) ->
    {{'response', Result :: term()}, Label :: term(), request_id_collection()}
    | 'no_response'.

wait_response(ReqIdCol, Tmo, Del) when is_map(ReqIdCol), is_boolean(Del) ->
    Timeout = timeout_value(Tmo),
    RevIdx = reqids_rev_index(ReqIdCol),
    receive
        {?ERPC_REPLY, Res, Reply} when is_map_key(Res, ReqIdCol) ->
            collection_result(down, Res, Reply, ReqIdCol, true, Del);
        {'DOWN', ReqId, process, _Pid, _Reason} when
            is_map_key(ReqId, RevIdx)
        ->
            Res = maps:get(ReqId, RevIdx),
            ok = release(Res),
            collection_result(down, Res, noconnection, ReqIdCol, true, Del)
    after Timeout ->
        no_response
    end;
wait_response(_, _, _) ->
    error({?MODULE, badarg}).

-dialyzer([{nowarn_function, check_response/3}, no_return]).

-spec check_response(
    Message :: term(), request_id_collection(), Delete :: boolean()
) ->
    {{'response', Result :: term()}, Label :: term(), request_id_collection()}
    | 'no_response'.

check_response({?ERPC_REPLY, Res, Reply}, ReqIdCol, Del) when
    is_map_key(Res, ReqIdCol), is_boolean(Del)
->
    collection_result(down, Res, Reply, ReqIdCol, true, Del);
check_response({'DOWN', ReqId, process, _Pid, _Reason}, ReqIdCol, Del) when
    is_map(ReqIdCol), is_boolean(Del)
->
    case maps:get(ReqId, reqids_rev_index(ReqIdCol), undefined) of
        undefined ->
            no_response;
        Res ->
            ok = release(Res),
            collection_result(down, Res, noconnection, ReqIdCol, true, Del)
    end;
check_response(_Msg, ReqIdCol, Del) when is_map(ReqIdCol), is_boolean(Del) ->
    no_response;
check_response(_, _, _) ->
    error({?MODULE, badarg}).

%% @private
%% `ReqId => Res'. Replies are keyed by the correlation reference, but `DOWN'
%% signals carry the monitor reference, so the receive loops need to recognise
%% both.
reqids_rev_index(ReqIdCol) ->
    maps:fold(
        fun(Res, {ReqId, _Label}, Acc) -> Acc#{ReqId => Res} end,
        #{},
        ReqIdCol
    ).

%% @private
collection_result(timeout, _, _, ReqIdCollection, _, _) ->
    Abandon = fun(Res, {ReqId, _Label}) ->
        _ = call_abandon(ReqId),
        release(Res)
    end,
    try
        maps:foreach(Abandon, ReqIdCollection)
    catch
        _:_ -> error({?MODULE, badarg})
    end,
    error({?MODULE, timeout});
collection_result(Type, Res, ResultReason, ReqIdCol, WrapResponse, Delete) ->
    ReqIdInfo =
        case Delete of
            true -> maps:take(Res, ReqIdCol);
            false -> {maps:get(Res, ReqIdCol), ReqIdCol}
        end,
    case ReqIdInfo of
        {{ReqId, Label}, NewReqIdCol} ->
            ok = finish(Res, ReqId),
            try
                Result = result(Type, ReqId, Res, ResultReason),
                Response =
                    if
                        WrapResponse -> {response, Result};
                        true -> Result
                    end,
                {Response, Label, NewReqIdCol}
            catch
                Class:Reason ->
                    erlang:Class({Reason, Label, NewReqIdCol})
            end;
        _ ->
            %% Invalid request id collection...
            error({?MODULE, badarg})
    end.

%% @private
timeout_value(infinity) ->
    infinity;
timeout_value(Timeout) when ?IS_VALID_TMO_INT(Timeout) ->
    Timeout;
timeout_value({abs, Timeout}) when is_integer(Timeout) ->
    case Timeout - erlang:monotonic_time(millisecond) of
        TMO when TMO < 0 -> 0;
        TMO when TMO > ?MAX_INT_TIMEOUT -> error({?MODULE, badarg});
        TMO -> TMO
    end;
timeout_value(_) ->
    error({?MODULE, badarg}).

-type stack_item() ::
    {
        Module :: atom(),
        Function :: atom(),
        Arity :: arity() | (Args :: [term()]),
        Location :: [
            {file, Filename :: string()}
            | {line, Line :: pos_integer()}
        ]
    }.

-type caught_call_exception() ::
    {throw, Throw :: term()}
    | {exit, {exception, Reason :: term()}}
    | {error, {exception, Reason :: term(), StackTrace :: [stack_item()]}}
    | {exit, {signal, Reason :: term()}}
    | {error, {?MODULE, Reason :: term()}}.

-spec multicall(Nodes, Fun) -> Result when
    Nodes :: [atom()],
    Fun :: function(),
    Result :: term().

multicall(Ns, Fun) ->
    multicall(Ns, Fun, infinity).

-spec multicall(Nodes, Fun, Timeout) -> Result when
    Nodes :: [atom()],
    Fun :: function(),
    Timeout :: ?TIMEOUT_TYPE,
    Result :: term().

multicall(Ns, Fun, Timeout) when is_function(Fun, 0) ->
    multicall(Ns, erlang, apply, [Fun, []], Timeout);
multicall(_Ns, _Fun, _Timeout) ->
    error({?MODULE, badarg}).

-spec multicall(Nodes, Module, Function, Args) -> Result when
    Nodes :: [atom()],
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    Result :: [{ok, ReturnValue :: term()} | caught_call_exception()].

multicall(Ns, M, F, A) ->
    multicall(Ns, M, F, A, infinity).

-spec multicall(Nodes, Module, Function, Args, TimeoutOrOpts) -> Result when
    Nodes :: [atom()],
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    TimeoutOrOpts ::
        ?TIMEOUT_TYPE
        | partisan_peer_service_manager:forward_opts(),
    Result :: [{ok, ReturnValue :: term()} | caught_call_exception()].

%% Partisan-specific overload, mirroring `call/5': the fifth argument may be
%% `forward_opts()' instead of a bare timeout, so a fan-out can be directed at a
%% channel of its own. `timeout' is read out of the map; everything else goes to
%% the transport.
multicall(Ns, M, F, A, Opts) when is_map(Opts) orelse is_list(Opts) ->
    Opts1 = opts_as_map(Opts),
    T = maps:get(timeout, Opts1, ?DEFAULT_TIMEOUT),
    ?IS_VALID_TMO(T) orelse error({?MODULE, badarg}),
    do_multicall(Ns, M, F, A, T, forward_opts(Opts1));
multicall(Ns, M, F, A, T) ->
    do_multicall(Ns, M, F, A, T, default_forward_opts()).

%% @private
do_multicall(Ns, M, F, A, T, Opts) ->
    try
        true = is_atom(M),
        true = is_atom(F),
        true = is_list(A),
        Tag = partisan:make_ref(),
        SendState = mcall_send_requests(Tag, Ns, M, F, A, T, Opts),
        mcall_receive_replies(Tag, SendState)
    catch
        error:NotIErr when NotIErr /= internal_error ->
            error({?MODULE, badarg})
    end.

%% @private
opts_as_map(Opts) when is_list(Opts) ->
    maps:from_list(Opts);
opts_as_map(Opts) when is_map(Opts) ->
    Opts.

-spec multicast(Nodes, Fun) -> 'ok' when
    Nodes :: [node()],
    Fun :: function().

multicast(N, Fun) ->
    multicast(N, erlang, apply, [Fun, []]).

-spec multicast(Nodes, Module, Function, Args) -> 'ok' when
    Nodes :: [node()],
    Module :: atom(),
    Function :: atom(),
    Args :: [term()].

multicast(Nodes, Mod, Fun, Args) ->
    try
        true = is_atom(Mod),
        true = is_atom(Fun),
        true = is_list(Args),
        multicast_send_requests(Nodes, Mod, Fun, Args)
    catch
        error:_ ->
            error({?MODULE, badarg})
    end.

-doc false.
-spec multicast(Nodes, Module, Function, Args, Opts) -> 'ok' when
    Nodes :: [node()],
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    Opts :: partisan_peer_service_manager:forward_opts().

%% Partisan-specific: `multicast/4' with caller-supplied `forward_opts()'.
multicast(Nodes, Mod, Fun, Args, Opts0) when is_map(Opts0); is_list(Opts0) ->
    try
        true = is_atom(Mod),
        true = is_atom(Fun),
        true = is_list(Args),
        multicast_send_requests(Nodes, Mod, Fun, Args, forward_opts(Opts0))
    catch
        error:_ ->
            error({?MODULE, badarg})
    end;
multicast(_Nodes, _Mod, _Fun, _Args, _Opts) ->
    error({?MODULE, badarg}).

multicast_send_requests(Nodes, Mod, Fun, Args) ->
    multicast_send_requests(Nodes, Mod, Fun, Args, default_forward_opts()).

multicast_send_requests([], _Mod, _Fun, _Args, _Opts) ->
    ok;
multicast_send_requests([Node | Nodes], Mod, Fun, Args, Opts) ->
    _ = send_erpc_cast(Node, Mod, Fun, Args, Opts),
    multicast_send_requests(Nodes, Mod, Fun, Args, Opts).

%% @private
%% Fire-and-forget: no correlation reference, no monitor, no reply. The target
%% still runs the call in a worker process of its own.
send_erpc_cast(Node, Mod, Fun, Args, Opts) ->
    Msg = {?ERPC_CAST, Mod, Fun, Args},
    partisan:forward_message(Node, partisan_rpc_backend, Msg, Opts).

-spec cast(Node, Fun) -> 'ok' when
    Node :: node(),
    Fun :: function().

cast(N, Fun) ->
    cast(N, erlang, apply, [Fun, []]).

-spec cast(Node, Module, Function, Args) -> 'ok' when
    Node :: node(),
    Module :: atom(),
    Function :: atom(),
    Args :: [term()].

cast(Node, Mod, Fun, Args) when
    is_atom(Node),
    is_atom(Mod),
    is_atom(Fun),
    is_list(Args)
->
    _ = send_erpc_cast(Node, Mod, Fun, Args, default_forward_opts()),
    ok;
cast(_Node, _Mod, _Fun, _Args) ->
    error({?MODULE, badarg}).

-doc false.
-spec cast(Node, Module, Function, Args, Opts) -> 'ok' when
    Node :: node(),
    Module :: atom(),
    Function :: atom(),
    Args :: [term()],
    Opts :: partisan_peer_service_manager:forward_opts().

%% Partisan-specific: `cast/4' with caller-supplied `forward_opts()'.
cast(Node, Mod, Fun, Args, Opts) when
    is_atom(Node),
    is_atom(Mod),
    is_atom(Fun),
    is_list(Args),
    (is_map(Opts) orelse is_list(Opts))
->
    _ = send_erpc_cast(Node, Mod, Fun, Args, forward_opts(Opts)),
    ok;
cast(_Node, _Mod, _Fun, _Args, _Opts) ->
    error({?MODULE, badarg}).

%%------------------------------------------------------------------------
%% Exported internals
%%------------------------------------------------------------------------

%% Note that most of these are used by 'rpc' as well...

execute_call(Ref, M, F, A) ->
    Reply =
        try
            {Ref, return, apply(M, F, A)}
        catch
            throw:Reason ->
                {Ref, throw, Reason};
            exit:Reason ->
                {Ref, exit, Reason};
            error:Reason:Stack ->
                case is_arg_error(Reason, M, F, A) of
                    true ->
                        {Ref, error, {?MODULE, Reason}};
                    false ->
                        ErpcStack = trim_stack(Stack, M, F, A),
                        {Ref, error, Reason, ErpcStack}
                end
        end,
    exit(Reply).

execute_call(M, F, A) ->
    {return, apply(M, F, A)}.

execute_cast(M, F, A) ->
    try
        apply(M, F, A)
    catch
        error:Reason:Stack ->
            %% Produce error reports with error
            %% exceptions produced for calls...
            case is_arg_error(Reason, M, F, A) of
                true ->
                    error({?MODULE, Reason});
                false ->
                    ErpcStack = trim_stack(Stack, M, F, A),
                    error({exception, {Reason, ErpcStack}})
            end
    end.

call_result(Type, ReqId, Res, Reason) ->
    result(Type, ReqId, Res, Reason).

is_arg_error(system_limit, _M, _F, A) ->
    try
        apply(?MODULE, nonexisting, A),
        false
    catch
        error:system_limit -> true;
        _:_ -> false
    end;
is_arg_error(_R, _M, _F, _A) ->
    false.

-define(IS_CUT_FRAME(F),
    ((element(1, (F)) == ?MODULE) andalso
        ((element(2, (F)) == execute_call) orelse
            (element(2, (F)) == execute_cast)))
).

trim_stack([CF | _], M, F, A) when ?IS_CUT_FRAME(CF) ->
    [{M, F, A, []}];
trim_stack([{M, F, A, _} = SF, CF | _], M, F, A) when ?IS_CUT_FRAME(CF) ->
    [SF];
trim_stack(S, M, F, A) ->
    try
        trim_stack_aux(S, M, F, A)
    catch
        throw:use_all -> S
    end.

%%------------------------------------------------------------------------
%% Internals
%%------------------------------------------------------------------------

trim_stack_aux([], _M, _F, _A) ->
    throw(use_all);
trim_stack_aux([{M, F, AL, _} = SF, CF | _], M, F, A) when
    ?IS_CUT_FRAME(CF),
    AL == length(A)
->
    [SF];
trim_stack_aux([CF | _], M, F, A) when ?IS_CUT_FRAME(CF) ->
    try
        [{M, F, length(A), []}]
    catch
        _:_ ->
            []
    end;
trim_stack_aux([SF | SFs], M, F, A) ->
    [SF | trim_stack_aux(SFs, M, F, A)].

%% @private
%% Abandons a request's failure-detection monitor. There is no remote-spawn
%% request to withdraw (upstream's `spawn_request_abandon/1'): the worker on
%% the target runs to completion and its reply, if any, is discarded by the
%% caller.
call_abandon(ReqId) ->
    partisan:demonitor(ReqId, [flush]).

-dialyzer([{nowarn_function, result/4}, no_return]).

-spec result
    ('down', ReqId, Res, Reason) -> term() when
        ReqId :: monitor_ref(),
        Res :: partisan:remote_reference(),
        Reason :: term();
    ('spawn_reply', ReqId, Res, Reason) -> no_return() when
        ReqId :: monitor_ref(),
        Res :: partisan:remote_reference(),
        Reason :: term();
    ('timeout', ReqId, Res, Reason) -> term() when
        ReqId :: monitor_ref(),
        Res :: partisan:remote_reference(),
        Reason :: term().

result(down, _ReqId, Res, {Res, return, Return}) ->
    Return;
result(down, _ReqId, Res, {Res, throw, Throw}) ->
    throw(Throw);
result(down, _ReqId, Res, {Res, exit, Exit}) ->
    exit({exception, Exit});
result(down, _ReqId, Res, {Res, error, Error, Stack}) ->
    error({exception, Error, Stack});
result(down, _ReqId, Res, {Res, error, {?MODULE, _} = ErpcErr}) ->
    error(ErpcErr);
result(down, _ReqId, _Res, noconnection) ->
    error({?MODULE, noconnection});
result(down, _ReqId, _Res, Reason) ->
    exit({signal, Reason});
result(spawn_reply, _ReqId, _Res, Reason) ->
    error({?MODULE, Reason});
result(timeout, ReqId, Res, _Reason) ->
    %% Drop the failure-detection monitor, then check whether the reply raced
    %% in while we were timing out. If it did, honour it rather than reporting
    %% a timeout for a request that was in fact answered.
    _ = call_abandon(ReqId),
    %% Retire the reply address first, then take a reply that already raced in.
    %% Anything arriving after this point is dropped by the runtime.
    ok = release(Res),
    receive
        {?ERPC_REPLY, Res, Reply} ->
            result(down, ReqId, Res, Reply)
    after 0 ->
        error({?MODULE, timeout})
    end.

deadline(infinity) ->
    infinity;
deadline(?MAX_INT_TIMEOUT) ->
    erlang:convert_time_unit(
        erlang:monotonic_time(millisecond) +
            ?MAX_INT_TIMEOUT,
        millisecond,
        native
    );
deadline(T) when ?IS_VALID_TMO_INT(T) ->
    Now = erlang:monotonic_time(),
    NativeTmo = erlang:convert_time_unit(T, millisecond, native),
    Now + NativeTmo.

time_left(infinity) ->
    infinity;
time_left(expired) ->
    0;
time_left(Deadline) ->
    case Deadline - erlang:monotonic_time() of
        TimeLeft when TimeLeft =< 0 ->
            0;
        TimeLeft ->
            erlang:convert_time_unit(TimeLeft - 1, native, millisecond) + 1
    end.

mcall_local_call(M, F, A) ->
    try
        {return, Return} = execute_call(M, F, A),
        {ok, Return}
    catch
        throw:Thrown ->
            {throw, Thrown};
        exit:Reason ->
            {exit, {exception, Reason}};
        error:Reason:Stack ->
            case is_arg_error(Reason, M, F, A) of
                true ->
                    {error, {?MODULE, Reason}};
                false ->
                    ErpcStack = trim_stack(Stack, M, F, A),
                    {error, {exception, Reason, ErpcStack}}
            end
    end.

mcall_send_request(T, N, M, F, A, Opts) when
    is_atom(N),
    is_atom(M),
    is_atom(F),
    is_list(A)
->
    %% we fail To simulate original behaviour
    partisan:is_reference(T) orelse error(function_clause),
    %% Upstream tags every reply with a single shared `T' via
    %% `{reply_tag, T}' / `{monitor, [{tag, T}]}', which are `spawn_request/5'
    %% options with no Partisan equivalent. Each request instead carries its
    %% own correlation reference, and the receive loop below demultiplexes on
    %% an index built from them.
    Res = new_res(),
    ReqId = send_erpc_request(N, Res, M, F, A, Opts),
    {Res, ReqId}.

%% `Opts' is the resolved `forward_opts()' for every request in the fan-out. It
%% is constant across the recursion — carried rather than recomputed so a
%% multicall cannot end up with different transport options per node.
mcall_send_requests(Tag, Ns, M, F, A, Tmo, Opts) ->
    DL = deadline(Tmo),
    mcall_send_requests(Tag, Ns, M, F, A, [], DL, undefined, 0, Opts).

mcall_send_requests(_Tag, [], M, F, A, RIDs, DL, local_call, NRs, _Opts) ->
    %% Timeout infinity and call on local node wanted;
    %% execute local call in this process...
    LRes = mcall_local_call(M, F, A),
    {ok, RIDs, #{local_call => LRes}, NRs, DL};
mcall_send_requests(_Tag, [], _M, _F, _A, RIDs, DL, _LC, NRs, _Opts) ->
    {ok, RIDs, #{}, NRs, DL};
mcall_send_requests(Tag, [N | Ns], M, F, A, RIDs, DL, LC, NRs, Opts) ->
    case N == partisan:node() andalso DL == infinity andalso LC == undefined of
        true ->
            mcall_send_requests(
                Tag,
                Ns,
                M,
                F,
                A,
                [local_call | RIDs],
                infinity,
                local_call,
                NRs,
                Opts
            );
        false ->
            try mcall_send_request(Tag, N, M, F, A, Opts) of
                RID ->
                    mcall_send_requests(
                        Tag,
                        Ns,
                        M,
                        F,
                        A,
                        [RID | RIDs],
                        DL,
                        LC,
                        NRs + 1,
                        Opts
                    )
            catch
                _:_ ->
                    %% Bad argument... Abandon requests and cleanup
                    %% any responses by receiving replies with a zero
                    %% timeout and then fail...
                    {badarg, RIDs, #{}, NRs, expired}
            end
    end;
mcall_send_requests(_Tag, _Ns, _M, _F, _A, RIDs, _DL, _LC, NRs, _Opts) ->
    %% Bad nodes list... Abandon requests and cleanup any responses
    %% by receiving replies with a zero timeout and then fail...
    {badarg, RIDs, #{}, NRs, expired}.

mcall_receive_replies(Tag, {SendRes, RIDs, Rpls, NRs, DL}) ->
    Index = mcall_index(RIDs),
    ResRpls = mcall_receive_replies(Tag, RIDs, Rpls, NRs, DL, Index),
    if
        SendRes /= ok ->
            %% Cleanup done; fail...
            error(SendRes);
        true ->
            mcall_map_replies(RIDs, ResRpls, [])
    end.

%% @private
%% Demultiplexing index for the receive loop below: every outstanding request
%% is reachable by its correlation reference (which its reply carries) and by
%% its monitor reference (which its `DOWN' carries).
mcall_index(RIDs) ->
    lists:foldl(
        fun
            (local_call, Acc) ->
                Acc;
            ({Res, ReqId} = RID, Acc) ->
                Acc#{Res => RID, ReqId => RID}
        end,
        #{},
        RIDs
    ).

mcall_receive_replies(_Tag, _ReqIds, Rpls, 0, _DL, _Index) ->
    Rpls;
mcall_receive_replies(Tag, ReqIDs, Rpls, NRs, DL, Index) ->
    Tmo = time_left(DL),
    receive
        {?ERPC_REPLY, Res, Reply} when is_map_key(Res, Index) ->
            {_, ReqId} = RID = maps:get(Res, Index),
            ok = finish(Res, ReqId),
            R = mcall_result(down, ReqId, Res, Reply),
            mcall_receive_replies(
                Tag, ReqIDs, Rpls#{RID => R}, NRs - 1, DL, Index
            );
        {'DOWN', ReqId, process, _Pid, _Reason} when is_map_key(ReqId, Index) ->
            {Res, _} = RID = maps:get(ReqId, Index),
            ok = release(Res),
            R = mcall_result(down, ReqId, Res, noconnection),
            mcall_receive_replies(
                Tag, ReqIDs, Rpls#{RID => R}, NRs - 1, DL, Index
            )
    after Tmo ->
        if
            ReqIDs == [] ->
                Rpls;
            true ->
                NewNRs = mcall_abandon(Tag, ReqIDs, Rpls, NRs),
                mcall_receive_replies(Tag, [], Rpls, NewNRs, expired, Index)
        end
    end.

mcall_result(ResType, ReqId, Tag, ResultReason) ->
    try
        {ok, result(ResType, ReqId, Tag, ResultReason)}
    catch
        Class:Reason ->
            {Class, Reason}
    end.

mcall_abandon(_Tag, [], _Rpls, NRs) ->
    NRs;
mcall_abandon(Tag, [local_call | RIDs], Rpls, NRs) ->
    mcall_abandon(Tag, RIDs, Rpls, NRs);
mcall_abandon(Tag, [{Res, ReqId} = RID | RIDs], Rpls, NRs) ->
    NewNRs =
        case maps:is_key(RID, Rpls) of
            true ->
                NRs;
            false ->
                _ = call_abandon(ReqId),
                ok = release(Res),
                NRs - 1
        end,
    mcall_abandon(Tag, RIDs, Rpls, NewNRs).

mcall_map_replies([], _Rpls, Res) ->
    Res;
mcall_map_replies([RID | RIDs], Rpls, Res) ->
    Timeout = {error, {?MODULE, timeout}},
    mcall_map_replies(RIDs, Rpls, [maps:get(RID, Rpls, Timeout) | Res]).
