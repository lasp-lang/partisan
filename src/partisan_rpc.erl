%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Christopher S. Meiklejohn. All Rights Reserved.
%% Copyright (c) 2022 Alejandro M. Ramallo. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% -----------------------------------------------------------------------------
%% @doc Partisan's counterpart of Erlang's `rpc' module.
%%
%% This is the **legacy** surface. Like OTP — whose own `rpc' documentation
%% steers new code to `erpc:call/4', `erpc:multicall/4' and
%% `erpc:send_request/4' — prefer {@link partisan_erpc} for new code. `rpc' is
%% kept because it is the target of the `rpc => partisan_rpc' rewrite applied to
%% modules derived from OTP sources, and because existing code calls it.
%%
%% Since OTP 23 the `rpc' module is implemented on top of `erpc', with a thin
%% layer translating exceptions into `{badrpc, Reason}'. This module mirrors
%% that arrangement, and mirrors OTP's split exactly:
%%
%% <ul>
%% <li>`call', `cast', `multicall', `async_call'/`yield'/`nb_yield' are shims
%% over {@link partisan_erpc}, which runs each request in its own process on the
%% target.</li>
%% <li>`block_call', `abcast', `sbcast' and `eval_everywhere' go to
%% `partisan_rpc_backend' — the counterpart of OTP's `rex' server, which OTP
%% likewise still keeps for exactly these operations. `block_call' in
%% particular is *defined* as executing on that server, serialised with other
%% `block_call's; that is what distinguishes it from `call'.</li>
%% </ul>
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_rpc).

-include("partisan.hrl").

-type error_reason() :: timeout | any().
-opaque key() :: partisan_erpc:request_id().

-export_type([key/0]).

%% API — shims over partisan_erpc
-export([call/4]).
-export([call/5]).
-export([cast/4]).
-export([cast/5]).
-export([multicall/3]).
-export([multicall/4]).
-export([multicall/5]).
-export([async_call/4]).
-export([async_call/5]).
-export([yield/1]).
-export([nb_yield/1]).
-export([nb_yield/2]).

%% API — served by partisan_rpc_backend (OTP's `rex' role)
-export([block_call/4]).
-export([block_call/5]).
-export([abcast/2]).
-export([abcast/3]).
-export([sbcast/2]).
-export([sbcast/3]).
-export([eval_everywhere/3]).
-export([eval_everywhere/4]).

-export([do_sbcast/2]).
-export([forward_opts/1]).
-export([prepare_opts/1]).

-dialyzer([{nowarn_function, call/4}, no_return]).
-dialyzer([{nowarn_function, call/5}, no_return]).
-dialyzer([{nowarn_function, yield/1}, no_return]).

%% Vendored verbatim from OTP's `kernel/src/rpc.erl' (the `?RPCIFY' macro and
%% `rpcify_exception/2'). It is pure translation with no transport in it, so it
%% is exactly the part worth keeping identical to upstream.
-define(RPCIFY(ERPC_),
    try ERPC_ of
        {'EXIT', _} = BadRpc_ ->
            {badrpc, BadRpc_};
        Result_ ->
            Result_
    catch
        Class_:Reason_ ->
            rpcify_exception(Class_, Reason_)
    end
).

%% =============================================================================
%% API
%% =============================================================================

-spec call(
    Node :: node(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [any()]
) -> Reply :: any() | {badrpc, error_reason()}.

call(Node, Module, Function, Arguments) ->
    call(Node, Module, Function, Arguments, infinity).

-spec call(
    Node :: node(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [any()],
    Timeout :: timeout() | partisan_peer_service_manager:forward_opts()
) ->
    Reply :: any() | {badrpc, error_reason()}.

call(Node, Module, Function, Arguments, Timeout) when ?IS_VALID_TMO(Timeout) ->
    call(Node, Module, Function, Arguments, #{timeout => Timeout});
call(Node, Module, Function, Arguments, Opts) when is_list(Opts) ->
    call(Node, Module, Function, Arguments, maps:from_list(Opts));
call(Node, Module, Function, Arguments, Opts0) when is_map(Opts0) ->
    Timeout = maps:get(timeout, Opts0, ?DEFAULT_TIMEOUT),

    ?IS_VALID_TMO(Timeout) orelse error({?MODULE, badarg}),

    Opts = forward_opts(Opts0),

    ?RPCIFY(
        partisan_erpc:call_with_opts(
            Node, Module, Function, Arguments, Timeout, Opts
        )
    ).

-spec cast(
    Node :: node(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [any()]
) -> true.

cast(Node, Module, Function, Arguments) ->
    cast(Node, Module, Function, Arguments, #{}).

-spec cast(
    Node :: node(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [any()],
    Opts :: partisan_peer_service_manager:forward_opts()
) -> true.

%% Partisan-specific arity: `cast/4' with per-call transport options.
cast(Node, Module, Function, Arguments, Opts) ->
    try
        ok = partisan_erpc:cast(Node, Module, Function, Arguments, Opts)
    catch
        _:_ -> ok
    end,
    true.

-spec multicall(module(), atom(), [any()]) -> {[any()], [node()]}.

multicall(Module, Function, Arguments) ->
    multicall(
        [partisan:node() | partisan:nodes()], Module, Function, Arguments
    ).

-spec multicall(
    [node()] | module(), module() | atom(), atom() | [any()], [any()] | timeout()
) -> {[any()], [node()]}.

multicall(Nodes, Module, Function, Arguments) when is_list(Nodes) ->
    multicall(Nodes, Module, Function, Arguments, infinity);
multicall(Module, Function, Arguments, Timeout) ->
    multicall(
        [partisan:node() | partisan:nodes()],
        Module,
        Function,
        Arguments,
        Timeout
    ).

-spec multicall(
    [node()],
    module(),
    atom(),
    [any()],
    timeout() | partisan_peer_service_manager:forward_opts()
) -> {[any()], [node()]}.

multicall(Nodes, Module, Function, Arguments, TimeoutOrOpts) ->
    %% Mirrors OTP: use `erpc:multicall/5' and convert its per-node results
    %% into the `{Replies, BadNodes}' shape `rpc' promises. `partisan_erpc'
    %% accepts `forward_opts()' in the timeout position, so a fan-out can select
    %% a channel; passing it straight through keeps one implementation of that
    %% overload rather than two.
    Results = partisan_erpc:multicall(
        Nodes, Module, Function, Arguments, TimeoutOrOpts
    ),
    rpcify_multicall(Nodes, Results, [], []).

-spec async_call(node(), module(), atom(), [any()]) -> key().

async_call(Node, Module, Function, Arguments) ->
    partisan_erpc:send_request(Node, Module, Function, Arguments).

-spec async_call(
    node(),
    module(),
    atom(),
    [any()],
    partisan_peer_service_manager:forward_opts()
) -> key().

%% Partisan-specific arity: `async_call/4' with per-call transport options. The
%% key it returns is used with `yield/1' and `nb_yield/1,2' exactly as usual —
%% the options affect only how the request is forwarded.
async_call(Node, Module, Function, Arguments, Opts) ->
    partisan_erpc:send_request(Node, Module, Function, Arguments, Opts).

-spec yield(key()) -> any() | {badrpc, error_reason()}.

yield(Key) ->
    ?RPCIFY(partisan_erpc:receive_response(Key)).

-spec nb_yield(key()) -> {value, any()} | timeout.

nb_yield(Key) ->
    nb_yield(Key, 0).

-spec nb_yield(key(), timeout()) -> {value, any()} | timeout.

nb_yield(Key, Tmo) ->
    try partisan_erpc:wait_response(Key, Tmo) of
        no_response ->
            timeout;
        {response, {'EXIT', _} = BadRpc} ->
            {value, {badrpc, BadRpc}};
        {response, R} ->
            {value, R}
    catch
        Class:Reason ->
            {value, rpcify_exception(Class, Reason)}
    end.

%% -----------------------------------------------------------------------------
%% @doc Evaluates `Module:Function(Arguments)' on `Node', **in the
%% `partisan_rpc_backend' process itself**.
%%
%% Unlike {@link call/4} this is serialised with every other `block_call' on the
%% target and blocks that server for the duration. That serialisation is the
%% defining semantic of `block_call' — not an implementation accident — which is
%% why OTP also still routes it through `rex' rather than through `erpc'.
%% @end
%% -----------------------------------------------------------------------------
-spec block_call(node(), module(), atom(), [any()]) ->
    any() | {badrpc, error_reason()}.

block_call(Node, Module, Function, Arguments) ->
    block_call(Node, Module, Function, Arguments, infinity).

-spec block_call(node(), module(), atom(), [any()], timeout()) ->
    any() | {badrpc, error_reason()}.

block_call(Node, Module, Function, Arguments, Timeout) when
    ?IS_VALID_TMO(Timeout)
->
    Res = partisan:make_ref(),
    Origin = partisan:self(),
    Msg = {?RPC_BLOCK_CALL, Res, Origin, Module, Function, Arguments},
    Opts = prepare_opts(partisan_config:get(forward_options, [])),

    %% NOTE: dialyzer reports the `{error, _}' clause as unreachable, because
    %% `partisan:forward_message/4' and the `partisan_peer_service_manager'
    %% callback are both specced `-> ok'. **That spec is inaccurate**, not this
    %% clause: the serialised path replies with whatever `do_send_message/5'
    %% returned, which is `{error, disconnected}' when the target has no usable
    %% connection on the requested channel. Handling it turns a silently dropped
    %% `block_call' into a `{badrpc, disconnected}'. Correcting the spec is a
    %% change to the manager behaviour's public contract and is deliberately not
    %% made here.
    case partisan:forward_message(Node, partisan_rpc_backend, Msg, Opts) of
        ok ->
            receive
                {?RPC_BLOCK_REPLY, Res, Reply} ->
                    Reply
            after Timeout ->
                {badrpc, timeout}
            end;
        {error, Reason} ->
            {badrpc, Reason}
    end;
block_call(_, _, _, _, _) ->
    error({?MODULE, badarg}).

-spec abcast(atom(), any()) -> abcast.

abcast(Name, Message) ->
    abcast([partisan:node() | partisan:nodes()], Name, Message).

-spec abcast([node()], atom(), any()) -> abcast.

abcast(Nodes, Name, Message) ->
    Opts = prepare_opts(partisan_config:get(forward_options, [])),
    _ = [
        partisan:forward_message(Node, Name, Message, Opts)
     || Node <- Nodes
    ],
    abcast.

-spec sbcast(atom(), any()) -> {[node()], [node()]}.

sbcast(Name, Message) ->
    sbcast([partisan:node() | partisan:nodes()], Name, Message).

-spec sbcast([node()], atom(), any()) -> {[node()], [node()]}.

sbcast(Nodes, Name, Message) ->
    %% Ask each node's backend to deliver to `Name' and report whether the name
    %% was registered there. Partitions the node list into good and bad.
    Reqs = [
        {Node, partisan_erpc:send_request(Node, ?MODULE, do_sbcast, [
            Name, Message
        ])}
     || Node <- Nodes
    ],
    lists:foldl(
        fun({Node, ReqId}, {Good, Bad}) ->
            try partisan_erpc:receive_response(ReqId, ?DEFAULT_TIMEOUT) of
                true -> {[Node | Good], Bad};
                false -> {Good, [Node | Bad]}
            catch
                _:_ -> {Good, [Node | Bad]}
            end
        end,
        {[], []},
        Reqs
    ).

%% @private Applied on the target by `sbcast/3'.
do_sbcast(Name, Message) ->
    case erlang:whereis(Name) of
        undefined ->
            false;
        Pid ->
            Pid ! Message,
            true
    end.

-spec eval_everywhere(module(), atom(), [any()]) -> abcast.

eval_everywhere(Module, Function, Arguments) ->
    eval_everywhere(
        [partisan:node() | partisan:nodes()], Module, Function, Arguments
    ).

-spec eval_everywhere([node()], module(), atom(), [any()]) -> abcast.

eval_everywhere(Nodes, Module, Function, Arguments) ->
    _ = [cast(Node, Module, Function, Arguments) || Node <- Nodes],
    abcast.

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
%% -----------------------------------------------------------------------------
%% @doc Resolves the `forward_opts()' for a call: the caller's options merged
%% over the globally configured `forward_options'.
%%
%% **Per-call options win.** The global value only fills in keys the caller did
%% not specify. This previously read
%% `partisan_config:get(forward_options, CallerOpts)', which inverts the
%% precedence — `partisan_config:get/2' returns the *configured* value whenever
%% one is set, so the caller's `channel' and `partition_key' were silently
%% discarded as soon as anything set the global.
%% {@link partisan_pluggable_peer_service_manager:forward_message/4} has always
%% merged in this order; this brings RPC in line with it.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_opts(map()) -> map().

forward_opts(Opts0) ->
    Global = opts_to_map(partisan_config:get(forward_options, #{})),
    prepare_opts(maps:merge(Global, maps:without([timeout], Opts0))).

-spec prepare_opts(list() | map()) -> map().

prepare_opts(L) when is_list(L) ->
    prepare_opts(maps:from_list(L));
prepare_opts(#{channel := _} = Opts) ->
    Opts;
prepare_opts(Opts) when is_map(Opts) ->
    Opts#{channel => ?DEFAULT_CHANNEL}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% `forward_options' may be configured as either a proplist or a map.
opts_to_map(L) when is_list(L) ->
    maps:from_list(L);
opts_to_map(M) when is_map(M) ->
    M.

%% @private
%% Vendored verbatim from OTP's `kernel/src/rpc.erl', with `erpc' replaced by
%% `partisan_erpc' in the error tags this module can actually see.
rpcify_exception(throw, {'EXIT', _} = BadRpc) ->
    {badrpc, BadRpc};
rpcify_exception(throw, Return) ->
    Return;
rpcify_exception(exit, {exception, Exit}) ->
    {badrpc, {'EXIT', Exit}};
rpcify_exception(exit, {signal, Reason}) ->
    {badrpc, {'EXIT', Reason}};
rpcify_exception(exit, Reason) ->
    exit(Reason);
rpcify_exception(error, {exception, Error, Stack}) ->
    {badrpc, {'EXIT', {Error, Stack}}};
rpcify_exception(error, {partisan_erpc, badarg}) ->
    error(badarg);
rpcify_exception(error, {partisan_erpc, noconnection}) ->
    {badrpc, nodedown};
rpcify_exception(error, {partisan_erpc, timeout}) ->
    {badrpc, timeout};
rpcify_exception(error, {partisan_erpc, notsup}) ->
    {badrpc, notsup};
rpcify_exception(error, {partisan_erpc, Error}) ->
    {badrpc, {'EXIT', Error}};
rpcify_exception(error, Reason) ->
    error(Reason).

%% @private
%% `partisan_erpc:multicall/5' returns one `{ok, Value} | {Class, Reason}' per
%% node, in node order. `rpc:multicall/5' promises `{Replies, BadNodes}'.
rpcify_multicall([], [], Replies, Bad) ->
    {lists:reverse(Replies), lists:reverse(Bad)};
rpcify_multicall([_Node | Nodes], [{ok, Value} | Rest], Replies, Bad) ->
    rpcify_multicall(Nodes, Rest, [Value | Replies], Bad);
rpcify_multicall([Node | Nodes], [_Failed | Rest], Replies, Bad) ->
    rpcify_multicall(Nodes, Rest, Replies, [Node | Bad]);
rpcify_multicall(_, _, Replies, Bad) ->
    {lists:reverse(Replies), lists:reverse(Bad)}.
