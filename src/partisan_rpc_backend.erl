%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Christopher S. Meiklejohn.  All Rights Reserved.
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
%% @doc The counterpart of OTP's `rex' server: the node-wide process that serves
%% the RPC operations which are *defined* as running on a server.
%%
%% It is not legacy machinery awaiting deletion. OTP still keeps `rex' and still
%% routes `block_call', `sbcast', `abcast' and `eval_everywhere' through it, and
%% so does Partisan. What changed in 6.0.0 is its scope: `call' no longer comes
%% here to be applied inline. `partisan_erpc' and `partisan_rpc:call/4,5' now
%% issue a correlated request that this server dispatches to a worker process,
%% one per request. `block_call' is the deliberate exception — see the
%% `?RPC_BLOCK_CALL' clause.
%%
%% == Wire protocols accepted ==
%%
%% This server accepts three request framings. Two are current, one exists only
%% for compatibility:
%%
%% <ul>
%% <li>`?ERPC_REQUEST' / `?ERPC_CAST' — the correlated protocol introduced in
%% 6.0.0. Each request carries its own correlation reference, so a caller with
%% several requests outstanding can tell the replies apart.</li>
%% <li>`?RPC_BLOCK_CALL' — `block_call', applied in this process by design.</li>
%% <li>`{call, M, F, A, Timeout, {origin, Caller}}' — <strong>the pre-6.0.0
%% framing</strong>. No 6.x node ever sends it. It is retained as a *receiver*
%% only, so that during a rolling upgrade an upgraded node still answers RPCs
%% issued by a peer still running 5.x.</li>
%% </ul>
%%
%% == Rolling upgrade ==
%%
%% Both framings are accepted for the whole of the 6.x series, so nodes may be
%% upgraded one at a time in any order and there is no coordination requirement:
%%
%% <ul>
%% <li>a 5.x caller reaching a 6.x node uses the legacy clause and gets the same
%% answer it always did — with the concurrency bound now applied, so an
%% overloaded node answers `{badrpc, overloaded}' instead of spawning without
%% limit;</li>
%% <li>a 6.x caller reaching a 5.x node cannot use the correlated protocol,
%% because a 5.x server has no clause for it and will silently discard the
%% message. The caller observes a timeout. <strong>Upgrade every node in a
%% cluster before relying on `partisan_erpc' between them.</strong> This
%% direction is the reason the legacy clause is kept: it makes the
%% partially-upgraded cluster work for the callers that already exist, rather
%% than making both directions work for callers that do not yet.</li>
%% </ul>
%%
%% The legacy clause — the clause, not this module — is scheduled for removal in
%% <strong>7.0.0</strong>. By then every supported caller emits the correlated
%% protocol, so keeping it would only preserve the framing's known defect: it
%% carries no request id, so a reply that arrives after its caller timed out can
%% be consumed by an unrelated later call in the same process.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_rpc_backend).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-ifdef(TEST).
%% `send_reply/3' is what a worker uses to answer its caller. Exported for test
%% so the undeliverable case can be asserted directly: it needs a target on an
%% unreachable node, which no request issued from a single node produces.
-export([send_reply/3]).
-endif.

-record(state, {
    %% Number of RPC worker processes currently running for inbound requests.
    %% Incremented when a worker is spawned, decremented on its `DOWN'.
    inflight = 0 :: non_neg_integer()
}).

-include("partisan.hrl").
-include("partisan_logger.hrl").

%% =============================================================================
%% API
%% =============================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% =============================================================================
%% GEN_SERVER_CALLBACKS
%% =============================================================================

init([]) ->
    {ok, #state{}}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({?ERPC_REQUEST, Res, Origin, M, F, A}, State) ->
    %% Correlated request/response (`partisan_erpc'). `Res' is the caller's
    %% correlation reference: it is echoed in the reply so a caller with
    %% several requests outstanding can tell them apart. The legacy `{call,
    %% ...}' protocol below has no such id, which is why a late reply there
    %% can be consumed by an unrelated later call.
    %%
    %% As with the legacy clause, user code runs in a fresh process — never in
    %% this callback.
    case admit(State) of
        {ok, State1} ->
            {_, _} = erlang:spawn_monitor(fun() ->
                execute_request(Res, Origin, M, F, A)
            end),
            {noreply, State1};
        overloaded ->
            ok = reject_overloaded(Res, Origin),
            {noreply, State}
    end;
handle_info({?RPC_BLOCK_CALL, Res, Origin, M, F, A}, State) ->
    %% `rpc:block_call/4,5'. Deliberately applied INLINE, in this process.
    %%
    %% This is the one case where running user code in the server callback is
    %% correct rather than a defect: `block_call' is *defined* as executing on
    %% this server, serialised with every other block_call on the node. OTP
    %% keeps `rex' for precisely this reason. `call' must never come here — it
    %% is handled by the worker-spawning clauses above.
    Reply =
        try
            erlang:apply(M, F, A)
        catch
            throw:Value ->
                Value;
            exit:Reason ->
                {badrpc, {'EXIT', Reason}};
            error:Reason:Stack ->
                {badrpc, {'EXIT', {Reason, Stack}}}
        end,

    Opts = partisan_rpc:prepare_opts(partisan_config:get(forward_options, [])),
    _ = partisan:forward_message(
        Origin, {?RPC_BLOCK_REPLY, Res, Reply}, Opts
    ),
    {noreply, State};
handle_info({?ERPC_CAST, M, F, A}, State) ->
    %% Fire-and-forget (`partisan_erpc:cast/4', `multicast/4'). No reply, no
    %% correlation reference — but still a worker process, so a slow cast
    %% cannot block this server.
    _ = erlang:spawn(fun() -> partisan_erpc:execute_cast(M, F, A) end),
    {noreply, State};
handle_info({call, M, F, A, _Timeout, {origin, Caller}}, State) ->
    %% The pre-6.0.0 framing, retained as a receiver for the 6.x series so a
    %% rolling upgrade works; removed in 7.0.0 (see the module doc).
    %%
    %% Execute the call in a fresh process rather than inline. Applying user
    %% code inline in this callback serialises every RPC arriving at this node:
    %% a single slow, blocking or hung `M:F(A)' stalls every other RPC behind it
    %% in this server's mailbox. OTP's own `rpc' and `erpc' spawn per request for
    %% exactly this reason.
    %%
    %% The concurrency bound applies here too. It has to: an un-upgraded peer is
    %% still a peer, and a cap that any 5.x caller can bypass is not a cap.
    case admit(State) of
        {ok, State1} ->
            {_, _} = erlang:spawn_monitor(fun() ->
                execute_call(M, F, A, Caller)
            end),
            {noreply, State1};
        overloaded ->
            %% Answer in the legacy protocol's own shape, so a 5.x caller sees a
            %% `{badrpc, overloaded}' rather than waiting out its timeout.
            ok = reject_overloaded_legacy(Caller),
            {noreply, State}
    end;
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, State) ->
    %% An RPC worker finished (or died). Release its slot. This is bounded,
    %% non-blocking work — the server never waits on a worker.
    N = State#state.inflight,
    {noreply, State#state{inflight = max(0, N - 1)}};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
%% Claims a concurrency slot for an inbound request, or reports `overloaded'.
%%
%% Each request runs in its own process, so without a bound a peer could spawn
%% without limit. OTP's `erpc' tolerates the unbounded form because the
%% distribution buffer applies backpressure; Partisan has no equivalent, so the
%% bound is enforced here. Rejecting outright rather than queueing is
%% deliberate: a queue with no credit scheme is an unbounded mailbox with extra
%% steps.
admit(#state{inflight = N} = State) ->
    case partisan_config:get(rpc_max_concurrency, 10000) of
        infinity ->
            {ok, State#state{inflight = N + 1}};
        Max when N < Max ->
            {ok, State#state{inflight = N + 1}};
        Max ->
            ?LOG_WARNING(#{
                description =>
                    "Rejecting RPC request, node at maximum concurrency",
                inflight => N,
                max => Max
            }),
            partisan_telemetry:execute(
                [partisan, rpc, overload],
                #{inflight => N},
                #{max => Max}
            ),
            overloaded
    end.

%% @private
%% Answers an over-cap request with an `erpc'-shaped error, so the caller
%% raises `error({partisan_erpc, overloaded})' — `partisan_erpc:result/4'
%% already translates `{Res, error, {partisan_erpc, _}}' that way, and
%% `partisan_rpc' turns it into `{badrpc, {'EXIT', overloaded}}'.
reject_overloaded(Res, Origin) ->
    Reply = {Res, error, {partisan_erpc, overloaded}},
    Opts = partisan_rpc:prepare_opts(partisan_config:get(forward_options, [])),
    _ = partisan:forward_message(Origin, {?ERPC_REPLY, Res, Reply}, Opts),
    ok.

%% @private
%% The same rejection in the pre-6.0.0 protocol's reply shape. That protocol has
%% no correlation reference and no error channel of its own, so `{badrpc, _}' —
%% which is what a 5.x `partisan_rpc:call/5' returns for any failure — is the
%% only way to say this.
reject_overloaded_legacy(Caller) ->
    Opts = partisan_rpc:prepare_opts(partisan_config:get(forward_options, [])),
    _ = partisan:forward_message(
        Caller, {rpc_response, {badrpc, overloaded}}, Opts
    ),
    ok.

%% @private
%% Runs `M:F(A)' for a correlated request and sends the outcome back to
%% `Origin', tagged with the caller's correlation reference.
%%
%% The reply payload is deliberately the *same* tuple shape that
%% `partisan_erpc:execute_call/4' builds — `{Res, return | throw | exit, V}'
%% or `{Res, error, E, Stack}' — so `partisan_erpc:result/4' can translate it
%% into the exception semantics `erpc' specifies without modification. Upstream
%% `erpc' delivers that tuple as a process exit reason over a distributed
%% monitor; we deliver it as an ordinary message, because in Partisan every
%% remote `DOWN' is relayed through the `partisan_monitor' server and using it
%% as a value channel would serialise every RPC result on the node through that
%% one process.
execute_request(Res, Origin, M, F, A) ->
    Reply =
        try
            {Res, return, erlang:apply(M, F, A)}
        catch
            throw:Reason ->
                {Res, throw, Reason};
            exit:Reason ->
                {Res, exit, Reason};
            error:Reason:Stack ->
                case partisan_erpc:is_arg_error(Reason, M, F, A) of
                    true ->
                        {Res, error, {partisan_erpc, Reason}};
                    false ->
                        Trimmed = partisan_erpc:trim_stack(Stack, M, F, A),
                        {Res, error, Reason, Trimmed}
                end
        end,

    Opts = partisan_rpc:prepare_opts(partisan_config:get(forward_options, [])),
    send_reply(Origin, {?ERPC_REPLY, Res, Reply}, Opts).

%% @private
%% Runs `M:F(A)' and returns the result to the origin. Executed in a worker
%% process spawned per request, never in the server process.
execute_call(M, F, A, Caller) ->
    Response =
        try
            erlang:apply(M, F, A)
        catch
            _:Reason ->
                {badrpc, Reason}
        end,

    %% Send the response to execution.
    Opts = partisan_rpc:prepare_opts(partisan_config:get(forward_options, [])),
    send_reply(Caller, {rpc_response, Response}, Opts).

%% @private
%% Sends a worker's reply back to the caller.
%%
%% An undeliverable reply is **not** an error in this process. It means the
%% caller's node became unreachable between the request and the reply, and the
%% caller detects that for itself — `partisan_erpc' holds a monitor precisely so
%% that it reports `noconnection' instead of waiting out its timeout. Crashing
%% the worker here would add a crash report for a routine condition and change
%% nothing the caller observes, so this logs and returns instead.
%%
%% This is the code the old `partisan:forward_message/3' spec hid: it read
%% `-> ok', so `ok = partisan:forward_message(...)' looked total. It never was.
send_reply(To, Message, Opts) ->
    case partisan:forward_message(To, Message, Opts) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_DEBUG(#{
                description => "Could not deliver RPC reply to caller",
                origin => To,
                reason => Reason
            }),
            ok
    end.
