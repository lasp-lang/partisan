%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc A broadcast handler that exists only to be broadcast through.
%%
%% The fan-out benchmark needs a handler whose own work is negligible, so that
%% what it measures is Partisan's cost of fanning a message out to the peer set
%% rather than the cost of whatever the handler does with it.
%%
%% It deliberately does **not** reuse `partisan_plumtree_backend', Partisan's own
%% control-plane handler. That one accepts a `#broadcast{}' record carrying a
%% timestamp it uses for tree construction, and broadcasting anything else raises
%% `function_clause' inside the callback — which
%% `partisan_plumtree_broadcast:broadcast/2' catches and logs as "Broadcast
%% cancelled". A benchmark pointed at it therefore completes, reports a
%% throughput, and measures *the cost of failing*.
%%
%% == Why this is a gen_server ==
%%
%% Not incidental, and worth knowing before writing any handler. Exporting
%% `claim/2' opts a handler into the **off-path split contract**: the group
%% server runs `claim/2' inline for the fast novelty check and then, when the
%% message is novel, hands the payload off with
%% `gen_server:cast(Mod, {'$partisan_apply', Id, Message})' — addressed to a
%% process registered under **the handler module's own name**
%% (`partisan_plumtree_broadcast:accept_broadcast/3').
%%
%% So a handler that exports `claim/2' without registering such a process gets
%% its novelty check called and its `handle_broadcast/2' silently never called:
%% the cast has nowhere to go, and `gen_server:cast/2' does not complain. That is
%% precisely how the first version of this module behaved — every peer recorded
%% every message and acknowledged none. A handler that does not export `claim/2'
%% keeps the synchronous `merge/2' path and needs no process.
%%
%% Owning the ETS table here also fixes a second trap: a table dies with its
%% creating process, and this is started over `rpc:call/4', whose worker exits
%% the moment the call returns.
%% @end
%% =============================================================================
-module(partisan_bench_handler).

-behaviour(gen_server).
-behaviour(partisan_plumtree_broadcast_handler).

-export([start/0]).
-export([stop/0]).

%% partisan_plumtree_broadcast_handler callbacks
-export([broadcast_data/1]).
-export([claim/2]).
-export([handle_broadcast/2]).
-export([merge/2]).
-export([is_stale/1]).
-export([graft/1]).
-export([exchange/1]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TAB, partisan_bench_handler_seen).

%% =============================================================================
%% CONTROL
%% =============================================================================

%% @doc Starts the handler process. Idempotent: every node in the cluster runs
%% it, and a repetition may run it again.
start() ->
    case erlang:whereis(?MODULE) of
        undefined ->
            case gen_server:start({local, ?MODULE}, ?MODULE, [], []) of
                {ok, _Pid} -> ok;
                {error, {already_started, _}} -> ok;
                {error, Reason} -> error({bench_handler_start_failed, Reason})
            end;
        _ ->
            ok
    end.

stop() ->
    case erlang:whereis(?MODULE) of
        undefined ->
            ok;
        Pid ->
            exit(Pid, kill),
            ok
    end.

%% =============================================================================
%% HANDLER CALLBACKS
%% =============================================================================

broadcast_data({Id, Payload}) ->
    {Id, Payload};
broadcast_data(Other) ->
    %% Anything else is a caller error. Fail loudly rather than inventing an id:
    %% a silently-accepted malformed payload is how the first version of this
    %% benchmark measured nothing.
    error({unexpected_broadcast_payload, Other}).

%% Records `Id' if it has not been seen, and reports whether it was novel.
%% Deliberately the whole cost of this handler's fast path.
claim(Id, _Payload) ->
    ets:insert_new(?TAB, {Id}).

%% Notifies the origin that this node has received the broadcast.
%%
%% Fan-out completion is otherwise unobservable from the sender:
%% `partisan_broadcast:broadcast/2' hands the message to the group server and
%% returns, so timing the call measures an enqueue — around 0 us — and says
%% nothing about the cost of reaching the peer set.
%%
%% The origin does not notify itself. It does not claim its own broadcast in any
%% case, but making the rule explicit keeps the expected acknowledgement count a
%% function of cluster size alone.
%% NOTE: the sender waits on `{fanout_seen, Id}' with `Id' bound to an integer,
%% which Erlang's selective-receive optimisation does not cover — it only applies
%% to a reference created in the same function. Each wait therefore rescans the
%% mailbox. Switching the acknowledgement to a per-operation reference was tried
%% and broke the scenario outright (every wait timed out), so the cause is not
%% simply the tag's type; it is left as a known limitation of this harness rather
%% than a half-understood change. It matters because it means this scenario's
%% latencies include a cost the harness itself imposes.
handle_broadcast(Id, {notify, OriginRef, OriginNode, _Payload}) ->
    case partisan:node() of
        OriginNode ->
            ok;
        _ ->
            _ = partisan:forward_message(OriginRef, {fanout_seen, Id}, #{
                channel => partisan:default_channel()
            }),
            ok
    end;
handle_broadcast(_Id, _Payload) ->
    ok.

merge(Id, Payload) ->
    case claim(Id, Payload) of
        true -> handle_broadcast(Id, Payload) == ok;
        false -> false
    end.

is_stale(Id) ->
    ets:member(?TAB, Id).

graft(Id) ->
    case ets:member(?TAB, Id) of
        true -> {ok, Id};
        false -> {error, not_found}
    end.

%% No anti-entropy: this handler holds no state worth reconciling, and an
%% exchange running during a measurement would be unmeasured background work.
exchange(_Peer) ->
    ignore.

%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================

init([]) ->
    _ = ets:new(?TAB, [named_table, public, set, {write_concurrency, true}]),
    {ok, #{}}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% The off-path apply: the cast `accept_broadcast/3' sends after `claim/2'
%% reports the message novel. Without this clause the payload is never applied.
handle_cast({'$partisan_apply', Id, Message}, State) ->
    _ = handle_broadcast(Id, Message),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
