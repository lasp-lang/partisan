%% -----------------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(partisan_plumtree_broadcast).

-moduledoc """
The process that runs one broadcast group — the supervised owner of a single
epidemic-broadcast context: its own mailbox, spanning-tree state and outstanding-lazy
table. Group identity is the handler module, so each broadcast handler runs in its own
group and independent gossip streams never share a tree or a mailbox. The public entry
points for broadcasting and managing groups are in `partisan_broadcast`.

## What the shell owns, and what it delegates

Tree construction and repair are delegated to a pluggable **tree engine**
(`partisan_broadcast_engine`); the shell owns everything else — the `gen_server`
process and mailbox, the membership poll, handler dispatch, the message transport, the
periodic ticks, and the rolling-upgrade router. The engine is pure and returns actions
the shell executes, so an alternate engine drops in with no change here.

Two engines exist: Plumtree (`partisan_plumtree_engine`, the default) and Thicket
(`partisan_thicket_engine`). The shell drives an engine in one of two dispatch modes,
read from the engine's `dispatch_mode/0`:

- **typed** — the shell decodes each inbound wire message and calls a Plumtree-shaped
  callback, passing the handler's verdicts (is this novel? is this `i_have` stale?).
  This is the default path.
- **raw** — the shell hands the engine whole wire messages via `handle_message/2` and
  executes its `deliver`/`fetch` actions against the group's single handler. This is how
  a self-describing engine such as Thicket runs.

## Non-blocking delivery

A handler may keep its heavy apply off the tree process: `claim/2` runs on the tree
process and does only the fast, atomic novelty check, while `handle_broadcast/2` runs
the apply in the handler's own process, delivered asynchronously. A slow apply then
never blocks the tree or the other handlers. A handler that implements only `merge/2`
keeps the synchronous path.

## Membership

The shell reads membership from a lock-free snapshot (`partisan_membership`), polling
its version on each tick and reconciling the engine's peer sets when it changes, rather
than subscribing to a synchronous event bus that would block the membership oracle.

## Rolling upgrade

Partisan's own control-plane group keeps a fixed registered name, so membership keeps
converging in a mixed-version cluster, and a compatibility router forwards
legacy-addressed messages for application handlers to their per-group process.

## Reading guide

`start_link/2` and `init/1` build the group and select the engine; the `handle_cast`
clauses dispatch inbound wire messages to the engine (typed or raw); `handle_info`
drives the lazy/repair and exchange ticks; `run_engine/2` runs an engine callback and
executes the actions it returns. Broadcasting and the debug queries are near the top.
""".

-behaviour(gen_server).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-define(SERVER, ?MODULE).

-type exchange() :: {module(), node(), reference(), pid()}.
-type exchanges() :: [exchange()].
-type selector() ::
    all
    | {peer, node()}
    | {mod, module()}
    | reference()
    | pid().
-type info_opt() :: node_spec | metadata | distance.

-record(state, {
    %% Registered name of this broadcast instance (group).
    name :: atom(),

    %% This node
    node :: node(),

    %% Set of registered modules that may handle messages that
    %% have been broadcast
    mods :: [module()],

    %% List of outstanding exchanges
    exchanges :: exchanges(),

    %% Last observed membership snapshot version (see partisan_membership).
    %% Polled on each lazy tick to refresh membership without gen_event.
    members_version = 0 :: non_neg_integer(),

    %% Lazy tick period in milliseconds. On every tick all outstanding
    %% lazy pushes are sent out
    lazy_tick_period :: non_neg_integer(),

    %% Exchange tick period in milliseconds that may or may not occur
    exchange_tick_period :: non_neg_integer(),

    %% Pluggable tree engine and its opaque state. The engine owns
    %% the tree topology (eager/lazy peers per root) and repair; the shell owns
    %% the process, membership poll, handler dispatch, transport and ticks.
    engine :: module(),
    engine_state :: partisan_broadcast_engine:state(),

    %% Dispatch mode (PDDR-000004). `typed' drives the Plumtree-shaped callbacks
    %% (the default, unchanged path). `raw' hands whole wire messages to the
    %% engine's handle_message/2 and executes its deliver/fetch actions against
    %% `handler_mod' — the single handler a raw group (e.g. Thicket) hosts.
    engine_mode = typed :: typed | raw,
    handler_mod :: module() | undefined
}).

-type state() :: #state{}.
-type nodeset() :: ordsets:ordset(node()).

-export_type([info_opt/0]).
-export_type([nodeset/0]).

%% API
-export([broadcast/2]).
-export([broadcast_channel/1]).
-export([group_name/1]).
-export([broadcast_members/0]).
-export([broadcast_members/1]).
-export([cancel_exchanges/1]).
-export([exchanges/0]).
-export([exchanges/1]).
-export([exchanges/2]).
-export([start_link/0]).
-export([start_link/2]).

%% Debug API
-export([get_peers/1]).
-export([get_peers/2]).
-export([get_eager_peers/1]).
-export([get_lazy_peers/1]).
-export([debug_get_peers/2]).
-export([debug_get_peers/3]).
-export([debug_get_peers/4]).
-export([debug_get_tree/2]).
-export([debug_get_tree/3]).
-export([debug_get_tree/4]).

%% gen_server callbacks
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% =============================================================================
%% API
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc Starts the broadcast server on this node.
%%
%% The initial membership list is fetched from the configured @{link
%% partisan_peer_service}.
%%
%% If the node is a singleton then the initial eager and lazy sets are empty.
%% If there are two nodes, each will be in the others
%% eager set and the lazy sets will be empty. When number of members is less
%% than 5, each node will initially have one other node in its eager set and
%% lazy set. If there are more than five nodes each node will have at most two
%% other nodes in its eager set and one in its lazy set, initially.
%%
%% In addition, after the broadcast server is started, all callbacks defined in
%% the configuration option `broadcast_mods' are registered.
%% By default the list of callbacks includes the module
%% {@link partisan_plumtree_backend} which is used by to generate membership
%% updates as the ring changes.
%%
%% @TODO we should spawn 1 broadcast server per channel and or channel
%% partition
%% @end
%% -----------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.

start_link() ->
    Opts = #{
        mods => partisan_config:get(broadcast_mods, []),
        lazy_tick_period => partisan_config:get(
            lazy_tick_period, ?DEFAULT_LAZY_TICK_PERIOD
        ),
        exchange_tick_period => partisan_config:get(
            exchange_tick_period, ?DEFAULT_EXCHANGE_TICK_PERIOD
        )
    },
    start_link(?SERVER, Opts).

%% -----------------------------------------------------------------------------
%% @doc Starts a broadcast instance (group) registered under `Name'.
%%
%% `Opts' is a map with the following keys:
%% <ul>
%% <li> `mods :: [module()]' - handler modules for this group. All should
%% implement the `partisan_plumtree_broadcast_handler' behaviour.</li>
%% <li> `lazy_tick_period :: non_neg_integer()' - Flush all outstanding lazy
%% pushes period (in milliseconds).</li>
%% <li> `exchange_tick_period :: non_neg_integer()' - Possibly perform an
%% exchange period (in milliseconds).</li>
%% </ul>
%%
%% Membership is read from the {@link partisan_membership} snapshot and refreshed
%% by polling its version on each lazy tick (no gen_event subscription).
%% @end
%% -----------------------------------------------------------------------------
-spec start_link(Name :: atom(), Opts :: map()) ->
    {ok, pid()} | ignore | {error, term()}.

start_link(Name, Opts) when is_atom(Name), is_map(Opts) ->
    StartOpts = [
        {spawn_opt, ?PARALLEL_SIGNAL_OPTIMISATION([])}
    ],
    gen_server:start_link({local, Name}, ?MODULE, [Name, Opts], StartOpts).

%% -----------------------------------------------------------------------------
%% @doc Broadcasts a message originating from this node.
%% The message will be delivered to each node at least once. The `Mod' passed
%% must be loaded on all members of the cluster and implement the
%% `partisan_plumtree_broadcast_handler' behaviour which is responsible for
%% handling the message on remote nodes as well as providing some other
%% information both locally and on other nodes.
%%
%% The broadcast will be sent over the channel defined by
%% {@link broadcast_channel/1}.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast(any(), module()) -> ok.

broadcast(Broadcast, Mod) ->
    try
        {MessageId, Payload} = Mod:broadcast_data(Broadcast),
        gen_server:cast(group_name(Mod), {broadcast, MessageId, Payload, Mod})
    catch
        Class:Reason:Stacktrace ->
            ?LOG_NOTICE(#{
                description =>
                    "Exception on callback broadcast_data. Broadcast cancelled.",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            })
    end.

%% -----------------------------------------------------------------------------
%% @doc Returns the channel to be used when sending broadcasting a message
%% on behalf of module `Mod'.
%%
%% The channel defined by the callback `Mod:broadcast_channel()' or default
%% channel i.e. {@link partisan:default_channel/0} if the callback is not
%% implemented.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_channel(Mod :: module()) -> partisan:channel().

broadcast_channel(Mod) ->
    try
        case erlang:function_exported(Mod, broadcast_channel, 0) of
            true ->
                Mod:broadcast_channel();
            false ->
                ?DEFAULT_CHANNEL
        end
    catch
        Class:Reason:Stacktrace ->
            ?LOG_NOTICE(#{
                description =>
                    "Exception on callback broadcast_channel, "
                    "returning default channel.",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            undefined
    end.

%% -----------------------------------------------------------------------------
%% @doc Returns the broadcast servers view of full cluster membership.
%% Wait indefinitely for a response is returned from the process.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_members() -> nodeset().

broadcast_members() ->
    broadcast_members(infinity).

%% -----------------------------------------------------------------------------
%% @doc Returns the broadcast servers view of full cluster membership.
%% Waits `Timeout' ms for a response from the server.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_members(infinity | pos_integer()) -> nodeset().

broadcast_members(Timeout) ->
    gen_server:call(?SERVER, broadcast_members, Timeout).

%% -----------------------------------------------------------------------------
%% @doc return a list of exchanges, started by broadcast on this node, that are
%% running.
%% @end
%% -----------------------------------------------------------------------------
-spec exchanges() -> {ok, exchanges()}.

exchanges() ->
    gen_server:call(?SERVER, exchanges, infinity).

%% -----------------------------------------------------------------------------
%% @doc Returns a list of running exchanges, started on `Node'.
%% @end
%% -----------------------------------------------------------------------------
-spec exchanges(node()) ->
    {ok, exchanges()}
    | {error, {badrpc, Reason :: any()}}.

exchanges(Node) ->
    exchanges(Node, infinity).

%% -----------------------------------------------------------------------------
%% @doc Returns a list of running exchanges, started on `Node'.
%% @end
%% -----------------------------------------------------------------------------
-spec exchanges(node(), timeout()) ->
    {ok, exchanges()}
    | {error, {badrpc, Reason :: any()}}.

exchanges(Node, Timeout) ->
    %% This will not work because gen_server uses disterl
    %% TODO reconsider turning this server into a partisan_gen_serv
    %% gen_server:call({?SERVER, Node}, exchanges, infinity).
    case partisan_rpc:call(Node, ?SERVER, exchanges, [], Timeout) of
        {ok, _} = OK ->
            OK;
        {badrpc, _} = Reason ->
            {error, Reason}
    end.

%% -----------------------------------------------------------------------------
%% @doc Cancel exchanges started by this node.
%% @end
%% -----------------------------------------------------------------------------
-spec cancel_exchanges(selector()) -> exchanges().

cancel_exchanges(Selector) ->
    gen_server:call(?SERVER, {cancel_exchanges, Selector}, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_peers(Root :: node()) -> list().

get_peers(Root) ->
    gen_server:call(?SERVER, {get_peers, Root}).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_peers(Root :: node(), Opts :: [partisan:info_opt()]) -> list().

get_peers(Root, Opts) when is_list(Opts) ->
    gen_server:call(?SERVER, {get_peers, Root, Opts}).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_eager_peers(Root :: node()) -> list().

get_eager_peers(Root) ->
    gen_server:call(?SERVER, {get_eager_peers, Root}).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_lazy_peers(Root :: node()) -> list().

get_lazy_peers(Root) ->
    gen_server:call(?SERVER, {get_lazy_peers, Root}).

%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================

-spec init(list()) -> {ok, state()}.

init([Name, Opts]) ->
    Mods = maps:get(mods, Opts, []),
    LazyTickPeriod = maps:get(lazy_tick_period, Opts),
    ExchangeTickPeriod = maps:get(exchange_tick_period, Opts),
    schedule_lazy_tick(LazyTickPeriod),
    schedule_exchange_tick(ExchangeTickPeriod),

    %% Membership comes from the oracle's lock-free snapshot; we poll its version
    %% on each lazy tick to refresh it (no gen_event fan-out — PDDR-000001).
    Members = partisan_membership:node_names(),
    Version = partisan_membership:version(),

    %% The tree engine owns the topology and outstanding-lazy state (PDDR-000002);
    %% default engine is Plumtree. A raw-dispatch engine (PDDR-000004, e.g. Thicket)
    %% receives the group Opts so it can read its own parameters; the typed engine's
    %% init input is left exactly as before.
    Engine = maps:get(engine, Opts, partisan_plumtree_engine),
    Mode = engine_mode(Engine),
    EngineState = Engine:init(engine_init_opts(Mode, Opts, Members)),

    State = #state{
        name = Name,
        node = partisan:node(),
        mods = lists:usort(Mods),
        exchanges = [],
        members_version = Version,
        lazy_tick_period = LazyTickPeriod,
        exchange_tick_period = ExchangeTickPeriod,
        engine = Engine,
        engine_state = EngineState,
        engine_mode = Mode,
        handler_mod = handler_mod(Mode, Mods)
    },

    {ok, State}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.

handle_call({get_peers, Root}, _From, State) ->
    {reply, engine_get_peers(Root, State), State};
handle_call({get_peers, Root, InfoOpts}, _From, State) ->
    {EagerPeers, LazyPeers} = engine_get_peers(Root, State),
    Info = try_node_info(Root, InfoOpts),
    {reply, {EagerPeers, LazyPeers, Info}, State};
handle_call(
    {get_eager_peers, Root},
    _From,
    #state{engine = E, engine_state = ES} = State
) ->
    {reply, E:all_eager_peers(Root, ES), State};
handle_call(
    {get_lazy_peers, Root}, _From, #state{engine = E, engine_state = ES} = State
) ->
    {reply, E:all_lazy_peers(Root, ES), State};
handle_call(
    broadcast_members, _From, #state{engine = E, engine_state = ES} = State
) ->
    {reply, E:all_members(ES), State};
handle_call(exchanges, _From, State = #state{exchanges = Exchanges}) ->
    {reply, Exchanges, State};
handle_call({cancel_exchanges, WhichExchanges}, _From, State) ->
    Cancelled = cancel_exchanges(WhichExchanges, State#state.exchanges),
    {reply, Cancelled, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.

handle_cast({broadcast, MessageId, Message, Mod}, State) ->
    ?LOG_DEBUG("received {broadcast, ~p, Msg, ~p}", [MessageId, Mod]),
    State1 = run_engine(
        fun(E, ES) -> E:broadcast(MessageId, Message, Mod, ES) end, State
    ),
    {noreply, State1};
handle_cast({'$partisan_engine', Msg}, State) ->
    %% A raw-dispatch engine's wire message (PDDR-000004): hand it to the engine
    %% whole and execute the actions it returns. Only raw groups exchange these.
    ?LOG_DEBUG("received engine message ~p", [Msg]),
    State1 = run_engine(fun(E, ES) -> E:handle_message(Msg, ES) end, State),
    {noreply, State1};
handle_cast(
    {broadcast, MessageId, Message, Mod, Round, Root, From} = Msg, State
) ->
    ?LOG_DEBUG(
        "received {broadcast, ~p, Msg, ~p, ~p, ~p, ~p}",
        [MessageId, Mod, Round, Root, From]
    ),
    route(Mod, Msg, State, fun() ->
        Novel = accept_broadcast(Mod, MessageId, Message),
        State1 = run_engine(
            fun(E, ES) ->
                E:handle_broadcast(
                    Novel, MessageId, Message, Mod, Round, Root, From, ES
                )
            end,
            State
        ),
        {noreply, State1}
    end);
handle_cast({prune, Root, From}, State) ->
    ?LOG_DEBUG("received ~p", [{prune, Root, From}]),
    State1 = run_engine(
        fun(E, ES) -> E:handle_prune(Root, From, ES) end, State
    ),
    {noreply, State1};
handle_cast({i_have, MessageId, Mod, Round, Root, From} = Msg, State) ->
    ?LOG_DEBUG("received ~p", [{i_have, MessageId, Mod, Round, Root, From}]),
    route(Mod, Msg, State, fun() ->
        Stale = partisan_util:safe_apply(Mod, is_stale, [MessageId], false),
        State1 = run_engine(
            fun(E, ES) ->
                E:handle_ihave(Stale, MessageId, Mod, Round, Root, From, ES)
            end,
            State
        ),
        {noreply, State1}
    end);
handle_cast({ignored_i_have, MessageId, Mod, Round, Root, From} = Msg, State) ->
    ?LOG_DEBUG(#{
        description => "received ~p",
        message => {ignored_i_have, MessageId, Mod, Round, Root, From}
    }),
    route(Mod, Msg, State, fun() ->
        State1 = run_engine(
            fun(E, ES) ->
                E:handle_ignored_ihave(MessageId, Mod, Round, Root, From, ES)
            end,
            State
        ),
        {noreply, State1}
    end);
handle_cast({graft, MessageId, Mod, Round, Root, From} = Msg, State) ->
    ?LOG_DEBUG("received ~p", [{graft, MessageId, Mod, Round, Root, From}]),
    route(Mod, Msg, State, fun() ->
        Result = partisan_util:safe_apply(
            Mod, graft, [MessageId], {error, nocallback}
        ),
        State1 = run_engine(
            fun(E, ES) ->
                E:handle_graft(Result, MessageId, Mod, Round, Root, From, ES)
            end,
            State
        ),
        {noreply, State1}
    end).

-spec handle_info(
    'exchange_tick' | 'lazy_tick' | {'DOWN', _, 'process', _, _}, state()
) ->
    {noreply, state()}.

handle_info(lazy_tick, State0) ->
    %% Refresh membership from the lock-free snapshot before the periodic tick:
    %% a typed engine flushes its lazy pushes; a raw engine runs its repair tick.
    State1 = maybe_refresh_members(State0),
    Tick =
        case State1#state.engine_mode of
            raw -> fun(E, ES) -> E:repair_tick(ES) end;
            typed -> fun(E, ES) -> E:lazy_tick(ES) end
        end,
    State2 = run_engine(Tick, State1),
    ok = maybe_emit_interior_load(State2),
    schedule_lazy_tick(State2#state.lazy_tick_period),
    {noreply, State2};
handle_info(exchange_tick, #state{exchange_tick_period = Period} = State) ->
    %% Anti-entropy exchange is a typed (Plumtree) mechanism; raw engines skip it.
    State1 =
        case State#state.engine_mode of
            raw -> State;
            typed -> maybe_exchange(State)
        end,
    schedule_exchange_tick(Period),
    {noreply, State1};
handle_info(
    {'DOWN', Ref, process, _Pid, _Reason}, State = #state{exchanges = Exchanges}
) ->
    %% An exchange has terminated
    Exchanges1 = lists:keydelete(Ref, 3, Exchanges),
    {noreply, State#state{exchanges = Exchanges1}};
handle_info(Event, State) ->
    ?LOG_INFO(#{description => "Unhandled info event", event => Event}),
    {noreply, State}.

-spec terminate(term(), state()) -> term().

terminate(_Reason, _State) ->
    ok.

-spec code_change(term() | {down, term()}, state(), term()) -> {ok, state()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =============================================================================
%% DEBUG API
%% =============================================================================

%% @private
try_node_info(Node, Opts) ->
    case partisan:node() of
        Node ->
            partisan:node_info(Opts);
        Peer ->
            case partisan_rpc:call(Peer, partisan, node_info, [Opts], 5000) of
                {badrpc, _} ->
                    #{};
                Info ->
                    Info
            end
    end.

%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Wait indefinitely for a response is returned from the process
-spec debug_get_peers(node(), node()) -> {nodeset(), nodeset()} | no_return().

debug_get_peers(Node, Root) ->
    debug_get_peers(Node, Root, infinity).

%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Waits `Timeout' ms for a response from the server
-spec debug_get_peers(node(), node(), infinity | pos_integer()) ->
    {nodeset(), nodeset()} | no_return().

debug_get_peers(Node, Root, Timeout) ->
    %% This will not work because gen_server uses disterl
    %% gen_server:call({?SERVER, Node}, {get_peers, Root}, Timeout).
    %% TODO reconsider turning this server into a partisan_gen_server
    case Node == partisan:node() of
        true ->
            get_peers(Root);
        false ->
            case partisan_rpc:call(Node, ?MODULE, get_peers, [Root], Timeout) of
                {badrpc, Reason} ->
                    error(Reason);
                {_, _} = Result ->
                    Result;
                {_, _, _} = Result ->
                    Result
            end
    end.

%% @doc return the peers for `Node' for the tree rooted at `Root'.
%% Waits `Timeout' ms for a response from the server
-spec debug_get_peers(node(), node(), [info_opt()], infinity | pos_integer()) ->
    {nodeset(), nodeset(), Info :: map()} | no_return().

debug_get_peers(Node, Root, Opts, Timeout) ->
    %% This will not work because gen_server uses disterl
    %% gen_server:call({?SERVER, Node}, {get_peers, Root}, Timeout).
    %% TODO reconsider turning this server into a partisan_gen_server
    case Node == partisan:node() of
        true ->
            get_peers(Root, Opts);
        false ->
            case
                partisan_rpc:call(
                    Node, ?MODULE, get_peers, [Root, Opts], Timeout
                )
            of
                {badrpc, Reason} ->
                    error(Reason);
                {_, _} = Result ->
                    Result;
                {_, _, _} = Result ->
                    Result
            end
    end.

%% -----------------------------------------------------------------------------
%% @doc return peers for all `Nodes' for tree rooted at `Root'
%% Wait indefinitely for a response is returned from the process
%% @end
%% -----------------------------------------------------------------------------
-spec debug_get_tree(node(), [node()]) ->
    [{node(), {nodeset(), nodeset()} | down}].

debug_get_tree(Root, Nodes) ->
    debug_get_tree(Root, Nodes, infinity).

%% -----------------------------------------------------------------------------
%% @doc return peers for all `Nodes' for tree rooted at `Root'
%% Wait `Timeout' for a response is returned from the process
%% @end
%% -----------------------------------------------------------------------------
-spec debug_get_tree(node(), [node()], timeout()) ->
    [{node(), {nodeset(), nodeset()} | down}].

debug_get_tree(Root, Nodes, Timeout) ->
    [
        begin
            try
                {Node, debug_get_peers(Node, Root, Timeout)}
            catch
                _:Reason ->
                    ?LOG_INFO(#{
                        description =>
                            "Call to get remote root tree failed.",
                        peer => Node,
                        root => Root,
                        reason => Reason
                    }),
                    {Node, down}
            end
        end
     || Node <- Nodes
    ].

%% -----------------------------------------------------------------------------
%% @doc return peers for all `Nodes' for tree rooted at `Root'
%% Wait `Timeout' for a response is returned from the process
%% @end
%% -----------------------------------------------------------------------------
-spec debug_get_tree(node(), [node()], [info_opt()], timeout()) ->
    [{node(), {nodeset(), nodeset(), Info :: map()} | down}].

debug_get_tree(Root, Nodes, Opts, Timeout) ->
    [
        begin
            try
                {Node, debug_get_peers(Node, Root, Opts, Timeout)}
            catch
                _:Reason ->
                    ?LOG_INFO(#{
                        description =>
                            "Call to get remote root tree failed.",
                        peer => Node,
                        root => Root,
                        reason => Reason
                    }),
                    {Node, down}
            end
        end
     || Node <- Nodes
    ].

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @private
%% @doc Decide whether a received broadcast is novel (this drives the eager/lazy
%% tree decision in {@link handle_broadcast/8}) and, for handlers that opt into
%% the non-blocking contract, deliver the heavy apply off this process.
%%
%% Handlers exporting `claim/2'  use the split contract: `claim/2'
%% runs here (fast, atomic) and, when the message is novel, the payload is
%% applied off-path via a cast to the handler's own process. Handlers that do
%% not export `claim/2' keep the synchronous `merge/2' path unchanged.
%% @end
%% -----------------------------------------------------------------------------
accept_broadcast(Mod, MessageId, Message) ->
    case erlang:function_exported(Mod, claim, 2) of
        true ->
            case
                partisan_util:safe_apply(
                    Mod, claim, [MessageId, Message], false
                )
            of
                true ->
                    %% Hand the heavy apply to the handler's own process and
                    %% return immediately. gen_server:cast/2 is non-blocking and
                    %% safe even if the handler process is not registered.
                    _ = gen_server:cast(
                        Mod, {'$partisan_apply', MessageId, Message}
                    ),
                    true;
                false ->
                    false
            end;
        false ->
            partisan_util:safe_apply(Mod, merge, [MessageId, Message], false)
    end.

%% -----------------------------------------------------------------------------
%% @doc Returns the registered name of the broadcast group that hosts `Mod'.
%% Partisan's default handlers (?BROADCAST_MODS) live in the group registered
%% under the legacy name (?SERVER), preserved for rolling upgrade; every other
%% handler gets a derived, per-group name that is identical on all nodes.
%% @end
%% -----------------------------------------------------------------------------
-spec group_name(module()) -> atom().

group_name(Mod) ->
    case lists:member(Mod, ?BROADCAST_MODS) of
        true -> ?SERVER;
        false -> list_to_atom("partisan_bcast_" ++ atom_to_list(Mod))
    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc Compat shim / router: if `Mod' is not handled by this group, forward the
%% original message to the group that does. Used during a rolling upgrade, when
%% an old node addresses every handler to the legacy name; otherwise run
%% `LocalFun' to process the message here.
%% @end
%% -----------------------------------------------------------------------------
route(Mod, Msg, State, LocalFun) ->
    case lists:member(Mod, State#state.mods) of
        true ->
            LocalFun();
        false ->
            Target = group_name(Mod),
            case
                Target =/= State#state.name andalso
                    whereis(Target) =/= undefined
            of
                true ->
                    _ = gen_server:cast(Target, Msg),
                    {noreply, State};
                false ->
                    %% No dedicated group here; handle locally so the message is
                    %% still delivered.
                    LocalFun()
            end
    end.

%% @private
%% @doc Run an engine callback `Fun(EngineModule, EngineState) -> {ES, Actions}',
%% execute the actions it returns via the transport, and store the new engine
%% state (PDDR-000002). Execution is mode-aware: a typed engine returns `{send,
%% Peer, Msg, Mod}' actions; a raw engine (PDDR-000004) returns Mod-less
%% send/deliver/fetch actions run against the group's single `handler_mod'.
run_engine(Fun, #state{engine = E, engine_state = ES} = State) ->
    {ES2, Actions} = Fun(E, ES),
    ok = execute_engine_actions(State#state.engine_mode, Actions, State),
    State#state{engine_state = ES2}.

%% @private Surfaces a raw engine's interior-load gauge (e.g. Thicket) after
%% its repair tick — the measurement PDDR-000002/000004 gate enabling such an
%% engine on. The engine itself stays pure (no telemetry inside it, so PropEr
%% simulation stays deterministic); this is a read-only query the shell makes
%% on the result. A no-op for engines that do not export `interior_load/1'.
maybe_emit_interior_load(
    #state{engine_mode = raw, engine = E, engine_state = ES, name = Name}
) ->
    case erlang:function_exported(E, interior_load, 1) of
        true ->
            partisan_telemetry:execute(
                [partisan, broadcast, interior_load],
                #{value => E:interior_load(ES)},
                #{group => Name, engine => E}
            );
        false ->
            ok
    end;
maybe_emit_interior_load(#state{}) ->
    ok.

%% @private
execute_engine_actions(typed, Actions, _State) ->
    execute_actions(Actions);
execute_engine_actions(raw, Actions, #state{handler_mod = Mod}) ->
    Self = partisan:node(),
    lists:foreach(fun(A) -> execute_raw_action(A, Mod, Self) end, Actions).

%% @private
execute_actions(Actions) ->
    lists:foreach(
        fun({send, Peer, Msg, Mod}) -> send(Msg, Mod, Peer) end, Actions
    ).

%% @private Execute one raw-dispatch action (PDDR-000004) for handler `Mod':
%%   * `send'    — wrap the wire message and cast it to the peer's group;
%%   * `deliver' — hand the received payload to the handler (store + apply), the
%%                 same primitive the typed path uses for a novel broadcast;
%%   * `fetch'   — re-supply a specific missing id via the handler's `graft/1',
%%                 stamping the piggyback load the action carries.
execute_raw_action({send, Peer, Msg}, Mod, _Self) ->
    send({'$partisan_engine', Msg}, Mod, Peer);
execute_raw_action({deliver, MessageId, Payload}, Mod, _Self) ->
    _ = accept_broadcast(Mod, MessageId, Payload),
    ok;
execute_raw_action({fetch, Peer, MessageId, Root, Load}, Mod, Self) ->
    case
        partisan_util:safe_apply(Mod, graft, [MessageId], {error, nocallback})
    of
        {ok, Payload} ->
            Data = {data, MessageId, Payload, Root, Load, Self},
            send({'$partisan_engine', Data}, Mod, Peer);
        _ ->
            ok
    end.

%% @private Engine dispatch mode: a raw engine exports `dispatch_mode/0 -> raw';
%% anything else is driven in the default typed mode.
engine_mode(Engine) ->
    _ = code:ensure_loaded(Engine),
    case erlang:function_exported(Engine, dispatch_mode, 0) of
        true -> Engine:dispatch_mode();
        false -> typed
    end.

%% @private A raw group hosts a single handler (used by the shell for
%% deliver/fetch); a typed group dispatches per-message and needs none here.
engine_init_opts(raw, Opts, Members) -> Opts#{members => Members};
engine_init_opts(typed, _Opts, Members) -> #{members => Members}.

handler_mod(raw, Mods) ->
    case lists:usort(Mods) of
        [Mod | _] -> Mod;
        [] -> undefined
    end;
handler_mod(typed, _Mods) ->
    undefined.

%% @private
engine_get_peers(Root, #state{engine = E, engine_state = ES}) ->
    E:get_peers(Root, ES).

%% -----------------------------------------------------------------------------
%% @private
%% @doc Poll the membership snapshot version; if it changed, re-read members and
%% apply joins/leaves. Replaces the previous gen_event-driven update path
%% : each broadcast instance pulls membership from the lock-free
%% snapshot rather than subscribing to `partisan_peer_service_events'.
%% @end
%% -----------------------------------------------------------------------------
maybe_refresh_members(#state{members_version = Version} = State) ->
    case partisan_membership:version() of
        Version ->
            State;
        NewVersion ->
            Members = partisan_membership:node_names(),
            run_engine(
                fun(E, ES) -> E:update_members(Members, ES) end,
                State#state{members_version = NewVersion}
            )
    end.

%% @private
maybe_exchange(#state{engine = E, engine_state = ES} = State) ->
    %% This checks for any channel connection, not specifically the broadcast
    %% channel.
    Connected = partisan_peer_connections:nodes(),
    Peer = E:select_exchange_peer(Connected, ES),
    maybe_exchange(Peer, State).

maybe_exchange(undefined, State) ->
    State;
maybe_exchange(_, #state{mods = []} = State) ->
    State;
maybe_exchange(Peer, State) ->
    %% limit the number of exchanges this node can start concurrently.
    %% the exchange must (currently?) implement any "inbound" concurrency limits
    Limit = partisan_config:get(broadcast_start_exchange_limit),

    case length(State#state.exchanges) >= Limit of
        true ->
            State;
        false ->
            maybe_exchange(Peer, State, State#state.mods)
    end.

%% @private
maybe_exchange(_Peer, State, []) ->
    State;
maybe_exchange(Peer, #state{mods = [_ | Mods]} = State, [H | T]) ->
    %% We place the current Mod at the end of the list i.e. results in a
    %% roundrobin algorithm for when limit =/= length(Mods)
    NewState = State#state{mods = Mods ++ [H]},

    case lists:keyfind(H, 1, State#state.exchanges) of
        {H, _, _, _} ->
            %% We skip current Mod as there is already an exchange for it
            ?LOG_DEBUG(
                "Ignoring exchange request for ~p with ~p, "
                "there is already another exchange running "
                "for the same handler.",
                [H, Peer]
            ),
            maybe_exchange(Peer, NewState, T);
        false ->
            maybe_exchange(Peer, exchange(Peer, State, H), T)
    end.

%% @private
exchange(Peer, #state{exchanges = Exchanges} = State, Mod) ->
    case catch Mod:exchange(Peer) of
        ignore ->
            ?LOG_DEBUG(
                "~p ignored exchange request with ~p.", [Mod, Peer]
            ),
            State;
        ok ->
            ?LOG_DEBUG(
                "~p accepted exchange request with ~p.", [Mod, Peer]
            ),
            State;
        {ok, Pid} ->
            ?LOG_DEBUG(
                "Started ~p exchange with ~p (~p).", [Mod, Peer, Pid]
            ),
            Ref = monitor(process, Pid),
            State#state{exchanges = [{Mod, Peer, Ref, Pid} | Exchanges]};
        {error, _Reason} ->
            State;
        _ ->
            State
    end.

%% @private
cancel_exchanges(all, Exchanges) ->
    kill_exchanges(Exchanges);
cancel_exchanges(WhichProc, Exchanges) when
    is_reference(WhichProc) orelse is_pid(WhichProc)
->
    KeyPos =
        case is_reference(WhichProc) of
            true -> 3;
            false -> 4
        end,
    case lists:keyfind(WhichProc, KeyPos, Exchanges) of
        false ->
            [];
        Exchange ->
            kill_exchange(Exchange),
            [Exchange]
    end;
cancel_exchanges(Which, Exchanges) ->
    Filter = exchange_filter(Which),
    ToCancel = [Ex || Ex <- Exchanges, Filter(Ex)],
    kill_exchanges(ToCancel).

%% @private
kill_exchanges(Exchanges) ->
    _ = [kill_exchange(Exchange) || Exchange <- Exchanges],
    Exchanges.

%% @private
kill_exchange({_, _, _, ExchangePid}) ->
    exit(ExchangePid, cancel_exchange).

%% @private
exchange_filter({peer, Peer}) ->
    fun({_, ExchangePeer, _, _}) ->
        Peer =:= ExchangePeer
    end;
exchange_filter({mod, Mod}) ->
    fun({ExchangeMod, _, _, _}) ->
        Mod =:= ExchangeMod
    end.

%% @private
-spec send(
    Msg :: partisan:message(),
    Mod :: module(),
    Peers :: [node()] | node()
) -> ok.

send(Msg, Mod, Peers) when is_list(Peers) ->
    _ = [send(Msg, Mod, P) || P <- Peers],
    ok;
send(Msg, Mod, Peer) ->
    instrument_transmission(Msg, Mod),
    Opts = #{channel => broadcast_channel(Mod)},
    %% Target the peer's group for `Mod' — the same deterministic name this
    %% group runs under on every node .
    partisan:cast_message(Peer, group_name(Mod), Msg, Opts).

%% @private
schedule_lazy_tick(Period) ->
    schedule_tick(lazy_tick, lazy_tick_period, Period).

%% @private
schedule_exchange_tick(Period) ->
    schedule_tick(exchange_tick, exchange_tick_period, Period).

%% @private
schedule_tick(Message, Timer, Default) ->
    TickMs = partisan_config:get(Timer, Default),
    %% self() (not ?MODULE) so each broadcast instance ticks itself.
    erlang:send_after(TickMs, self(), Message).

%% @private
instrument_transmission(Message, Mod) ->
    case partisan_config:get(transmission_logging_mfa, undefined) of
        undefined ->
            ok;
        {Module, Function, Args} ->
            ToLog =
                try
                    Mod:extract_log_type_and_payload(Message)
                catch
                    _:Error ->
                        ?LOG_INFO(
                            "Couldn't extract log type and payload. Reason ~p",
                            [Error]
                        ),
                        []
                end,

            lists:foreach(
                fun({Type, Payload}) ->
                    erlang:apply(Module, Function, Args ++ [Type, Payload])
                end,
                ToLog
            )
    end.
