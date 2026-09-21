%% =============================================================================
%%  partisan_peer_discovery_agent.erl -
%%
%%  Copyright (c) 2022 Alejandro M. Ramallo. All rights reserved.
%%
%%  Licensed under the Apache License, Version 2.0 (the "License");
%%  you may not use this file except in compliance with the License.
%%  You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%%  Unless required by applicable law or agreed to in writing, software
%%  distributed under the License is distributed on an "AS IS" BASIS,
%%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%  See the License for the specific language governing permissions and
%%  limitations under the License.
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc This state machine polls a discovery backend (the callback module) and
%% joins the peers it reports.
%%
%% A discovery answer is a hint for joining, never a membership authority: the
%% agent only ever adds peers, through `partisan_peer_service:add_members/1'.
%% A member the backend stops reporting stays a member; a node leaves the
%% cluster only through `partisan_peer_service:leave/0,1'. The reason is that
%% every backend answers from a readiness- or health-gated, eventually
%% consistent source (DNS, an orchestrator's endpoint list), which omits a
%% node that is booting or briefly unhealthy. Treating that omission as a
%% departure removed the node from the membership and gossiped the removal,
%% and the node then shut its own peer service down on receiving it
%% ("membership doesn't contain us") — once per poll, for as long as it stayed
%% out of the answer. Pinned by `partisan_peer_discovery_agent_test' and
%% `partisan_SUITE:discovery_never_evicts_test/1'.
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_peer_discovery_agent).
-behaviour(gen_statem).

-include_lib("kernel/include/logger.hrl").
-include("partisan_util.hrl").

-record(state, {
    enabled :: boolean(),
    callback_mod :: module() | undefined,
    callback_config :: map() | undefined,
    callback_state :: any() | undefined,
    initial_delay :: integer() | undefined,
    polling_interval :: integer() | undefined,
    timeout :: integer() | undefined,
    peers = [] :: [partisan:node_spec()]
}).

-type state() :: #state{}.

%% API
-export([start/0]).
-export([start_link/0]).
-export([lookup/0]).
-export([enable/0]).
-export([disable/0]).
-export([status/0]).

%% gen_statem callbacks
-export([init/1]).
-export([callback_mode/0]).
-export([terminate/3]).
-export([code_change/4]).

%% gen_statem states
-export([enabled/3]).
-export([disabled/3]).

%% =============================================================================
%% CALLBACKS
%% =============================================================================

%% -----------------------------------------------------------------------------
%% Initializes the peer discovery agent implementation
%% -----------------------------------------------------------------------------
-callback init(Opts :: map()) ->
    {ok, State :: any()}
    | {error, Reason :: any()}.

%% -----------------------------------------------------------------------------
%%
%% -----------------------------------------------------------------------------
-callback lookup(State :: any(), Timeout :: timeout()) ->
    {ok, [partisan:node_spec()], NewState :: any()}
    | {error, Reason :: any(), NewState :: any()}.

%% =============================================================================
%% API
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec start() ->
    {ok, pid()} | ignore | {error, term()}.

start() ->
    Opts = partisan_config:get(peer_discovery, #{}),
    gen_statem:start({local, ?MODULE}, ?MODULE, [Opts], []).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.

start_link() ->
    Opts = partisan_config:get(peer_discovery, #{}),
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
lookup() ->
    gen_statem:call(?MODULE, lookup, 5000).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec enable() -> ok.

enable() ->
    gen_statem:call(?MODULE, enable, 5000).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec disable() -> ok.

disable() ->
    gen_statem:call(?MODULE, disable, 5000).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec status() -> enabled | disabled.

status() ->
    gen_statem:call(?MODULE, status, 5000).

%% =============================================================================
%% GEN_STATEM CALLBACKS
%% =============================================================================

init([#{enabled := true, type := Mod} = Opts]) when is_atom(Mod) ->
    CBConfig = maps:get(config, Opts, #{}),

    case Mod:init(CBConfig) of
        {ok, CBState} ->
            Delay = maps:get(initial_delay, Opts, timer:seconds(10)),
            Interval = maps:get(polling_interval, Opts, timer:seconds(60)),
            Timeout = maps:get(timeout, Opts, timer:seconds(5)),

            State = #state{
                enabled = true,
                callback_mod = Mod,
                callback_config = CBConfig,
                initial_delay = Delay,
                polling_interval = Interval,
                timeout = Timeout,
                callback_state = CBState
            },

            Action =
                case Delay > 0 of
                    true ->
                        ?LOG_INFO(#{
                            description =>
                                "Peer discovery agent will start after "
                                "initial delay",
                            delay_msecs => Delay
                        }),
                        {state_timeout, Delay, lookup, []};
                    false ->
                        {next_event, internal, lookup}
                end,

            {ok, enabled, State, [Action]};
        {error, Reason} ->
            ?LOG_ERROR(#{
                description =>
                    "Peer discovery agent could not start due to "
                    "misconfiguration.",
                reason => Reason
            }),
            {stop, Reason}
    end;
init([#{enabled := true} = Opts]) ->
    {stop, {invalid_config, Opts}};
init([Opts]) when is_list(Opts) ->
    init([maps:from_list(Opts)]);
init(_) ->
    State = #state{enabled = false},
    {ok, disabled, State}.

callback_mode() ->
    state_functions.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% =============================================================================
%% STATE FUNCTIONS
%% =============================================================================

%% -----------------------------------------------------------------------------
%% @doc In this state the agent uses the callback module to discover peers
%% by calling its lookup/2 callback.
%% @end
%% -----------------------------------------------------------------------------
enabled({call, From}, enable, _State) ->
    ok = gen_statem:reply(From, ok),
    keep_state_and_data;
enabled({call, From}, disable, State) ->
    ok = gen_statem:reply(From, ok),
    {next_state, disabled, State};
enabled({call, From}, lookup, State0) ->
    {Members, State} = lookup(State0),
    ok = gen_statem:reply(From, {ok, Members}),
    {keep_state, State};
enabled(state_timeout, lookup, State) ->
    %% The polling interval timeout, we need to perform a lookup
    {keep_state, State, [{next_event, internal, lookup}]};
enabled(internal, lookup, State0) ->
    %% Hand the whole list to the peer service rather than joining the peers
    %% one by one, so that the manager decides how to join based on the
    %% topology/strategy. add_members/1 skips the ones already known and
    %% never removes a member (see the module doc).
    {Members, State} = lookup(State0),
    ok = partisan_peer_service:add_members(Members),

    %% Schedule next lookup
    Action = {state_timeout, State#state.polling_interval, lookup, []},
    {keep_state, State, [Action]};
enabled(EventType, EventContent, State) ->
    handle_common_event(EventType, EventContent, enabled, State).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
disabled({call, From}, disable, _State) ->
    ok = gen_statem:reply(From, ok),
    keep_state_and_data;
disabled({call, From}, enable, #state{callback_mod = undefined}) ->
    ok = gen_statem:reply(From, {error, missing_configuration}),
    keep_state_and_data;
disabled({call, From}, enable, State) ->
    ok = gen_statem:reply(From, ok),
    {next_state, enabled, State, [{next_event, internal, next}]};
disabled({call, From}, lookup, _) ->
    ok = gen_statem:reply(From, {error, disabled}),
    keep_state_and_data;
disabled(EventType, EventContent, State) ->
    handle_common_event(EventType, EventContent, disabled, State).

%% =============================================================================
%% PRIVATE
%% =============================================================================

%% @private
handle_common_event({call, From}, status, StateName, _State) ->
    ok = gen_statem:reply(From, StateName),
    keep_state_and_data;
handle_common_event(EventType, EventContent, _StateName, State) ->
    case EventType of
        {call, From} ->
            ok = gen_statem:reply(From, {error, unknown_call});
        _ ->
            ok
    end,

    ?LOG_DEBUG(#{
        description => "Unhandled event",
        callback_mod => State#state.callback_mod,
        type => EventType,
        content => EventContent
    }),

    keep_state_and_data.

%% @private
-spec lookup(state()) -> {[partisan:node_spec()], state()}.

lookup(State0) ->
    CBMod = State0#state.callback_mod,
    CBState0 = State0#state.callback_state,
    Timeout = State0#state.timeout,

    {ok, Peers, CBState} = CBMod:lookup(CBState0, Timeout),

    ?LOG_DEBUG(#{
        description => "Got peer discovery lookup response",
        callback_mod => CBMod,
        response => Peers
    }),

    State = State0#state{callback_state = CBState},
    {[partisan:node_spec() | Peers], State}.
