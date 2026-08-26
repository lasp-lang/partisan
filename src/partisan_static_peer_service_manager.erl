%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher Meiklejohn.  All Rights Reserved.
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

-module(partisan_static_peer_service_manager).
-author("Vitor Enes Duarte <vitorenesduarte@gmail.com>").

-behaviour(gen_server).
-behaviour(partisan_peer_service_manager).

-include("partisan_logger.hrl").
-include("partisan.hrl").

%% partisan_peer_service_manager callbacks
-export([cast_message/2]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([decode/1]).
-export([forward_message/2]).
-export([forward_message/3]).
-export([forward_message/4]).
-export([get_local_state/0]).
-export([inject_partition/2]).
-export([join/1]).
-export([leave/0]).
-export([leave/1]).
-export([members/0]).
-export([members_for_orchestration/0]).
-export([on_down/2]).
-export([on_down/3]).
-export([on_up/2]).
-export([on_up/3]).
-export([partitions/0]).
-export([receive_message/3]).
-export([reserve/1]).
-export([resolve_partition/1]).
-export([send_message/2]).
-export([start_link/0]).
-export([supports_capability/1]).
-export([sync_join/1]).
-export([update_members/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(IS_ON_EVENT_FUN(X),
    (is_function(X, 0) orelse is_function(X, 1) orelse is_function(X, 2))
).

-record(state, {
    myself :: partisan:node_spec(),
    pending :: pending(),
    membership :: membership(),
    up_funs = #{} :: node_subs(),
    down_funs = #{} :: node_subs(),
    channel_up_funs = #{} :: channel_subs(),
    channel_down_funs = #{} :: channel_subs(),
    sync_joins = #{} :: sync_joins(),
    %% Nodes and channels we have already announced as up. Connections are
    %% stored before the handshake completes, so the connection table cannot
    %% tell us whether a `connected' signal is the first one for a node —
    %% these make the up/down events edge-triggered.
    up_nodes = sets:new() :: sets:set(node()),
    up_channels = sets:new() :: sets:set({node(), partisan:channel()})
}).

-type state_t() :: #state{}.
-type pending() :: [partisan:node_spec()].
-type membership() :: sets:set(partisan:node_spec()).
-type node_subs() :: #{
    '_' | node() => [
        partisan_peer_service_manager:on_event_fun()
    ]
}.
-type channel_subs() :: #{
    {'_' | node(), '_' | partisan:channel()} => [
        partisan_peer_service_manager:on_event_fun()
    ]
}.
-type sync_joins() :: #{partisan:node_spec() => [gen_server:from()]}.

%%%===================================================================
%%% partisan_peer_service_manager callbacks
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    Opts = [
        {spawn_opt, ?PARALLEL_SIGNAL_OPTIMISATION([])}
    ],
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], Opts).

%% @doc Return membership list.
members() ->
    gen_server:call(?MODULE, members, infinity).

%% @doc Return membership list.
members_for_orchestration() ->
    gen_server:call(?MODULE, members_for_orchestration, infinity).

%% @doc Return local node's view of cluster membership.
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).

%% @doc Trigger function on connection close for a given node.
on_down(Arg, Fun) ->
    on_down(Arg, Fun, #{}).

%% -----------------------------------------------------------------------------
%% @doc Trigger `Fun' when the connection to `Arg' closes. With a `channel'
%% option the trigger is per channel and `Fun' is passed the channel as a
%% second argument; without one it fires when the node's last connection
%% goes, which is what node monitoring is built on.
%%
%% `Arg' is a node, a node spec, or `any'/`'_'' for every node.
%% @end
%% -----------------------------------------------------------------------------
on_down(#{name := Node}, Fun, Opts) ->
    on_down(Node, Fun, Opts);
on_down(any, Fun, Opts) ->
    on_down('_', Fun, Opts);
on_down(Node, Fun, Opts) when
    is_atom(Node) andalso is_map(Opts) andalso ?IS_ON_EVENT_FUN(Fun)
->
    gen_server:call(?MODULE, {on_down, Node, Fun, Opts}, infinity).

%% @doc Trigger function on connection open for a given node.
on_up(Arg, Fun) ->
    on_up(Arg, Fun, #{}).

%% -----------------------------------------------------------------------------
%% @doc Trigger `Fun' when a connection to `Arg' opens. See `on_down/3' for
%% the argument forms and the meaning of the `channel' option.
%% @end
%% -----------------------------------------------------------------------------
on_up(#{name := Node}, Fun, Opts) ->
    on_up(Node, Fun, Opts);
on_up(any, Fun, Opts) ->
    on_up('_', Fun, Opts);
on_up(Node, Fun, Opts) when
    is_atom(Node) andalso is_map(Opts) andalso ?IS_ON_EVENT_FUN(Fun)
->
    gen_server:call(?MODULE, {on_up, Node, Fun, Opts}, infinity).

%% -----------------------------------------------------------------------------
%% @doc Replaces the peer set with `Nodes'. Peers not already known are
%% joined; members and pending peers absent from `Nodes' are dropped, exactly
%% as `leave/1' drops them.
%% @end
%% -----------------------------------------------------------------------------
update_members(Nodes) when is_list(Nodes) ->
    %% Anything that is not a node spec would otherwise be dropped by the
    %% server and quietly shrink the peer set — a list of node *names* would
    %% remove every peer. Fail loudly instead.
    lists:all(fun(S) -> is_map(S) andalso is_map_key(name, S) end, Nodes) orelse
        error(badarg),
    gen_server:call(?MODULE, {update_members, Nodes}, infinity).

%% @doc Send message to a remote manager.
send_message(Name, Message) ->
    gen_server:call(?MODULE, {send_message, Name, Message}, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    Term :: partisan:any_pid() | partisan:any_name(),
    MEssage :: partisan:message()
) -> ok.

cast_message(Term, Message) ->
    FullMessage = {'$gen_cast', Message},
    _ = forward_message(Term, FullMessage, #{}),
    ok.

%% -----------------------------------------------------------------------------
%% @doc Cast a message to a remote gen_server.
%% @end
%% -----------------------------------------------------------------------------
cast_message(Node, ServerRef, Message) ->
    cast_message(Node, ServerRef, Message, #{}).

%% -----------------------------------------------------------------------------
%% @doc Cast a message to a remote gen_server.
%% @end
%% -----------------------------------------------------------------------------
cast_message(Name, ServerRef, Message, Options) ->
    FullMessage = {'$gen_cast', Message},
    _ = forward_message(Name, ServerRef, FullMessage, Options),
    ok.

%% -----------------------------------------------------------------------------
%% @doc Gensym support for forwarding.
%% @end
%% -----------------------------------------------------------------------------
forward_message(Term, Message) ->
    forward_message(Term, Message, #{}).

%% -----------------------------------------------------------------------------
%% @doc Gensym support for forwarding.
%% @end
%% -----------------------------------------------------------------------------
forward_message(PidOrName, Message, _Opts) when
    is_pid(PidOrName); is_atom(PidOrName)
->
    _ = erlang:send(PidOrName, Message),
    ok;
forward_message({global, _} = ServerRef, Message, _Opts) ->
    partisan_peer_service_manager:deliver(ServerRef, Message);
forward_message({via, _, _} = ServerRef, Message, _Opts) ->
    partisan_peer_service_manager:deliver(ServerRef, Message);
forward_message({Name, Node}, Message, Opts) when
    is_atom(Name), is_atom(Node)
->
    case Node == partisan:node() of
        true ->
            _ = erlang:send(Name, Message),
            ok;
        false ->
            forward_message(Node, Name, Message, Opts)
    end;
forward_message(RemoteRef, Message, Opts) ->
    %% A reference is admitted because a process alias (`erlang:alias/1') is
    %% one, and an encoded alias is used as a one-shot reply address by
    %% `partisan_erpc'. Delivery for it is handled by the `is_reference' clause
    %% of `partisan_peer_service_manager:do_deliver/2'.
    partisan_remote_ref:is_pid(RemoteRef) orelse
        partisan_remote_ref:is_name(RemoteRef) orelse
        partisan_remote_ref:is_reference(RemoteRef) orelse
        error(badarg),

    Node = partisan_remote_ref:node(RemoteRef),
    Target = partisan_remote_ref:target(RemoteRef),

    forward_message(Node, Target, Message, Opts).

%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
forward_message(Node, ServerRef, Message, Opts) when is_list(Opts) ->
    forward_message(Node, ServerRef, Message, maps:from_list(Opts));
forward_message(Node, ServerRef, Message, Opts) when is_map(Opts) ->
    %% Local-node forwarding bypasses the forwarding machinery entirely: this
    %% manager dispatches over peer connections and we hold none to ourselves.
    case Node =:= partisan:node() of
        true ->
            partisan_peer_service_manager:deliver(ServerRef, Message);
        false ->
            Channel = maps:get(channel, Opts, ?DEFAULT_CHANNEL),
            gen_server:call(
                ?MODULE,
                {forward_message, Node, Channel, ServerRef, Message, Opts},
                infinity
            )
    end.

%% @doc Receive message from a remote manager.
receive_message(_Peer, Channel, Message) ->
    gen_server:call(?MODULE, {receive_message, Channel, Message}, infinity).

%% @doc Attempt to join a remote node.
join(Node) ->
    gen_server:call(?MODULE, {join, Node}, infinity).

%% @doc Join a remote node, returning once the connection is established.
sync_join(#{name := _} = Node) ->
    gen_server:call(?MODULE, {sync_join, Node}, infinity).

%% @doc Leave the cluster, dropping every peer connection. Membership is
%% reduced to this node.
leave() ->
    gen_server:call(?MODULE, {leave, partisan:node()}, infinity).

%% @doc Remove a peer from the cluster, dropping its connections. Membership
%% here is explicit, so the peer stays out until it is joined again.
leave(Node) ->
    gen_server:call(?MODULE, {leave, Node}, infinity).

%% @doc Decode state.
decode(State) ->
    sets:to_list(State).

%% @doc Reserve a slot for the particular tag.
reserve(Tag) ->
    gen_server:call(?MODULE, {reserve, Tag}, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec supports_capability(Arg :: atom()) -> boolean().

supports_capability(monitoring) ->
    true;
supports_capability(_) ->
    false.

%% @doc Inject a partition.
inject_partition(_Origin, _TTL) ->
    {error, not_implemented}.

%% @doc Resolve a partition.
resolve_partition(_Reference) ->
    {error, not_implemented}.

%% @doc Return partitions.
partitions() ->
    {error, not_implemented}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([]) -> {ok, state_t()}.
init([]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    ok = partisan_peer_connections:init(),

    %% Process connection exits.
    process_flag(trap_exit, true),

    Membership = empty_membership(),
    Myself = partisan:node_spec(),

    %% Seed the lock-free membership snapshot before any reader starts.
    %% Readers (broadcast among them) go through `partisan_membership', so a
    %% manager that only publishes on change leaves them with an empty
    %% membership that does not even contain this node.
    ok = partisan_membership:set(members(Membership)),

    {ok, #state{
        myself = Myself,
        pending = [],
        membership = Membership
    }}.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};
handle_call({on_up, Name, Fun, #{channel := Channel}}, _From, State) when
    is_atom(Channel)
->
    Funs = partisan_util:maps_append(
        {Name, Channel}, Fun, State#state.channel_up_funs
    ),
    {reply, ok, State#state{channel_up_funs = Funs}};
handle_call({on_up, Name, Fun, _}, _From, State) ->
    Funs = partisan_util:maps_append(Name, Fun, State#state.up_funs),
    {reply, ok, State#state{up_funs = Funs}};
handle_call({on_down, Name, Fun, #{channel := Channel}}, _From, State) when
    is_atom(Channel)
->
    Funs = partisan_util:maps_append(
        {Name, Channel}, Fun, State#state.channel_down_funs
    ),
    {reply, ok, State#state{channel_down_funs = Funs}};
handle_call({on_down, Name, Fun, _}, _From, State) ->
    Funs = partisan_util:maps_append(Name, Fun, State#state.down_funs),
    {reply, ok, State#state{down_funs = Funs}};
handle_call({leave, #{name := Node}}, From, State) ->
    handle_call({leave, Node}, From, State);
handle_call({leave, Node}, _From, #state{} = State0) when is_atom(Node) ->
    State =
        case Node == partisan:node() of
            true ->
                %% We are the one leaving: every peer goes, we remain.
                lists:foldl(
                    fun internal_leave/2,
                    State0,
                    peer_names(State0#state.membership)
                );
            false ->
                internal_leave(Node, State0)
        end,
    {reply, ok, State};
handle_call({join, Spec}, _From, #state{} = State) ->
    {reply, ok, internal_join(Spec, State)};
handle_call(
    {sync_join, #{name := Node}},
    _From,
    #state{myself = #{name := Node}} = State
) ->
    %% Ignoring self join.
    {reply, ok, State};
handle_call({sync_join, Spec}, From, #state{} = State0) ->
    %% The reply is deferred until the peer connects; see the `connected'
    %% clause of `handle_info/2'.
    State = internal_join(Spec, State0),
    Waiting = partisan_util:maps_append(Spec, From, State#state.sync_joins),
    {noreply, State#state{sync_joins = Waiting}};
handle_call({update_members, Desired}, _From, #state{} = State0) ->
    DesiredNames = [Name || #{name := Name} <- Desired],
    Myself = partisan:node(),

    %% Anything we know about and is no longer wanted goes, ourselves aside.
    Obsolete = [
        Name
     || Name <- known_names(State0),
        Name =/= Myself,
        not lists:member(Name, DesiredNames)
    ],
    State1 = lists:foldl(fun internal_leave/2, State0, Obsolete),

    %% Whatever is left over is new and gets joined.
    Known = known_names(State1),
    State = lists:foldl(
        fun(#{name := Name} = Spec, Acc) ->
            case Name =:= Myself orelse lists:member(Name, Known) of
                true -> Acc;
                false -> internal_join(Spec, Acc)
            end
        end,
        State1,
        Desired
    ),
    {reply, ok, State};
handle_call({send_message, Name, Message}, _From, #state{} = State) ->
    Result = do_send_message(Name, Message),
    {reply, Result, State};
handle_call(
    {forward_message, Name, Channel, ServerRef, Message, _Options},
    _From,
    #state{} = State
) ->
    Result = do_send_message(
        Name, Channel, {forward_message, ServerRef, Message}
    ),
    {reply, Result, State};
handle_call({receive_message, Channel, Message}, _From, State) ->
    handle_message(Message, Channel, State);
handle_call(members, _From, #state{membership = Membership} = State) ->
    Members = [P || #{name := P} <- members(Membership)],
    {reply, {ok, Members}, State};
handle_call(
    members_for_orchestration, _From, #state{membership = Membership} = State
) ->
    {reply, {ok, members(Membership)}, State};
handle_call(get_local_state, _From, #state{membership = Membership} = State) ->
    {reply, {ok, Membership}, State};
handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.
handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),
    {noreply, State}.

handle_info({'EXIT', From, Reason}, #state{} = State) ->
    %% A connection died. Prune it, and if it was the last one for that node
    %% or channel, tell the subscribers — this is what node monitoring reads.
    try partisan_peer_connections:prune(From, Reason) of
        {Info, [Connection]} ->
            #{name := Node} = partisan_peer_connections:node_spec(Info),
            Channel = partisan_peer_connections:channel(Connection),

            partisan_peer_connections:count(Node, Channel) == 0 andalso
                channel_down(Node, Channel, State),

            partisan_peer_connections:count(Info) == 0 andalso
                node_down(Node, State),

            {noreply, State};
        _ ->
            {noreply, State}
    catch
        error:badarg ->
            %% The connection pid was not in the table.
            {noreply, State}
    end;
handle_info(
    {connected, Spec, Channel, _Tag, _RemoteState},
    #state{
        pending = Pending0,
        membership = Membership0
    } = State0
) ->
    #{name := Node} = Spec,

    State1 =
        case lists:member(Spec, Pending0) of
            true ->
                %% Move out of pending and into our membership.
                Pending = Pending0 -- [Spec],
                Membership = sets:add_element(Spec, Membership0),

                %% Announce to the peer service.
                ok = update_membership(Membership),

                %% Establish any new connections.
                ok = establish_connections(Pending, Membership),

                ?LOG_INFO(#{
                    description => "Join ACCEPTED",
                    peer_node => Node,
                    member_view_count => sets:size(Membership)
                }),

                State0#state{pending = Pending, membership = Membership};
            false ->
                %% An additional channel to a peer we already know, or a
                %% reconnection. Neither changes the membership.
                State0
        end,

    State2 = channel_up(Node, Channel, State1),
    State = node_up(Node, State2),

    {noreply, reply_sync_joins(Spec, State)};
handle_info(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled info event", event => Event}),
    {noreply, State}.

%% @private
-spec terminate(term(), state_t()) -> term().

terminate(_Reason, #state{}) ->
    Fun = fun(_K, Pids) ->
        lists:foreach(
            fun({_ListenAddr, _Channel, Pid}) ->
                gen_server:stop(Pid, normal, infinity),
                ok
            end,
            Pids
        )
    end,
    ok = partisan_peer_connections:foreach(Fun).

%% @private
-spec code_change(term() | {down, term()}, state_t(), term()) ->
    {ok, state_t()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc Membership is explicit here: it starts as just this node and only
%% grows through `join/1', `sync_join/1' or `update_members/1'. Nothing is
%% read back from disk — the peer set the operator configures is the only
%% source of truth, so a restart must not resurrect peers that were removed.
empty_membership() ->
    sets:add_element(partisan:node_spec(), sets:new()).

%% @private
%% @doc Registers `Spec' as a pending peer and starts connecting to it.
internal_join(#{name := Node} = Spec, #state{} = State) ->
    ok = partisan_util:maybe_connect_disterl(Node),

    Pending =
        case lists:member(Spec, State#state.pending) of
            true -> State#state.pending;
            false -> [Spec | State#state.pending]
        end,

    ok = partisan_peer_service_manager:connect(Spec),

    State#state{pending = Pending}.

%% @private
%% @doc Drops `Node' from the membership, from pending, and kills its
%% connections. A node can be present under more than one spec — an address
%% change, say — so every spec carrying its name goes.
internal_leave(Node, #state{} = State0) when is_atom(Node) ->
    Membership = sets:filter(
        fun(#{name := Name}) -> Name =/= Node end, State0#state.membership
    ),
    Pending = [
        Spec
     || #{name := Name} = Spec <- State0#state.pending, Name =/= Node
    ],

    ok = partisan_peer_service_manager:disconnect([Node]),
    ok = update_membership(Membership),

    ?LOG_INFO(#{
        description => "Leave PROCESSED",
        peer_node => Node,
        member_view_count => sets:size(Membership)
    }),

    State0#state{membership = Membership, pending = Pending}.

%% @private
%% @doc Names of every peer we know of, whether connected or still pending.
known_names(#state{} = State) ->
    Pending = [Name || #{name := Name} <- State#state.pending],
    peer_names(State#state.membership) ++ Pending.

%% @private
members(Membership) ->
    sets:to_list(Membership).

%% @private
%% @doc Names of every member other than ourselves.
peer_names(Membership) ->
    Myself = partisan:node(),
    [Name || #{name := Name} <- members(Membership), Name =/= Myself].

%% @private
%% @doc Publishes `Membership' to the lock-free snapshot readers use
%% (`partisan_membership') and notifies its subscribers.
update_membership(Membership) ->
    MemberList = members(Membership),
    ok = partisan_membership:set(MemberList),
    ok = partisan_membership:notify(MemberList).

%% =============================================================================
%% CONNECTION EVENTS
%% =============================================================================
%%
%% `on_up'/`on_down' subscriptions are what node monitoring is built on
%% (`partisan_monitor' registers wildcard callbacks at boot). Node-level
%% events are edge-triggered: `node_up' fires on a peer's first connection
%% and `node_down' when its last one goes, with the channel-level pair doing
%% the same per channel.

%% @private
node_up(Node, #state{up_nodes = Up} = State) ->
    case sets:is_element(Node, Up) of
        true ->
            State;
        false ->
            ok = apply_node_funs(Node, State#state.up_funs),
            State#state{up_nodes = sets:add_element(Node, Up)}
    end.

%% @private
node_down(Node, #state{up_nodes = Up} = State) ->
    case sets:is_element(Node, Up) of
        false ->
            State;
        true ->
            ok = apply_node_funs(Node, State#state.down_funs),
            State#state{up_nodes = sets:del_element(Node, Up)}
    end.

%% @private
channel_up(Node, Channel, #state{up_channels = Up} = State) ->
    case sets:is_element({Node, Channel}, Up) of
        true ->
            State;
        false ->
            ok = apply_channel_funs(
                Node, Channel, State#state.channel_up_funs
            ),
            State#state{up_channels = sets:add_element({Node, Channel}, Up)}
    end.

%% @private
channel_down(Node, Channel, #state{up_channels = Up} = State) ->
    case sets:is_element({Node, Channel}, Up) of
        false ->
            State;
        true ->
            ok = apply_channel_funs(
                Node, Channel, State#state.channel_down_funs
            ),
            State#state{up_channels = sets:del_element({Node, Channel}, Up)}
    end.

%% @private
%% @doc Callbacks registered for `Node' plus those registered for every node.
apply_node_funs(Node, Subs) ->
    Funs = maps:get('_', Subs, []) ++ maps:get(Node, Subs, []),
    apply_event_funs(Funs, Node, undefined).

%% @private
%% @doc Channel subscriptions are keyed by `{Node, Channel}', either half of
%% which may be the `'_'' wildcard, so all four combinations match.
apply_channel_funs(Node, Channel, Subs) ->
    Funs = lists:append([
        maps:get({'_', '_'}, Subs, []),
        maps:get({'_', Channel}, Subs, []),
        maps:get({Node, '_'}, Subs, []),
        maps:get({Node, Channel}, Subs, [])
    ]),
    apply_event_funs(Funs, Node, Channel).

%% @private
%% @doc A subscriber takes no argument, the node, or the node and channel.
%% A callback is not allowed to take the manager down with it.
apply_event_funs(Funs, Node, Channel) ->
    _ = [
        case erlang:fun_info(F, arity) of
            {arity, 0} -> catch F();
            {arity, 1} -> catch F(Node);
            {arity, 2} -> catch F(Node, Channel)
        end
     || F <- Funs
    ],
    ok.

%% @private
%% @doc Releases any `sync_join/1' callers waiting on `Spec'.
reply_sync_joins(Spec, #state{sync_joins = Waiting} = State) ->
    case maps:take(Spec, Waiting) of
        {Froms, Rest} ->
            _ = [gen_server:reply(From, ok) || From <- Froms],
            State#state{sync_joins = Rest};
        error ->
            State
    end.

%% =============================================================================
%% CONNECTIONS
%% =============================================================================

%% @private
establish_connections(Pending, Membership) ->
    %% Reconnect disconnected members and members waiting to join.
    Members = members(Membership),
    AllPeers = lists:filter(
        fun(#{name := N}) -> partisan:node() =/= N end,
        Members ++ Pending
    ),
    lists:foreach(fun partisan_peer_service_manager:connect/1, AllPeers),
    ok.

handle_message({forward_message, ServerRef, Message}, _Channel, State) ->
    partisan_peer_service_manager:deliver(ServerRef, Message),
    {reply, ok, State};
handle_message(Msg, _Channel, State) ->
    %% An envelope this version does not recognise must not take the manager
    %% down. `handle_message/3' is called straight from `handle_call/3' here,
    %% so without this clause `function_clause' crashed the manager *and* threw
    %% into the connection process. Covered by
    %% `partisan_manager_conformance_test:unknown_envelope_is_survivable/1'.
    ?LOG_WARNING(#{description => "Unhandled message", message => Msg}),
    {reply, ok, State}.

%% @private
-spec do_send_message(
    Node :: node() | partisan:node_spec(), Message :: term()
) ->
    ok | {error, disconnected | not_yet_connected} | {error, term()}.

do_send_message(Node, Message) ->
    do_send_message(Node, ?DEFAULT_CHANNEL, Message).

%% @private
%% `dispatch_pid/2' falls back to the default channel when `Channel' has no
%% connection (see `channel_fallback').
-spec do_send_message(
    Node :: node() | partisan:node_spec(),
    Channel :: partisan:channel(),
    Message :: term()
) ->
    ok | {error, disconnected | not_yet_connected} | {error, term()}.

do_send_message(Node, Channel, Message) ->
    %% Find a connection for the remote node, if we have one.
    case partisan_peer_connections:dispatch_pid(Node, Channel) of
        {ok, Pid} ->
            gen_server:cast(Pid, {send_message, Message});
        {error, _} = Error ->
            Error
    end.
