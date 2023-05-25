%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Christopher Meiklejohn.  All Rights Reserved.
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
%% @doc This module realises the {@link partisan_peer_service_manager}
%% behaviour implementing a full-mesh topology.
%%
%% == Characteristics ==
%% <ul>
%% <li>Uses TCP/IP.</li>
%% <li>All nodes communicate and maintain connections with all other nodes.</li>
%% <li>Nodes periodically send heartbeat messages. The service considers a node
%% "failed" when it misses X heartbeats.</li>
%% <li>Point-to-point messaging with a single network hop.</li>
%% <li>Eventually consistent membership maintained in a CRDT and replicated
%% using gossip.</li>
%% <li>Scalability limited to hundres of nodes (60-200 nodes).</li>
%% </ul>
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_pluggable_peer_service_manager).

-behaviour(gen_server).
-behaviour(partisan_peer_service_manager).

-include("partisan_logger.hrl").
-include("partisan.hrl").

-define(SET_FROM_LIST(L), sets:from_list(L, [{version, 2}])).
-define(IS_ON_EVENT_FUN(X), (
    is_function(X, 0) orelse is_function(X, 1) orelse is_function(X, 2)
)).

-ifdef(TEST).
    -define(INTERPOSITION, true).
-endif.

%% returns ok
-define(FIRE_FWD_PRE_INTERPOSITIONS(Node, Msg, S),
    ?FIRE_PRE_INTERPOSITIONS(forward_message, Node, Msg, S)
).

%% returns ok
-define(FIRE_RECV_PRE_INTERPOSITIONS(Node, Msg, S),
    ?FIRE_PRE_INTERPOSITIONS(receive_message, Node, Msg, S)
).

%% returns message
-define(FIRE_FWD_INTERPOSITIONS(Node, Msg, S),
    ?FIRE_INTERPOSITIONS(forward_message, Node, Msg, S)
).

%% returns message
-define(FIRE_RECV_INTERPOSITIONS(Node, Msg, S),
    ?FIRE_INTERPOSITIONS(receive_message, Node, Msg, S)
).

%% returns ok
-define(FIRE_FWD_POST_INTERPOSITIONS(Node, Msg0, Msg1, S),
    ?FIRE_POST_INTERPOSITIONS(forward_message, Node, Msg0, Msg1, S)
).

%% returns ok
-define(FIRE_RECV_POST_INTERPOSITIONS(Node, Msg0, Msg1, S),
    ?FIRE_POST_INTERPOSITIONS(receive_message, Node, Msg0, Msg1, S)
).


-ifdef(INTERPOSITION).

    %% returns ok
    -define(FIRE_PRE_INTERPOSITIONS(Type, Node, Msg, S),
        PreIFuns = case S of
            #state{} ->
                S#state.pre_interposition_funs;
            S when is_list(S) ->
                S
        end,
        maps:fold(
            fun(_Node, Fun, ok) ->
                ?LOG_DEBUG(
                    "Firing pre-interposition fun for message: ~p",
                    [Msg]
                ),
                Fun({forward_message, Node, Msg}),
                ok
            end,
            ok,
            PreIFuns
        )
    ).

    %% returns message
    -define(FIRE_INTERPOSITIONS(Type, Node, Msg, S),
        IFuns = case S of
            #state{} ->
                S#state.interposition_funs;
            S when is_list(S) ->
                S
        end,
        maps:fold(
            fun(_Name, Fun, M) ->
                ?LOG_DEBUG(
                    "Firing interposition fun for message: ~p",
                    [Msg]
                ),
                Fun({Type, Node, M})
            end,
            Msg0,
            IFuns
        )
    ).

    %% returns ok
    -define(FIRE_POST_INTERPOSITIONS(Type, Node, Msg0, Msg1, S),
        PostIFuns = case S of
            #state{} ->
                S#state.post_interposition_funs;
            S when is_list(S) ->
                S
        end,
        maps:fold(
            fun(_Name, Fun, ok) ->
                ?LOG_DEBUG(
                    "Firing post-interposition fun for messages: [~p, ~p]",
                    [Msg0, Msg1]
                ),
                Fun(
                    {Type, Node, Msg0},
                    {Type, Node, Msg1}
                ),
                ok
            end,
            ok,
            PostIFuns
        )
    ).

-else.

    %% returns ok
    -define(FIRE_PRE_INTERPOSITIONS(_Type, _Node, _Msg, Term),
        %% To supress variable unused compiler Warning
        _ = Term,
        ok
    ).

    %% returns message
    -define(FIRE_INTERPOSITIONS(_Type, _Node, Msg, _Term), Msg).

    %% returns ok
    -define(FIRE_POST_INTERPOSITIONS(_Type, _Node, _Msg0, _Msg1, _Term), ok).

-endif.


-record(state, {
    name                        ::  node(),
    node_spec                   ::  partisan:node_spec(),
    actor                       ::  partisan:actor(),
    vclock                      ::  partisan_vclock:vclock(),
    %% A materialised view of the membership_strategy_state as a list
    members                     ::  [partisan:node_spec()],
    %% The nodes we still need to establish connections with
    pending                     ::  [partisan:node_spec()],
    membership_strategy         ::  atom(),
    membership_strategy_state   ::  term(),
    leaving = false             ::  boolean(),
    distance_metrics            ::  map(),
    sync_joins                  ::  #{partisan:node_spec() => sets:set(from())},
    out_links                   ::  [term()],
    down_funs                   ::  node_subs(),
    channel_down_funs           ::  channel_subs(),
    up_funs                     ::  node_subs(),
    channel_up_funs             ::  channel_subs(),
    pre_interposition_funs      ::  interposition_map(x_interpos_fun()),
    interposition_funs          ::  interposition_map(interpos_fun()),
    post_interposition_funs     ::  interposition_map(x_interpos_fun())
}).



-type t()                   ::  #state{}.
-type from()                ::  {pid(), atom()}.
-type on_event_fun()        ::  partisan_peer_service_manager:on_event_fun().
-type node_subs()           ::  #{'_' | node() => on_event_fun()}.
-type channel_subs()        ::  #{
                                    {'_' | node(), partisan:channel()} =>
                                        on_event_fun()
                                }.
-type interposition_map(T)  ::  #{any() => T}.
-type interpos_arg()        ::  {receive_message, node(), any()}.
-type interpos_fun()        ::  fun((interpos_arg()) -> interpos_arg()).
-type x_interpos_fun()      ::  fun((interpos_arg()) -> ok).
-type tag()                 ::  atom().
-type info()                ::  connections
                                | retransmit
                                | periodic
                                | instrumentation
                                | distance
                                | tree_refresh
                                | {'EXIT', partisan:any_pid(), any()}
                                | {
                                    connected,
                                    partisan:node_spec(),
                                    partisan:channel(),
                                    tag(),
                                    t()
                                }.

%% %% API
-export([member/1]).


%% PARTISAN_PEER_SERVICE_MANAGER CALLBACKS
-export([add_interposition_fun/2]).
-export([add_post_interposition_fun/2]).
-export([add_pre_interposition_fun/2]).
-export([cast_message/2]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([decode/1]).
-export([forward_message/2]).
-export([forward_message/3]).
-export([forward_message/4]).
-export([get_interposition_funs/0]).
-export([get_local_state/0]).
-export([get_pre_interposition_funs/0]).
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
-export([remove_interposition_fun/1]).
-export([remove_post_interposition_fun/1]).
-export([remove_pre_interposition_fun/1]).
-export([reserve/1]).
-export([resolve_partition/1]).
-export([send_message/2]).
-export([start_link/0]).
-export([supports_capability/1]).
-export([sync_join/1]).
-export([update_members/1]).


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
%% @doc Returns true if node `Node' is a member in the membership list.
%% Otherwise returns `false'.
%% @end
%% -----------------------------------------------------------------------------
member(#{name := Node}) ->
    member(Node);

member(Node) when is_atom(Node) ->
    gen_server:call(?MODULE, {member, Node}, infinity).



%% =============================================================================
%% PARTISAN_PEER_SERVICE_MANAGER CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Same as start_link([]).
%% @end
%% -----------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% -----------------------------------------------------------------------------
%% @doc Return membership list.
%% @end
%% -----------------------------------------------------------------------------
members() ->
    gen_server:call(?MODULE, members, infinity).


%% -----------------------------------------------------------------------------
%% @doc Return membership list.
%% @end
%% -----------------------------------------------------------------------------
members_for_orchestration() ->
    gen_server:call(?MODULE, members_for_orchestration, infinity).


%% -----------------------------------------------------------------------------
%% @doc Update membership.
%% @end
%% -----------------------------------------------------------------------------
update_members(Members) ->
    gen_server:call(?MODULE, {update_members, Members}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Return local node's view of cluster membership.
%% @end
%% -----------------------------------------------------------------------------
get_local_state() ->
    gen_server:call(?MODULE, get_local_state, infinity).


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection close for a given node.
%% `Fun' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_down(Arg, Fun) ->
    on_down(Arg, Fun, #{}).


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection close for a given node.
%% `Fun' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_down(#{name := Node}, Fun, Opts) ->
    on_down(Node, Fun, Opts);

on_down(any, Fun, Opts) ->
    on_down('_', Fun, Opts);

on_down(Node, Fun, Opts)
when is_atom(Node) andalso is_map(Opts) andalso ?IS_ON_EVENT_FUN(Fun) ->
    gen_server:call(?MODULE, {on_down, Node, Fun, Opts}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection open for a given node.
%% `Fun' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_up(Arg, Fun) ->
    on_up(Arg, Fun, #{}).


%% -----------------------------------------------------------------------------
%% @doc Trigger function on connection open for a given node.
%% `Fun' is a function object taking zero or a single argument, where the
%% argument is the Node name.
%% @end
%% -----------------------------------------------------------------------------
on_up(#{name := Node}, Fun, Opts) ->
    on_up(Node, Fun, Opts);

on_up(any, Fun, Opts) ->
    on_up('_', Fun, Opts);

on_up(Node, Fun, Opts)
when is_atom(Node) andalso is_map(Opts) andalso ?IS_ON_EVENT_FUN(Fun) ->
    gen_server:call(?MODULE, {on_up, Node, Fun, Opts}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Attempt to join a remote node.
%% @end
%% -----------------------------------------------------------------------------
join(#{name := _} = NodeSpec) ->
    gen_server:call(?MODULE, {join, NodeSpec}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Attempt to join a remote node.
%% @end
%% -----------------------------------------------------------------------------
sync_join(#{name := _} = NodeSpec) ->
    gen_server:call(?MODULE, {sync_join, NodeSpec}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Leave the cluster.
%% @end
%% -----------------------------------------------------------------------------
leave() ->
    gen_server:call(?MODULE, {leave, partisan:node_spec()}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Remove another node from the cluster.
%% @end
%% -----------------------------------------------------------------------------
leave(#{name := _} = NodeSpec) ->
    gen_server:call(?MODULE, {leave, NodeSpec}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Send message to a remote peer service manager.
%% @end
%% -----------------------------------------------------------------------------
send_message(Node, Message) ->
    %% TODO maybe deprecate, not used by Partisan and we can always do
    %% partisan_rpc:call/4
    Cmd = {send_message, Node, Message},
    gen_server:call(?MODULE, Cmd, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    Term :: partisan:any_pid() | partisan:any_name(),
    Message :: partisan:message()) -> ok.

cast_message(Term, Message) ->
    forward_message(Term, {'$gen_cast', Message}, #{}).


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
cast_message(Node, ServerRef, Message, Options) ->
    %% TODO maybe deprecate, since we have partisan_gen_server:cast
    %% and partisan_gen_statem:cast ?
    forward_message(Node, ServerRef, {'$gen_cast', Message}, Options).


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
forward_message(Pid, Message, Opts) when is_pid(Pid) ->
    forward_message(partisan:node(Pid), Pid, Message, Opts);

forward_message(Name, Message, Opts) when is_atom(Name) ->
    forward_message(partisan:node(), Name, Message, Opts);

forward_message({Name, Node}, Message, Opts) ->
    forward_message(Node, Name, Message, Opts);

forward_message(RemoteRef, Message, Opts) ->
    partisan_remote_ref:is_pid(RemoteRef)
        orelse partisan_remote_ref:is_name(RemoteRef)
        orelse error(badarg),

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
    %% If attempting to forward to the local node or using disterl, bypass.
    Bypass =
        Node =:= partisan:node()
        orelse partisan_config:get(connect_disterl, false),

    case Bypass of
        true ->
            partisan_peer_service_manager:deliver(ServerRef, Message);
        false ->
            %% Get forwarding options and combine with message
            %% specific options.
            FwdOpts = maps:merge(
                partisan_config:get(forward_options, #{}), Opts
            ),

            %% Attempt to get the partition key, if possible.
            PartitionKey = maps:get(
                partition_key, FwdOpts, ?DEFAULT_PARTITION_KEY
            ),

            %% Use a clock provided by the sender,
            %% otherwise, use a generated one.
            Clock = maps:get(clock, FwdOpts, undefined),

            PaddedMessage = partisan_util:maybe_pad_term(Message),

            Cmd = {
                forward_message,
                Node,
                Clock,
                PartitionKey,
                ServerRef,
                PaddedMessage,
                FwdOpts
            },

            %% Is fast forward disabled?
            DisableFastForward =
                partisan_config:get(disable_fast_forward, false),

            %% Needs ack?
            NeedsAck = maps:get(ack, FwdOpts, false),

            %% Use causal delivery?
            CausalDelivery =
                maps:get(causal_label, FwdOpts, undefined) =:= undefined,

            %% Should we use fast forwarding?
            %%
            %% Conditions:
            %% - fastforward is not disabled
            %% - not labeled for causal delivery
            %% - message does not need acknowledgement
            FastForward =
                not DisableFastForward
                andalso not NeedsAck
                andalso not CausalDelivery,

            case FastForward of
                true ->
                    %% Concurrent execution
                    %% Attempt to fast-path by accessing the connection
                    %% directly
                    case partisan_peer_connections:dispatch(Cmd) of
                        ok ->
                            ok;
                        {error, _} ->
                            gen_server:call(?MODULE, Cmd, infinity)
                    end;
                false ->
                    %% Serialized execution
                    gen_server:call(?MODULE, Cmd, infinity)
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc Receive message from a remote manager.
%% @end
%% -----------------------------------------------------------------------------
receive_message(
    Node,
    Channel,
    {forward_message, _SrcNode, _Clock, _ServerRef, _Msg} = Cmd) ->
    %% Process the message and generate the acknowledgement.
    gen_server:call(?MODULE, {receive_message, Node, Channel, Cmd}, infinity);

receive_message(
    Node,
    Channel,
    {forward_message, ServerRef, {'$partisan_padded', _Padding, Msg}}) ->
    receive_message(Node, Channel, {forward_message, ServerRef, Msg});

receive_message(
    _,
    _Channel,
    {forward_message, _ServerRef, {causal, Label, _, _, _, _, _} = Msg}) ->
    partisan_causality_backend:receive_message(Label, Msg);

receive_message(Node, Channel, {forward_message, ServerRef, Msg} = Cmd) ->
    %% We received a message for a destination in this node.
    case partisan_config:get(disable_fast_receive, false) of
        true ->
            %% Serialize execution
            gen_server:call(
                ?MODULE, {receive_message, Node, Channel, Cmd}, infinity
            );
        false ->
            %% Concurrent execution
            partisan_peer_service_manager:deliver(ServerRef, Msg)
    end;

receive_message(Node, Channel, Msg) ->
    gen_server:call(?MODULE, {receive_message, Node, Channel, Msg}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Decode state.
%% @end
%% -----------------------------------------------------------------------------
decode(Membership) ->
    Membership.

%% -----------------------------------------------------------------------------
%% @doc Reserve a slot for the particular tag.
%% @end
%% -----------------------------------------------------------------------------
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


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_pre_interposition_fun(any(), x_interpos_fun()) -> ok.

add_pre_interposition_fun(Name, Fun) ->
    gen_server:call(
        ?MODULE,
        {add_pre_interposition_fun, Name, Fun},
        infinity
    ).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_pre_interposition_funs() -> interposition_map(x_interpos_fun()).

get_pre_interposition_funs() ->
    gen_server:call(?MODULE, get_pre_interposition_funs, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec remove_pre_interposition_fun(any()) -> ok.

remove_pre_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_pre_interposition_fun, Name}, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_interposition_fun(any(), interpos_fun()) -> ok.

add_interposition_fun(Name, InterpositionFun) ->
    gen_server:call(
        ?MODULE, {add_interposition_fun, Name, InterpositionFun}, infinity
    ).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get_interposition_funs() -> interposition_map(interpos_fun()).

get_interposition_funs() ->
    gen_server:call(?MODULE, get_interposition_funs, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec remove_interposition_fun(any()) -> ok.

remove_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_interposition_fun, Name}, infinity).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec add_post_interposition_fun(any(), x_interpos_fun()) -> ok.

add_post_interposition_fun(Name, PostInterpositionFun) ->
    gen_server:call(
        ?MODULE,
        {add_post_interposition_fun, Name, PostInterpositionFun},
        infinity
    ).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec remove_post_interposition_fun(any()) -> ok.

remove_post_interposition_fun(Name) ->
    gen_server:call(?MODULE, {remove_post_interposition_fun, Name}, infinity).


%% -----------------------------------------------------------------------------
%% @doc Inject a partition.
%% @end
%% -----------------------------------------------------------------------------
inject_partition(_Origin, _TTL) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Resolve a partition.
%% @end
%% -----------------------------------------------------------------------------
resolve_partition(_Reference) ->
    {error, not_implemented}.


%% -----------------------------------------------------------------------------
%% @doc Return partitions.
%% @end
%% -----------------------------------------------------------------------------
partitions() ->
    {error, not_implemented}.



%% =============================================================================
%% GEN_SERVER CALLBACKS
%% =============================================================================



-spec init([]) -> {ok, t()}.

init([]) ->
    %% Seed the random number generator.
    partisan_config:seed(),

    case partisan_config:get(binary_padding, false) of
        true ->
            %% Use 64-byte binary to force shared heap usage to cut down on copying.
            BinaryPaddingTerm = rand_bits(512),
            partisan_config:set(binary_padding_term, BinaryPaddingTerm);
        _ ->
            undefined
    end,

    %% Process connection exits.
    process_flag(trap_exit, true),

    %% Schedule periodic.
    schedule_periodic(),

    %% Schedule instrumentation.
    schedule_instrumentation(),

    %% Schedule distance metric.
    schedule_distance(),

    %% Schedule periodic connections.
    schedule_connections(),

    %% Schedule periodic retransmissionj.
    schedule_retransmit(),

    %% Schedule tree peers refresh.
    schedule_tree_refresh(),

    Name = partisan:node(),
    Actor = gen_actor(Name),
    VClock = partisan_vclock:fresh(),

    %% We init the connections table and we become owners, if we crash the
    %% table will be destroyed.
    ok = partisan_peer_connections:init(),

    MStrategy = partisan_config:get(membership_strategy),

    {ok, Members, MState} =
        partisan_membership_strategy:init(MStrategy, Actor),

    {ok, #state{
        name = Name,
        node_spec = partisan:node_spec(),
        actor = Actor,
        pending = [],
        vclock = VClock,
        pre_interposition_funs =  #{},
        interposition_funs =  #{},
        post_interposition_funs =  #{},
        distance_metrics = #{},
        sync_joins = #{},
        up_funs = #{},
        channel_up_funs = #{},
        down_funs = #{},
        channel_down_funs = #{},
        out_links = [],
        members = Members,
        membership_strategy = MStrategy,
        membership_strategy_state = MState
    }}.


-spec handle_call(term(), gen_server:from(), t()) ->
    {reply, term(), t()}
    | {noreply, t()}
    | {stop, normal, t()}.

handle_call({reserve, _Tag}, _From, State) ->
    {reply, {error, no_available_slots}, State};

handle_call({on_up, Name, Fun, #{channel := Channel}}, _From, State)
when is_atom(Channel) ->
    Funs0 = State#state.channel_up_funs,
    Funs = partisan_util:maps_append({Name, Channel}, Fun, Funs0),
    {reply, ok, State#state{channel_up_funs = Funs}};

handle_call({on_up, Name, Fun, _}, _From, State) ->
    Funs0 = State#state.up_funs,
    Funs = partisan_util:maps_append(Name, Fun, Funs0),
    {reply, ok, State#state{up_funs = Funs}};

handle_call({on_down, Name, Fun, #{channel := Channel}}, _From, State)
when is_atom(Channel) ->
    Funs0 = State#state.channel_down_funs,
    Funs = partisan_util:maps_append({Name, Channel}, Fun, Funs0),
    {reply, ok, State#state{channel_down_funs = Funs}};

handle_call({on_down, Name, Fun, _}, _From, State) ->
    Funs0 = State#state.down_funs,
    Funs = partisan_util:maps_append(Name, Fun, Funs0),
    {reply, ok, State#state{down_funs = Funs}};

handle_call({add_pre_interposition_fun, Name, Fun}, _From, #state{} = State) ->
    Funs = maps:put(Name, Fun, State#state.pre_interposition_funs),
    %% eqwalizer:ignore Funs
    {reply, ok, State#state{pre_interposition_funs = Funs}};

handle_call({remove_pre_interposition_fun, Name}, _From, #state{} = State) ->
    Funs = maps:remove(Name, State#state.pre_interposition_funs),
    {reply, ok, State#state{pre_interposition_funs = Funs}};

handle_call({add_interposition_fun, Name, Fun}, _From, #state{} = State) ->
    Funs = maps:put(Name, Fun, State#state.interposition_funs),
    %% eqwalizer:ignore Funs
    {reply, ok, State#state{interposition_funs = Funs}};

handle_call({remove_interposition_fun, Name}, _From, #state{} = State) ->
    Funs = maps:remove(Name, State#state.interposition_funs),
    {reply, ok, State#state{interposition_funs = Funs}};

handle_call(get_interposition_funs, _From, #state{} = State) ->
    {reply, {ok, State#state.interposition_funs}, State};

handle_call(get_pre_interposition_funs, _From, #state{} = State) ->
    {reply, {ok, State#state.pre_interposition_funs}, State};

handle_call({add_post_interposition_fun, Name, Fun}, _From, #state{} = State) ->
    Funs = maps:put(Name, Fun, State#state.post_interposition_funs),
    %% eqwalizer:ignore Funs
    {reply, ok, State#state{post_interposition_funs =Funs}};

handle_call({remove_post_interposition_fun, Name}, _From, #state{}=State) ->
    Funs = maps:remove(Name, State#state.post_interposition_funs),
    {reply, ok, State#state{post_interposition_funs = Funs}};

handle_call({update_members, _}, _, #state{leaving = true} = State) ->
    %% We are leaving so do nothing
    {reply, ok, State};

handle_call({update_members, Members}, _From, #state{} = State0) ->
    %% For compatibility with external membership services.
    Mod = State0#state.membership_strategy,
    MState = State0#state.membership_strategy_state,

    {Joiners, Leavers} = Mod:compare(Members, MState),

    %% Issue leaves.
    State1 = lists:foldl(
        fun(NodeSpec , S) -> internal_leave(NodeSpec, S) end,
        State0,
        Leavers
    ),
    %% Issue joins.
    State = lists:foldl(
        fun(NodeSpec, S) -> internal_join(NodeSpec, undefined, S) end,
        State1,
        Joiners
    ),

    %% Finally schedule the removal of connections
    %% We do this async because internal_leave will schedule the sending of
    %% membership update messages
    LeavingNodes = [Node || #{name := Node} <- Leavers],
    gen_server:cast(?MODULE, {kill_connections, LeavingNodes}),

    {reply, ok, State};

handle_call({leave, #{name := Name} = NodeSpec}, From, State0) ->
    %% Perform leave.
    State = internal_leave(NodeSpec, State0),

    case Name == State0#state.name of
        true ->
            %% Self leave
            gen_server:reply(From, ok),

            %% We need to stop (to cleanup all connections and state) or do it
            %% manually. However, we cannot do it straight away as
            %% internal_leave/2 has send some async messages we need to process
            %% (casts to ourself) so we cast ourselves a shutdown message.
            gen_server:cast(?MODULE, stop),
            {noreply, State#state{leaving = true}};

        false ->
            gen_server:cast(?MODULE, {kill_connections, [NodeSpec]}),
            {reply, ok, State}
    end;

handle_call({join, #{name := N}}, _From, #state{name = N} = State0) ->
    %% Ignoring self join.
    {reply, ok, State0};

handle_call({join, NodeSpec}, _From, State0) ->
    State = internal_join(NodeSpec, undefined, State0),
    {reply, ok, State};

handle_call({sync_join, #{name := N}}, _From, #state{name = N} = State0) ->
    %% Ignoring self join.
    {reply, ok, State0};

handle_call({sync_join, NodeSpec}, From, State0) ->
    ?LOG_DEBUG(#{
        description => "Starting synchronous join with peer",
        node => State0#state.name,
        peer => NodeSpec
    }),
    State = internal_join(NodeSpec, From, State0),
    {noreply, State};

handle_call({send_message, Node, Message}, _From, State) ->

    schedule_self_message_delivery(
        Node,
        Message,
        ?DEFAULT_PARTITION_KEY,
        State
    ),
    {reply, ok, State};

handle_call(
    {forward_message, Node, Clock, PartitionKey, ServerRef, Msg, Opts},
    From,
    State) ->
    %% We avoid referencing all the state in the DeliveryFun
    PreIFuns = State#state.pre_interposition_funs,

    %% Run all interposition functions.
    DeliveryFun =
        fun() ->
            %% Fire pre-interposition functions.
            ok = ?FIRE_FWD_PRE_INTERPOSITIONS(Node, Msg, PreIFuns),

            %% Once pre-interposition returns, then schedule for delivery.
            Cmd = {
                forward_message,
                From,
                Node,
                Clock,
                PartitionKey,
                ServerRef,
                Msg,
                Opts
            },
            gen_server:cast(?MODULE, Cmd)
        end,

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver,
            %% preserving serial order of messages.
            DeliveryFun();
        true ->
            %% Allow the system to proceed, and the message will be delivered
            %% once pre-interposition is done.
            spawn_link(DeliveryFun)
    end,

    {noreply, State};

handle_call({receive_message, Node, Channel, Msg}, From, State) ->
    PreIFuns = State#state.pre_interposition_funs,
    DeliveryFun = fun() ->
        ok = ?FIRE_RECV_PRE_INTERPOSITIONS(Node, Msg, PreIFuns),

        %% Once pre-interposition returns, then schedule for delivery.
        gen_server:cast(?MODULE, {receive_message, Node, Channel, From, Msg})
    end,

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver,
            %% preserving serial order of messages.
            DeliveryFun();
        true ->
            %% Allow the system to proceed, and the message will be delivered
            %% once pre-interposition is done.
            spawn_link(DeliveryFun)
    end,

    {noreply, State};

handle_call(members_for_orchestration, _From, State) ->
    {reply, {ok, State#state.members}, State};

handle_call(members, _From, State) ->
    Members = [Node || #{name := Node} <- State#state.members],
    {reply, {ok, Members}, State};

handle_call({member, Node}, _From, State) ->
    IsMember = lists:any(
        fun(#{name := X}) -> X =:= Node end,
        State#state.members
    ),
    {reply, IsMember, State};

handle_call(get_local_state, _From, State) ->
    {reply, {ok, State#state.membership_strategy_state}, State};

handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.



-spec handle_cast(term(), t()) ->
    {noreply, t()}
    | {stop, normal, t()}.

handle_cast(stop, State) ->
    %% We send ourselves this message when we left the cluster
    %% We stop to cleanup, supervisor will start us again,
    %% terminate/1 will kill all connections.
    {stop, normal, State};

handle_cast({kill_connections, Nodes}, State) ->
    ok = kill_connections(Nodes, State),
    {noreply, State};

handle_cast({receive_message, Node, Channel, From, Msg0}, State) ->

    %% Filter messages using interposition functions.
    Msg1 = ?FIRE_RECV_INTERPOSITIONS(Node, Msg0, State),
    ok = ?FIRE_RECV_POST_INTERPOSITIONS(Node, Msg0, Msg1, State),

    case Msg1 of
        undefined ->
            %% eqwalizer:ignore From
            gen_server:reply(From, ok),
            {noreply, State};

        {'$delay', Msg} ->
            ?LOG_DEBUG(
                "Delaying receive_message due to interposition result: ~p",
                [Msg]
            ),
            gen_server:cast(
                ?MODULE, {receive_message, Node, Channel, From, Msg}
            ),
            {noreply, State};

        _ ->
            handle_message(Msg1, From, Channel, State)
    end;

handle_cast(
    {forward_message, From, Node, Clock, PartitionKey, ServerRef, Msg0, Opts},
    State) ->

    #state{
        vclock = VClock0
    } = State,

    Msg = ?FIRE_FWD_INTERPOSITIONS(Node, Msg0, State),

    %% Increment the clock.
    VClock = partisan_vclock:increment(State#state.name, VClock0),

    %% Are we using causality?
    %% eqwalizer:ignore Opts
    CausalLabel = maps:get(causal_label, Opts, undefined),

    %% Use local information for message unless it's a causal message.
    {MsgClock, FullMessage} =
        case CausalLabel of
            undefined ->
                %% Generate a message clock or use the provided clock.
                LocalClock = case Clock of
                    undefined ->
                        {undefined, VClock};
                    Clock ->
                        Clock
                end,

                {LocalClock, Msg};

            CausalLabel ->
                case Clock of
                    undefined ->
                        %% First time through.
                        %% We don't have a clock yet,
                        %% get one using the causality backend.
                        {ok, LocalClock0, CausalMessage} =
                            partisan_causality_backend:emit(
                                CausalLabel, Node, ServerRef, Msg
                            ),

                        %% Wrap the clock with a scope.
                        %% TODO: Maybe do this wrapping inside of the causality backend.
                        LocalClock = {CausalLabel, LocalClock0},

                        %% Return clock and wrapped message.
                        {LocalClock, CausalMessage};

                    _ ->
                        %% Retransmission.
                        %% Get the clock and message we used last time.
                        {ok, LocalClock, CausalMessage} =
                            partisan_causality_backend:reemit(
                                CausalLabel, Clock
                            ),

                        %% Return clock and wrapped message.
                        {LocalClock, CausalMessage}
                end
            end,

    case Msg of
        undefined ->
            %% Store for reliability, if necessary.
            %% eqwalizer:ignore Opts
            case maps:get(ack, Opts, false) of
                true ->
                    %% Acknowledgements.
                    %% eqwalizer:ignore Opts
                    case maps:get(retransmission, Opts, false) of
                        true ->
                            RescheduleableMessage = {
                                forward_message,
                                From,
                                Node,
                                MsgClock,
                                PartitionKey,
                                ServerRef,
                                Msg0,
                                Opts
                            },
                            partisan_acknowledgement_backend:store(
                                MsgClock, RescheduleableMessage
                            );
                        false ->
                            ok
                    end;
                false ->
                    ok
            end,

            ok = ?FIRE_FWD_POST_INTERPOSITIONS(Node, Msg0, FullMessage, State),

            ?LOG_DEBUG(
                "~p: Message ~p after send interposition is: ~p",
                [State#state.name, Msg0, FullMessage]
            ),

            case From of
                undefined ->
                    ok;
                _ ->
                    %% eqwalizer:ignore From
                    gen_server:reply(From, ok)
            end,

            {noreply, State#state{vclock = VClock}};

        {'$delay', NewMessage} ->
            ?LOG_DEBUG(
                "Delaying receive_message due to interposition result: ~p",
                [NewMessage]
            ),
            gen_server:cast(
                ?MODULE,
                {
                    forward_message,
                    From,
                    Node,
                    Clock,
                    PartitionKey,
                    ServerRef,
                    NewMessage,
                    Opts
                }
            ),
            {noreply, State};

        _ ->
            %% Store for reliability, if necessary.
            Result = case maps:get(ack, Opts, false) of
                false ->
                    %% Tracing.
                    WrappedMessage = {forward_message, ServerRef, FullMessage},

                    ok = ?FIRE_FWD_POST_INTERPOSITIONS(
                        Node,
                        {forward_message, ServerRef, Msg0},
                        WrappedMessage,
                        State
                    ),

                    %% Send message along.
                    do_send_message(
                        Node,
                        PartitionKey,
                        WrappedMessage,
                        Opts,
                        State
                    );
                true ->
                    %% Tracing.
                    WrappedMessage = {
                        forward_message,
                        State#state.name,
                        MsgClock,
                        ServerRef,
                        FullMessage
                    },

                    ?LOG_DEBUG(
                        "should acknowledge message: ~p", [WrappedMessage]
                    ),

                    ok = ?FIRE_FWD_POST_INTERPOSITIONS(
                        Node,
                        {
                            forward_message,
                            State#state.name,
                            MsgClock,
                            ServerRef,
                            Msg0
                        },
                        WrappedMessage, State
                    ),

                    ?LOG_DEBUG(
                        "~p: Sending message ~p with clock: ~p",
                        [State#state.name, Msg, MsgClock]
                    ),
                    ?LOG_DEBUG(
                        "~p: Message after send interposition is: ~p",
                        [State#state.name, Msg]
                    ),

                    %% Acknowledgements.
                    case maps:get(retransmission, Opts, false) of
                        false ->
                            RescheduleableMessage = {
                                forward_message,
                                From,
                                Node,
                                MsgClock,
                                PartitionKey,
                                ServerRef,
                                Msg0,
                                Opts
                            },
                            partisan_acknowledgement_backend:store(
                                MsgClock, RescheduleableMessage
                            );
                        true ->
                            ok
                    end,

                    %% Send message along.
                    do_send_message(
                        Node,
                        PartitionKey,
                        WrappedMessage,
                        Opts,
                        State
                    )
            end,

            case From of
                undefined ->
                    ok;
                _ ->
                    gen_server:reply(From, Result)
            end,

            {noreply, State#state{vclock = VClock}}
    end;

handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),
    {noreply, State}.


-spec handle_info(info(), t()) -> {noreply, t()}.

handle_info(tree_refresh, State) ->
    %% Get lazily computed outlinks.
    OutLinks = retrieve_outlinks(),

    %% Reschedule.
    schedule_tree_refresh(),

    {noreply, State#state{out_links = OutLinks}};

handle_info(distance, State0) ->
    %% Establish any new connections.
    State = establish_connections(State0),

    %% Record time.
    Time = erlang:timestamp(),

    %% Send distance requests.
    SrcNode = partisan:node_spec(),

    ok = lists:foreach(
        fun(Peer) ->
            schedule_self_message_delivery(
                Peer,
                {ping, SrcNode, Peer, Time},
                ?DEFAULT_PARTITION_KEY,
                State,
                #{channel => ?MEMBERSHIP_CHANNEL}
            )
        end,
        State#state.members
    ),

    schedule_distance(),

    {noreply, State};

handle_info(instrumentation, State) ->
    MessageQueueLen = process_info(self(), message_queue_len),
    ?LOG_DEBUG("message_queue_len: ~p", [MessageQueueLen]),
    schedule_instrumentation(),
    {noreply, State};

handle_info(periodic, #state{} = State0) ->
    #state{
        membership_strategy=MStrategy,
        membership_strategy_state=MState0
    } = State0,

    {ok, Members, OutgoingMessages, MState} =
        partisan_membership_strategy:periodic(MStrategy, MState0),

    %% Send outgoing messages.
    ok = lists:foreach(
        fun({Node, Message}) ->
            schedule_self_message_delivery(
                Node,
                Message,
                ?DEFAULT_PARTITION_KEY,
                State0,
                #{channel => ?MEMBERSHIP_CHANNEL}
            )
        end,
        OutgoingMessages
    ),

    State1 = State0#state{
        members = Members,
        membership_strategy_state = MState
    },

    %% Establish any new connections.
    State = establish_connections(State1),

    schedule_periodic(),

    {noreply, State};

handle_info(retransmit, State) ->
    RetransmitFun = fun({_, {forward_message, From, Node, Clock, PartitionKey, ServerRef, Message, Options}}) ->
        ?LOG_DEBUG(
            "~p no acknowledgement yet, "
            "restranmitting message ~p with clock ~p to ~p",
            [State#state.name, Message, Clock, Node]
        ),

        ok = ?FIRE_FWD_PRE_INTERPOSITIONS(Node, Message, State),

        %% Schedule message for redelivery.
        RetryOptions = Options#{retransmission => true},

        gen_server:cast(
            ?MODULE,
            {
                forward_message,
                From,
                Node,
                Clock,
                PartitionKey,
                ServerRef,
                Message,
                RetryOptions
            }
        )
    end,

    {ok, Outstanding} = partisan_acknowledgement_backend:outstanding(),

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver,
            %% preserving serial order of messages.
            lists:foreach(RetransmitFun, Outstanding);
        true ->
            %% Allow the system to proceed, and the message will be delivered
            %% once pre-interposition is done.
            lists:foreach(
                fun(OutstandingMessage) ->
                    spawn_link(fun() ->
                        RetransmitFun(OutstandingMessage)
                    end)
                end,
                Outstanding
            )
    end,

    %% Reschedule retransmission.
    schedule_retransmit(),

    {noreply, State};


handle_info(connections, State0) ->
    %% TODO #244 move connection establishing to a helper process as these tasks
    %% interleave with message forwarding. Also consider having a process per
    %% channel or channel connection.
    State1 = establish_connections(State0),

    %% Advance sync_join's if we have enough open connections to remote host.
    State = maybe_reply_sync_joins(State1),

    schedule_connections(),

    {noreply, State};

handle_info({'EXIT', Pid, Reason}, State0)->
    ?LOG_DEBUG(#{
        description => "Connection closed",
        reason => Reason
    }),

    %% A connection has closed, prune it from the connections table
    %% eqwalizer:ignore Pid
    try partisan_peer_connections:prune(Pid) of
        {Info, [Connection]} ->
            NodeSpec = partisan_peer_connections:node_spec(Info),
            #{name := Node} = NodeSpec,
            Channel = partisan_peer_connections:channel(Connection),

            case partisan_peer_connections:count(Node, Channel) of
                0 ->
                    % We notify all subscribers.
                    ok = down(NodeSpec, Channel, State0);
                _ ->
                    ok
            end,

            State =
                case partisan_peer_connections:count(Info) of
                    0 ->
                        %% This was the last connection so the node is down.
                        %% We notify all subscribers.
                        ok = down(NodeSpec, State0),
                        %% If still a member we add it to pending, so that we
                        %% can compute the on_up signal
                        maybe_append_pending(NodeSpec, State0);
                    _ ->
                        State0
                end,

            {noreply, State}

    catch
        error:badarg ->
            %% Weird, connection pid did not exist
            {noreply, State0}
    end;

handle_info(
    {connected, NodeSpec, _Channel, _Tag, RemoteState}, State0) ->
    #state{
        pending = Pending0,
        members = Members0,
        membership_strategy = MStrategy,
        membership_strategy_state = MState0
    } = State0,

    ?LOG_DEBUG(#{
        description => "Node connected!",
        node => NodeSpec,
        pending => Pending0,
        membership => State0#state.members
    }),

    State1 = case lists:member(NodeSpec, Pending0) of
        true ->
            %% Move out of pending.
            Pending = Pending0 -- [NodeSpec],

            %% Update membership by joining with remote membership.
            {ok, Members, OutgoingMessages, MState} =
                partisan_membership_strategy:join(
                    MStrategy, NodeSpec, RemoteState, MState0
                ),

            %% Gossip the new membership.
            lists:foreach(
                fun({Node, Message}) ->
                    schedule_self_message_delivery(
                        Node,
                        Message,
                        ?DEFAULT_PARTITION_KEY,
                        State0,
                        #{channel => ?MEMBERSHIP_CHANNEL}
                    )
                end,
                OutgoingMessages
            ),

            %% Notify event handlers
            ok = case Members == Members0 of
                true ->
                    ok;
                false ->
                    partisan_peer_service_events:update(Members)
            end,

            %% notify subscribers
            up(NodeSpec, State0),

            State0#state{
                pending = Pending,
                members = Members,
                membership_strategy_state = MState
            };

        false ->
            State0
    end,

    %% Notify for sync join.
    State = maybe_reply_sync_joins(State1),

    {noreply, State};

handle_info(Msg, State) ->
    handle_message(Msg, undefined, ?DEFAULT_CHANNEL, State).


-spec terminate(term(), t()) -> term().

terminate(_Reason, #state{}) ->
    ok = partisan_peer_connections:kill_all().


-spec code_change(term() | {down, term()}, t(), term()) ->
    {ok, t()}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
gen_actor(Name) ->
    Unique = erlang:unique_integer([positive]),
    TS = integer_to_list(Unique),
    Term = atom_to_list(Name) ++ TS,
    crypto:hash(sha, Term).


%% -----------------------------------------------------------------------------
%% @private
%% @doc Establish any new connections and prunes no longer valid nodes.
%% @end
%% -----------------------------------------------------------------------------
-spec establish_connections(t()) -> t().

establish_connections(State) ->
    Pending = State#state.pending,
    Members = State#state.members,

    %% Compute list of nodes that should be connected.
    Nodes = Members ++ Pending,

    %% Reconnect disconnected members and members waiting to join.
    LoL = lists:foldl(
        fun
            (#{name := Name}, Acc) when Name == State#state.name ->
                %% We exclude ourselves
                Acc;
            (#{name := _} = Node, Acc) ->
                %% TODO this should be a fold that will return the invalid
                %% NodeSpecs (nodes that have an invalid IP address because we
                %% already have a connection to NodeSpec.node on another IP
                %% address).
                %% We then remove those NodeSpes from the membership set and
                %% update ourselves without the need for sending any leave/join
                %% gossip.
                {ok, L} = partisan_peer_service_manager:connect(
                    Node, #{prune => true}
                ),
                [L | Acc]
        end,
        [],
        Nodes
    ),
    prune(lists:append(LoL), State).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
prune([], State) ->
    State;

prune(L, State) ->
    Mod = State#state.membership_strategy,
    MState0 = State#state.membership_strategy_state,
    {ok, Members, MState} = partisan_membership_strategy:prune(Mod, L, MState0),
    State#state{members = Members, membership_strategy_state = MState}.


%% @private
kill_connections(Nodes, State) ->
    Fun = fun(Node) ->
        ok = down(Node, State)
    end,
    partisan_peer_service_manager:disconnect(Nodes, Fun).


%% @private
handle_message(
    {ping, SrcNode, DestNode, SrcTime}, From, _Channel, #state{} = State0) ->

    %% Establish any new connections.
    State = establish_connections(State0),

    %% Send ping response.
    schedule_self_message_delivery(
        SrcNode,
        {pong, SrcNode, DestNode, SrcTime},
        ?DEFAULT_PARTITION_KEY,
        State,
        %% we coerce channel, regardless of _Channel
        #{channel => ?MEMBERSHIP_CHANNEL}
    ),

    maybe_reply(From, ok),

    {noreply, State};

handle_message(
    {pong, SrcNode, DestNode, SrcTime}, From, _Channel, #state{} = State) ->

    %% Compute difference.
    DistanceMetrics0 = State#state.distance_metrics,
    ArrivalTime = erlang:timestamp(),
    Difference = timer:now_diff(ArrivalTime, SrcTime),

    ?LOG_TRACE(
        "Updating distance metric for node ~p => ~p communication: ~p",
        [SrcNode, DestNode, Difference]
    ),

    %% Update differences.
    DistanceMetrics = maps:put(DestNode, Difference, DistanceMetrics0),

    %% Store in pdict.
    put(distance_metrics, DistanceMetrics),

    maybe_reply(From, ok),

    {noreply, State#state{distance_metrics=DistanceMetrics}};

handle_message(
    {membership_strategy, ProtocolMsg}, From, _Channel, #state{} = State0) ->

    #state{
        members = Members0,
        membership_strategy = MStrategy,
        membership_strategy_state = MState0
    } = State0,

    %% Process the protocol message.
    {ok, Members, OutgoingMessages, MState} =
        partisan_membership_strategy:handle_message(
            MStrategy, ProtocolMsg, MState0
        ),


    %% Update users of the peer service.
    case Members == Members0 of
        true ->
            ok;
        false ->
            partisan_peer_service_events:update(Members)
    end,

    %% Send outgoing messages.
    lists:foreach(
        fun({Node, Message}) ->
            schedule_self_message_delivery(
                Node,
                Message,
                ?DEFAULT_PARTITION_KEY,
                State0,
                %% we coerce channel, regardless of _Channel
                #{channel => ?MEMBERSHIP_CHANNEL}
            )
        end,
        OutgoingMessages
    ),

    State1 = State0#state{
        members = Members,
        membership_strategy_state = MState
    },

    {Pending, LeavingNodes} = pending_leavers(State1),
    State2 = State1#state{pending = Pending},

    %% Establish any new connections.
    State = establish_connections(State2),

    gen_server:cast(?MODULE, {kill_connections, LeavingNodes}),

    case lists:member(partisan:node_spec(), Members) of
        false ->
            ?LOG_INFO(#{
                description => "Shutting down: membership doesn't contain us",
                reason => "We've been removed from the cluster."
            }),

            ?LOG_DEBUG(#{
                membership => Members
            }),

            %% Shutdown if we've been removed from the cluster.
            {stop, normal, State};

        true ->
            maybe_reply(From, ok),
            {noreply, State}
    end;

%% Causal and acknowledged messages.
handle_message(
    {forward_message, SrcNode, MsgClock, ServerRef, Msg},
    From,
    Channel,
    #state{} = State) when is_tuple(Msg) andalso element(1, Msg) == causal ->

    {causal, Label, _, _, _, _, _} = Msg,

    %% Send message acknowledgement.
    send_acknowledgement(SrcNode, Channel, MsgClock, State),

    case partisan_causality_backend:is_causal_message(Msg) of
        true ->
            partisan_causality_backend:receive_message(Label, Msg);
        false ->
            %% Attempt message delivery.
            partisan_peer_service_manager:deliver(ServerRef, Msg)
    end,

    maybe_reply(From, ok),

    {noreply, State};

%% Acknowledged messages.
handle_message(
    {forward_message, SrcNode, MsgClock, ServerRef, Msg},
    From,
    Channel,
    #state{} = State) ->
    %% Send message acknowledgement.
    send_acknowledgement(SrcNode, Channel, MsgClock, State),

    partisan_peer_service_manager:deliver(ServerRef, Msg),

    maybe_reply(From, ok),

    {noreply, State};

%% Causal messages.
handle_message(
    {forward_message, ServerRef, {causal, Label, _, _, _, _, _} = Msg},
               From,
               _Channel,
               State) ->
    case partisan_causality_backend:is_causal_message(Msg) of
        true ->
            partisan_causality_backend:receive_message(Label, Msg);
        false ->
            %% Attempt message delivery.
            partisan_peer_service_manager:deliver(ServerRef, Msg)
    end,

    maybe_reply(From, ok),

    {noreply, State};

%% Best-effort messages.
%% TODO: Maybe remove me.
handle_message({forward_message, ServerRef, Msg},
               From,
               _Channel,
               State) ->
    partisan_peer_service_manager:deliver(ServerRef, Msg),
    maybe_reply(From, ok),
    {noreply, State};

handle_message({ack, MsgClock},
               From,
               _Channel,
               State) ->
    partisan_acknowledgement_backend:ack(MsgClock),
    maybe_reply(From, ok),
    {noreply, State};

handle_message(Msg,
               _From,
               _Channel,
               State) ->
    ?LOG_WARNING(#{description => "Unhandled message", message => Msg}),
    {noreply, State}.


%% @private
schedule_distance() ->
    case partisan_config:get(distance_enabled, false) of
        true ->
            DistanceInterval = partisan_config:get(distance_interval, 10000),
            erlang:send_after(DistanceInterval, ?MODULE, distance);
        false ->
            ok
    end.


%% @private
schedule_instrumentation() ->
    case partisan_config:get(instrumentation, false) of
        true ->
            erlang:send_after(1000, ?MODULE, instrumentation);
        _ ->
            ok
    end.


%% @private
schedule_periodic() ->
    case partisan_config:get(periodic_enabled, false) of
        true ->
            Time = partisan_config:get(periodic_interval, ?PERIODIC_INTERVAL),
            erlang:send_after(Time, ?MODULE, periodic);
        false ->
            ok
    end.


%% @private
schedule_retransmit() ->
    Time = partisan_config:get(retransmit_interval, 1000),
    erlang:send_after(Time, ?MODULE, retransmit).


%% @private
schedule_connections() ->
    Time = partisan_config:get(connection_interval, 1000),
    erlang:send_after(Time, ?MODULE, connections).


%% @private
do_send_message(Node, PartitionKey, Message, Options, State) ->
    %% Find a connection for the remote node, if we have one.
    Channel = maps:get(channel, Options, ?DEFAULT_CHANNEL),
    Res = partisan_peer_connections:dispatch_pid(Node, Channel, PartitionKey),

    case Res of
        {ok, Pid} ->
            gen_server:cast(Pid, {send_message, Message});

        {error, Reason} ->
            %% We were connected, but we're not anymore, or never connected
            case partisan_config:get(broadcast, false) of
                true ->
                    case maps:get(transitive, Options, false) of
                        true ->
                            ?LOG_DEBUG(
                                "Performing tree forward from node ~p "
                                "to node ~p and message: ~p",
                                [partisan:node(), Node, Message]
                            ),
                            TTL = partisan_config:get(relay_ttl, ?RELAY_TTL),
                            do_tree_forward(
                                Node,
                                PartitionKey,
                                Message,
                                Options,
                                TTL,
                                State
                            );
                        false ->
                            ok
                    end;
                false ->
                    case Reason of
                        disconnected ->
                            ?LOG_TRACE(
                                "Node ~p was connected, "
                                "but is now disconnected!",
                                [Node]
                            ),
                            {error, disconnected};
                        not_yet_connected ->
                            ?LOG_TRACE(
                                "Node ~p not yet connected!",
                                [Node]
                            ),
                            {error, not_yet_connected}
                    end
            end
    end.


%% @private
up(NodeOrSpec, State) ->
    apply_funs(NodeOrSpec, State#state.up_funs).



%% up(NodeOrSpec, Channel, State) ->
%%     apply_funs(NodeOrSpec, State#state.up_funs).

%% @private
down(NodeOrSpec, State) ->
    apply_funs(NodeOrSpec, State#state.down_funs).


%% @private
down(NodeOrSpec, _Channel, State) ->
    %% TODO use Channel
    apply_funs(NodeOrSpec, State#state.channel_down_funs).


%% @private
apply_funs(Node, Mapping) when is_atom(Node) ->
    ?LOG_DEBUG(#{
        description => "Node status change notification",
        node => Node,
        funs => Mapping
    }),

    Funs = lists:append(
        %% Notify functions matching the wildcard '_'
        maps:get('_', Mapping, []),
        %% Notify functions matching Node
        maps:get(Node, Mapping, [])
    ),

    _ = [
        begin
            case erlang:fun_info(F, arity) of
                {arity, 0} -> catch F();
                {arity, 1} -> catch F(Node)
            end
        end || F <- Funs
    ],

    ok;

apply_funs(#{name := Node}, Mapping) ->
    apply_funs(Node, Mapping).


%% @private
pending_leavers(#state{} = State) ->
    Members = ?SET_FROM_LIST(State#state.members),
    Pending0 = ?SET_FROM_LIST(State#state.pending),
    Connected = ?SET_FROM_LIST(partisan_peer_connections:node_specs()),

    %% Connected nodes that are no longer members
    Leavers = sets:to_list(sets:subtract(Connected, Members)),

    %% Disconnected nodes that are members
    Pending = sets:to_list(
        sets:subtract(sets:union(Members, Pending0), Connected)
    ),

    {Pending, Leavers}.



%% @private
internal_leave(#{name := Name} = Node, State0) ->

    #state{
        membership_strategy = MStrategy,
        membership_strategy_state = MState0
    } = State0,

    ?LOG_DEBUG(#{
        description => "Processing leave",
        leaving_node => Name
    }),

    {ok, Members, OutgoingMessages, MState} =
        partisan_membership_strategy:leave(MStrategy, Node, MState0),


    ?LOG_DEBUG(#{
        description => "Processing leave",
        leaving_node => Name,
        outgoing_messages => OutgoingMessages,
        new_membership => Members
    }),

    State1 = State0#state{
        members = Members,
        membership_strategy_state = MState
    },

    %% Establish any new connections.
    %% This will also prune no longer valid node_specs, setting the new
    %% membership in State
    State = establish_connections(State1),

    %% Transmit outgoing messages.
    lists:foreach(
        fun
            ({#{name := Peername}, Message}) ->
                schedule_self_message_delivery(
                    Peername,
                    Message,
                    ?DEFAULT_PARTITION_KEY,
                    State,
                    #{channel => ?MEMBERSHIP_CHANNEL}
                )
        end,
        OutgoingMessages
    ),

    partisan_peer_service_events:update(State#state.members),

    State.


%% @private
internal_join(#{name := Node} = NodeSpec, From, #state{} = State0) ->
    ok = partisan_util:maybe_connect_disterl(Node),

    %% Sleep before connecting, to avoid a rush on connections.
    avoid_rush(),

    %% Add to list of pending connections.
    Pending0 = State0#state.pending,
    Pending = Pending0 ++ [NodeSpec],

    State = maybe_add_sync_join(
        NodeSpec, From, State0#state{pending = Pending}
    ),

    %% Establish any new connections.
    establish_connections(State).


%% @private
maybe_add_sync_join(_, undefined, State) ->
    State;

maybe_add_sync_join(NodeSpec, From, State) ->
    SyncJoins0 = State#state.sync_joins,
    SyncJoins =
        try
            Fun = fun(Value) -> sets:add_element(From, Value) end,
            maps:update_with(NodeSpec, Fun, SyncJoins0)
        catch
            error:{badkey, NodeSpec} ->
                maps:put(
                    NodeSpec,
                    sets:from_list([From], [{version, 2}]),
                    SyncJoins0
                )
        end,

    State#state{sync_joins = SyncJoins}.


maybe_reply_sync_joins(State) ->
    Fun = fun(#{name := Node} = NodeSpec, Set, Acc) ->
        case partisan:is_fully_connected(NodeSpec) of
            true ->
                ?LOG_DEBUG("Node ~p is now fully connected.", [Node]),
                [
                    gen_server:reply(FromPid, ok)
                    || FromPid <- sets:to_list(Set)
                ],
                %% We remove the entry from map
                Acc;

            false ->
                %% We keep the Node in the new map
                maps:put(NodeSpec, Set, Acc)
        end
    end,

    SyncJoins = maps:fold(Fun, maps:new(), State#state.sync_joins),

    State#state{sync_joins = SyncJoins}.


%% @private
rand_bits(BitLen) ->
    Bytes = (BitLen + 7) div 8,
    <<Result:BitLen/bits, _/bits>> = crypto:strong_rand_bytes(Bytes),
    Result.


%% @private
avoid_rush() ->
    %% Sleep before connecting, to avoid a rush on connections.
    Jitter = partisan_config:get(connection_jitter, ?CONNECTION_JITTER),
    case partisan_config:get(jitter, false) of
        true ->
            timer:sleep(rand:uniform(Jitter));
        false ->
            timer:sleep(Jitter)
    end.

%% @private
do_tree_forward(Node, PartitionKey, Message, Opts, TTL, State) ->
    MyName = partisan:node(),

    ?LOG_TRACE(
        "Attempting to forward message ~p from ~p to ~p.",
        [Message, MyName, Node]
    ),

    %% Preempt with user-supplied outlinks.
    UserOutLinks = maps:get(out_links, Opts, undefined),

    OutLinks = case UserOutLinks of
        undefined ->
            try retrieve_outlinks() of
                Value ->
                    Value
            catch
                _:Reason ->
                    ?LOG_ERROR(#{
                        description => "Outlinks retrieval failed",
                        reason => Reason
                    }),
                    []
            end;
        OL ->
            OL -- [MyName]
    end,

    %% Send messages, but don't attempt to forward again, if we aren't
    %% connected.
    _ = lists:foreach(
        fun(Peer) ->
            ?LOG_TRACE(
                "Forwarding relay message ~p to node ~p "
                "for node ~p from node ~p",
                [Message, Peer, Node, MyName]
            ),

            RelayMessage = {relay_message, Node, Message, TTL - 1},

            schedule_self_message_delivery(
                Peer,
                RelayMessage,
                PartitionKey,
                State,
                maps:without([transitive], Opts)
            )
        end,
        OutLinks
    ),
    ok.


%% @private
retrieve_outlinks() ->
    ?LOG_TRACE(#{description => "About to retrieve outlinks..."}),

    Root = partisan:node(),
    Result = partisan_plumtree_broadcast:debug_get_peers(Root, Root, 1000),

    OutLinks = try Result of
        {EagerPeers, _LazyPeers} ->
            ordsets:to_list(EagerPeers)
    catch
        _:_ ->
            ?LOG_INFO(#{description => "Request to get outlinks timed out..."}),
            []
    end,

    ?LOG_TRACE("Finished getting outlinks: ~p", [OutLinks]),

    OutLinks -- [Root].


%% @private
schedule_tree_refresh() ->
    case partisan_config:get(broadcast, false) of
        true ->
            Period = partisan_config:get(tree_refresh, 1000),
            erlang:send_after(Period, ?MODULE, tree_refresh);
        false ->
            ok
    end.


%% @private
schedule_self_message_delivery(Node, Message, PartitionKey, State) ->
    schedule_self_message_delivery(Node, Message, PartitionKey, State, #{}).


%% @private
schedule_self_message_delivery(Node, Message, PartitionKey, State, Options) ->
    PreIFuns = State#state.pre_interposition_funs,

    DeliveryFun = fun() ->
        ok  = ?FIRE_FWD_PRE_INTERPOSITIONS(Node, Message, PreIFuns),

        %% Once pre-interposition returns, then schedule for delivery.
        gen_server:cast(?MODULE, {
            forward_message,
            undefined, % from
            Node,
            undefined, % clock
            PartitionKey,
            ?MODULE, % ServerRef
            Message,
            Options
        })
    end,

    case partisan_config:get(replaying, false) of
        false ->
            %% Fire all pre-interposition functions, and then deliver,
            %% preserving serial order of messages.
            DeliveryFun();
        true ->
            %% Allow the system to proceed, and the message will be delivered
            %% once pre-interposition is done.
            spawn_link(DeliveryFun)
    end,

    ok.


%% @private
send_acknowledgement(Node, Channel, MsgClock, State) ->
    %% Generate message.
    Message = {ack, MsgClock},

    %% Send on the default channel.
    schedule_self_message_delivery(
        Node,
        Message,
        ?DEFAULT_PARTITION_KEY,
        State,
        #{channel => Channel}
    ).


%% @private
maybe_reply(From, Response) ->
    case From of
        undefined ->
            ok;
        _ ->
            gen_server:reply(From, Response)
    end.


%% @private
maybe_append_pending(NodeSpec, #state{} = State) ->
    Pending0 = State#state.pending,
    Members = State#state.members,

    Pending = case lists:member(NodeSpec, Members) of
        true ->
            Pending0 ++ [NodeSpec];
        false ->
            Pending0
    end,
    State#state{pending = Pending}.


