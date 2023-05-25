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
-export([on_up/2]).
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
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).



-record(state, {
    myself              :: partisan:node_spec(),
    pending             :: pending(),
    membership          :: membership()
}).

-type state_t()         :: #state{}.
-type pending()         :: [partisan:node_spec()].
-type membership()      :: sets:set(partisan:node_spec()).


%%%===================================================================
%%% partisan_peer_service_manager callbacks
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
on_down(_Name, _Function) ->
    {error, not_implemented}.

%% @doc Trigger function on connection open for a given node.
on_up(_Name, _Function) ->
    {error, not_implemented}.

%% @doc Update membership.
update_members(_Nodes) ->
    {error, not_implemented}.

%% @doc Send message to a remote manager.
send_message(Name, Message) ->
    gen_server:call(?MODULE, {send_message, Name, Message}, infinity).

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    Term :: partisan:any_pid() | partisan:any_name(),
    MEssage :: partisan:message()) -> ok.

cast_message(Term, Message) ->
    FullMessage = {'$gen_cast', Message},
    forward_message(Term, FullMessage, #{}).


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
    forward_message(Name, ServerRef, FullMessage, Options).


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
    Channel = maps:get(channel, Opts, ?DEFAULT_CHANNEL),
    gen_server:call(
        ?MODULE,
        {forward_message, Node, Channel, ServerRef, Message, Opts},
        infinity
    ).

%% @doc Receive message from a remote manager.
receive_message(_Peer, Channel, Message) ->
    gen_server:call(?MODULE, {receive_message, Channel, Message}, infinity).

%% @doc Attempt to join a remote node.
join(Node) ->
    gen_server:call(?MODULE, {join, Node}, infinity).

%% @doc Attempt to join a remote node.
sync_join(_Node) ->
    {error, not_implemented}.

%% @doc Leave the cluster.
leave() ->
    gen_server:call(?MODULE, {leave, partisan:node()}, infinity).

%% @doc Remove another node from the cluster.
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
    false;

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

    Membership = maybe_load_state_from_disk(),
    Myself = partisan:node_spec(),

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

handle_call({leave, _Node}, _From, State) ->
    {reply, error, State};

handle_call({join, #{name := Node} = Spec}, _From, #state{} = State) ->
    %% eqwalizer:ignore
    ok = partisan_util:maybe_connect_disterl(Node),

    %% Add to list of pending connections.
    Pending = [Spec | State#state.pending],

    %% Trigger connection.
    %% eqwalizer:ignore
    ok = partisan_peer_service_manager:connect(Spec),
    %% eqwalizer:ignore
    {reply, ok, State#state{pending = Pending}};

handle_call({send_message, Name, Message}, _From, #state{} = State) ->
    %% eqwalizer:ignore
    Result = do_send_message(Name, Message),
    {reply, Result, State};

handle_call(
    {forward_message, Name, _Channel, ServerRef, Message, _Options},
    _From,
    #state{} = State) ->
    %% eqwalizer:ignore
    Result = do_send_message(Name, {forward_message, ServerRef, Message}),
    {reply, Result, State};

handle_call({receive_message, Channel, Message}, _From, State) ->
    handle_message(Message, Channel, State);

handle_call(members, _From, #state{membership=Membership}=State) ->
    Members = [P || #{name := P} <- members(Membership)],
    {reply, {ok, Members}, State};

handle_call(members_for_orchestration, _From, #state{membership=Membership}=State) ->
    {reply, {ok, Membership}, State};

handle_call(get_local_state, _From, #state{membership=Membership}=State) ->
    {reply, {ok, Membership}, State};

handle_call(Event, _From, State) ->
    ?LOG_WARNING(#{description => "Unhandled call event", event => Event}),
    {reply, ok, State}.

%% @private
-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.
handle_cast(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled cast event", event => Event}),    {noreply, State}.

handle_info({'EXIT', From, _Reason}, #state{}=State) ->
    _  = catch partisan_peer_connections:prune(From),
    {noreply, State};

handle_info({connected, Node, _Channel, _Tag, _RemoteState},
               #state{pending=Pending0,
                      membership=Membership0}=State) ->
    case lists:member(Node, Pending0) of
        true ->
            %% Move out of pending.
            Pending = Pending0 -- [Node],

            %% Add to our membership.
            Membership = sets:add_element(Node, Membership0),

            %% Announce to the peer service.
            partisan_peer_service_events:update(Membership),

            %% Establish any new connections.
            ok = establish_connections(Pending, Membership),

            %% Compute count.
            Count = sets:size(Membership),

            ?LOG_INFO(#{
                description => "Join ACCEPTED",
                peer_node => Node,
                member_view_count => Count
            }),

            %% Return.
            {noreply, State#state{pending=Pending, membership=Membership}};
        false ->
            {noreply, State}
    end;

handle_info(Event, State) ->
    ?LOG_WARNING(#{description => "Unhandled info event", event => Event}),
    {noreply, State}.

%% @private
-spec terminate(term(), state_t()) -> term().

terminate(_Reason, #state{}) ->
    Fun = fun
        (_K, Pids) ->
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
-spec code_change(term() | {down, term()}, state_t(), term()) -> {ok, state_t()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
empty_membership() ->
    LocalState = sets:add_element(partisan:node_spec(), sets:new()),
    persist_state(LocalState),
    LocalState.

%% @private
data_root() ->
    case application:get_env(partisan, partisan_data_dir) of
        {ok, PRoot} ->
            filename:join(PRoot, "default_peer_service");
        undefined ->
            undefined
    end.

%% @private
write_state_to_disk(State) ->
    case data_root() of
        undefined ->
            ok;
        Dir ->
            File = filename:join(Dir, "cluster_state"),
            ok = filelib:ensure_dir(File),
            ok = file:write_file(File, term_to_binary(State))
    end.

%% @private
maybe_load_state_from_disk() ->
    case data_root() of
        undefined ->
            empty_membership();
        Dir ->
            case filelib:is_regular(filename:join(Dir, "cluster_state")) of
                true ->
                    {ok, Bin} = file:read_file(filename:join(Dir, "cluster_state")),
                    binary_to_term(Bin);
                false ->
                    empty_membership()
            end
    end.

%% @private
persist_state(State) ->
    write_state_to_disk(State).

%% @private
members(Membership) ->
    sets:to_list(Membership).

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
    {reply, ok, State}.


%% @private
-spec do_send_message(Node :: partisan:node_spec(), Message :: term()) ->
    ok | {error, disconnected | not_yet_connected} | {error, term()}.

do_send_message(Node, Message) ->
    %% Find a connection for the remote node, if we have one.
    case partisan_peer_connections:dispatch_pid(Node) of
        {ok, Pid} ->
            gen_server:cast(Pid, {send_message, Message});

        {error, _} = Error ->
            Error
    end.
