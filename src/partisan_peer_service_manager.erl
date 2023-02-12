%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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

-module(partisan_peer_service_manager).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan_logger.hrl").
-include("partisan.hrl").


-type server_ref()      ::  partisan_remote_ref:p()
                            | partisan_remote_ref:encoded_pid()
                            | partisan_remote_ref:n()
                            | partisan_remote_ref:encoded_name()
                            | pid()
                            | (RegName :: atom())
                            | {RegName :: atom(), node()}
                            | {global, atom()}
                            | {via, module(), ViaName :: atom()}.


-type forward_opts()    ::  #{
                                ack => boolean(),
                                causal_label => atom(),
                                channel => partisan:channel(),
                                clock => any(),
                                partition_key => non_neg_integer(),
                                transitive => boolean()
                            } |
                            [
                                {ack, boolean()}
                                | {causal_label, atom()}
                                | {channel, partisan:channel()}
                                | {clock, any()}
                                | {partition_key, non_neg_integer()}
                                | {transitive, boolean()}
                            ].

-type connect_opts()        ::  #{prune => boolean()}.
-type partitions()          ::  [{reference(), partisan:node_spec()}].
-type on_event_fun()        ::  fun(() -> ok)
                                | fun((node()) -> ok)
                                | fun((node(), partisan:channel()) -> ok).

-export_type([connect_opts/0]).
-export_type([forward_opts/0]).
-export_type([on_event_fun/0]).
-export_type([partitions/0]).
-export_type([server_ref/0]).

%% API
-export([connect/1]).
-export([connect/2]).
-export([disconnect/1]).
-export([disconnect/2]).
-export([mynode/0]).
-export([myself/0]).
-export([process_forward/2]).
-export([send_message/2]).
-export([supports_capability/2]).



%% =============================================================================
%% BEHAVIOUR CALLBACKS
%% =============================================================================



-callback start_link() -> {ok, pid()} | ignore | {error, term()}.

-callback members() -> [node()]. %% TODO: Deprecate me.

-callback members_for_orchestration() -> [partisan:node_spec()].

-callback update_members([node()]) -> ok | {error, not_implemented}.

-callback get_local_state() -> term().

-callback on_down(node(), on_event_fun()) -> ok | {error, not_implemented}.

-callback on_down(node(), on_event_fun(), #{channel => partisan:channel()}) ->
    ok | {error, not_implemented}.

-callback on_up(node(), on_event_fun()) -> ok | {error, not_implemented}.

-callback on_up(node(), on_event_fun(), #{channel => partisan:channel()}) ->
    ok | {error, not_implemented}.

-callback join(partisan:node_spec()) -> ok.

-callback sync_join(partisan:node_spec()) -> ok | {error, not_implemented}.

-callback leave() -> ok.

-callback leave(partisan:node_spec()) -> ok.

-callback send_message(node(), partisan:message()) -> ok.

-callback receive_message(node(), partisan:channel(), partisan:message()) -> ok.

-callback cast_message(
    ServerRef :: server_ref(),
    Msg :: partisan:message()) -> ok.

-callback cast_message(
    ServerRef :: server_ref(),
    Msg :: partisan:message(),
    Opts :: forward_opts()) -> ok.

-callback cast_message(
    Node :: node(),
    ServerRef :: server_ref(),
    Msg :: partisan:message(),
    Opts :: forward_opts()) -> ok.

-callback forward_message(
    ServerRef :: server_ref(),
    Msg :: partisan:message()) -> ok.

-callback forward_message(
    ServerRef :: server_ref(),
    Msg :: partisan:message(),
    Opts :: forward_opts()) -> ok.

-callback forward_message(
    Node :: node(),
    ServerRef :: server_ref(),
    Msg :: partisan:message(),
    Opts :: forward_opts()) -> ok.


-callback decode(term()) -> term().

-callback reserve(atom()) -> ok | {error, no_available_slots}.

-callback supports_capability(Arg :: atom()) -> boolean().

-callback partitions() -> {ok, partitions()} | {error, not_implemented}.

-callback inject_partition(partisan:node_spec(), ttl()) ->
    {ok, reference()} | {error, not_implemented}.

-callback resolve_partition(reference()) -> ok | {error, not_implemented}.

-optional_callbacks([supports_capability/1]).
-optional_callbacks([on_up/3]).
-optional_callbacks([on_down/3]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc If `Mod' implements callback `supports_capability/1' returns the result
%% of calling the callback passing argument `Arg'. Otherwise, returns `false'.
%% @end
%% -----------------------------------------------------------------------------
-spec supports_capability(Mode :: module(), Arg :: atom()) -> boolean().

supports_capability(Mod, Arg) ->
    case erlang:function_exported(Mod, supports_capability, 1) of
        true ->
            Mod:supports_capability(Arg);
        false ->
            false
    end.


%% -----------------------------------------------------------------------------
%% @doc Tries to create a new connection to a node, but only if required.
%% If successful it stores the new connection record in the
%% {@link partisan_peer_connections} table.
%%
%% This function calls {@link connect/2} with options `#{prune => false}'.
%% @end
%% -----------------------------------------------------------------------------
-spec connect(Node :: partisan:node_spec()) -> ok.

connect(NodeSpec) ->
    connect(NodeSpec, #{prune => false}).


%% -----------------------------------------------------------------------------
%% @doc Create a new connection to a node specified by `NodeSpec' and
%% options `Opts'.
%% If a new connection is created it will be stored in the
%% {@link partisan_peer_connections} table.
%%
%% If option `prune' is `true' returns the tuple `{ok, L :: [
%% partisan:node_spec()]}' where list L is the list of nodes specifications for
%% all stale nodes. Otherwise returns `ok'.
%%
%% A specification is stale if there is another specification for the same
%% node for which we already have one or more active connections. A stale
%% specification will exist when a node has crashed (without leaving the
%% cluster) and later on returned with a different IP address i.e. a normal
%% situation on cloud orchestration platforms. In this case the membership set
%% ({@link partisan_membership_set}) will have two node specifications for the
%% same node (with differing values for the `listen_addrs' property).
%%
%% See the section **Stale Specifications** in {@link partisan_membership_set}.
%% @end
%% -----------------------------------------------------------------------------
-spec connect(Node :: partisan:node_spec(), connect_opts()) ->
    ok | {ok, StaleSpecs :: [partisan:node_spec()]}.

connect(#{listen_addrs := ListenAddrs} = NodeSpec, #{prune := true}) ->
    ToPrune = lists:foldl(
        fun(ListenAddr, Acc) ->
            maybe_connect(NodeSpec, ListenAddr, Acc)
        end,
        [],
        ListenAddrs
    ),
    {ok, ToPrune};

connect(#{listen_addrs := ListenAddrs} = NodeSpec, #{prune := false}) ->
    ok = lists:foreach(
        fun(ListenAddr) ->
            maybe_connect(NodeSpec, ListenAddr, ok)
        end,
        ListenAddrs
    ).


%% -----------------------------------------------------------------------------
%% @doc Kill all connections with node in `Nodes' and for each call function
%% `Fun' passing the node as argument
%% @end
%% -----------------------------------------------------------------------------
disconnect(Nodes) when is_list(Nodes) ->
    disconnect(Nodes, fun(_) -> ok end).


%% -----------------------------------------------------------------------------
%% @doc Kill all connections with node in `Nodes' and for each call function
%% `Fun' passing the node as argument
%% @end
%% -----------------------------------------------------------------------------
disconnect(Nodes, Fun) when is_list(Nodes), is_function(Fun, 1) ->
    Node = partisan:node(),
    _ = [
        begin
            _ = case partisan_config:get(connect_disterl, false) of
                true ->
                    net_kernel:disconnect(N);
                false ->
                    ok
            end,
            ok = partisan_peer_connections:erase(N),
            catch Fun(N)
        end || N <- Nodes, N =/= Node
    ],
    ok.


%% -----------------------------------------------------------------------------
%% @doc Send a message to a remote peer_service_manager.
%% @end
%% -----------------------------------------------------------------------------
-spec send_message(node(), partisan:message()) -> ok.

send_message(Node, Message) ->
    ?PEER_SERVICE_MANAGER:send_message(Node, Message).


%% -----------------------------------------------------------------------------
%% @doc Internal function used by peer_service manager implementations to
%% forward a message to a process identified by `ServerRef' that is either
%% local or located at remote process when the remote node is connected via
%% disterl.
%% Trying to send a message to a remote server reference when the process is
%% located at a node connected with Partisan will return `ok' but will not
%% succeed.
%% @end
%% -----------------------------------------------------------------------------
-spec process_forward(ServerRef :: server_ref(), Msg :: any()) -> ok.

process_forward(ServerRef, Msg) ->
    try
        do_process_forward(ServerRef, Msg)
    catch
        Class:Reason:Stacktrace ->
            ?LOG_DEBUG(#{
                description => "Error forwarding message",
                message => Msg,
                destination => ServerRef,
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ok
    end.


%% @deprecated use {@link partisan:node_spec/0} instead
-spec myself() -> partisan:node_spec().

myself() ->
    partisan:node_spec().


%% @deprecated use {@link partisan:node/0} instead
-spec mynode() -> atom().

mynode() ->
    partisan:node().



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
maybe_connect(#{name := Node} = NodeSpec, ListenAddr, Acc) ->
    Channels =
        case maps:find(channels, NodeSpec) of
            {ok, Value} -> Value;
            error -> partisan_config:channels()
        end,

    %% We check count using Node and not NodeSpec cause it is much faster and
    %% we are only interested in knowing if we have at least one connection
    %% even if this was a stale NodeSpec.
    %% See the section Stale Specifications in partisan_membership_set.
    case partisan_peer_connections:connection_count(Node) of
        0 ->
            %% Found disconnected or not found
            %% We are going to try with a first connection using the default
            %% channel
            ?LOG_DEBUG(#{
                description =>
                    "We have no connections with peer. Trying to connect",
                peer => NodeSpec
            }),

            %% We start with default channel
            Channel = ?DEFAULT_CHANNEL,
            ChannelOpts = maps:get(Channel, Channels),

            %% We start a client which will connect with
            %% partisan_peer_service_server on the peer node
            Result = partisan_peer_service_client:start_link(
                NodeSpec, ListenAddr, Channel, ChannelOpts, self()
            ),

            case Result of
                {ok, Pid} ->
                    ?LOG_DEBUG("Node ~p connected, pid: ~p", [NodeSpec, Pid]),
                    ok = partisan_peer_connections:store(
                        NodeSpec, Pid, Channel, ListenAddr
                    ),
                    Acc;
                ignore ->
                    ?LOG_DEBUG(#{
                        description => "Node failed connection.",
                        node_spec => NodeSpec,
                        reason => ignore
                    }),
                    Acc;
                {error, normal} ->
                    ?LOG_DEBUG(#{
                        description => "Node isn't online just yet.",
                        node_spec => NodeSpec
                    }),
                    Acc;
                {error, Reason} ->
                    ?LOG_DEBUG(#{
                        description => "Node failed connection.",
                        node_spec => NodeSpec,
                        reason => Reason
                    }),
                    Acc
            end;
        _ ->
            %% Found node with some channels already connected, we will try to
            %% complete the parallelism per channel across all channels
            ChannelsList = maps:to_list(Channels),
            maybe_connect(ChannelsList, NodeSpec, ListenAddr, Acc)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc This function is called by maybe_connect/3 only when the Node in
%% NodeSpec has at least one active connection.
%% @end
%% -----------------------------------------------------------------------------
maybe_connect([{Channel, ChannelOpts}|T], NodeSpec, ListenAddr, Acc) ->
    %% There is at least one connection for Node.
    Parallelism = get_opt(parallelism, ChannelOpts),

    case partisan_peer_connections:connection_count(NodeSpec, Channel) of
        Count when Count < Parallelism ->
            ?LOG_DEBUG(
                "~p of ~p connected for channel ~p) Connecting node ~p.",
                [Count, Parallelism, Channel, NodeSpec]
            ),

            %% Count might be == 0, but we try to connect anyway and we deal
            %% with that case below.
            Result = partisan_peer_service_client:start_link(
                NodeSpec, ListenAddr, Channel, ChannelOpts, self()
            ),

            case Result of
                {ok, Pid} ->
                    ?LOG_DEBUG(
                        "Node ~p connected, pid: ~p", [NodeSpec, Pid]
                    ),
                    ok = partisan_peer_connections:store(
                        NodeSpec, Pid, Channel, ListenAddr
                    ),
                    Acc;

                {error, Reason} when Count == 0 ->
                    %% The connection we have must have been created using a
                    %% different partisan:node_spec() for Node. Since we have a
                    %% connection we need to assume NodeSpec might be stale (a
                    %% previous version of the spec for example if a Node
                    %% crashed and come back again with a (i) different set of
                    %% ListenAddrs, or (ii) different values for channel and/or
                    %% parallellism).
                    %% maybe_stale/6 will try to determine is this is an
                    %% instance of case (i). At the moment we cannot deal with
                    %% instances of case (ii).
                    maybe_stale(
                        NodeSpec, Channel, ListenAddr, Acc, Count, Reason
                    );

                Error ->
                    %% We have some connections to this ListenAddr already
                    ?LOG_ERROR(#{
                        description => "Node failed to connect",
                        error => Error,
                        node_spec => NodeSpec,
                        listen_address => ListenAddr,
                        channel => Channel,
                        channel_opts => ChannelOpts
                    }),
                    Acc
            end;

        Count when Count == Parallelism ->
            Acc
    end,

    %% We continue with next channel, even though we might have not finished
    %% connecting all the connections required by the parallelism of the
    %% current channel. We will try on the next tick.
    %% It is fairer this way, so that we can get connections one channel at a
    %% time.
    maybe_connect(T, NodeSpec, ListenAddr, Acc);

maybe_connect([], _, _, Acc) ->
    Acc.


%% @private
maybe_stale(_, _, _, ok = Acc, 0, _) ->
    %% Options.prune == false
    Acc;

maybe_stale(NodeSpec, Channel, ListenAddr, Acc, 0, Reason) ->
    Node = maps:get(name, NodeSpec),
    %% TODO check is we are already connected using connection_count
    %% (Node, Channel). If so, then this ListenAddr is not longer valid
    %% we need to accumulate this NodeSpec and return to the caller of
    %% maybe_connect.

    %% Do we have a connection to the node for this channel on this addr?
    %% If so, check the connected spec with this one, cause this might be
    %% invalid.
    %% If not, then we cannot rule out the NodeSpec as valid.
    ListenAddrCount =
        partisan_peer_connections:connection_count(Node, Channel, ListenAddr),


    case ListenAddrCount > 0 of
        true ->
            ListenAddrs = maps:get(listen_addrs, NodeSpec),

            %% We got connections for Node, so we fetch the associated Info
            %% which contains the node_spec
            {ok, Info} = partisan_peer_connections:info(Node),

            %% We are trying to determine if NodeSpec might be a previous
            %% instance.
            case partisan_peer_connections:node_spec(Info) of
                Connected when Connected == NodeSpec ->
                    %% It is the same node_spec, so we are just having problems
                    %% opening more connections at the time being.
                    ?LOG_DEBUG(#{
                        description => "Node failed to connect",
                        reason => Reason,
                        node_spec => NodeSpec,
                        listen_address => ListenAddr,
                        channel => Channel
                    }),
                    Acc;

                #{listen_addrs := L} when L == ListenAddrs ->
                    %% The specs differ on channels or parallelism
                    ?LOG_DEBUG(#{
                        description => "Node failed to connect",
                        reason => Reason,
                        node_spec => NodeSpec,
                        listen_address => ListenAddr,
                        channel => Channel
                    }),
                    Acc;

                Connected ->
                    %% Listen addresses differ!
                    %% TODO use info and connections timestamps
                    case partisan_peer_connections:is_connected(NodeSpec) of
                        true ->
                            %% Ummmm....we got some connections, so we keep it
                            ?LOG_DEBUG(#{
                                description => "Node failed to connect",
                                reason => Reason,
                                node_spec => NodeSpec,
                                listen_address => ListenAddr,
                                channel => Channel
                            }),
                            Acc;
                        false ->
                            %% IP has changed
                            %% We add it to the invalid list
                            ?LOG_INFO(#{
                                description =>
                                    "Flagging node specification to be pruned",
                                reason => duplicate,
                                node_spec => NodeSpec,
                                active => Connected
                            }),
                            [NodeSpec|Acc]
                    end
            end;

        false ->
            Acc
    end.


%% @private
get_opt(parallelism, #{parallelism := Value}) ->
    Value;

get_opt(parallelism, #{}) ->
    partisan_config:parallelism().



%% @private
do_process_forward({global, Name}, Message) ->
    Pid = global:whereis_name(Name),
    Pid ! Message,
    ok;

do_process_forward({via, Module, Name}, Message) ->
    Pid = Module:whereis_name(Name),
    Pid ! Message,
    ok;

do_process_forward(ServerRef, Message)
when is_pid(ServerRef) orelse is_atom(ServerRef) ->
    ServerRef ! Message,

    Trace =
        (
            is_pid(ServerRef) andalso
            not is_process_alive(ServerRef)
        ) orelse (
            not is_pid(ServerRef) andalso (
                whereis(ServerRef) == undefined orelse
                not is_process_alive(whereis(ServerRef))
            )
        ),

    ?LOG_TRACE_IF(
        Trace, "Process ~p is NOT ALIVE.", [ServerRef]
    ),

    ok;

do_process_forward(ServerRef, Message) ->
    ?LOG_DEBUG(
        "node ~p received message ~p for ~p",
        [partisan:node(), Message, ServerRef]
    ),

    case partisan_remote_ref:to_term(ServerRef) of
        Pid when is_pid(Pid) ->
            ?LOG_TRACE_IF(
                not is_process_alive(Pid),
                "Process ~p is NOT ALIVE for message: ~p", [ServerRef, Message]
            ),
            Pid ! Message,
            ok;

        Name when is_atom(Name) ->
            Name ! Message,
            ok
    end.
