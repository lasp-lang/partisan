%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Helium Systems, Inc. All Rights Reserved.
%% Copyright (c) 2016 Christopher Meiklejohn. All Rights Reserved.
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
%% @doc This module implements the Partisan API.
%% Some of the functions in this module are the Partisan counterparts of a
%% subset of the functions found in the {@link erlang} and {@link net_kernel}
%% modules.
%%
%% -----------------------------------------------------------------------------
-module(partisan).

-include("partisan.hrl").
-include("partisan_logger.hrl").

-type any_pid()             ::  remote_pid() | pid().
-type any_reference()       ::  remote_reference() | reference().
-type any_name()            ::  remote_name() | atom().
-type remote_pid()          ::  partisan_remote_ref:p().
-type remote_reference()    ::  partisan_remote_ref:r().
-type remote_name()         ::  partisan_remote_ref:n().

-type monitor_opt()         ::  erlang:monitor_option()
                                | {channel, channel()}.
-type demonitor_opt()       ::  flush | info.
-type net_kernel_opt()      ::  nodedown_reason
                                | connection_id % OTP 25
                                | {node_type, visible | hidden | all}.
-type channel_opt()         ::   net_kernel_opt()
                                | {channel, channel()}
                                | {channel_fallback, boolean()}.
-type monitor_nodes_opt()   ::   net_kernel_opt()
                                | channel_opt().
-type send_dst()            ::  erlang:send_destination()
                                | server_ref().

-type server_ref()          ::  partisan_peer_service_manager:server_ref().
-type forward_opts()        ::  partisan_peer_service_manager:forward_opts().
-type node_type()           ::  this | known | visible | connected | hidden.
-type channel()             ::  atom().
-type channel_opts()        ::  #{
                                    parallelism := non_neg_integer(),
                                    monotonic => boolean(),
                                    compression => boolean() | 0..9
                                }.
-type actor()               ::  binary().
-type listen_addr()         ::  #{
                                    ip := inet:ip_address(),
                                    port := 1..65535
                                }.
-type node_spec()           ::  #{
                                    name := node(),
                                    listen_addrs := [listen_addr()],
                                    channels := #{channel() => channel_opts()}
                                }.
-type node_info()           ::  #{info_opt() => term()}.
-type info_opt()            ::  metadata
                                | name
                                | channels
                                | listen_addrs
                                | listen_ip
                                | listen_port
                                | connection_count.


-type message()             ::  term().
-type time()                ::  non_neg_integer().
-type send_after_dst()      ::  pid()
                                | (RegName :: atom())
                                | (Pid :: remote_pid())
                                | (RegName :: remote_name()).
-type send_after_opts()     ::  forward_opts() | [{abs, boolean()}].


-export_type([actor/0]).
-export_type([channel/0]).
-export_type([channel_opts/0]).
-export_type([demonitor_opt/0]).
-export_type([forward_opts/0]).
-export_type([info_opt/0]).
-export_type([listen_addr/0]).
-export_type([message/0]).
-export_type([monitor_nodes_opt/0]).
-export_type([monitor_opt/0]).
-export_type([node_spec/0]).
-export_type([node_type/0]).
-export_type([any_pid/0]).
-export_type([any_reference/0]).
-export_type([any_name/0]).
-export_type([remote_pid/0]).
-export_type([remote_reference/0]).
-export_type([remote_name/0]).
-export_type([server_ref/0]).


%% API
-export([start/0]).
-export([stop/0]).

%% Erlang API (erlang.erl counterparts)
-export([cancel_timer/1]).
-export([cancel_timer/2]).
-export([demonitor/1]).
-export([demonitor/2]).
-export([disconnect_node/1]).
-export([exit/2]).
-export([is_alive/0]).
-export([is_pid/1]).
-export([is_process_alive/1]).
-export([is_reference/1]).
-export([make_ref/0]).
-export([monitor/1]).
-export([monitor/2]).
-export([monitor/3]).
-export([node/0]).
-export([node/1]).
-export([process_info/1]).
-export([process_info/2]).
-export([self/0]).
-export([send/2]).
-export([send/3]).
-export([send_after/3]).
-export([send_after/4]).
-export([spawn/2]).
-export([spawn/4]).
-export([spawn_monitor/2]).
-export([spawn_monitor/4]).
-export([whereis/1]).

%% Erlang API (net_kernel.erl counterparts)
-export([monitor_node/2]).
-export([monitor_node/3]).
-export([monitor_nodes/1]).
-export([monitor_nodes/2]).

%% Partisan API
-export([broadcast/2]).
-export([cast_message/2]).
-export([cast_message/3]).
-export([cast_message/4]).
-export([channel_opts/1]).
-export([default_channel/0]).
-export([forward_message/2]).
-export([forward_message/3]).
-export([forward_message/4]).
-export([is_connected/1]).
-export([is_connected/2]).
-export([is_fully_connected/1]).
-export([is_local/1]).
-export([is_local_name/1]).
-export([is_local_name/2]).
-export([is_local_pid/1]).
-export([is_local_pid/2]).
-export([is_local_reference/1]).
-export([is_local_reference/2]).
-export([is_self/1]).
-export([join/1]).
-export([kill_connections/1]).
-export([leave/0]).
-export([node_spec/0]).
-export([node_spec/1]).
-export([node_spec/2]).
-export([nodes/0]).
-export([nodes/1]).
-export([nodestring/0]).
-export([self/1]).


-compile({no_auto_import, [demonitor/2]}).
-compile({no_auto_import, [is_pid/1]}).
-compile({no_auto_import, [is_process_alive/1]}).
-compile({no_auto_import, [is_reference/0]}).
-compile({no_auto_import, [make_ref/0]}).
-compile({no_auto_import, [monitor/2]}).
-compile({no_auto_import, [monitor/3]}).
-compile({no_auto_import, [monitor_node/2]}).
-compile({no_auto_import, [node/0]}).
-compile({no_auto_import, [node/1]}).
-compile({no_auto_import, [nodes/1]}).
-compile({no_auto_import, [process_info/2]}).
-compile({no_auto_import, [self/0]}).
-compile({no_auto_import, [whereis/1]}).
-compile({no_auto_import, [spawn/2]}).
-compile({no_auto_import, [spawn/4]}).
-compile({no_auto_import, [spawn_monitor/2]}).
-compile({no_auto_import, [spawn_monitor/4]}).

-eqwalizer({nowarn_function, make_ref/0}).
-eqwalizer({nowarn_function, self/0}).
-eqwalizer({nowarn_function, self/1}).
-eqwalizer({nowarn_function, is_local_name/2}).
-eqwalizer({nowarn_function, is_local_pid/2}).
-eqwalizer({nowarn_function, is_local_reference/2}).
-eqwalizer({nowarn_function, to_net_kernel_opts/1}).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Start the application.
%% @end
%% -----------------------------------------------------------------------------
start() ->
    application:ensure_all_started(partisan).


%% -----------------------------------------------------------------------------
%% @doc Stop the application.
%% @end
%% -----------------------------------------------------------------------------
stop() ->
    application:stop(partisan).



%% -----------------------------------------------------------------------------
%% @doc Returns a new partisan_remote_ref.
%% This is the same as calling
%% `partisan_remote_ref:from_term(erlang:make_ref())'.
%% @end
%% -----------------------------------------------------------------------------
-spec make_ref() -> remote_reference() | no_return().

make_ref() ->
    partisan_remote_ref:from_term(erlang:make_ref()).


%% -----------------------------------------------------------------------------
%% @doc Returns the partisan encoded pid for the calling process.
%% This is equivalent to calling `partisan_remote_ref:from_term(self())'.
%%
%% Notice that this call is more expensive than It's erlang counterpart. So you
%% might want to cache the result in your process state.
%% See function {@link self/1} which allows you to cache the result in the
%% process dictionary and take notice of the warning related to doing this on
%% an Erlang Shell process.
%% @end
%% -----------------------------------------------------------------------------
-spec self() -> remote_pid().

self() ->
    partisan_remote_ref:from_term(erlang:self()).


%% -----------------------------------------------------------------------------
%% @doc Returns the partisan encoded pid for the calling process.
%%
%% If `Opts' is the empty list, this is equivalent to calling
%% `partisan_remote_ref:from_term(self())'.
%%
%% Otherwise, if the option `cache' is present in `Opts' the function lazily
%% caches the result of calling {@link partisan_remote_ref:from_term/1} in
%% the process dictionary the first time and retrieves the value in subsequent
%% calls.
%% <blockquote class="warning">
%% <h4 class="warning">NOTICE</h4>
%% <p>You SHOULD avoid using this function in the Erlang Shell. This is because
%% when an Erlang Shell process crashes it will copy the contents of It's
%% dictionary to the new shell process and thus you will end up with the wrong
%% partisan remote reference.
%% </p>
%% </blockquote>
%% @end
%% -----------------------------------------------------------------------------
-spec self(Opts :: [cache]) -> remote_pid().

self([]) ->
    partisan_remote_ref:from_term(erlang:self());

self([cache]) ->
    Key = {?MODULE, ?FUNCTION_NAME},

    case get(Key) of
        undefined ->
            Ref = partisan_remote_ref:from_term(erlang:self()),
            _ = put(Key, Ref),
            Ref;
        Ref ->
            Ref
    end.


%% -----------------------------------------------------------------------------
%% @deprecated Use monitor/2 instead.
%% @doc
%% @end
%% -----------------------------------------------------------------------------
monitor(Term) ->
    monitor(process, Term).


%% -----------------------------------------------------------------------------
%% @doc Sends a monitor request of type `Type' to the entity identified by
%% `Item'. If the monitored entity does not exist or it changes monitored
%% state, the caller of `monitor/2' is notified by a message on the following
%% format:
%% `{Tag, MonitorRef, Type, Object, Info}'
%%
%% This is the Partisan's equivalent to {@link erlang:monitor/2}.
%%
%% Failure: `notalive' if the `partisan_monitor' server is not alive.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor
    (process, pid() | atom() | {atom(), node()}) ->
        reference();
    (process, remote_pid() | remote_name()) ->
        remote_reference() | no_return();
    (port, port() | atom()) ->
        reference() |  no_return();
    (time_offset, clock_service) ->
        reference() | no_return().

%% Dialyzer does not support overloaded contracts
-dialyzer([{nowarn_function, monitor/2}]).

monitor(Type, Item) ->
    monitor(Type, Item, []).


%% -----------------------------------------------------------------------------
%% @doc Sends a monitor request of type `Type' to the entity identified by
%% `Item'. If the monitored entity does not exist or it changes monitored
%% state, the caller of `monitor/2' is notified by a message on the following
%% format:
%%
%% `{Tag, MonitorRef, Type, Object, Info}'
%%
%% where it differs from the message sent by {@link erlang:monitor/3} only when
%% the item being monitored is a remote process identifier
%% {@link erlang:monitor/3} in which case:
%% <ul>
%% <li>`MonitorRef' is a {@link remote_reference()}</li>
%% <li>`Object' is a {@link remote_pid()} or {@link
%% remote_name()}</li>
%% </ul>
%%
%% This is the Partisan's equivalent to {@link erlang:monitor/2}. It differs
%% from the Erlang implementation only when monitoring a `process'. For all
%% other cases (monitoring a `port' or `time_offset') this function calls
%% `erlang:monitor/2'.
%%
%% As opposed to {@link erlang:monitor/2} this function does not support
%% aliases.
%%
%% === Monitoring a `process` ===
%% Creates monitor between the current process and another process identified
%% by Item, which can be a `pid()' (local or remote), an atom `RegisteredName'
%% or a tuple `{RegisteredName, Node}'' for a registered process, located
%% elsewhere.
%%
%% In the case of a local `pid()' or a remote `pid()'
%% A process monitor by name resolves the `RegisteredName' to `pid()' or
%% `port()' only once at the moment of monitor instantiation, later changes to
%% the name registration will not affect the existing monitor.
%%
%% Failure: if the `partisan_monitor' server is not alive, a reference will be
%% returned with an immediate 'DOWN' signal with reason `notalive'.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor
    (process, pid() | atom(), [monitor_opt()]) ->
        reference();
    (port, port() | atom(), [erlang:monitor_option()]) ->
        reference();
    (time_offset, clock_service, [erlang:monitor_option()]) ->
        reference();
    (process, {atom(), node()}, [monitor_opt()]) ->
        reference() | remote_reference();
    (process, remote_pid(), [monitor_opt()]) ->
        remote_reference();
    (process, remote_name(), [monitor_opt()]) ->
        remote_reference().

%% Dialyzer does not support overloaded contracts
-dialyzer([{nowarn_function, monitor/3}]).

monitor(process, RegisteredName, Opts) when is_atom(RegisteredName) ->
    erlang:monitor(process, RegisteredName, to_erl_monitor_opts(Opts));

monitor(process, Pid, Opts) when erlang:is_pid(Pid) ->
    erlang:monitor(process, Pid, to_erl_monitor_opts(Opts));

monitor(process, {RegisteredName, Node}, Opts)
when is_atom(RegisteredName) ->
    case partisan_config:get(connect_disterl) orelse partisan:node() == Node of
        true ->
            erlang:monitor(process, RegisteredName, to_erl_monitor_opts(Opts));
        false ->
            Ref = partisan_remote_ref:from_term(RegisteredName, Node),
            partisan_monitor:monitor(Ref, Opts)
    end;

monitor(process, Term, Opts) when erlang:is_pid(Term) orelse is_atom(Term) ->
    erlang:monitor(process, Term, to_erl_monitor_opts(Opts));

monitor(process, RemoteRef, Opts) ->
    %% eqwalizer:ignore RemoteRef
    partisan_monitor:monitor(RemoteRef, Opts);

monitor(Type, Term, Opts) when Type == port orelse Type == time_offset ->
    erlang:monitor(Type, Term, to_erl_monitor_opts(Opts)).


%% -----------------------------------------------------------------------------
%% @doc
%%
%% It differs from {@link erlang:demonitor/1} in that it doesn't fail if
%% `MonitorRef' refers to a monitoring started by another process.
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(MonitorRef :: reference() | remote_reference()) -> true.

demonitor(Ref) ->
    _ = demonitor(Ref, []),
    true.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(
    MonitorRef :: reference() | remote_reference(),
    OptionList :: [demonitor_opt()]) -> boolean().

demonitor(Ref, Opts) when erlang:is_reference(Ref) ->
    erlang:demonitor(Ref, Opts);

demonitor(Ref, Opts) ->
    %% partisan_monitor:demonitor will raise a badarg if Ref is not valid
    partisan_monitor:demonitor(Ref, Opts).


%% -----------------------------------------------------------------------------
%% @doc Monitor the status of the node `Node'. If Flag is true, monitoring is
%% turned on. If `Flag' is `false', monitoring is turned off.
%%
%% Making several calls to `monitor_node(Node, true)' for the same `Node' from
%% is not an error; it results in as many independent monitoring instances as
%% the number of different calling processes i.e. If a process has made two
%% calls to `monitor_node(Node, true)' and `Node' terminates, only one
%% `nodedown' message is delivered to the process (this differs from {@link
%% erlang:monitor_node/2}).
%%
%% If `Node' fails or does not exist, the message `{nodedown, Node}' is
%% delivered to the calling process. If there is no connection to Node, a
%% `nodedown' message is delivered. As a result when using a membership
%% strategy that uses a partial view, you can not monitor nodes that are not
%% members of the view.
%%
%% If `Node' is the caller's node, the function returns `false'.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_node(node() | node_spec(), boolean()) -> boolean().

monitor_node(#{name := Node}, Flag) ->
    monitor_node(Node, Flag, []);

monitor_node(Node, Flag) ->
    monitor_node(Node, Flag, []).


%% -----------------------------------------------------------------------------
%% @doc Monitor the status of the node `Node'. If Flag is true, monitoring is
%% turned on. If `Flag' is `false', monitoring is turned off.
%%
%% Making several calls to `monitor_node(Node, true)' for the same `Node' from
%% is not an error; it results in as many independent monitoring instances as
%% the number of different calling processes i.e. If a process has made two
%% calls to `monitor_node(Node, true)' and `Node' terminates, only one
%% `nodedown' message is delivered to the process (this differs from {@link
%% erlang:monitor_node/2}).
%%
%% If `Node' fails or does not exist, the message `{nodedown, Node}' is
%% delivered to the calling process. If there is no connection to Node, a
%% `nodedown' message is delivered. As a result when using a membership
%% strategy that uses a partial view, you can not monitor nodes that are not
%% members of the view.
%%
%% If `Node' is the caller's node, the function returns `false'.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_node(
    Node :: node(),
    Flag :: boolean(),
    Options :: [allow_passive_connect]) -> true.

monitor_node(Node, Flag, Opts) ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            erlang:monitor_node(Node, Flag, Opts);
        false ->
            %% No opts for partisan_monitor
            partisan_monitor:monitor_node(Node, Flag)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_nodes(Flag :: boolean()) -> ok | error | {error, term()}.

monitor_nodes(Flag) ->
    monitor_nodes(Flag, []).


%% -----------------------------------------------------------------------------
%% @doc The calling process subscribes or unsubscribes to node status change
%% messages. A nodeup message is delivered to all subscribing processes when a
%% new node is connected, and a nodedown message is delivered when a node is
%% disconnected.
%% @end
%% -----------------------------------------------------------------------------
-spec monitor_nodes(Flag :: boolean(), [monitor_nodes_opt()]) ->
    ok | error | {error, term()}.

monitor_nodes(Flag, Opts) ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            NKOpts = to_net_kernel_opts(Opts),
            net_kernel:monitor_nodes(Flag, NKOpts);
        false ->
            partisan_monitor:monitor_nodes(Flag, Opts)
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_local(Arg) -> Result when
    Arg :: pid() | port() | reference()
            | remote_pid()
            | remote_reference(),
    Result :: boolean().

is_local(Arg) ->
    Node = node(Arg),
    Node == 'nonode@nohost' orelse Node =:= node().


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local_name(Arg :: atom() | remote_name()) ->
    boolean() | no_return().

is_local_name(Arg) when is_atom(Arg) ->
    true;

is_local_name(Arg) ->
    partisan_remote_ref:is_local_name(Arg).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local_name(
    Arg :: atom() | remote_name(), Name :: atom()) ->
    boolean() | no_return().

is_local_name(Name, Name) when is_atom(Name) ->
    true;

is_local_name(Arg, Name) when is_atom(Name) ->
    partisan_remote_ref:is_local_name(Arg, Name);

is_local_name(Arg, _) ->
    is_local_name(Arg) orelse error(badarg),
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local_pid(Arg :: pid() | remote_pid()) ->
    boolean() | no_return().


is_local_pid(Pid) when erlang:is_pid(Pid) ->
    is_local(Pid);

is_local_pid(Arg) ->
    partisan_remote_ref:is_local_pid(Arg).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local_pid(
    Arg :: pid() | remote_pid(), Pid :: pid()) ->
    boolean() | no_return().

is_local_pid(Pid, Pid) when erlang:is_pid(Pid) ->
    is_local(Pid);

is_local_pid(Arg, Pid) when erlang:is_pid(Pid) ->
    partisan_remote_ref:is_local_pid(Arg, Pid);

is_local_pid(Arg, _) ->
    is_local_pid(Arg) orelse error(badarg),
    false.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local_reference(Arg :: reference() | remote_reference()) ->
    boolean() | no_return().

is_local_reference(Ref) when erlang:is_reference(Ref) ->
    is_local(Ref);

is_local_reference(Arg) ->
    partisan_remote_ref:is_local_reference(Arg).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_local_reference(
    Arg :: reference() | remote_reference(), LocalRef :: reference()) ->
    boolean() | no_return().

is_local_reference(Ref, Ref) when erlang:is_reference(Ref) ->
    is_local(Ref);

is_local_reference(Arg, Ref) when erlang:is_reference(Ref) ->
    partisan_remote_ref:is_local_reference(Arg, Ref);

is_local_reference(Arg, _) ->
    is_local_reference(Arg) orelse error(badarg),
    false.


%% -----------------------------------------------------------------------------
%% @doc Returns true if `Arg' is the caller's process identifier.
%% @end
%% -----------------------------------------------------------------------------
-spec is_self(Arg) -> Result when
    Arg :: pid() | remote_pid(),
    Result :: boolean().

is_self(Arg) when erlang:is_pid(Arg) ->
    Arg =:= erlang:self();

is_self(Arg) ->
    partisan_remote_ref:is_local_pid(Arg, erlang:self()).


%% -----------------------------------------------------------------------------
%% @doc Shortcut for `partisan_peer_service:join/1'.
%% @end
%% -----------------------------------------------------------------------------
-spec join(node_spec()) -> ok.

join(NodeSpec) ->
    partisan_peer_service:join(NodeSpec).


%% -----------------------------------------------------------------------------
%% @doc Shortcut for `partisan_peer_service:leave/0'.
%% @end
%% -----------------------------------------------------------------------------
-spec leave() -> ok.

leave() ->
    partisan_peer_service:leave().


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
kill_connections(Node) when is_atom(Node) ->
    kill_connections([Node]);

kill_connections(Nodes) when is_list(Nodes) ->
    partisan_peer_service_manager:disconnect(Nodes).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec node() -> node().

node() ->
    partisan_config:get(name).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node as a binary string.
%% @end
%% -----------------------------------------------------------------------------
-spec nodestring() -> binary().

nodestring() ->
    partisan_config:get(nodestring).


%% -----------------------------------------------------------------------------
%% @doc Returns the node where Arg originates. Arg can be a process identifier,
%% a reference, a port or a partisan remote reference.
%% @end
%% -----------------------------------------------------------------------------
-spec node
    (pid() | port() | reference()) -> node();
    (partisan_remote_ref:t()) -> node() | no_return().

node(Arg)
when erlang:is_pid(Arg) orelse erlang:is_reference(Arg) orelse is_port(Arg) ->
    Node = erlang:node(Arg),

    case partisan_config:get(connect_disterl) of
        true ->
            %% If node is down we will get 'nonode@nohost' and we should return
            %% this value, even if partisan:node() has been set
            Node;
        false ->
            case Node of
                'nonode@nohost' ->
                    %% Return the partisan node
                    node();
                Other ->
                    %% This is the case when the use has assigned a nodename
                    %% via vm.args but disabled erlang distribution
                    %% In this case erlang:node() == partisan:node()
                    Other
            end
    end;

node(Arg) ->
    %% eqwalizer:ignore We assume this is a partisan_remote_ref:t()
    partisan_remote_ref:node(Arg).


%% -----------------------------------------------------------------------------
%% @doc Returns a list of all nodes connected to this node via Partisan.
%% Equivalent to {@link erlang:nodes/1}.
%% Sames as calling `nodes(visible)'.
%%
%% Notice that if `connect_disterl' is `true' (possibly the case when testing),
%% this function will NOT return the disterl nodes. For that you still need to
%% call {@link erlang:nodes/1}.
%% @end
%% -----------------------------------------------------------------------------
-spec nodes() -> [node()].

nodes() ->
    nodes(visible).


%% -----------------------------------------------------------------------------
%% @doc Returns a list of nodes according to the argument specified. The
%% returned result, when the argument is a list, is the list of nodes
%% satisfying the disjunction(s) of the list elements.
%%
%% Differences with Erlang:
%% <ul>
%% <li>`hidden' - always returns the empty list (there is no concept of hidden
%% nodes in Partisan).</li>
%% <li>`this' - returns the list containing the result of calling
%% {@link node/0}</li>
%% <li>`known' - will return the nodes known by the {@link
%% partisan_peer_service} i.e. {@link partisan_peer_service:members/0} but not
%% the nodes referred to by process identifiers,
%% port identifiers, and references located on this node.</li>
%% <li>`visible' - equivalent to Erlang.</li>
%% </ul>
%% @end
%% -----------------------------------------------------------------------------
-spec nodes(Arg :: node_type() | [node_type()]) -> [node()].

nodes(Arg) ->
    case partisan_config:get(connect_disterl) of
        true ->
            erlang:nodes(Arg);
        false when Arg == hidden ->
            [];
        false when Arg == this ->
            [node()];
        false when Arg == known ->
            {ok, Nodes} = partisan_peer_service:members(),
            Nodes;
        false when Arg == visible ->
            partisan_peer_connections:nodes();
        false when is_list(Arg) ->
            L = lists:foldl(fun(X, Acc) -> [nodes(X) | Acc] end, [], Arg),
            lists:flatten(L)
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_connected(NodeOrSpec :: node_spec() | node()) -> boolean().

is_connected(NodeOrSpec) ->
    partisan_peer_connections:is_connected(NodeOrSpec).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_connected(NodeOrSpec :: node_spec() | node(), Channel :: channel()) ->
    boolean().

is_connected(NodeOrSpec, Channel) ->
    partisan_peer_connections:is_connected(NodeOrSpec, Channel).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_fully_connected(NodeOrSpec :: node_spec() | node()) -> boolean().

is_fully_connected(NodeOrSpec) ->
    partisan_peer_connections:is_fully_connected(NodeOrSpec).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec disconnect_node(Node :: node()) -> boolean() | ignored.

disconnect_node(Node) ->
    case Node == node() of
        true ->
            ok = partisan_peer_service:leave(),
            true;
        false ->
            try node_spec(Node) of
                {ok, NodeSpec} ->
                    ok = partisan_peer_service:leave(NodeSpec),
                    true;
                {error, _} ->
                    false
            catch
                _:_ ->
                    false
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns true if the local node is alive (that is, if the node can be
%% part of a distributed system), otherwise false.
%% @end
%% -----------------------------------------------------------------------------
-spec is_alive() -> boolean().

is_alive() ->
    undefined =/= erlang:whereis(?PEER_SERVICE_MANAGER).


%% -----------------------------------------------------------------------------
%% @doc
%% Fails with `badarg' if `Arg' is a remote reference for another node.
%% @end
%% -----------------------------------------------------------------------------
-spec whereis(Arg :: atom() | remote_name()) ->
    pid() | port() | undefined.

whereis(Arg) when is_atom(Arg) ->
    erlang:whereis(Arg);

whereis(Arg) ->
    case partisan_remote_ref:to_term(Arg) of
        Ref when is_atom(Ref) ->
            erlang:whereis(Ref);
        _ ->
            error(badarg)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec process_info(Arg :: pid() | remote_pid()) ->
    [tuple()] | undefined.

process_info(Arg) when erlang:is_pid(Arg) ->
    erlang:process_info(Arg);

process_info(Arg) ->
    try partisan_remote_ref:to_term(Arg) of
        Term when erlang:is_pid(Term) ->
            erlang:process_info(Term);
        _ ->
            throw(badarg)
    catch
        _:_ ->
            error(badarg)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%%
%% -----------------------------------------------------------------------------
-spec process_info(
    Arg :: pid() | remote_pid(),
    Item :: atom() | [atom()]) -> [tuple()] | undefined.

process_info(Arg, ItemOrItems) when erlang:is_pid(Arg) ->
    erlang:process_info(Arg, ItemOrItems);

process_info(Arg, ItemOrItems) ->
    try partisan_remote_ref:to_term(Arg) of
        Term when erlang:is_pid(Term) ->
            erlang:process_info(Term, ItemOrItems);
        _ ->
            throw(badarg)
    catch
        _:_ ->
            error(badarg)
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the node specification of the local node.
%% This is the information required when other nodes wish to join this node
%% (See {@link partisan_peer_service:join/1}).
%%
%% Notice that the values of the keys must be sorted for the peer service to be
%% able to compare node specifications, and prevent duplicates in the
%% membership view data structure. This is important in case you find
%% yourself building this representation manually in order to implement a
%% particular orchestration strategy. As Erlang maps are naturally sorted, the
%% only property that you need to keep sorted is `listen_addrs' as it is
%% implemented as a list.
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec() -> node_spec().

node_spec() ->
    %% Channels and ListenAddrs are sorted already
    #{
        name => node(),
        listen_addrs => partisan_config:get(listen_addrs),
        channels => partisan_config:get(channels)
    }.


%% -----------------------------------------------------------------------------
%% @doc Return the partisan node_spec() for node named `Node'.
%%
%% This function retrieves the `node_spec()' from the remote node using RPC and
%% returns `{error, Reason}' if the RPC fails. Otherwise, assumes the node is
%% running on the same host and returns a `node_spec()' with with nodename
%% `Name' and host 'Host' and same metadata as `myself/0'.
%%
%% If configuration option `connect_disterl' is `true', the RPC will be
%% implemented using the `rpc' module. Otherwise it will use `partisan_rpc'.
%%
%% You should only use this function when distributed erlang is enabled
%% (configuration option `connect_disterl' is `true') or if the node is running
%% on the same host and you are using this for testing purposes as there is no
%% much sense in running a partisan cluster on a single host.
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec(node()) -> {ok, node_spec()} | {error, Reason :: any()}.

node_spec(Node) when is_atom(Node) ->
    node_spec(Node, #{}).


%% -----------------------------------------------------------------------------
%% @doc Return the tuple `{ok, node_spec()' for node named `Node' or the tuple
%% `{error, Reason}'.
%%
%% This function first checks If there is a partisan connection to `Node', if
%% so returns the cached specification that was used for creating the
%% connection. If no connection is present (the case for a p2p topology), then
%% it tries to use @@link partisan_rpc} to retrieve the node specification from
%% the remote node. This later alternative requires the partisan configuration
%% `forward_opts` to have `broadcast' and `transitive' enabled.
%%
%% NOTICE: At the moment partisan_rpc might not work correctly w/ a p2p
%% topology.
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec(
    Node :: binary() | list() | node(),
    Opts :: #{rpc_timeout => timeout()}) ->
    {ok, node_spec()} | {error, Reason :: any()}.

node_spec(Node, Opts) when is_binary(Node) ->
    node_spec(binary_to_atom(Node), Opts);

node_spec(Node, Opts) when is_list(Node) ->
    node_spec(list_to_atom(lists:flatten(Node)), Opts);

node_spec(Node, Opts) when is_atom(Node), is_map(Opts) ->
    Timeout = maps:get(rpc_timeout, Opts, 5000),

    case partisan:node() of
        Node ->
            {ok, partisan:node_spec()};

        _ ->
            case is_connected(Node) of
                true ->
                    {ok, Info} = partisan_peer_connections:info(Node),
                    {ok, partisan_peer_connections:node_spec(Info)};

                false ->
                    M = ?MODULE,
                    F = node_spec,
                    A = [],
                    case partisan_rpc:call(Node, M, F, A, Timeout) of
                        #{name := Node} = Spec ->
                            %% eqwalizer:ignore
                            {ok, Spec};
                        {badrpc, Reason} ->
                            {error, Reason}
                    end
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node_info() -> node_info().

node_info() ->
    node_info([]).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec node_info([info_opt()]) -> map().

node_info([]) ->
    #{};

node_info(L) when is_list(L) ->
    node_info(L, #{}).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec default_channel() -> channel().

default_channel() ->
    ?DEFAULT_CHANNEL.


%% -----------------------------------------------------------------------------
%% @doc Returns a channel with name `Name'.
%% Fails if a channel named `Name' doesn't exist.
%% @end
%% -----------------------------------------------------------------------------
-spec channel_opts(Channel :: channel()) -> channel_opts() | no_return().

channel_opts(Channel) when is_atom(Channel) ->
    partisan_config:channel_opts(Channel).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_pid(any()) ->
    boolean() | no_return().

is_pid(Arg) when erlang:is_pid(Arg) ->
    true;

is_pid(Arg) ->
    partisan_remote_ref:is_pid(Arg).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_reference(reference() | remote_reference()) ->
    boolean() | no_return().

is_reference(Arg) when erlang:is_reference(Arg) ->
    true;

is_reference(Arg) ->
    partisan_remote_ref:is_reference(Arg).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec is_process_alive(pid() | remote_pid()) ->
    boolean() | no_return().

is_process_alive(Pid) when erlang:is_pid(Pid) ->
    erlang:is_process_alive(Pid);

is_process_alive(RemoteRef) ->
    case partisan_remote_ref:is_local_pid(RemoteRef) of
        true ->
            %% eqwalizer:ignore It's local so this call won't fail
            erlang:is_process_alive(partisan_remote_ref:to_term(RemoteRef));
        false ->
            Node = node(RemoteRef),
            Res = partisan_rpc:call(
                Node, ?MODULE, is_process_alive, [RemoteRef], 5000
            ),
            case Res of
                {badrpc, Reason} ->
                    error(Reason);
                Bool when is_boolean(Bool) ->
                    Bool
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec exit(Pid :: pid() | remote_pid(), Reason :: term()) -> true.

exit(Pid, Reason) when erlang:is_pid(Pid) ->
    erlang:exit(Pid, Reason);

exit(RemoteRef, Reason) ->
    try partisan_remote_ref:to_term(RemoteRef) of
        Pid when erlang:is_pid(Pid) ->
            erlang:exit(Pid, Reason);
        Name when is_atom(Name) ->
            %% eqwalizer:ignore We know it's a pid
            erlang:exit(whereis(Name), Reason)
    catch
        error:badarg ->
            %% Not local
            Node = node(RemoteRef),
            Res =
                partisan_rpc:call(
                    Node, ?MODULE, exit, [RemoteRef, Reason], 5000
                ),
            case Res of
                {badrpc, _} ->
                    ?LOG_WARNING(#{
                        description => "The call might have failed",
                        reason => Res,
                        remote_ref => RemoteRef
                    }),
                    %% The call should not fail
                    true;
                true ->
                    true
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec send(Dest :: send_dst(), Msg :: message()) -> message().

send(Dest, Msg) ->
    ok = send(Dest, Msg, []),
    Msg.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec send(Dest :: send_dst(), Msg :: message(), Opts :: forward_opts()) ->
    ok | nosuspend | noconnect.

send(Dest, Msg, Opts) ->
    case partisan_config:get(connect_disterl) of
        true ->
            %% eqwalizer:ignore
            erlang:send(Dest, Msg, to_erl_send_opts(Opts));
        false ->
            %% eqwalizer:ignore
            forward_message(Dest, Msg, Opts)
    end.


%% -----------------------------------------------------------------------------
%% @doc Equivalent to calling `send_after(Time, Dest, Msg, [])'.
%% @end
%% -----------------------------------------------------------------------------
-spec send_after(
    Time :: time(),
    Destination :: send_after_dst(),
    Msg :: message()) -> TRef :: reference().

send_after(Time, Dest, Msg) ->
    send_after(Time, Dest, Msg, []).


%% -----------------------------------------------------------------------------
%% @doc Equivalent to the native `erlang:send_after/4'.
%% It calls the native implementation for local destinations. For remote
%% destinations it spawns a process that uses a timer and accepts cancellation (
%% via `cancel_timer/1,2'), so this is less efficient than the native
%% implementation.
%% @end
%% -----------------------------------------------------------------------------
-spec send_after(
    Time :: time(),
    Destination :: send_after_dst(),
    Message :: message(),
    Opts :: send_after_opts()) -> TRef :: reference().

send_after(Time, Dest, Msg, Opts0)
when ?IS_VALID_TIME(Time) andalso is_list(Opts0) andalso
(
    (erlang:is_pid(Dest) andalso erlang:node(Dest) == erlang:node())
    orelse is_atom(Dest)
) ->
    %% Local send
    Opts = to_erl_send_after_opts(Opts0),
    erlang:send_after(Time, Dest, Msg, Opts);


send_after(Time, {RegName, Node}, Msg, Opts0)
when ?IS_VALID_TIME(Time) andalso
is_list(Opts0) andalso
is_atom(RegName) andalso
is_atom(Node) andalso
Node == erlang:node() ->
    %% Local send
    Opts = to_erl_send_after_opts(Opts0),
    erlang:send_after(Time, RegName, Msg, Opts);

send_after(Time, Dest, Msg, Opts)
when ?IS_VALID_TIME(Time) andalso is_list(Opts) ->
    case partisan_remote_ref:is_type(Dest) of
        true ->
            Caller = erlang:self(),
            Pid = spawn(
                fun() ->
                    %% Setup an alias which the receiver can use to send us an
                    %% cancel message
                    Alias = alias(),
                    Me = erlang:self(),

                    Caller ! {send_after_init, Me, Alias},

                    %% Set the timer and wait for the timeout or cancel message
                    TimerRef = erlang:start_timer(Time, Me, send),

                    try
                        receive
                            {timeout, TimerRef, send} ->
                                catch partisan:send(Dest, Msg, Opts),
                                ok;

                            {cancel, Alias, Pid, Info} ->
                                CancelOpts = [{info, Info}],
                                Result = erlang:cancel_timer(
                                    TimerRef, CancelOpts
                                ),
                                case partisan_util:get(info, Opts, true) of
                                    true ->
                                        Canceled = {
                                            send_after_canceled, Alias, Result
                                        },
                                        Pid ! Canceled;
                                    false ->
                                        ok
                                end
                        end
                    catch
                        _:_ ->
                            ok
                    after
                        unalias(Alias)
                    end

                end
            ),

            receive
                {send_after_init, Pid, Ref} ->
                    Ref
            after
                3000 ->
                    error(timeout)
            end;

        false ->
            Info = #{
                cause => #{
                    2 =>
                        "should be a local pid, local registered name, "
                        "a tuple containing registered name and node, "
                        "or a partisan_remote_ref for a remote pid or "
                        "registered name."
                }
            },
            erlang:error(
                badarg, [Time, Dest, Msg, Opts], [{error_info, Info}]
            )
    end;

send_after(Time, Dest, Msg, Opts) when not ?IS_VALID_TIME(Time) ->
    Info = #{cause => #{1 => "should be a pos_integer"}},
    erlang:error(
        badarg, [Time, Dest, Msg, Opts], [{error_info, Info}]
    );

send_after(Time, Dest, Msg, Opts) when not is_list(Opts) ->
    Info = #{cause => #{4 => "should be a list"}},
    erlang:error(
        badarg, [Time, Dest, Msg, Opts], [{error_info, Info}]
    ).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec cancel_timer(Ref :: reference()) ->
    ok | time() | false.

cancel_timer(Ref) ->
    cancel_timer(Ref, []).


-spec cancel_timer(Ref :: reference(), Opts :: list()) ->
    ok | time() | false.

cancel_timer(Ref, Opts) when erlang:is_reference(Ref), is_list(Opts) ->
    Me = erlang:self(),

    case partisan_util:get(async, Opts, false) of
        true ->
            _ = spawn(fun() -> do_cancel_timer(Ref, Opts, Me) end),
            ok;

        false ->
            do_cancel_timer(Ref, Opts, Me)
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec spawn(Node :: node(), Fun :: fun(() -> any())) -> remote_pid().

spawn(Node, Fun) ->
    case Node == node() of
        true ->
            Pid = erlang:spawn(Fun),
            %% eqwalizer:ignore This MUST be a pid()
            partisan_remote_ref:from_term(Pid);
        false ->
            case partisan_rpc:call(Node, erlang, spawn, [Fun], 5000) of
                {badrpc, Reason} ->
                    ?LOG_WARNING(#{
                        description =>
                            "Can not start erlang:apply/1 on remote node",
                        node => Node
                    }),
                    Pid = erlang:spawn(
                        fun() ->
                            erlang:exit(process_exit_reason(Reason))
                        end
                    ),
                    %% eqwalizer:ignore It's a pid so this call won't fail
                    partisan_remote_ref:from_term(Pid);

                Pid when erlang:is_pid(Pid) ->
                    %% eqwalizer:ignore It's a pid so this call won't fail
                    partisan_remote_ref:from_term(Pid, Node);

                Encoded ->
                    %% eqwalizer:ignore This MUST be remote_pid()
                    Encoded
            end
    end.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec spawn(
    Node :: node(), Mod :: module(), Function :: atom(), Args :: [term()]) -> remote_pid().

spawn(Node, Module, Function, Args) ->
    case Node == node() of
        true ->
            Pid = erlang:spawn(Module, Function, Args),
            %% eqwalizer:ignore It's a pid so this call won't fail
            partisan_remote_ref:from_term(Pid);
        false ->
            SpawnArgs = [Module, Function, Args],
            %% We call ourselves in Node and not erlang module, as we need
            %% a remote_pid() even when pid_encoding is disabled
            case partisan_rpc:call(Node, erlang, spawn, SpawnArgs, 5000) of
                {badrpc, Reason} ->
                    ?LOG_WARNING(#{
                        description =>
                            "Can not start erlang:apply/1 on remote node",
                        node => Node
                    }),
                    Pid = erlang:spawn(
                        fun() ->
                            erlang:exit(process_exit_reason(Reason))
                        end
                    ),
                    %% eqwalizer:ignore It's a pid so this call won't fail
                    partisan_remote_ref:from_term(Pid);

                EncodedPid ->
                    %% eqwalizer:ignore we know this is correct
                    EncodedPid
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec spawn_monitor(Node :: node(), Fun :: fun(() -> any())) ->
    {remote_pid(), remote_reference()}
    | {pid(), reference()}.

spawn_monitor(Node, Fun) ->
    Pid = spawn(Node, Fun),
    Ref = monitor(process, Pid),
    {Pid, Ref}.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec spawn_monitor(
    Node :: node(), Mod :: module(), Function :: atom(), Args :: [term()]) ->
    {remote_pid(), remote_reference()}
    | {pid(), reference()}.

spawn_monitor(Node, Module, Function, Args) ->
    Pid = spawn(Node, Module, Function, Args),
    Ref = monitor(process, Pid),
    {Pid, Ref}.



%% -----------------------------------------------------------------------------
%% @doc Cast message to a remote ref
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    ServerRef :: server_ref(),
    Msg :: message()) -> ok.

cast_message(Term, Message) ->
    ?PEER_SERVICE_MANAGER:cast_message(Term, Message).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()) -> ok.

cast_message(ServerRef, Msg, Opts) ->
    ?PEER_SERVICE_MANAGER:cast_message(ServerRef, Msg, Opts).


%% -----------------------------------------------------------------------------
%% @doc Cast message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec cast_message(
    Node :: node(),
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()) -> ok.

cast_message(Node, ServerRef, Message, Options) ->
    ?PEER_SERVICE_MANAGER:cast_message(Node, ServerRef, Message, Options).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(
    ServerRef :: server_ref(),
    Msg :: message()) -> ok.

forward_message(ServerRef, Message) ->
    ?PEER_SERVICE_MANAGER:forward_message(ServerRef, Message).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()) -> ok.

forward_message(ServerRef, Message, Opts) ->
    ?PEER_SERVICE_MANAGER:forward_message(ServerRef, Message, Opts).


%% -----------------------------------------------------------------------------
%% @doc Forward message to registered process on the remote side.
%% @end
%% -----------------------------------------------------------------------------
-spec forward_message(
    Node :: node(),
    ServerRef :: server_ref(),
    Msg :: message(),
    Opts :: forward_opts()) -> ok.

forward_message(Node, ServerRef, Message, Opts) ->
    ?PEER_SERVICE_MANAGER:forward_message(Node, ServerRef, Message, Opts).


%% -----------------------------------------------------------------------------
%% @doc Broadcasts a message originating from this node.
%%
%% The message will be delivered to each node at least once. The `Mod' passed
%% is responsible for handling the message on remote nodes as well as providing
%% some other information both locally and and on other nodes.
%% `Mod' must be loaded on all members of the clusters and implement the
%% `partisan_plumtree_broadcast_handler' behaviour.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast(any(), module()) -> ok.

broadcast(Broadcast, Mod) ->
    partisan_plumtree_broadcast:broadcast(Broadcast, Mod).



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
node_info(L, Acc) ->
    node_info(L, Acc, node_spec()).


%% @private
node_info([name|T], Acc0, #{name := Val} = Spec) ->
    Acc1 = Acc0#{name => Val},
    node_info(T, Acc1, Spec);

node_info([listen_addrs|T], Acc0, #{listen_addrs := Val} = Spec) ->
    Acc1 = Acc0#{listen_addrs => Val},
    node_info(T, Acc1, Spec);

node_info([channels|T], Acc0, #{channels := Val} = Spec) ->
    Acc1 = Acc0#{channels => Val},
    node_info(T, Acc1, Spec);

node_info([listen_ip|T], Acc0, #{listen_addrs := [#{ip := Val} | _]} = Spec) ->
    Acc1 = Acc0#{listen_ip => Val},
    node_info(T, Acc1, Spec);

node_info([listen_port|T], Acc0, #{listen_addrs := [#{port := Val} | _]} = Spec) ->
    Acc1 = Acc0#{listen_port => Val},
    node_info(T, Acc1, Spec);

node_info([metadata|T], Acc0, Spec) ->
    Default = #{},
    Acc1 = Acc0#{metadata => partisan_config:get(metadata, Default)},
    node_info(T, Acc1, Spec);

node_info([connection_count|T], Acc0, Spec) ->
    Acc1 = Acc0#{connection_count => partisan_peer_connections:count()},
    node_info(T, Acc1, Spec);

node_info([_|T], Acc, Spec) ->
    node_info(T, Acc, Spec);

node_info([], Acc, _) ->
    Acc.


%% @private
-spec to_erl_send_opts([tuple()]) -> [nosuspend | noconnect | tuple()].

to_erl_send_opts(Opts) ->
    to_erl_opts(Opts).


%% @private
%%  Returns [{abs, boolean()}].
-spec to_erl_send_after_opts([tuple()]) -> [tuple()].

to_erl_send_after_opts(Opts) ->
    to_erl_opts(Opts).


%% @private
-spec to_erl_monitor_opts(list()) -> [erlang:monitor_option()].

to_erl_monitor_opts(Opts) ->
    to_erl_opts(Opts).


%% @private
to_erl_opts(Opts0) when is_list(Opts0) ->
    case lists:keytake(channel, 1, Opts0) of
        {value, _, Opts} ->
            Opts;
        false ->
            Opts0
    end.


%% @private
-spec to_net_kernel_opts([monitor_nodes_opt()]) -> [net_kernel_opt()].

to_net_kernel_opts(Opts0) when is_list(Opts0) ->
    case lists:keytake(channel, 1, Opts0) of
        {value, _, Opts1} ->
            case lists:keytake(channel_fallback, 1, Opts1) of
                {value, _, Opts} ->
                    Opts;
                false ->
                    Opts1
            end;
        false ->
            Opts0
    end.


%% @private
maybe_wait_for_cancel_timer_result(Ref, Info, undefined) ->
    maybe_wait_for_cancel_timer_result(Ref, Info, erlang:self());

maybe_wait_for_cancel_timer_result(Ref, Info, Pid) when erlang:is_pid(Pid) ->
    maybe_wait_for_cancel_timer_result(Ref, Info, Pid, Pid == erlang:self()).


%% @private
maybe_wait_for_cancel_timer_result(_, false, _, _) ->
    ok;

maybe_wait_for_cancel_timer_result(Ref, true, _, true) ->
    receive
        {send_after_canceled, Ref, Result} ->
            Result
    after
        500 ->
            false
    end;

maybe_wait_for_cancel_timer_result(Ref, true, Pid, _) ->
    receive
        {send_after_canceled, Ref, Result} ->
            Pid ! {cancel_timer, Ref, Result}
    after
        500 ->
            Pid ! {cancel_timer, Ref, false}
    end.


%% @private
do_cancel_timer(Ref, Opts, Pid) ->
    Me = erlang:self(),
    Info = partisan_util:get(info, Opts, true),

    case erlang:cancel_timer(Ref) of
        false ->
            %% Timer not found, maybe is our custom solution for send_after
            Info = partisan_util:get(info, Opts, true),
            Ref ! {cancel, Ref, Me, Info},
            maybe_wait_for_cancel_timer_result(Ref, Info, Pid);

        Result when Info == true, erlang:is_pid(Pid) ->
            Pid ! {cancel_timer, Ref, Result};

        Result when Info == true ->
            Result;

        _ ->
            ok
    end.


%% @private
process_exit_reason(disconnected) ->
    noconnection;
process_exit_reason(not_yet_connected) ->
    noconnection;
process_exit_reason(_) ->
    noproc.



