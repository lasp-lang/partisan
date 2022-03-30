%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Helium Systems, Inc.  All Rights Reserved.
%% Copyright (c) 2016 Christopher Meiklejohn.  All Rights Reserved.
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

-module(partisan).

-include("partisan.hrl").

-export([start/0]).
-export([stop/0]).

-export([demonitor/1]).
-export([demonitor/2]).
-export([monitor/1]).
-export([monitor_node/2]).
-export([node/0]).
-export([node/1]).
-export([node_spec/0]).
-export([default_channel/0]).

-compile({no_auto_import, [monitor_node/2]}).
-compile({no_auto_import, [demonitor/2]}).
-compile({no_auto_import, [node/0]}).
-compile({no_auto_import, [node/1]}).



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
%% @doc when you attempt to monitor a partisan_remote_reference, it is not
%% guaranteed that you will receive the DOWN message. A few reasons for not
%% receiving the message are message loss, tree reconfiguration and the node
%% is no longer reachable.
%% @end
%% -----------------------------------------------------------------------------
monitor(Pid) when is_pid(Pid) ->
    erlang:monitor(process, Pid);

monitor({partisan_remote_reference, _, {partisan_process_reference, _}} = R) ->
    partisan_monitor:monitor(R).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(MonitorRef :: reference() | remote_ref(encoded_ref())) -> true.

demonitor(Ref) ->
    demonitor(Ref, []).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec demonitor(
    MonitorRef :: reference() | remote_ref(encoded_ref()),
    OptionList :: [flush | info]) -> boolean().

demonitor(MRef, Opts) when is_reference(MRef) ->
    erlang:demonitor(MRef, Opts);

demonitor(
    {partisan_remote_reference, _, {partisan_process_reference, _}} = R,
    Opts) ->
    partisan_monitor:demonitor(R, Opts).


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
-spec monitor_node(node() | node_spec(), boolean(), [flush | info]) ->
    boolean().

monitor_node(Node, Flag, Opts) ->
    partisan_monitor:monitor_node(Node, Flag, Opts).


%% -----------------------------------------------------------------------------
%% @doc Returns the name of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec node() -> node().

node() ->
    partisan_config:get(name, erlang:node()).


%% -----------------------------------------------------------------------------
%% @doc Returns the node where Arg originates. Arg can be a process identifier,
%% a reference, a port or a partisan remote refererence.
%% @end
%% -----------------------------------------------------------------------------
node(Arg) when is_pid(Arg) orelse is_reference(Arg) orelse is_port(Arg) ->
    erlang:node(Arg);

node({partisan_remote_reference, Node, _}) ->
    Node.


%% -----------------------------------------------------------------------------
%% @doc Returns the node specification of the local node.
%% @end
%% -----------------------------------------------------------------------------
-spec node_spec() -> node_spec().

node_spec() ->
    Parallelism = partisan_config:get(parallelism, ?PARALLELISM),
    Channels = partisan_config:get(channels, ?CHANNELS),
    Name = partisan_config:get(name),
    ListenAddrs = partisan_config:get(listen_addrs),

    #{
        name => Name,
        listen_addrs => ListenAddrs,
        channels => Channels,
        parallelism => Parallelism
    }.


default_channel() ->
    ?DEFAULT_CHANNEL.
