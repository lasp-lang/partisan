%% -------------------------------------------------------------------
%%
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

-module(partisan_peer_service_manager).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan_logger.hrl").
-include("partisan.hrl").


-type server_ref()  ::  partisan_remote_ref:p()
                        | process_ref()
                        | partisan_remote_ref:n()
                        | registered_name_ref()
                        | Name :: atom()
                        | {Name :: atom(), node()}
                        | {global, atom()}
                        | {via, module(), ViaName :: atom()}
                        | pid().

-type forward_opts()     ::  #{
                            ack => boolean(),
                            causal_label => atom(),
                            channel => channel(),
                            clock => any(),
                            partition_key => non_neg_integer(),
                            transitive => boolean()
                        }.

-export_type([forward_opts/0]).

-export([mynode/0]).
-export([myself/0]).
-export([process_forward/2]).
-export([send_message/2]).


%% =============================================================================
%% BEHAVIOUR CALLBACKS
%% =============================================================================



-callback start_link() -> {ok, pid()} | ignore | {error, term()}.

-callback members() -> [node()]. %% TODO: Deprecate me.

-callback members_for_orchestration() -> [node_spec()].

-callback myself() -> node_spec().

-callback update_members([node()]) -> ok | {error, not_implemented}.

-callback get_local_state() -> term().

-callback on_down(node(), function()) -> ok | {error, not_implemented}.

-callback on_up(node(), function()) -> ok | {error, not_implemented}.

-callback join(node_spec()) -> ok.

-callback sync_join(node_spec()) -> ok | {error, not_implemented}.

-callback leave() -> ok.

-callback leave(node_spec()) -> ok.

-callback send_message(node(), message()) -> ok.

-callback receive_message(node(), message()) -> ok.

% -callback cast_message(
%     partisan_remote_ref:p() | partisan_remote_ref:n() | pid(), message()) -> ok.

-callback cast_message(
    partisan_remote_ref:p() | partisan_remote_ref:n() | pid(),
    message(),
    forward_opts()) -> ok.

% -callback cast_message(node(), pid(), message()) -> ok.

-callback cast_message(node(), pid(), message(), forward_opts()) -> ok.

-callback forward_message(
    partisan_remote_ref:p() | partisan_remote_ref:n() | pid() | atom(),
    message()) -> ok.

-callback forward_message(
    partisan_remote_ref:p() | partisan_remote_ref:n() | pid() | atom(),
    message(),
    forward_opts()) -> ok.

% -callback forward_message(node(), pid(), message()) -> ok.

-callback forward_message(node(), server_ref(), message(), forward_opts()) -> ok.


-callback decode(term()) -> term().

-callback reserve(atom()) -> ok | {error, no_available_slots}.

-callback partitions() -> {ok, partitions()} | {error, not_implemented}.

-callback inject_partition(node_spec(), ttl()) ->
    {ok, reference()} | {error, not_implemented}.

-callback resolve_partition(reference()) -> ok | {error, not_implemented}.



%% =============================================================================
%% API
%% =============================================================================


%% -----------------------------------------------------------------------------
%% @doc Send a message to a remote peer_service_manager.
%% @end
%% -----------------------------------------------------------------------------
-spec send_message(node(), message()) -> ok.

send_message(Node, Message) ->
    (?PEER_SERVICE_MANAGER):send_message(Node, Message).


%% -----------------------------------------------------------------------------
%% @doc Internal function used by peer_service manager implementations to
%% forward a message to a  process identified by `ServerRef' that is either
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


%% @deprecated use partisan:node_spec/0 instead.
-spec myself() -> node_spec().

myself() ->
    partisan:node_spec().


%% @deprecated use partisan:node/0 instead.
-spec mynode() -> atom().

mynode() ->
    partisan:node().



%% =============================================================================
%% PRIVATE
%% =============================================================================



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

do_process_forward(ServerRef, Message)
when is_tuple(ServerRef) orelse is_binary(ServerRef) ->
    ?LOG_DEBUG(
        "node ~p recieved message ~p for ~p",
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
