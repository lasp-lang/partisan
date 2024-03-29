%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Christopher S. Meiklejohn.  All Rights Reserved.
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
%% @author Christopher Meiklejohn <christopher.meiklejohn@gmail.com>
%% @copyright 2013 Christopher Meiklejohn.
%% @doc
%%
%% Ringleader parse transformation.
%%
%% First pass of a ringleader parse transformation to write send calls
%% to use ringleader.
%%

-module(partisan_transformed_module).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan_logger.hrl").

-export([local_send/1]).
-export([get_pid/0]).
-export([send_to_pid/2]).

-compile([{parse_transform, partisan_transform}]).




%% =============================================================================
%% API
%% =============================================================================



local_send(Message) ->
    Pid = self(),
    ?LOG_DEBUG("Local pid is: ~p", [Pid]),

    Pid ! Message,

    receive
        Message ->
            Message
    after
        1000 ->
            error
    end.


get_pid() ->
    self().


send_to_pid(Pid, Message) when is_pid(Pid) ->
    Pid ! Message;

send_to_pid(Ref, Message) ->
    Node = partisan_remote_ref:node(Ref),

    case Node == partisan:node() of
        true ->
            {encoded_pid, List} = partisan_remote_ref:target(Ref),
            send_to_pid(list_to_pid(List), Message);
        false ->
            partisan:forward_message(Ref, Message)
    end.