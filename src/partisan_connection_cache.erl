%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher S. Meiklejohn.  All Rights Reserved.
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

-module(partisan_connection_cache).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").


-include("partisan.hrl").
-include("partisan_logger.hrl").

-export([update/1,
         dispatch/1]).

-spec update(partisan_peer_service_connections:t()) -> partisan_peer_service_connections:t().
update(Connections) ->
    ets:delete_all_objects(?CACHE),

    dict:fold(fun(#{name := Name}, V, _AccIn) ->
                      true = ets:insert(?CACHE, [{Name, V}])
              end, [], Connections).

dispatch({forward_message, Name, Channel, _Clock, PartitionKey, ServerRef, Message, _Options}) ->
    ?LOG_TRACE(#{
        description => "Dispatching message",
        message => Message
    }),

    %% Find a connection for the remote node, if we have one.
    case ets:lookup(?CACHE, nodename(Name)) of
        [] ->
            %% Trap back to gen_server.
            ?LOG_INFO(#{
                description => "Connection cache miss for node",
                node => Name
            }),
            {error, trap};
        [{Name, []}] ->
            ?LOG_INFO(#{
                description => "Connection cache miss for node",
                node => Name
            }),
            {error, trap};
        [{Name, Pids}] ->
            Pid = partisan_util:dispatch_pid(PartitionKey, Channel, Pids),

            case partisan_config:get(tracing, ?TRACING) of
                true ->
                    ?LOG_TRACE(#{
                        description => "Dispatching message",
                        message => Message,
                        to => Pid
                    }),

                    case is_process_alive(Pid) of
                        true ->
                            ok;
                        false ->
                            ?LOG_TRACE(#{
                                description => "Dispatching message, process is NOT ALIVE",
                                message => Message,
                                to => Pid
                            })
                    end;
                false ->
                    ok
            end,

            gen_server:cast(
                Pid, {send_message, {forward_message, ServerRef, Message}}
            )
    end;

dispatch({forward_message, Name, ServerRef, Message, _Options}) ->
    ?LOG_TRACE(#{
        description => "Dispatching message",
        message => Message
    }),

    %% Find a connection for the remote node, if we have one.
    case ets:lookup(?CACHE, nodename(Name)) of
        [] ->
            %% Trap back to gen_server.
            ?LOG_INFO(#{
                description => "Connection cache miss for node",
                node => Name
            }),
            {error, trap};
        [{Name, []}] ->
            ?LOG_INFO(#{
                description => "Connection cache miss for node",
                node => Name
            }),
            {error, trap};
        [{Name, Pids}] ->
            Pid = partisan_util:dispatch_pid(Pids),

            case partisan_config:get(tracing, ?TRACING) of
                true ->
                    ?LOG_TRACE(#{
                        description => "Dispatching message",
                        message => Message,
                        to => Pid
                    }),

                    case is_process_alive(Pid) of
                        true ->
                            ok;
                        false ->
                            ?LOG_TRACE(#{
                                description => "Dispatching message, process is NOT ALIVE",
                                message => Message,
                                to => Pid
                            })
                    end;
                false ->
                    ok
            end,

            gen_server:cast(Pid, {send_message, {forward_message, ServerRef, Message}})
    end.


%% @private
nodename(Name) when is_atom(Name) ->
    Name;

nodename(#{name := Name}) ->
    Name.