%% =============================================================================
%%  partisan_peer_discovery_list.erl -
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
%% @doc An implementation of the {@link partisan_peer_discovery_agent} behaviour
%% that uses a static list of node names for service discovery.
%%
%% It is enabled by using the following options in the sys.conf file
%%
%% ```bash
%% {partisan, [
%%     {peer_discovery, [
%%          {type, partisan_peer_discovery_list},
%%          {config, #{
%%              name => mynode
%%              addresses => [
%%                  <<"192.168.40.1:9000">>,
%%                  <<"192.168.40.10:9000">>,
%%                  <<"mynode@192.168.40.100:9000">>,
%%              ]
%%          }}
%%     ]}
%% ]}
%% '''
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_peer_discovery_list).
-behaviour(partisan_peer_discovery_agent).

-include_lib("kernel/include/logger.hrl").
-include("partisan_util.hrl").


-record(state, {
    peers = []      ::  [partisan:node_spec()]
}).

-type state()       ::  #state{}.
-type name()        ::  atom() | binary() | string().
-type options()     ::  #{
                            addresses := [name() | {name(), inet:port_number()}]
                        }.


-export([init/1]).
-export([lookup/2]).



%% =============================================================================
%% AGENT CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec init(Opts :: options()) ->
    {ok, State :: state()} | {error, Reason ::  any()}.

init(#{addresses := Nodes}) when is_list(Nodes) ->
    try
        State = #state{
            peers = to_peer_list(Nodes)
        },
        {ok, State}
    catch
        throw:Reason ->
            {error, Reason}
    end;

init(Opts) ->
    {error, {invalid_options, Opts}}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec lookup(State :: state(), timeout()) ->
    {ok, [partisan:node_spec()], NewState :: state()}
    | {error, Reason :: any(), NewState :: state()}.

lookup(State, _Timeout) ->
    {ok, State#state.peers, State}.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
to_peer_list(Addrs) ->
    Myself = partisan:node(),
    Channels = partisan_config:get(channels),

    lists:foldl(
        fun(Addr, Acc) ->
            case to_peer(Addr, Channels) of
                #{name := Myself} ->
                    Acc;
                Peer ->
                    [Peer | Acc]
            end
        end,
        [],
        Addrs
    ).


%% @private
to_peer(Address, Channels) when is_binary(Address) ->
    to_peer(binary_to_list(Address), Channels);

to_peer(Address, Channels) when is_list(Address) ->
    to_peer(split_nodestring_port(Address), Channels);

to_peer({Node, Port}, Channels) when is_atom(Node), ?IS_PORT_NBR(Port) ->
    to_peer({atom_to_list(Node), Port}, Channels);

to_peer({Node, Port}, Channels) when is_list(Node), ?IS_PORT_NBR(Port) ->
    IPAddr =
        case string:split(Node, "@") of
            [_Name, Host] ->
                case inet_parse:address(Host) of
                    {ok, Value} ->
                        Value;

                    {error, _} ->
                        case inet:getaddr(Host, inet) of
                            {ok, Value} ->
                                Value;

                            {error, _} ->
                                throw(badarg)
                        end
                end;

            _ ->
                throw(badarg)
        end,

    #{
        name => list_to_atom(Node),
        listen_addrs => [#{ip => IPAddr, port => Port}],
        channels => Channels
    }.


%% @private
split_nodestring_port(HostPort) ->
    case string:split(HostPort, ":") of
        [_] ->
            {HostPort, partisan_config:get(listen_port)};

        [Nodestring, Port] ->
            try
                {Nodestring, list_to_integer(Port)}
            catch
                error:_ ->
                    throw(badarg)
            end;
        _ ->
            throw(badarg)
    end.
