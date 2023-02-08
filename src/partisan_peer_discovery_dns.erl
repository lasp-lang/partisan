%% =============================================================================
%%  partisan_peer_discovery_dns -
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
%% @doc An implementation of the {@link bondy_peer_discovery_agent} behaviour
%% that uses DNS for service discovery.
%%
%% It is enabled by using the following options in the bondy.conf file
%%
%% ```bash
%% cluster.peer_discovery_agent.type = partisan_peer_discovery_dnt
%% cluster.peer_discovery_agent.config.service_name = my-service-name
%% '''
%% Where service_name is the service to be used by the DNS lookup.
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_peer_discovery_dns).
-behaviour(partisan_peer_discovery_agent).

-include_lib("kernel/include/logger.hrl").
-include_lib("kernel/include/inet.hrl").
-include("partisan_util.hrl").

-type options()     ::  #{
                            record_type := a | srv | fqdns,
                            name := binary() | string(),
                            nodename := binary() | string()
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
    {ok, State :: any()} | {error, Reason ::  any()}.

init(#{name := Name} = Opts) when is_binary(Name) ->
    init(Opts#{name => binary_to_list(Name)});

init(#{nodename := Nodename} = Opts) when is_binary(Nodename) ->
    init(Opts#{nodename => binary_to_list(Nodename)});

init(#{record_type := Type, name := Name, nodename := Nodename} = Opts)
when is_list(Name)
andalso is_list(Nodename)
andalso (Type == a orelse Type == srv orelse Type == fqdns) ->
    {ok, Opts};

init(Opts) ->
    {error, {invalid_options, Opts}}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec lookup(State :: any(), timeout()) ->
    {ok, [bondy_peer_discovery_agent:peer()], NewState :: any()}
    | {error, Reason :: any(), NewState :: any()}.

lookup(#{record_type := Type, name := Name} = S, Timeout) ->

    Results = lookup(Name, Type, Timeout),

    ?LOG_DEBUG(
        fun([]) ->
            #{
            description => "DNS lookup response",
            response => Results,
            name => Name
            }
        end,
        []
    ),

    case Results =/= [] of
        true ->
            Nodename = maps:get(nodename, S),
            Channels = partisan_config:get(channels),
            Peers = to_peer_list(Results, Nodename, Channels),
            {ok, Peers, S};
        false ->
            {ok, [], S}
    end.



%% =============================================================================
%% PRIVATE
%% =============================================================================




%% @private
lookup(Name, Type0, Timeout) ->
    Type = dns_type(Type0),
    Results = inet_res:lookup(Name, in, Type, [], Timeout),
    Port = partisan_config:get(peer_port),
    [format_data(Type0, DNSData, Port) || DNSData <- Results].


%% @private
dns_type(a) ->
    a;
dns_type(srv) ->
    srv;
dns_type(fqdns) ->
    a.


%% @private
format_data(a, IPAddr, Port) when ?IS_IP(IPAddr) ->
    Host = inet_parse:ntoa(IPAddr),
    {Host, #{ip => IPAddr, port => Port}};

format_data(fqdns, IPAddr, Port) when ?IS_IP(IPAddr) ->
    {ok, {hostent, Host, _, _, _, _}} = inet_res:gethostbyaddr(IPAddr),
    {Host, #{ip => IPAddr, port => Port}};

format_data(srv, {_, _, Port, Host}, _) ->
    %% We use the port returned by the DNS lookup
    IPAddr =
        case inet:parse_address(Host) of
            {error, einval} ->
                {ok, #hostent{h_addr_list = [Val | _]}} =
                    inet_res:getbyname(Host, a),
                Val;
            {ok, Val} ->
                Val
        end,
    {Host, #{ip => IPAddr, port => Port}}.


%% @private
to_peer_list(Results, Nodename, Channels) ->
    to_peer_list(Results, Nodename, Channels, maps:new()).


%% @private
to_peer_list([], _, _, Acc) ->
    maps:values(Acc);

to_peer_list([{Host, ListAddr} | T], Nodename, Channels, Acc0) ->
    Node = list_to_atom(string:join([Nodename, Host], "@")),

    %% We use a map to accumulate all the IPAddrs per node
    Acc =
        case maps:find(Node, Acc0) of
            {ok, #{listen_addrs := ListenAddrs0} = Spec0} ->
                %% We update the node specification by adding the IPAddr
                Spec = Spec0#{
                    listen_addrs => [ListAddr | ListenAddrs0]
                },
                maps:put(Node, Spec, Acc0);

            error ->
                %% We insert a new node specification
                Spec = #{
                    name => Node,
                    channels => Channels,
                    listen_addrs => [ListAddr]
                },
                maps:put(Node, Spec, Acc0)
        end,

    to_peer_list(T, Nodename, Channels, Acc).



