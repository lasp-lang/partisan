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
%% @doc An implementation of the {@link partisan_peer_discovery_agent} behaviour
%% that uses DNS for service discovery.
%%
%% It is enabled by using the following options in the sys.conf file
%%
%% ```bash
%% {partisan, [
%%     {peer_discovery, [
%%          {type, partisan_peer_discovery_dns},
%%          {config, #{
%%              record_type => fqdns,
%%              query => "foo.local",
%%              node_basename => "foo"
%%          }}
%%     ]}
%% ]}
%% '''
%%
%% @end
%% -----------------------------------------------------------------------------
-module(partisan_peer_discovery_dns).
-behaviour(partisan_peer_discovery_agent).

-include_lib("kernel/include/logger.hrl").
-include_lib("kernel/include/inet.hrl").
-include("partisan_util.hrl").

-type options()             ::  #{
                                    record_type := record_type(),
                                    query := binary() | string(),
                                    node_basename := binary() | string()
                                }.
-type deprecated_options()  ::  #{
                                    record_type := record_type(),
                                    name := binary() | string(),
                                    nodename := binary() | string()
                                }.

-type record_type() ::  a | srv | fqdns
                        | list() | binary().
-export([init/1]).
-export([lookup/2]).



%% =============================================================================
%% AGENT CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec init(Opts :: options() | deprecated_options()) ->
    {ok, State :: any()} | {error, Reason ::  any()}.

init(#{name := Name} = Opts) ->
    %% Deprecated
    init(Opts#{query => Name});

init(#{nodename := Name} = Opts) ->
    %% Deprecated
    init(Opts#{node_basename => Name});

init(#{query := Name} = Opts) when is_binary(Name) ->
    init(Opts#{query => binary_to_list(Name)});

init(#{node_basename := Nodename} = Opts) when is_atom(Nodename) ->
    init(Opts#{node_basename => atom_to_list(Nodename)});

init(#{node_basename := Nodename} = Opts) when is_binary(Nodename) ->
    init(Opts#{node_basename => binary_to_list(Nodename)});

init(#{record_type := Type0, query := Query, node_basename := Basename} = Opts)
when is_list(Query)
andalso is_list(Basename) ->
    try
        Type = record_type(Type0),
        {ok, Opts#{record_type => Type}}

    catch
        throw:badarg ->
            {error, {invalid_options, Opts}}
    end;

init(Opts) ->
    {error, {invalid_options, Opts}}.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec lookup(State :: any(), timeout()) ->
    {ok, [partisan:node_spec()], NewState :: any()}
    | {error, Reason :: any(), NewState :: any()}.

lookup(#{record_type := Type, query := Query} = S, Timeout) ->

    Results = lookup(Query, Type, Timeout),

    ?LOG_DEBUG(
        fun([]) ->
            #{
            description => "DNS lookup response",
            response => Results,
            query => Query
            }
        end,
        []
    ),

    case Results =/= [] of
        true ->
            Basename = maps:get(node_basename, S),
            Channels = partisan_config:get(channels),
            Peers = to_peer_list(Results, Basename, Channels),
            {ok, Peers, S};
        false ->
            {ok, [], S}
    end.



%% =============================================================================
%% PRIVATE
%% =============================================================================


%% @private
record_type(a) ->
    a;
record_type(srv) ->
    srv;
record_type(fqdns) ->
    fqdns;
record_type("a") ->
    a;
record_type("srv") ->
    srv;
record_type("fqdns") ->
    fqdns;
record_type(Str) when is_binary(Str) ->
    record_type(binary_to_list(Str));
record_type(_) ->
    throw(badarg).



%% @private
lookup(Query, Type0, Timeout) ->
    Type = dns_type(Type0),
    Results = inet_res:lookup(Query, in, Type, [], Timeout),
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
                {ok, HostEnt} = inet_res:getbyname(Host, a),
                #hostent{h_addr_list = [Val | _]} = HostEnt,
                Val;

            {ok, Val} ->
                Val
        end,

    {Host, #{ip => IPAddr, port => Port}}.


%% @private
to_peer_list(Results, Basename, Channels) ->
    to_peer_list(Results, Basename, Channels, maps:new()).


%% @private
to_peer_list([], _, _, Acc) ->
    maps:values(Acc);

to_peer_list([{Host, ListAddr} | T], Basename, Channels, Acc0) ->
    Node = list_to_atom(string:join([Basename, Host], "@")),

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

    to_peer_list(T, Basename, Channels, Acc).



