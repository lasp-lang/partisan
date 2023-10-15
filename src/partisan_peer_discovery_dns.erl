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

-type config()             ::  #{
                                    record_type := record_type(),
                                    query := binary() | string(),
                                    node_basename := binary() | string(),
                                    options => options()
                                }.
-type deprecated_config()  ::  #{
                                    record_type := record_type(),
                                    name := binary() | string(),
                                    nodename := binary() | string(),
                                    options => options()
                                }.

-type record_type()         ::  string_or_type(a | aaaa | srv | fqdns).

-type options()             ::  #{
                                    alt_nameservers => [nameserver()],
                                    nameservers => [nameserver()],
                                    inet6 => string_or_type(boolean())
                                }.
-type nameserver()          ::  {
                                    string_or_type(inet:ip_address()),
                                    string_or_type(1..65535)
                                }
                                | binary() | string().
-type string_or_type(T)     ::  T | binary() | string().
-export([init/1]).
-export([lookup/2]).



%% =============================================================================
%% AGENT CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec init(Opts :: config() | deprecated_config()) ->
    {ok, State :: any()} | {error, Reason ::  any()}.

init(#{name := Name} = Opts) ->
    %% Deprecated
    init(maps:put(query, Name, maps:remove(name, Opts)));

init(#{nodename := Name} = Opts) ->
    %% Deprecated
    init(maps:put(node_basename, Name, maps:remove(nodename, Opts)));

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
        InetOpts = parse_inet_opts(maps:get(options, Opts, #{})),
        {ok, Opts#{record_type => Type, options => InetOpts}}

    catch
        error:badarg ->
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

lookup(#{record_type := Type, query := Query, options := Opts} = S, Timeout) ->
    Results = lookup(Query, Type, Opts, Timeout),

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
-spec parse_inet_opts(map()) -> list().

parse_inet_opts(Opts) ->
    Fun = fun
        (Nameservers, L0)
            when is_list(L0) andalso
            (
                Nameservers == alt_nameservers orelse
                Nameservers == nameservers
            ) ->
            L = parse_nameservers(L0),
            {true, L};

        (inet6, Term) when is_boolean(Term) ->
            true;

        (inet6, Term) when Term == "true"; Term == <<"true">> ->
            {true, true};

        (inet6, Term) when Term == "false"; Term == <<"false">> ->
            {true, false};

        (_, _) ->
            false
    end,
    maps:to_list(maps:filtermap(Fun, Opts)).


%% @private
parse_nameservers(L) ->
    DefaultPort = 53,
    Fun = fun
        ({IPAddr, Port}) ->
            {
                partisan_util:parse_ip_address(IPAddr),
                partisan_util:parse_port_nbr(Port)
            };
        (IPAddr) ->
            {
                partisan_util:parse_ip_address(IPAddr),
                DefaultPort
            }
    end,
    lists:map(Fun, L).


%% @private
lookup(Query, Type0, Opts, Timeout) ->
    Type = dns_type(Type0),
    Results = inet_res:lookup(Query, in, Type, Opts, Timeout),
    Port = partisan_config:get(listen_port),
    [format_data(Type0, DNSData, Port) || DNSData <- Results].


%% @private
record_type(Str) when is_list(Str) ->
    record_type(list_to_existing_atom(Str));

record_type(Str) when is_binary(Str) ->
    record_type(binary_to_existing_atom(Str));

record_type(fqdns) ->
    fqdns;

record_type(Term) when is_atom(Term) ->
    dns_type(Term);

record_type(_) ->
    error(badarg).


%% @private
dns_type(fqdns) ->
    a;
dns_type(a = Term) ->
    Term;
dns_type(aaaa = Term) ->
    Term;
dns_type(caa = Term) ->
    Term;
dns_type(cname = Term) ->
    Term;
dns_type(gid = Term) ->
    Term;
dns_type(hinfo = Term) ->
    Term;
dns_type(ns = Term) ->
    Term;
dns_type(mb = Term) ->
    Term;
dns_type(md = Term) ->
    Term;
dns_type(mg = Term) ->
    Term;
dns_type(mf = Term) ->
    Term;
dns_type(minfo = Term) ->
    Term;
dns_type(mx = Term) ->
    Term;
dns_type(naptr = Term) ->
    Term;
dns_type(null = Term) ->
    Term;
dns_type(ptr = Term) ->
    Term;
dns_type(soa = Term) ->
    Term;
dns_type(spf = Term) ->
    Term;
dns_type(srv = Term) ->
    Term;
dns_type(txt = Term) ->
    Term;
dns_type(uid = Term) ->
    Term;
dns_type(uinfo = Term) ->
    Term;
dns_type(unspec = Term) ->
    Term;
dns_type(uri = Term) ->
    Term;
dns_type(wks = Term) ->
    Term;
dns_type(Term) ->
    error({badarg, [Term]}).


%% @private
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

    {Host, #{ip => IPAddr, port => Port}};

format_data(A, IPAddr, Port)
when (A == a orelse A == aaaa) andalso ?IS_IP(IPAddr) ->
    Host = inet_parse:ntoa(IPAddr),
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



