%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(partisan_util).

-include("partisan_logger.hrl").
-include("partisan_util.hrl").
-include("partisan.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([apply/4]).
-export([encode/1]).
-export([encode/2]).
-export([format_posix_error/1]).
-export([get/2]).
-export([get/3]).
-export([maps_append/3]).
-export([maybe_connect_disterl/1]).
-export([maybe_pad_term/1]).
-export([parse_ip_address/1]).
-export([parse_listen_address/1]).
-export([parse_port_nbr/1]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get(Key :: term(), Arg :: list() | map()) -> Value :: any() | no_return().

get(Key, Arg) when is_list(Arg) ->
    case lists:keyfind(Key, 1, Arg) of
        {Key, Value} ->
            Value;
        false ->
            error(badkey)
    end;

get(Key, Arg) when is_map(Arg) ->
    maps:get(Key, Arg).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec get(Key :: term(), Arg :: list() | map(), Default :: any()) ->
    Value :: any().

get(Key, Arg, Default) when is_list(Arg) ->
    case lists:keyfind(Key, 1, Arg) of
        {Key, Value} ->
            Value;
        false ->
            Default
    end;

get(Key, Arg, Default) when is_map(Arg) ->
    maps:get(Key, Arg, Default).



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec maybe_connect_disterl(Node :: node()) -> ok.

maybe_connect_disterl(Node ) ->
    case partisan_config:get(connect_disterl, false) of
        true ->
            _ = net_kernel:connect_node(Node),
            ok;
        false ->
            ok
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec maybe_pad_term(Term :: term()) ->
    term() | {'$partisan_padded', Padding :: term(), Term :: term()}.

maybe_pad_term(Term) ->
    case partisan_config:get(binary_padding, false) of
        true ->
            Padding = partisan_config:get(binary_padding_term, undefined),
            {'$partisan_padded', Padding, Term};
        false ->
            Term
    end.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
maps_append(Key, Value, Map) ->
    maps:update_with(
        Key,
        fun
            ([]) ->
                [Value];
            (L) when is_list(L) ->
                [Value | L];
            (X) ->
                [Value, X]
        end,
        [Value],
        Map
    ).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec apply(
    Mod :: module(), Fun :: atom(), Args :: list(), Default :: any()) ->
    any() | no_return().

apply(Mod, Fun, Args, Default) ->
    erlang:function_exported(Mod, module_info, 0)
        orelse code:ensure_loaded(Mod),

    Arity = length(Args),

    case erlang:function_exported(Mod, Fun, Arity) of
        true ->
            erlang:apply(Mod, Fun, Args);
        false ->
            Default
    end.

%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec safe_apply(
    Mod :: module(), Fun :: atom(), Args :: list(), Default :: any()) -> any().

safe_apply(Mod, Fun, Args, Default) ->
    try
        apply(Mod, Fun, Args, Default)
    catch
        Class:Reason:Stacktrace ->
            Formatted = erl_error:format_exception(Class, Reason, Stacktrace),
            ?LOG_ERROR(#{
                description => "Error while applying MFA",
                exception => Formatted
            }),
            Default
    end.



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
encode(Term) ->
    encode(Term, []).


%% -----------------------------------------------------------------------------
%% @doc If `pid_encoding' or `ref_encoding' configuration options are enabled,
%% this function will ignore Opts and return an iolist(). Otherwise, the
%% function calls `erlang:term_to_iovec(Term, Opts)'.'
%% @end
%% -----------------------------------------------------------------------------
-spec encode(Term :: term(), Opts :: list()) -> erlang:ext_iovec() | iolist().

encode(Term, Opts) ->
    PidEnc = partisan_config:get(pid_encoding, true),
    RefEnc = partisan_config:get(ref_encoding, true),

    case PidEnc orelse RefEnc of
        true ->
            erlang:iolist_to_iovec([131, encode_(Term)]);
        false ->
            erlang:term_to_iovec(Term, Opts)
    end.


%% -----------------------------------------------------------------------------
%% @doc Given a POSIX error reason see {@link inet} and {@link file}, returns a
%% descriptive binary string of the error in English and adding `Reason' at the
%% end of the description inside parenthesis.
%% @end
%% -----------------------------------------------------------------------------
-spec format_posix_error(Reason :: inet:posix() | 'system_limit') ->
    Message :: binary() | Reason :: inet:posix() | 'system_limit'.

format_posix_error(Reason) when is_atom(Reason) ->
    case inet:format_error(Reason) of
        "unknown" ++ _ ->
            Reason;
        Message ->
            iolist_to_binary([Message, " (", atom_to_list(Reason), ")"])
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec parse_ip_address(Address :: string() | binary() | inet:ip_address()) ->
    IPAddress :: inet:ip_address() | no_return().

parse_ip_address(Address) when is_list(Address) ->
    case inet:parse_address(Address) of
        {ok, IPAddress} ->
            IPAddress;

        {error, _} ->
            error(badarg)
    end;

parse_ip_address(Term) when is_binary(Term) ->
    parse_ip_address(binary_to_list(Term));

parse_ip_address(Term) when ?IS_IP(Term) ->
    Term;

parse_ip_address(_) ->
    error(badarg).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
parse_port_nbr(N) when ?IS_PORT_NBR(N) ->
    N;

parse_port_nbr(Term) when is_binary(Term) ->
    parse_port_nbr(binary_to_integer(Term));

parse_port_nbr(Term) when is_list(Term) ->
    parse_port_nbr(list_to_integer(Term));

parse_port_nbr(_) ->
    error(badarg).


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec parse_listen_address(Term :: map() | list() | binary() | tuple()) ->
    partisan:listen_addr() | no_return().

parse_listen_address(#{ip := IPAddress, port := N} = Addr)
when ?IS_IP(IPAddress) andalso ?IS_PORT_NBR(N) ->
    Addr;

parse_listen_address(#{ip := IP, port := N}) ->
    #{ip => parse_ip_address(IP), port => parse_port_nbr(N)};

parse_listen_address({IPAddr, N}) when ?IS_IP(IPAddr) andalso ?IS_PORT_NBR(N) ->
    #{ip => IPAddr, port => N};

parse_listen_address({IPAddr, N}) ->
    parse_listen_address(#{ip => IPAddr, port => N});

parse_listen_address(Term) when is_list(Term) ->
    case string:split(Term, ":") of
        [IPAddr, N] ->
            parse_listen_address(#{ip => IPAddr, port => N});
        [Term] ->
            Port = partisan_config:get(listen_port),
            parse_listen_address(#{ip => Term, port => Port});
        _ ->
            error(badarg)
    end;

parse_listen_address(Term) when is_binary(Term) ->
    parse_listen_address(binary_to_list(Term));

parse_listen_address(_) ->
    error(badarg).



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
encode_([]) ->
    106;

encode_({}) ->
    [104, 0];

encode_(T) when is_atom(T) ->
    <<131, Rest/binary>> = term_to_binary(T),
    [Rest];

encode_(T) when is_binary(T) ->
    Len = byte_size(T),
    [109, <<Len:32/integer-big>>, T];

encode_(T) when is_tuple(T) ->
    Len = tuple_size(T),
    case Len > 255 of
        false ->
            [104, Len, [encode_(E) || E <- tuple_to_list(T)]];
        true ->
            [104, <<Len:32/integer-big>>, [encode_(E) || E <- tuple_to_list(T)]]
    end;

encode_([alias|Ref]) ->
    %% Improper list with 2 elements to support partisan_remote_ref format
    [108, <<1:32/integer-big>>, [encode_(alias), encode_(Ref)]];

encode_([Node|Bin]) when is_atom(Node), is_binary(Bin) ->
    %% Improper list with 2 elements to support partisan_remote_ref format
    [108, <<1:32/integer-big>>, [encode_(Node), encode_(Bin)]];

encode_(T) when is_list(T) ->
    %% TODO improper lists
    Len = length(T),
    case Len < 64436 andalso lists:all(fun(E) when is_integer(E), E >= 0, E < 256 ->
                                                true;
                                            (_) ->
                                                 false
                                        end, T) of
        true ->
            [107, <<Len:16/integer-big>>, T];
        false ->
            [108, <<Len:32/integer-big>>, [[encode_(E) || E <- T]], 106]
    end;

encode_(T) when is_map(T) ->
    Len = maps:size(T),
    [116, <<Len:32/integer-big>>, [[encode_(K), encode_(V)] || {K, V} <- maps:to_list(T)]];
encode_(T) when is_reference(T) ->
    case partisan_config:get(ref_encoding, true) of
        false ->
            <<131, Rest/binary>> = term_to_binary(T),
            Rest;
        true ->
            <<131, Rest/binary>> = term_to_binary(
                partisan_remote_ref:from_term(T)
            ),
            Rest
    end;

encode_(T) when is_pid(T) ->
    case partisan_config:get(pid_encoding, true) of
        false ->
            <<131, Rest/binary>> = term_to_binary(T),
            Rest;
        true ->
            <<131, Rest/binary>> = term_to_binary(pid(T)),
            Rest
    end;

encode_(T) ->
    %% fallback clause
    <<131, Rest/binary>> = term_to_binary(T),
    Rest.




%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec pid(pid()) -> partisan:remote_pid().

pid(Pid) when is_pid(Pid) ->
    Node = erlang:node(Pid),
    Disterl = partisan_config:get(connect_disterl, false),
    pid(Pid, Node, Disterl).

%% @private
pid(Pid, _, false) ->
    partisan_remote_ref:from_term(Pid);

pid(Pid, Node, true) ->
    case Node == node() of
        true ->
            %% This is super dangerous.
            case partisan_config:get(register_pid_for_encoding, false) of
                true ->
                    Name = case process_info(Pid, registered_name) of
                        {registered_name, OldName} ->
                            ?LOG_DEBUG(
                                "unregistering pid: ~p with name: ~p",
                                [Pid, OldName]
                            ),

                            %% TODO: Race condition on unregister/register.
                            unregister(OldName),
                            OldName;
                        [] ->
                            Unique = erlang:unique_integer(
                                [monotonic, positive]
                            ),
                            list_to_atom(
                                "partisan_registered_name_" ++
                                integer_to_list(Unique)
                            )
                    end,

                    ?LOG_DEBUG(
                        "registering pid: ~p as name: ~p at node: ~p",
                        [Pid, Name, Node]
                    ),
                    true = erlang:register(Name, Pid),
                    partisan_remote_ref:from_term(Name, Node);
                false ->
                    partisan_remote_ref:from_term(Pid, Node)
            end;

        false ->
            %% This is even more super dangerous.
            case partisan_config:get(register_pid_for_encoding, false) of
                true ->
                    Unique = erlang:unique_integer([monotonic, positive]),
                    Name = list_to_atom(
                        "partisan_registered_name_" ++ integer_to_list(Unique)
                    ),

                    [_, B, C] = string:split(pid_to_list(Pid), ".", all),
                    PidString = "<0." ++ B ++ "." ++ C,

                    ?LOG_DEBUG(
                        "registering pid: ~p as name: ~p at node: ~p",
                        [Pid, Name, Node]
                    ),

                    RegisterFun =
                        fun() ->
                            LocalPid = list_to_pid(PidString),
                            RegName = process_info(LocalPid, registered_name),
                            case RegName of
                                {registered_name, OldName}
                                when is_atom(OldName) ->
                                    ?LOG_DEBUG(
                                        "unregistering pid: ~p with name: ~p",
                                        [Pid, OldName]
                                    ),

                                    %% TODO: Race condition on unregister/
                                    %% register.
                                    unregister(OldName),
                                    ok;
                                _ ->
                                    ok
                            end,

                            erlang:register(Name, LocalPid)
                    end,

                    %% TODO: Race here unless we wait.
                    _ = partisan_rpc:call(
                        Node, erlang, spawn, [RegisterFun], 5000
                    ),
                    partisan_remote_ref:from_term(Name, Node);

                false ->
                    [_, B, C] = string:split(pid_to_list(Pid), ".", all),
                    PidString = "<0." ++ B ++ "." ++ C,
                    partisan_remote_ref:from_term(list_to_pid(PidString), Node)
            end
    end.



%% =============================================================================
%% EUNIT TESTS
%% =============================================================================



-ifdef(TEST).


parse_listen_address_test_() ->
    Addr = #{ip => {127,0,0,1}, port => 53688},
    [
        ?_assertEqual(Addr, parse_listen_address("127.0.0.1:53688")),
        ?_assertEqual(Addr, parse_listen_address(<<"127.0.0.1:53688">>)),
        ?_assertEqual(Addr, parse_listen_address(Addr)),
        ?_assertEqual(Addr, parse_listen_address(#{
            ip => "127.0.0.1", port => "53688"}
        )),
        ?_assertEqual(Addr, parse_listen_address(#{
            ip => <<"127.0.0.1">>, port => <<"53688">>}
        )),
        ?_assertEqual(Addr, parse_listen_address(#{
            ip => {127,0,0,1}, port => <<"53688">>}
        )),
        ?_assertEqual(Addr, parse_listen_address(#{
            ip => {127,0,0,1}, port => 53688}
        )),
        ?_assertEqual(Addr, parse_listen_address(
            {{127,0,0,1}, 53688}
        )),
        ?_assertEqual(Addr, parse_listen_address(
            {"127.0.0.1", "53688"}
        )),
        ?_assertEqual(Addr, parse_listen_address(
            {<<"127.0.0.1">>, <<"53688">>}
        )),
        ?_assertError(badarg, parse_listen_address(#{
            ip => " 127.0.0.1 ", port => 53688}
        )),
        ?_assertError(badarg, parse_listen_address(#{
            ip => {127,0,0,1}, port => " 53688 "}
        )),
        ?_assertError(badarg, parse_listen_address(#{
            ip => {127,0,0,1}, port => 0}
        )),
        ?_assertError(badarg, parse_listen_address(#{
            ip => {127,0,0,1}, port => "0"}
        )),
        ?_assertError(badarg, parse_listen_address(#{
            ip => {127,0,0,1}, port => <<"0">>}
        ))
    ].



-endif.