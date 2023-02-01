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
-include("partisan.hrl").


-export([maps_append/3]).
-export([encode/1]).
-export([encode/2]).
-export([maybe_connect_disterl/1]).
-export([maybe_pad_term/1]).
-export([get/2]).
-export([get/3]).



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
-spec maybe_connect_disterl(Node :: atom()) -> ok.

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
    L = atom_to_list(T),
    Len = length(L),
    %% TODO this function uses a DEPRECATED FORMAT!
    %% https://www.erlang.org/doc/apps/erts/erl_ext_dist.html#map_ext
    %% TODO utf-8 atoms
    case Len > 256 of
        false ->
            [115, Len, L];
        true->
            [100, <<Len:16/integer-big>>, L]
    end;

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
-spec pid(pid()) -> partisan_remote_ref:p().

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
                    Unique = erlang:unique_integer([monotonic, positive]),

                    Name = case process_info(Pid, registered_name) of
                        {registered_name, OldName} ->
                            ?LOG_DEBUG(
                                "unregistering pid: ~p with name: ~p",
                                [Pid, OldName]
                            ),

                            %% TODO: Race condition on unregister/register.
                            unregister(OldName),
                            atom_to_list(OldName);
                        [] ->
                            "partisan_registered_name_" ++
                                integer_to_list(Unique)
                    end,

                    ?LOG_DEBUG(
                        "registering pid: ~p as name: ~p at node: ~p",
                        [Pid, Name, Node]
                    ),
                    true = erlang:register(list_to_atom(Name), Pid),
                    partisan_remote_ref:from_term(Name, Node);
                false ->
                    partisan_remote_ref:from_term(Pid, Node)
            end;

        false ->
            %% This is even mmore super dangerous.
            case partisan_config:get(register_pid_for_encoding, false) of
                true ->
                    Unique = erlang:unique_integer([monotonic, positive]),

                    Name = list_to_atom(
                        "partisan_registered_name_" ++ integer_to_list(Unique)
                    ),

                    [_, B, C] = string:split(pid_to_list(Pid), ".", all),
                    PidString = "<0." ++ B ++ "." ++ C,

                    RegisterFun =
                        fun() ->
                            LocalPid = list_to_pid(PidString),
                            RegName = process_info(LocalPid, registered_name),
                            NewName = case RegName of
                                {registered_name, OldName} ->
                                    ?LOG_DEBUG(
                                        "unregistering pid: ~p with name: ~p",
                                        [Pid, OldName]
                                    ),

                                    %% TODO: Race condition on unregister/
                                    %% register.
                                    unregister(OldName),
                                    atom_to_list(OldName);
                                [] ->
                                    Name
                            end,

                            erlang:register(list_to_atom(NewName), LocalPid)
                    end,
                    ?LOG_DEBUG(
                        "registering pid: ~p as name: ~p at node: ~p",
                        [Pid, Name, Node]
                    ),
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




