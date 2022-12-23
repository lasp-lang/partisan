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
-export([term_to_iolist/1]).



%% =============================================================================
%% API
%% =============================================================================


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
term_to_iolist(Term) ->
    [131, term_to_iolist_(Term)].



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
term_to_iolist_([]) ->
    106;

term_to_iolist_({}) ->
    [104, 0];

term_to_iolist_(T) when is_atom(T) ->
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

term_to_iolist_(T) when is_binary(T) ->
    Len = byte_size(T),
    [109, <<Len:32/integer-big>>, T];

term_to_iolist_(T) when is_tuple(T) ->
    Len = tuple_size(T),
    case Len > 255 of
        false ->
            [104, Len, [term_to_iolist_(E) || E <- tuple_to_list(T)]];
        true ->
            [104, <<Len:32/integer-big>>, [term_to_iolist_(E) || E <- tuple_to_list(T)]]
    end;

term_to_iolist_(T) when is_list(T) ->
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
            [108, <<Len:32/integer-big>>, [[term_to_iolist_(E) || E <- T]], 106]
    end;

term_to_iolist_(T) when is_map(T) ->
    Len = maps:size(T),
    [116, <<Len:32/integer-big>>, [[term_to_iolist_(K), term_to_iolist_(V)] || {K, V} <- maps:to_list(T)]];
term_to_iolist_(T) when is_reference(T) ->
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

term_to_iolist_(T) when is_pid(T) ->
    case partisan_config:get(pid_encoding, true) of
        false ->
            <<131, Rest/binary>> = term_to_binary(T),
            Rest;
        true ->
            <<131, Rest/binary>> = term_to_binary(pid(T)),
            Rest
    end;

term_to_iolist_(T) ->
    %% fallback clause
    <<131, Rest/binary>> = term_to_binary(T),
    Rest.




%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec pid(pid()) -> partisan_remote_ref:p().

pid(Pid) ->
    Node = erlang:node(Pid),

    case Node == partisan:node() of
        true ->
            %% This is super dangerous.
            case partisan_config:get(register_pid_for_encoding, false) of
                true ->
                    Unique = erlang:unique_integer([monotonic, positive]),

                    Name = case process_info(Pid, registered_name) of
                        {registered_name, OldName} ->
                            ?LOG_DEBUG("unregistering pid: ~p with name: ~p", [Pid, OldName]),

                            %% TODO: Race condition on unregister/register.
                            unregister(OldName),
                            atom_to_list(OldName);
                        [] ->
                            "partisan_registered_name_" ++ integer_to_list(Unique)
                    end,

                    ?LOG_DEBUG("registering pid: ~p as name: ~p at node: ~p", [Pid, Name, Node]),
                    true = erlang:register(list_to_atom(Name), Pid),
                    {partisan_remote_ref, Node, {encoded_name, Name}};
                false ->
                    {partisan_remote_ref, Node, {encoded_pid, pid_to_list(Pid)}}
            end;
        false ->
            %% This is even mmore super dangerous.
            case partisan_config:get(register_pid_for_encoding, false) of
                true ->
                    Unique = erlang:unique_integer([monotonic, positive]),

                    Name = "partisan_registered_name_" ++ integer_to_list(Unique),
                    [_, B, C] = string:split(pid_to_list(Pid), ".", all),
                    RewrittenProcessIdentifier = "<0." ++ B ++ "." ++ C,

                    RegisterFun = fun() ->
                        RewrittenPid = list_to_pid(RewrittenProcessIdentifier),

                        NewName = case process_info(RewrittenPid, registered_name) of
                            {registered_name, OldName} ->
                                ?LOG_DEBUG("unregistering pid: ~p with name: ~p", [Pid, OldName]),

                                %% TODO: Race condition on unregister/register.
                                unregister(OldName),
                                atom_to_list(OldName);
                            [] ->
                                Name
                        end,

                        erlang:register(list_to_atom(NewName), RewrittenPid)
                    end,
                    ?LOG_DEBUG("registering pid: ~p as name: ~p at node: ~p", [Pid, Name, Node]),
                    %% TODO: Race here unless we wait.
                    _ = rpc:call(Node, erlang, spawn, [RegisterFun]),
                    {partisan_remote_ref, Node, {encoded_name, Name}};
                false ->
                    [_, B, C] = string:split(pid_to_list(Pid), ".", all),
                    RewrittenProcessIdentifier = "<0." ++ B ++ "." ++ C,
                    {partisan_remote_ref, Node, {encoded_pid, RewrittenProcessIdentifier}}
            end
    end.




