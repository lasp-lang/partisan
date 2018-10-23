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

-module(partisan_compose_orchestration_strategy).

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-include("partisan.hrl").

-behavior(partisan_orchestration_strategy).

-export([clients/1,
         servers/1,
         upload_artifact/3,
         download_artifact/2]).

%% @private
upload_artifact(#orchestration_strategy_state{eredis=Eredis}, Node, Payload) ->
    {ok, <<"OK">>} = eredis:q(Eredis, ["SET", Node, Payload]),

    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("Pushed artifact to Redis: ~p => ~p", [Node, Payload]);
        false ->
            ok
    end,

    %% Store membership with node tag.
    Myself = partisan_peer_service_manager:myself(),
    MyselfPayload = term_to_binary(Myself),
    Tag = partisan_config:get(tag, client),
    TaggedNode = prefix(atom_to_list(Tag) ++ "/" ++ atom_to_list(node())),
    {ok, <<"OK">>} = eredis:q(Eredis, ["SET", TaggedNode, MyselfPayload]),

    case partisan_config:get(tracing, ?TRACING) of 
        true ->
            lager:info("Pushed additional artifact to Redis: ~p.", [Node]);
        false ->
            ok
    end,

    ok.

%% @private
download_artifact(#orchestration_strategy_state{eredis=Eredis}, Node) ->
    %% lager:info("Retrieving object ~p from redis.", [Node]),

    try
        case eredis:q(Eredis, ["GET", Node]) of
            {ok, Payload} ->
                case partisan_config:get(tracing, ?TRACING) of 
                    true ->
                        lager:info("Received artifact from Redis: ~p", [Node]);
                    false ->
                        ok
                end,

                Payload;
            {error,no_connection} ->
                undefined
        end
    catch
        _:Error ->
            lager:info("Exception caught: ~p", [Error]),
            undefined
    end.

%% @private
clients(State) ->
    retrieve_keys(State, "client").

%% @private
servers(State) ->
    retrieve_keys(State, "server").

%% @private
retrieve_keys(#orchestration_strategy_state{eredis=Eredis}, Tag) ->
    case eredis:q(Eredis, ["KEYS", prefix(Tag ++ "/*")]) of
        {ok, Nodes} ->
            Nodes1 = lists:map(fun(N) -> binary_to_list(N) end, Nodes),

            Nodes2 = lists:flatmap(fun(N) ->
                    case eredis:q(Eredis, ["GET", N]) of
                        {ok, Myself} ->
                            [binary_to_term(Myself)];
                        _ ->
                            []
                    end
                end, Nodes1),

            case partisan_config:get(tracing, ?TRACING) of 
                true ->
                    lager:info("Received ~p keys from Redis: ~p", [Tag, Nodes2]);
                false ->
                    ok
            end,

            sets:from_list(Nodes2);
        {error, no_connection} ->
            sets:new()
    end.

%% @private
prefix(File) ->
    EvalIdentifier = partisan_config:get(evaluation_identifier, undefined),
    EvalTimestamp = partisan_config:get(evaluation_timestamp, 0),
    "partisan" ++ "/" ++ atom_to_list(EvalIdentifier) ++ "/" ++ integer_to_list(EvalTimestamp) ++ "/" ++ File.