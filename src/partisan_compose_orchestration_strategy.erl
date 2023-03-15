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
-behavior(partisan_orchestration_strategy).

-include("partisan_logger.hrl").
-include("partisan.hrl").

-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([clients/1,
         servers/1,
         upload_artifact/3,
         download_artifact/2]).

%% @private
upload_artifact(#orchestration_strategy_state{eredis=Eredis}, Node, Payload) ->
    {ok, <<"OK">>} = eredis:q(Eredis, ["SET", Node, Payload]),

    ?LOG_TRACE(#{
        description => "Pushed artifact to Redis",
        node => Node,
        payload => Payload
    }),

    %% Store membership with node tag.
    Myself = partisan:node_spec(),
    MyselfPayload = term_to_binary(Myself),
    Tag = partisan_config:get(tag, client),
    TaggedNode = prefix(
        atom_to_list(Tag) ++ "/" ++ atom_to_list(partisan:node())
    ),
    {ok, <<"OK">>} = eredis:q(Eredis, ["SET", TaggedNode, MyselfPayload]),

    ?LOG_TRACE(#{
        description => "Pushed additional artifact to Redis",
        node => Node
    }),

    ok.

%% @private
download_artifact(#orchestration_strategy_state{eredis=Eredis}, Node) ->
    try
        case eredis:q(Eredis, ["GET", Node]) of
            {ok, Payload} ->
                ?LOG_TRACE(#{
                    description => "Received artifact from Redis",
                    node => Node
                }),
                Payload;
            {error,no_connection} ->
                undefined
        end
    catch
        Class:Reason:Stacktrace ->
            ?LOG_WARNING(#{
                description => "Exception caught",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
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
        {ok, Nodes} when is_list(Nodes) ->
            Nodes1 = lists:map(
                fun(N) when is_binary(N) -> binary_to_list(N) end,
                Nodes
            ),

            Nodes2 = lists:flatmap(fun(N) ->
                    case eredis:q(Eredis, ["GET", N]) of
                        {ok, Myself} when is_binary(Myself) ->
                            [binary_to_term(Myself)];
                        _ ->
                            []
                    end
                end, Nodes1),

            ?LOG_TRACE(#{
                description => "Received keys from Redis",
                tag => Tag,
                nodes => Nodes2
            }),

            sets:from_list(Nodes2);
        {error, no_connection} ->
            sets:new()
    end.

%% @private
prefix(File) ->
    EvalIdentifier = partisan_config:get(evaluation_identifier, undefined),
    EvalTimestamp = partisan_config:get(evaluation_timestamp, 0),
    "partisan" ++ "/" ++ atom_to_list(EvalIdentifier) ++ "/" ++ integer_to_list(EvalTimestamp) ++ "/" ++ File.