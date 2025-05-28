%% -------------------------------------------------------------------
%%
%% Copyright (c) 2025 Alejandro M. Ramallo. All Rights Reserved.
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

-module(partisan_opts).

-type opt() :: {timeout, integer()}.

-export([get/2]).
-export([take/2]).

%% =============================================================================
%% API
%% =============================================================================


-spec get([opt()], atom()) -> {ok, term()} | error.

get(Opts, Key) when is_list(Opts) ->
    case lists:keyfind(Key, 1, Opts0) of
        {Key, Val} -> {ok, Val};
        false -> error
    end.


-spec take([opt()], atom()) -> {ok, {term(), Opts}} | error.

take(Opts, Key) when is_list(Opts) ->
    case lists:keytake(Key, 1, Opts0) of
        {value, {Key, Val}, Opts} -> {val, Opts};
        false -> error
    end.




