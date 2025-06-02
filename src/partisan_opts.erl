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

-type t()   :: [opt()].
-type opt() :: {timeout, integer()}.

-export_type([t/0]).


-export([get/2]).
-export([take/2]).

%% =============================================================================
%% API
%% =============================================================================


-spec get([opt()], atom()) -> {ok, term()} | error.

get(Opts, Key) when is_list(Opts) ->
    case lists:keyfind(Key, 1, Opts) of
        {Key, Val} -> {ok, Val};
        false -> error
    end.


-spec take([opt()], atom()) -> {ok, {term(), list()}} | error.

take(Opts0, Key) when is_list(Opts0) ->
    case lists:keytake(Key, 1, Opts0) of
        {value, {Key, Val}, Opts} -> {Val, Opts};
        false -> error
    end.




