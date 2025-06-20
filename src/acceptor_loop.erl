%%-------------------------------------------------------------------
%%
%% Copyright (c) 2016, James Fish <james@fishcakez.com>
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%-------------------------------------------------------------------
%% @private
-module(acceptor_loop).

-export([accept/5]).

-callback acceptor_continue({ok, Sock} | {error, Reason}, Parent, Misc) ->
    no_return() when
      Sock :: gen_tcp:socket(),
      Reason :: timeout | closed | system_limit | inet:posix(),
      Parent :: pid(),
      Misc :: term().

-callback acceptor_terminate(Reason, Parent, Misc) -> no_return() when
      Reason :: term(),
      Parent :: pid(),
      Misc :: term().

-spec accept(LSock, Timeout, Parent, Mod, Misc) -> no_return() when
      LSock :: gen_tcp:socket(),
      Timeout :: timeout(),
      Parent :: pid(),
      Mod :: module(),
      Misc :: term().
accept(LSock, Timeout, Parent, Mod, Misc) ->
    Mod:acceptor_continue(gen_tcp:accept(LSock, Timeout), Parent, Misc).
