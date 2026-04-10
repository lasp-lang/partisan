%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Helium Systems, Inc.  All Rights Reserved.
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

-module(partisan_app).

-behaviour(application).

-include("partisan.hrl").

-export([start/2, stop/1]).



%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Starts the application.
%% @end
%% -----------------------------------------------------------------------------
start(_StartType, _StartArgs) ->
    ok = ensure_otp_modules(),
    case partisan_sup:start_link() of
        {ok, Pid} ->
            {ok, Pid};
        Other ->
            {error, Other}
    end.


%% @private
%% Ensure the generated OTP modules (partisan_gen_server, etc.) are loaded.
%% They are normally generated at compile time by the post-compile hook.
%% This runtime check handles cases where the compile-time hook couldn't
%% write to the correct ebin (e.g., checkout dependencies).
ensure_otp_modules() ->
    case code:ensure_loaded(partisan_gen_server) of
        {module, _} ->
            ok;
        {error, _} ->
            %% Modules not found on code path. Try to generate them.
            %% This will fail in a release where OTP source is unavailable,
            %% which indicates the release was assembled without the
            %% generated modules — that is a build-time error.
            case partisan_gen_transform:generate_all() of
                ok -> ok;
                {error, Errors} ->
                    error({partisan_otp_modules_missing, Errors})
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc Stop the application.
%% @end
%% -----------------------------------------------------------------------------
stop(_State) ->
    ok.
