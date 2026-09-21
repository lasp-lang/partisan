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
-include("partisan_logger.hrl").

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
            %% A startup diagnostic must never break application start.
            _ = maybe_warn_peer_plane_security(),
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
    %% Check the WHOLE generated set, not just one representative: a partial
    %% ebin (interrupted build, selective load) would otherwise pass here and
    %% surface later as an `undef' at first use instead of at app start.
    Generated = partisan_otp_modules:partisan_modules(),
    AllLoaded = lists:all(
        fun(M) ->
            case code:ensure_loaded(M) of
                {module, _} -> true;
                {error, _} -> false
            end
        end,
        Generated
    ),
    case AllLoaded of
        true ->
            ok;
        false ->
            %% One or more modules not found on the code path. Try to generate
            %% them. This will fail in a release where OTP source is
            %% unavailable, which indicates the release was assembled without
            %% the generated modules — that is a build-time error.
            case partisan_gen_transform:generate_all() of
                ok -> ok;
                {error, Errors} -> error({partisan_otp_modules_missing, Errors})
            end
    end.

%% @private
%% Emit a one-time startup message describing the peer-plane security posture:
%% a WARNING when TLS is on but peers are not verified (encrypted yet MITM-able),
%% and a NOTICE when the peer plane is plaintext and unauthenticated.
maybe_warn_peer_plane_security() ->
    %% Best-effort: never allowed to crash application start (e.g. on a fresh
    %% peer node where the logger/config may not be fully ready).
    try
        case partisan_config:get(tls, false) of
            false ->
                ?LOG_NOTICE(#{
                    description =>
                        "Partisan peer plane is plaintext and unauthenticated "
                        "(tls = false). Keep the peer port on a trusted network; "
                        "enable cluster mTLS (verify_peer + a CA) on untrusted ones."
                }),
                ok;
            true ->
                ServerOK = verifies_peer(
                    partisan_config:get(tls_server_options, [])
                ),
                ClientOK = verifies_peer(
                    partisan_config:get(tls_client_options, [])
                ),
                case ServerOK andalso ClientOK of
                    true ->
                        ok;
                    false ->
                        ?LOG_WARNING(#{
                            description =>
                                "Partisan cluster TLS is enabled but peers are not "
                                "verified (verify_peer missing on one or both sides): "
                                "connections are encrypted but NOT authenticated and "
                                "are MITM-able. Set {verify, verify_peer} with a CA in "
                                "tls_server_options and tls_client_options."
                        }),
                        ok
                end
        end
    catch
        _:_ -> ok
    end.

%% @private
verifies_peer(Opts) when is_list(Opts) ->
    proplists:get_value(verify, Opts) =:= verify_peer.

%% -----------------------------------------------------------------------------
%% @doc Stop the application.
%% @end
%% -----------------------------------------------------------------------------
stop(_State) ->
    ok.
