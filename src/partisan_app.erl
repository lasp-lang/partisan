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
%% a WARNING when the node does not authenticate its peers, with or without
%% TLS, and a NOTICE when it does but the peer plane is plaintext.
maybe_warn_peer_plane_security() ->
    %% Best-effort: never allowed to crash application start (e.g. on a fresh
    %% peer node where the logger/config may not be fully ready).
    try
        %% `partisan_peer_auth' holds the definition of each condition.
        TLS = partisan_config:get(tls, false) =:= true,
        Authenticated = partisan_peer_auth:authenticates_peers(),
        Methods = partisan_peer_auth:methods(),

        case {TLS, Authenticated} of
            {true, true} ->
                ok;
            {false, true} ->
                ?LOG_NOTICE(#{
                    description =>
                        "Partisan peer plane is plaintext (tls = false). Peers "
                        "authenticate when they connect, but traffic is not "
                        "encrypted or signed. Keep the peer port on a trusted "
                        "network, or enable cluster TLS.",
                    peer_auth_methods => Methods
                });
            {false, false} ->
                ?LOG_WARNING(#{
                    description =>
                        "Partisan peer plane is plaintext and unauthenticated "
                        "(tls = false, and peer_auth_methods is empty or admits "
                        "anonymous). Any host that reaches the peer port can "
                        "join the cluster, stop this node or run code on it. "
                        "Keep the peer port on a trusted network; set "
                        "peer_auth_methods or enable cluster mTLS (verify_peer "
                        "+ a CA) on untrusted ones.",
                    peer_auth_methods => Methods
                });
            {true, false} ->
                ?LOG_WARNING(#{
                    description =>
                        "Partisan cluster TLS is enabled but peers are not "
                        "authenticated: connections are encrypted but any host "
                        "can connect, and they are MITM-able. Set "
                        "peer_auth_methods, or set {verify, verify_peer} with a "
                        "CA in tls_server_options and tls_client_options, "
                        "without {fail_if_no_peer_cert, false} in "
                        "tls_server_options.",
                    peer_auth_methods => Methods
                })
        end
    catch
        _:_ -> ok
    end.

%% -----------------------------------------------------------------------------
%% @doc Stop the application.
%% @end
%% -----------------------------------------------------------------------------
stop(_State) ->
    ok.
