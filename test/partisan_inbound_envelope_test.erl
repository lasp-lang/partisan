%% =============================================================================
%% SPDX-FileCopyrightText: 2026 Alejandro Ramallo
%% SPDX-License-Identifier: Apache-2.0
%% =============================================================================
%%
%% @doc An inbound envelope this version does not recognise must not wedge the
%% connection that delivered it.
%%
%% `partisan_peer_service_server:216' calls `Manager:receive_message/3'
%% synchronously, in the connection process itself. In
%% `partisan_pluggable_peer_service_manager', an unrecognised envelope misses
%% every specific `receive_message/3' clause and reaches the catch-all, which
%% issues `gen_server:call(..., infinity)'. That lands in `handle_message/4's
%% own catch-all -- the one clause that logs and returns without calling
%% `maybe_reply/2'. The caller is then never answered, never re-arms its
%% `{active, once}' socket, and that peer link is dead for good.
%%
%% This matters beyond malformed frames: it is what any future protocol
%% addition does to a peer running an older release, so it constrains how the
%% wire format can evolve at all.
%%
%% Not covered: the other managers. `partisan_hyparview_peer_service_manager',
%% `partisan_client_server_peer_service_manager' and
%% `partisan_static_peer_service_manager' have no `handle_message' catch-all,
%% so an unknown envelope raises `function_clause' and crashes the manager
%% instead of hanging the caller. That is a different defect and is deliberately
%% left alone here.
%% @end
%% =============================================================================
-module(partisan_inbound_envelope_test).

-include_lib("eunit/include/eunit.hrl").

-define(MANAGER, partisan_pluggable_peer_service_manager).

inbound_envelope_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        {timeout, 30, fun unknown_envelope_answers_the_caller/0},
        {timeout, 30, fun known_envelope_still_delivers/0}
    ]}.


%% =============================================================================
%% CASES
%% =============================================================================


%% -----------------------------------------------------------------------------
%% The envelope is deliberately a shape no clause matches. The assertion is
%% only that the call returns at all: a caller left blocked here is a connection
%% process that has stopped reading its socket.
%% -----------------------------------------------------------------------------
unknown_envelope_answers_the_caller() ->
    Unknown = {forward_message, make_ref(), hello, #{some_future_option => true}},

    ?assertEqual(
        returned,
        call_in_child(fun() ->
            ?MANAGER:receive_message(partisan:node(), undefined, Unknown)
        end)
    ).


%% -----------------------------------------------------------------------------
%% Guard: the envelope every peer speaks today must keep working, and must
%% still reach the destination. Without this, replying to everything
%% unconditionally would look like a fix.
%% -----------------------------------------------------------------------------
known_envelope_still_delivers() ->
    Self = self(),
    Ref = make_ref(),
    Known = {forward_message, Self, {delivered, Ref}},

    ?assertEqual(
        returned,
        call_in_child(fun() ->
            ?MANAGER:receive_message(partisan:node(), undefined, Known)
        end)
    ),

    receive
        {delivered, Ref} -> ok
    after
        2000 -> erlang:error(known_envelope_was_not_delivered)
    end.


%% =============================================================================
%% HELPERS
%% =============================================================================


setup() ->
    stop_partisan(),
    {ok, _} = application:ensure_all_started(partisan),
    ok.


cleanup(_) ->
    stop_partisan().


stop_partisan() ->
    case lists:keymember(partisan, 1, application:which_applications()) of
        true -> application:stop(partisan);
        false -> ok
    end.


%% @private
%% @doc Runs `Fun' in a child process and reports whether it returned. The child
%% is required because the call under test blocks forever when the defect is
%% present -- it cannot be given a timeout, since the production caller uses
%% `infinity' and that is precisely the thing being tested.
call_in_child(Fun) ->
    Parent = self(),

    Pid = spawn(fun() ->
        _ = (catch Fun()),
        Parent ! {returned, self()}
    end),

    receive
        {returned, Pid} ->
            returned
    after
        2000 ->
            exit(Pid, kill),
            blocked
    end.
