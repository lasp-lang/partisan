%% @doc Encoded reply destinations: the predicate and the malformed-decode path.
%%
%% An RPC reply is addressed to a one-shot alias (`new_res/0`). The two-node
%% proof is in `partisan_rpc_two_node_test`; this guards the pieces.
-module(partisan_forward_destination_test).

-include_lib("eunit/include/eunit.hrl").

%% `from_term/1' reads the configured node name; no listener is needed.
forward_destination_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(_) ->
        [
            ?_test(a_reference_destination_is_recognised()),
            ?_test(a_reference_destination_is_accepted_by_the_dispatcher()),
            ?_test(a_name_destination_is_still_accepted()),
            ?_test(a_malformed_encoded_pid_is_rejected()),
            ?_test(a_malformed_encoded_reference_is_rejected())
        ]
    end}.

setup() ->
    %% HyParView owns the predicate and decoder under test.
    application:set_env(
        partisan, peer_service_manager, partisan_hyparview_peer_service_manager
    ),
    ok = partisan_config:init().

cleanup(_) ->
    application:unset_env(partisan, peer_service_manager),
    ok = partisan_config:init(),
    ok.

%% Wire forms.
a_reference_destination_is_recognised() ->
    Ref = partisan_remote_ref:from_term(make_ref()),
    ?assert(partisan_remote_ref:is_reference(Ref)),
    ?assertNot(partisan_remote_ref:is_pid(Ref)),
    ?assertNot(partisan_remote_ref:is_name(Ref)).

%% A valid reference may fail for connection reasons, never as invalid.
a_reference_destination_is_accepted_by_the_dispatcher() ->
    Ref = partisan_remote_ref:from_term(make_ref()),
    Result = partisan:forward_message(Ref, hello, #{timeout => 100}),
    ?assertNotEqual({error, {invalid_destination, Ref}}, Result).

a_name_destination_is_still_accepted() ->
    Ref = partisan_remote_ref:from_term(?MODULE),
    Result = partisan:forward_message(Ref, hello, #{timeout => 100}),
    ?assertNotEqual({error, {invalid_destination, Ref}}, Result).

%% These shapes pass the predicate but carry no node; `node/1' would raise.
a_malformed_encoded_pid_is_rejected() ->
    Bad = {encoded_pid, make_ref()},
    ?assertEqual(
        {error, {invalid_destination, Bad}},
        partisan:forward_message(Bad, hello, #{timeout => 100})
    ).

a_malformed_encoded_reference_is_rejected() ->
    Bad = {encoded_ref, make_ref()},
    ?assertEqual(
        {error, {invalid_destination, Bad}},
        partisan:forward_message(Bad, hello, #{timeout => 100})
    ).
